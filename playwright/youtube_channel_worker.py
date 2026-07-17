"""Refresh channel-level YouTube statistics with a persistent channel cache."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any, Iterable

from googleapiclient.discovery import build

from common.youtube_pipeline import retry_delay, utc_now
from common.youtube_state import YouTubeStateStore
from youtube_pipeline_events import EventConsumer, EventProducer, pipeline_event


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(_env(name, str(default))))
    except ValueError:
        return default


def batched(values: Iterable[str], size: int = 50) -> Iterable[list[str]]:
    batch: list[str] = []
    for value in values:
        if not value or value in batch:
            continue
        batch.append(value)
        if len(batch) == min(50, max(1, size)):
            yield batch
            batch = []
    if batch:
        yield batch


def fetch_channel_statistics(client: Any, channel_ids: list[str]) -> dict[str, dict]:
    """Fetch at most 50 channel statistics in one quota-efficient request."""
    if not channel_ids or len(channel_ids) > 50:
        raise ValueError("channels.list accepts between 1 and 50 channel IDs")
    response = (
        client.channels()
        .list(part="statistics", id=",".join(channel_ids), maxResults=len(channel_ids))
        .execute()
    )
    return {
        item["id"]: item.get("statistics") or {}
        for item in response.get("items", [])
        if item.get("id")
    }


def _subscriber_count(statistics: dict[str, Any]) -> int | None:
    value = statistics.get("subscriberCount")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def main() -> None:
    api_key = _env("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is required for channel refresh")

    bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    registry = _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    schema_path = _env("SCHEMA_PATH", "/app/schemas/playwright_event.avsc")
    request_topic = _env("YOUTUBE_CHANNEL_REQUEST_TOPIC", "youtube.channel.requests")
    result_topic = _env("YOUTUBE_CHANNEL_RESULT_TOPIC", "youtube.channel.results")
    state_path = _env("YOUTUBE_PIPELINE_STATE_DB", "/app/state/youtube-pipeline.sqlite")
    batch_size = min(50, _env_int("YOUTUBE_CHANNEL_BATCH_SIZE", 50, 1))
    run_limit = _env_int("YOUTUBE_CHANNEL_RUN_LIMIT", 200, 1)
    daily_budget = _env_int("YOUTUBE_CHANNEL_DAILY_REQUEST_BUDGET", 100, 1)
    active_days = _env_int("YOUTUBE_CHANNEL_ACTIVE_DAYS", 30, 1)
    active_hours = _env_int("YOUTUBE_CHANNEL_ACTIVE_REFRESH_HOURS", 24, 1)
    inactive_hours = _env_int("YOUTUBE_CHANNEL_INACTIVE_REFRESH_HOURS", 168, 1)
    max_attempts = _env_int("YOUTUBE_CHANNEL_MAX_ATTEMPTS", 3, 1)
    now = utc_now()
    summary = {
        "event": "youtube_channel_summary",
        "requests": 0,
        "new_channels": 0,
        "cache_hits": 0,
        "refreshed": 0,
        "missing": 0,
        "failed": 0,
        "api_calls": 0,
        "budget_exhausted": False,
    }

    with YouTubeStateStore(state_path) as state:
        with EventConsumer(
            bootstrap_servers=bootstrap,
            schema_registry_url=registry,
            topic=request_topic,
            group_id=_env("YOUTUBE_CHANNEL_GROUP_ID", "youtube-channel-refresh-v1"),
        ) as consumer:
            events = consumer.poll_batch(limit=max(run_limit * 2, run_limit), idle_seconds=2)
            for event in events:
                if event.get("event_type") != "youtube.channel.requested":
                    continue
                channel_id = event.get("channel_id")
                if not channel_id:
                    continue
                inserted = state.enqueue_channel(
                    channel_id=channel_id,
                    first_seen_at=now,
                    last_video_published_at=event.get("published_at"),
                )
                summary["new_channels" if inserted else "cache_hits"] += 1
                summary["requests"] += 1
            consumer.commit()

        used = state.api_requests_today("channels.list", now)
        remaining = max(0, daily_budget - used)
        due = state.due_channels(now=now, limit=min(run_limit, remaining * 50))
        if not due:
            summary["budget_exhausted"] = remaining == 0
            print(json.dumps(summary, sort_keys=True))
            return

        client = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
        producer = EventProducer(
            bootstrap_servers=bootstrap,
            schema_registry_url=registry,
            schema_path=schema_path,
        )
        for rows in batched((row["channel_id"] for row in due), batch_size):
            if summary["api_calls"] >= remaining:
                summary["budget_exhausted"] = True
                break
            observed_at = utc_now()
            try:
                statistics_by_id = fetch_channel_statistics(client, rows)
                state.record_api_usage(
                    endpoint="channels.list",
                    request_count=1,
                    resource_count=len(rows),
                    success_count=len(statistics_by_id),
                    error_count=len(rows) - len(statistics_by_id),
                    quota_bucket="channel_statistics",
                    observed_at=observed_at,
                )
                summary["api_calls"] += 1
                result_events = []
                for channel_id in rows:
                    statistics = statistics_by_id.get(channel_id)
                    if statistics is None:
                        state.record_channel_failure(
                            channel_id=channel_id,
                            attempted_at=observed_at,
                            next_attempt_at=None,
                            error_class="ChannelNotFound",
                            error_message="Channel was not returned by channels.list",
                            permanent=True,
                        )
                        summary["missing"] += 1
                        continue
                    subscriber_count = _subscriber_count(statistics)
                    hidden = bool(statistics.get("hiddenSubscriberCount"))
                    state.record_channel_success(
                        channel_id=channel_id,
                        observed_at=observed_at,
                        subscriber_count=subscriber_count,
                        hidden_subscriber_count=hidden,
                        active_after=observed_at - timedelta(days=active_days),
                        active_interval=timedelta(hours=active_hours),
                        inactive_interval=timedelta(hours=inactive_hours),
                    )
                    result_events.append(
                        pipeline_event(
                            "youtube.channel.observed",
                            channel_id,
                            channel_id=channel_id,
                            collected_at=observed_at,
                            subscriber_count=subscriber_count,
                            collection_status="success",
                            content_type="channel",
                            platform_event_id=channel_id,
                            url=f"https://www.youtube.com/channel/{channel_id}",
                            raw_source_payload=json.dumps(
                                statistics, ensure_ascii=False, sort_keys=True
                            ),
                        )
                    )
                    summary["refreshed"] += 1
                if result_events:
                    producer.publish(result_topic, result_events)
            except BaseException as exc:
                state.record_api_usage(
                    endpoint="channels.list",
                    request_count=1,
                    resource_count=len(rows),
                    success_count=0,
                    error_count=len(rows),
                    quota_bucket="channel_statistics",
                    observed_at=observed_at,
                )
                summary["api_calls"] += 1
                for channel_id in rows:
                    row = state.channel_state(channel_id) or {}
                    attempt = int(row.get("attempt_count") or 0) + 1
                    permanent = attempt >= max_attempts
                    state.record_channel_failure(
                        channel_id=channel_id,
                        attempted_at=observed_at,
                        next_attempt_at=(
                            None
                            if permanent
                            else observed_at
                            + retry_delay(
                                attempt,
                                base=timedelta(hours=1),
                                maximum=timedelta(days=1),
                            )
                        ),
                        error_class=type(exc).__name__,
                        error_message=str(exc),
                        permanent=permanent,
                    )
                    summary["failed"] += 1

    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
