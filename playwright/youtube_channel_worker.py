"""Refresh channel-level YouTube statistics with a persistent channel cache."""

from __future__ import annotations

import json
import os
import time
from datetime import timedelta
from typing import Any, Iterable

from googleapiclient.discovery import build

from common.youtube_pipeline import finalize_worker_summary, retry_delay, utc_now
from common.youtube_state import YouTubeStateStore
from youtube_pipeline_events import (
    EventConsumer,
    EventProducer,
    drain_outbox,
    pipeline_event,
)


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
    run_started = time.monotonic()
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
    producer = EventProducer(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        schema_path=schema_path,
    )
    summary: dict[str, Any] = {
        "event": "youtube_channel_summary",
        "requests": 0,
        "new_channels": 0,
        "cache_hits": 0,
        "refreshed": 0,
        "missing": 0,
        "failed": 0,
        "api_calls": 0,
        "budget_exhausted": False,
        "outbox_redrained": 0,
    }

    with YouTubeStateStore(state_path) as state:
        summary["outbox_redrained"] = drain_outbox(
            state,
            producer,
            include_deferred=True,
        )
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
        legacy_remaining = max(0, daily_budget - used)
        requested_calls = (run_limit + batch_size - 1) // batch_size
        quota_decision = state.quota_decision(
            endpoint="channels.list",
            workload="channels",
            requested_calls=requested_calls,
            now=now,
        )
        remaining = min(legacy_remaining, quota_decision.allowed_calls)
        due = state.due_channels(now=now, limit=min(run_limit, remaining * batch_size))
        if not due:
            summary["budget_exhausted"] = remaining == 0
            if quota_decision.reason:
                summary["quota_reason"] = quota_decision.reason
            completed = finalize_worker_summary(
                summary,
                elapsed_seconds=time.monotonic() - run_started,
                processed=0,
            )
            state.record_worker_health(
                worker_name="youtube_channel",
                observed_at=utc_now(),
                status="throttled" if remaining == 0 else "idle",
                processed_count=0,
                success_count=0,
                error_count=0,
                cache_hit_count=summary["cache_hits"],
                cache_miss_count=summary["new_channels"],
                latency_ms=(time.monotonic() - run_started) * 1000,
                details=completed,
            )
            print(
                json.dumps(
                    completed,
                    sort_keys=True,
                )
            )
            return

        client = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
        for rows in batched((row["channel_id"] for row in due), batch_size):
            if summary["api_calls"] >= remaining:
                summary["budget_exhausted"] = True
                break
            observed_at = utc_now()
            request_started = time.monotonic()
            try:
                statistics_by_id = fetch_channel_statistics(client, rows)
            except BaseException as exc:
                latency_ms = (time.monotonic() - request_started) * 1000
                summary["api_calls"] += 1
                with state.transaction():
                    state.record_api_usage(
                        endpoint="channels.list",
                        request_count=1,
                        resource_count=len(rows),
                        success_count=0,
                        error_count=len(rows),
                        quota_bucket="channels",
                        observed_at=observed_at,
                        priority=quota_decision.priority,
                        retry_count=sum(
                            int((state.channel_state(channel_id) or {}).get("attempt_count") or 0)
                            for channel_id in rows
                        ),
                        latency_ms=latency_ms,
                        queue_depth=max(0, len(due) - len(rows) - summary["refreshed"]),
                        status="error",
                        error_code=type(exc).__name__,
                    )
                    for channel_id in rows:
                        row = state.channel_state(channel_id) or {}
                        attempt = int(row.get("attempt_count") or 0) + 1
                        permanent = attempt >= max_attempts
                        next_attempt = (
                            None
                            if permanent
                            else observed_at
                            + retry_delay(
                                attempt,
                                base=timedelta(hours=1),
                                maximum=timedelta(days=1),
                            )
                        )
                        state.record_channel_failure(
                            channel_id=channel_id,
                            attempted_at=observed_at,
                            next_attempt_at=next_attempt,
                            error_class=type(exc).__name__,
                            error_message=str(exc),
                            permanent=permanent,
                        )
                        state.enqueue_outbox(
                            worker_name="youtube_channel",
                            aggregate_id=channel_id,
                            topic=result_topic,
                            event=pipeline_event(
                                "youtube.channel.failed",
                                channel_id,
                                channel_id=channel_id,
                                collected_at=observed_at,
                                attempt_count=attempt,
                                collection_status=(
                                    "permanent_error" if permanent else "retryable_error"
                                ),
                                next_attempt_at=(
                                    next_attempt.isoformat() if next_attempt else None
                                ),
                                error_class=type(exc).__name__,
                                error_message=str(exc)[:1000],
                                content_type="channel",
                                platform_event_id=channel_id,
                                url=f"https://www.youtube.com/channel/{channel_id}",
                            ),
                            created_at=observed_at,
                        )
                        summary["failed"] += 1
            else:
                latency_ms = (time.monotonic() - request_started) * 1000
                summary["api_calls"] += 1
                with state.transaction():
                    state.record_api_usage(
                        endpoint="channels.list",
                        request_count=1,
                        resource_count=len(rows),
                        success_count=len(statistics_by_id),
                        error_count=len(rows) - len(statistics_by_id),
                        quota_bucket="channels",
                        observed_at=observed_at,
                        priority=quota_decision.priority,
                        latency_ms=latency_ms,
                        queue_depth=max(0, len(due) - len(rows) - summary["refreshed"]),
                        status=("success" if len(statistics_by_id) == len(rows) else "partial"),
                    )
                    for channel_id in rows:
                        statistics = statistics_by_id.get(channel_id)
                        if statistics is None:
                            state.record_channel_failure(
                                channel_id=channel_id,
                                attempted_at=observed_at,
                                next_attempt_at=None,
                                error_class="ChannelNotFound",
                                error_message=("Channel was not returned by channels.list"),
                                permanent=True,
                            )
                            event = pipeline_event(
                                "youtube.channel.failed",
                                channel_id,
                                channel_id=channel_id,
                                collected_at=observed_at,
                                collection_status="permanent_error",
                                error_class="ChannelNotFound",
                                error_message=("Channel was not returned by channels.list"),
                                content_type="channel",
                                platform_event_id=channel_id,
                                url=f"https://www.youtube.com/channel/{channel_id}",
                            )
                            summary["missing"] += 1
                        else:
                            subscriber_count = _subscriber_count(statistics)
                            hidden = bool(statistics.get("hiddenSubscriberCount"))
                            state.record_channel_success(
                                channel_id=channel_id,
                                observed_at=observed_at,
                                subscriber_count=subscriber_count,
                                hidden_subscriber_count=hidden,
                                active_after=(observed_at - timedelta(days=active_days)),
                                active_interval=timedelta(hours=active_hours),
                                inactive_interval=timedelta(hours=inactive_hours),
                            )
                            event = pipeline_event(
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
                                    statistics,
                                    ensure_ascii=False,
                                    sort_keys=True,
                                ),
                            )
                            summary["refreshed"] += 1
                        state.enqueue_outbox(
                            worker_name="youtube_channel",
                            aggregate_id=channel_id,
                            topic=result_topic,
                            event=event,
                            created_at=observed_at,
                        )
            drain_outbox(state, producer)

        processed = summary["refreshed"] + summary["missing"] + summary["failed"]
        completed = finalize_worker_summary(
            summary,
            elapsed_seconds=time.monotonic() - run_started,
            processed=processed,
        )
        state.record_worker_health(
            worker_name="youtube_channel",
            observed_at=utc_now(),
            status="partial" if summary["failed"] or summary["missing"] else "success",
            processed_count=processed,
            success_count=summary["refreshed"],
            error_count=summary["failed"] + summary["missing"],
            retry_count=summary["failed"],
            cache_hit_count=summary["cache_hits"],
            cache_miss_count=summary["new_channels"],
            latency_ms=(time.monotonic() - run_started) * 1000,
            details=completed,
        )
    print(
        json.dumps(
            completed,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
