"""Dedicated due-target YouTube metrics worker using only ``videos.list``."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from common.youtube_pipeline import next_metrics_refresh_at, parse_datetime
from common.youtube_state import YouTubeStateStore
from engagement import parse_count
from youtube_pipeline_events import EventProducer, pipeline_event


UTC = timezone.utc
MAX_VIDEOS_PER_REQUEST = 50


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def _video_id(target: dict) -> str | None:
    supplied = str(target.get("platform_event_id") or "").strip()
    if supplied:
        return supplied
    parsed = urlparse(str(target.get("url") or ""))
    if parsed.hostname in {"youtu.be", "www.youtu.be"}:
        return parsed.path.strip("/").split("/", 1)[0] or None
    return parse_qs(parsed.query).get("v", [None])[0]


def _load_due_targets(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    targets: list[dict] = []
    with path.open(encoding="utf-8-sig") as stream:
        for line in stream:
            if not line.strip():
                continue
            target = json.loads(line)
            if target.get("source") == "youtube" and _video_id(target):
                targets.append(target)
    return targets


def _canonical_json(value: dict) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _observation_id(video_id: str, observed_at: datetime) -> str:
    identity = f"youtube\x1f{video_id}\x1f{observed_at.astimezone(UTC).isoformat()}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _growth_multiplier(
    target: dict,
    current_view_count: int | None,
    observed_at: datetime,
) -> float:
    previous_view_count = parse_count(target.get("view_count"))
    previous_observed_at = parse_datetime(
        target.get("last_metrics_refresh_at") or target.get("metadata_refreshed_at")
    )
    if (
        current_view_count is None
        or previous_view_count is None
        or previous_observed_at is None
        or observed_at <= previous_observed_at
    ):
        return 1.0
    elapsed_hours = (observed_at - previous_observed_at).total_seconds() / 3600
    views_per_hour = max(0, current_view_count - previous_view_count) / elapsed_hours
    if views_per_hour < float(_env("YOUTUBE_HIGH_GROWTH_VIEWS_PER_HOUR", "1000")):
        return 1.0
    return max(
        0.25,
        min(1.0, float(_env("YOUTUBE_HIGH_GROWTH_INTERVAL_MULTIPLIER", "0.5"))),
    )


def _build_update(
    target: dict,
    *,
    video_id: str,
    observed_at: datetime,
    statistics: dict | None,
) -> dict:
    observed_value = observed_at.astimezone(UTC).isoformat()
    counts = {
        "view_count": parse_count((statistics or {}).get("viewCount")),
        "like_count": parse_count((statistics or {}).get("likeCount")),
        "comment_count": parse_count((statistics or {}).get("commentCount")),
    }
    growth_multiplier = _growth_multiplier(target, counts["view_count"], observed_at)
    try:
        next_refresh = next_metrics_refresh_at(
            target.get("event_ts") or observed_at,
            observed_at,
            growth_multiplier=growth_multiplier,
        ).isoformat()
    except ValueError:
        next_refresh = None
    available = {f"{name}_available": value is not None for name, value in counts.items()}
    update = {
        "user_id": target.get("user_id"),
        "url": target.get("url"),
        "event_ts": target.get("event_ts"),
        "source": "youtube",
        "platform_event_id": video_id,
        "metadata_refreshed_at": observed_value,
        "last_metrics_refresh_at": observed_value,
        "next_metrics_refresh_at": next_refresh,
        "metrics_refresh_count": int(target.get("metrics_refresh_count") or 0) + 1,
        "metrics_refresh_status": "available" if statistics is not None else "unavailable",
        "metrics_error_code": None if statistics is not None else "videos_list_not_returned",
        "producer_name": "youtube_metrics_worker",
        "producer_run_id": _env("PIPELINE_RUN_ID", "standalone"),
        "collection_method": "youtube_data_api",
        "api_endpoint": "videos.list",
        "owner_channel_id": None,
        "collaborator_channel_ids": None,
        **counts,
        **available,
        "reply_count": None,
        "retweet_count": None,
        "bookmark_count": None,
        "score": None,
        "follower_count": None,
        "subscriber_count": None,
        "subreddit_member_count": None,
        "reply_count_available": False,
        "retweet_count_available": False,
        "bookmark_count_available": False,
        "score_available": False,
    }
    update["observation_id"] = _observation_id(video_id, observed_at)
    update["coverage_json"] = _canonical_json(
        {
            metric: update[f"{metric}_available"]
            for metric in (
                "bookmark_count",
                "comment_count",
                "like_count",
                "reply_count",
                "retweet_count",
                "score",
                "view_count",
            )
        }
    )
    update["payload_fingerprint"] = hashlib.sha256(
        _canonical_json(update).encode("utf-8")
    ).hexdigest()
    return update


def collect_metrics(
    targets: list[dict],
    *,
    youtube,
    usage_store: YouTubeStateStore | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[dict]:
    """Collect due target metrics in API batches of at most 50 IDs."""

    targets_by_id = {video_id: target for target in targets if (video_id := _video_id(target))}
    video_ids = list(targets_by_id)
    if usage_store:
        budget_units = int(
            _env(
                "YOUTUBE_VIDEOS_DAILY_QUOTA_UNITS",
                _env("YOUTUBE_VIDEOS_DAILY_REQUEST_BUDGET", "500"),
            )
        )
        used_units = usage_store.api_requests_today("videos.list", now())
        video_ids = video_ids[: max(0, budget_units - used_units) * MAX_VIDEOS_PER_REQUEST]

    updates: list[dict] = []
    for start in range(0, len(video_ids), MAX_VIDEOS_PER_REQUEST):
        batch_ids = video_ids[start : start + MAX_VIDEOS_PER_REQUEST]
        observed_at = now()
        response = youtube.videos().list(part="statistics", id=",".join(batch_ids)).execute()
        items = {
            item["id"]: item.get("statistics") or {}
            for item in response.get("items", [])
            if item.get("id")
        }
        if usage_store:
            usage_store.record_api_usage(
                endpoint="videos.list",
                request_count=1,
                resource_count=len(batch_ids),
                success_count=1,
                error_count=0,
                quota_bucket="recent_metrics",
                observed_at=observed_at,
            )
        updates.extend(
            _build_update(
                targets_by_id[video_id],
                video_id=video_id,
                observed_at=observed_at,
                statistics=items.get(video_id),
            )
            for video_id in batch_ids
        )
    return updates


def _publish(updates: list[dict]) -> int:
    if not updates or not _env("KAFKA_BOOTSTRAP"):
        return 0
    producer = EventProducer(
        bootstrap_servers=_env("KAFKA_BOOTSTRAP", "kafka:9092"),
        schema_registry_url=_env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
        schema_path=_env("SCHEMA_PATH", "/app/schemas/playwright_event.avsc"),
    )
    events = [
        pipeline_event(
            "youtube.engagement.snapshot",
            update["platform_event_id"],
            collected_at=parse_datetime(update["metadata_refreshed_at"]),
            attempt_count=update.get("metrics_refresh_count") or 1,
            published_at=update.get("event_ts"),
            event_id=update["observation_id"],
            payload_fingerprint=update["payload_fingerprint"],
            view_count=update.get("view_count"),
            like_count=update.get("like_count"),
            comment_count=update.get("comment_count"),
            last_metrics_refresh_at=update.get("last_metrics_refresh_at"),
            next_metrics_refresh_at=update.get("next_metrics_refresh_at"),
            metrics_refresh_count=update.get("metrics_refresh_count"),
            metrics_refresh_status=update.get("metrics_refresh_status"),
            collection_status=update.get("metrics_refresh_status"),
            payload_json=_canonical_json(update),
        )
        for update in updates
    ]
    return producer.publish(
        _env("YOUTUBE_ENGAGEMENT_TOPIC", "youtube.engagement.snapshots"), events
    )


def _write_updates(path: Path, updates: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        for update in updates:
            stream.write(json.dumps(update, ensure_ascii=True, sort_keys=True) + "\n")
    temporary.replace(path)


def main() -> None:
    from googleapiclient.discovery import build

    targets_path = Path(_env("INSIGHT_REFRESH_TARGETS_PATH", "/app/insight-refresh/targets.jsonl"))
    output_path = Path(_env("INSIGHT_REFRESH_OUTPUT_PATH", "/app/insight-refresh/youtube.jsonl"))
    targets = _load_due_targets(targets_path)
    if not targets:
        _write_updates(output_path, [])
        print(
            json.dumps(
                {
                    "event": "youtube_metrics_complete",
                    "targets": 0,
                    "observations": 0,
                    "max_batch_size": MAX_VIDEOS_PER_REQUEST,
                },
                sort_keys=True,
            )
        )
        return
    api_key = _env("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is required for YouTube metrics")

    state_path = _env("YOUTUBE_PIPELINE_STATE_DB")
    usage_store = YouTubeStateStore(state_path) if state_path else None
    try:
        youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
        updates = collect_metrics(targets, youtube=youtube, usage_store=usage_store)
        _publish(updates)
        _write_updates(output_path, updates)
    finally:
        if usage_store:
            usage_store.close()
    print(
        json.dumps(
            {
                "event": "youtube_metrics_complete",
                "targets": len(targets),
                "observations": len(updates),
                "max_batch_size": MAX_VIDEOS_PER_REQUEST,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
