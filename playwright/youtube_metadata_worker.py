"""Bounded yt-dlp enrichment and descriptive metadata evolution worker."""

from __future__ import annotations

import json
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any

import yt_dlp

from common.youtube_pipeline import (
    YouTubeStateStore,
    isoformat,
    parse_hour_offsets,
    retry_delay,
    utc_now,
)
from youtube_pipeline_events import EventConsumer, EventProducer, pipeline_event


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(_env(name, str(default))))
    except ValueError:
        return default


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(_env(name, str(default))))
    except ValueError:
        return default


def normalize_yt_dlp_metadata(info: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "channel_id",
        "channel",
        "uploader_id",
        "upload_date",
        "timestamp",
        "duration",
        "language",
        "tags",
        "categories",
        "chapters",
        "thumbnails",
        "subtitles",
        "automatic_captions",
        "view_count",
        "like_count",
        "comment_count",
        "availability",
        "age_limit",
        "live_status",
        "was_live",
        "webpage_url",
    )
    normalized = {field: info.get(field) for field in fields}
    normalized["video_id"] = normalized.pop("id", None)
    return normalized


def extract_metadata(video_id: str, *, jitter_seconds: float = 0.0) -> tuple[dict, dict]:
    options = {
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "extract_flat": False,
        "cachedir": True,
        "retries": 0,
        "fragment_retries": 0,
    }
    if jitter_seconds > 0:
        options["sleep_interval_requests"] = random.uniform(0, jitter_seconds)
    with yt_dlp.YoutubeDL(options) as downloader:
        info = downloader.extract_info(
            f"https://www.youtube.com/watch?v={video_id}",
            download=False,
        )
    if not isinstance(info, dict):
        raise RuntimeError(f"yt-dlp returned no metadata for {video_id}")
    return info, normalize_yt_dlp_metadata(info)


def _blocked(error: BaseException) -> bool:
    message = str(error).lower()
    return any(
        marker in message
        for marker in (
            "http error 429",
            "too many requests",
            "ip blocked",
            "request blocked",
            "sign in to confirm",
        )
    )


def _ingest_discoveries(
    state: YouTubeStateStore,
    *,
    bootstrap: str,
    registry: str,
    topic: str,
    limit: int,
) -> int:
    with EventConsumer(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        topic=topic,
        group_id=_env("YOUTUBE_METADATA_GROUP_ID", "youtube-metadata-enrichment-v1"),
    ) as consumer:
        events = consumer.poll_batch(limit=limit, idle_seconds=2)
        for event in events:
            if event.get("event_type") != "youtube.discovery.discovered":
                continue
            observed_at = utc_now()
            state.record_discovery(
                video_id=event["video_id"],
                query_id=event.get("query_id") or "unknown",
                first_seen_at=observed_at,
                published_at=event.get("published_at"),
                correlation_id=event.get("correlation_id") or event["video_id"],
            )
        consumer.commit()
        return len(events)


def main() -> None:
    bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    registry = _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    schema_path = _env("SCHEMA_PATH", "/app/schemas/playwright_event.avsc")
    discovery_topic = _env("YOUTUBE_DISCOVERY_TOPIC", "youtube.discovery.events")
    metadata_topic = _env("YOUTUBE_METADATA_TOPIC", "youtube.metadata.events")
    changes_topic = _env("YOUTUBE_METADATA_CHANGES_TOPIC", "youtube.metadata.changes")
    transcript_topic = _env("YOUTUBE_TRANSCRIPT_REQUEST_TOPIC", "youtube.transcript.requests")
    comment_topic = _env("YOUTUBE_COMMENT_REQUEST_TOPIC", "youtube.comment.requests")
    state_path = _env("YOUTUBE_PIPELINE_STATE_DB", "/app/state/youtube-pipeline.sqlite")
    output_dir = Path(_env("YOUTUBE_OUTPUT_DIR", "/app/api/yt_raw_json"))
    batch_size = _env_int("YOUTUBE_METADATA_BATCH_SIZE", 20, 1)
    concurrency = min(2, _env_int("YOUTUBE_METADATA_CONCURRENCY", 1, 1))
    max_attempts = _env_int("YOUTUBE_METADATA_MAX_ATTEMPTS", 3, 1)
    jitter = _env_float("YOUTUBE_METADATA_JITTER_SECONDS", 3.0)
    cooldown = timedelta(
        seconds=_env_int("YOUTUBE_METADATA_BLOCK_COOLDOWN_SECONDS", 21600, 60)
    )
    offsets = parse_hour_offsets(_env("YOUTUBE_METADATA_REFRESH_HOURS"))
    producer = EventProducer(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        schema_path=schema_path,
    )
    summary = {
        "event": "youtube_metadata_summary",
        "discovery_events": 0,
        "due": 0,
        "enriched": 0,
        "unchanged": 0,
        "failed": 0,
        "circuit_open": False,
    }

    with YouTubeStateStore(state_path) as state:
        summary["discovery_events"] = _ingest_discoveries(
            state,
            bootstrap=bootstrap,
            registry=registry,
            topic=discovery_topic,
            limit=max(batch_size * 4, batch_size),
        )
        now = utc_now()
        if state.breaker_open("yt_dlp", now):
            summary["circuit_open"] = True
            print(json.dumps(summary, sort_keys=True))
            return

        due = state.due_metadata(now, batch_size)
        summary["due"] = len(due)
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                row["video_id"]: executor.submit(
                    extract_metadata,
                    row["video_id"],
                    jitter_seconds=jitter,
                )
                for row in due
            }
            for row in due:
                video_id = row["video_id"]
                attempt_count = int(row.get("attempt_count") or 0) + 1
                observed_at = utc_now()
                try:
                    raw, metadata = futures[video_id].result()
                    output_dir.mkdir(parents=True, exist_ok=True)
                    (output_dir / f"{video_id}-{int(observed_at.timestamp())}.json").write_text(
                        json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str),
                        encoding="utf-8",
                    )
                    current_hash, previous_hash, changed_fields = state.record_metadata_success(
                        video_id=video_id,
                        observed_at=observed_at,
                        metadata=metadata,
                        offsets=offsets,
                    )
                    event = pipeline_event(
                        "youtube.metadata.observed",
                        video_id,
                        correlation_id=row["correlation_id"],
                        channel_id=metadata.get("channel_id"),
                        collected_at=observed_at,
                        attempt_count=attempt_count,
                        title=metadata.get("title"),
                        description=metadata.get("description"),
                        published_at=row.get("published_at"),
                        language=metadata.get("language"),
                        duration_seconds=metadata.get("duration"),
                        thumbnail_url=(metadata.get("thumbnails") or [{}])[-1].get("url")
                        if metadata.get("thumbnails")
                        else None,
                        view_count=metadata.get("view_count"),
                        like_count=metadata.get("like_count"),
                        comment_count=metadata.get("comment_count"),
                        metadata_source="yt-dlp",
                        metadata_schema_version="1.0",
                        yt_dlp_version=yt_dlp.version.__version__,
                        enrichment_status="success",
                        metadata_status="success",
                        metadata_collected_at=isoformat(observed_at),
                        last_metadata_refresh_at=isoformat(observed_at),
                        metadata_hash=current_hash,
                        previous_metadata_hash=previous_hash,
                        changed_fields=changed_fields,
                        canonical_metadata=json.dumps(
                            metadata, ensure_ascii=False, sort_keys=True, default=str
                        ),
                        raw_source_payload=json.dumps(
                            raw, ensure_ascii=False, sort_keys=True, default=str
                        ),
                    )
                    producer.publish(metadata_topic, [event])
                    if previous_hash != current_hash:
                        producer.publish(
                            changes_topic,
                            [
                                {
                                    **event,
                                    "event_type": "youtube.metadata.changed",
                                }
                            ],
                        )
                        summary["enriched"] += 1
                    else:
                        summary["unchanged"] += 1
                    if previous_hash is None:
                        request_fields = {
                            "correlation_id": row["correlation_id"],
                            "collected_at": observed_at,
                            "attempt_count": 1,
                            "channel_id": metadata.get("channel_id"),
                            "published_at": row.get("published_at"),
                            "collection_status": "pending",
                        }
                        producer.publish(
                            transcript_topic,
                            [
                                pipeline_event(
                                    "youtube.transcript.requested",
                                    video_id,
                                    **request_fields,
                                )
                            ],
                        )
                        producer.publish(
                            comment_topic,
                            [
                                pipeline_event(
                                    "youtube.comment.requested",
                                    video_id,
                                    **request_fields,
                                )
                            ],
                        )
                except BaseException as exc:
                    blocked = _blocked(exc)
                    permanent = attempt_count >= max_attempts and not blocked
                    delay = retry_delay(
                        attempt_count,
                        base=timedelta(minutes=5),
                        maximum=timedelta(hours=6),
                        jitter_seconds=jitter,
                    )
                    state.record_metadata_failure(
                        video_id=video_id,
                        attempted_at=observed_at,
                        next_attempt_at=None if permanent else observed_at + delay,
                        error=exc,
                        permanent=permanent,
                    )
                    summary["failed"] += 1
                    if blocked:
                        state.open_breaker(
                            "yt_dlp", now=observed_at, cooldown=cooldown, reason=str(exc)
                        )
                        summary["circuit_open"] = True
                        break
                finally:
                    if jitter > 0:
                        time.sleep(random.uniform(0, jitter))

    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
