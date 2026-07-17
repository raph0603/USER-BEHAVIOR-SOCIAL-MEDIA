"""Independent, stateful YouTube transcript request worker."""

from __future__ import annotations

import json
import os
import time
from datetime import timedelta

from common.transcripts import fetch_transcript
from common.youtube_pipeline import finalize_worker_summary, parse_datetime, utc_now
from common.youtube_state import YouTubeStateStore
from youtube_pipeline_events import EventConsumer, EventProducer, pipeline_event


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(_env(name, str(default))))
    except ValueError:
        return default


def _retry_at(row: dict, attempted_at, attempt_count: int):
    offsets = [6, 24, 72, 168]
    if attempt_count > len(offsets):
        return None
    first_seen = parse_datetime(row["first_seen_at"]) or attempted_at
    scheduled = first_seen + timedelta(hours=offsets[attempt_count - 1])
    return max(scheduled, attempted_at + timedelta(minutes=5))


def _status(result: dict, attempt_count: int) -> tuple[str, bool]:
    result_status = result.get("status")
    error_code = str(result.get("error_code") or "").lower()
    if result_status == "success":
        return "available", True
    if "disabled" in error_code:
        return "disabled", True
    if "age" in error_code and "restrict" in error_code:
        return "age_restricted", True
    if error_code in {"ip_blocked", "request_blocked"}:
        return "ip_blocked", False
    if result_status == "rate_limited":
        return "rate_limited", False
    if result_status == "not_available":
        return "not_found", False
    if attempt_count >= 5:
        return "permanent_error", True
    return "retryable_error", False


def _ingest_requests(state, bootstrap, registry, topic, limit):
    with EventConsumer(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        topic=topic,
        group_id=_env("YOUTUBE_TRANSCRIPT_GROUP_ID", "youtube-transcripts-v1"),
    ) as consumer:
        events = consumer.poll_batch(limit=limit * 2, idle_seconds=2)
        for event in events:
            if event.get("event_type") != "youtube.transcript.requested":
                continue
            state.enqueue_request(
                "transcript",
                video_id=event["video_id"],
                correlation_id=event.get("correlation_id") or event["video_id"],
                first_seen_at=utc_now(),
                published_at=event.get("published_at"),
                request=event,
            )
        consumer.commit()
        return len(events)


def main() -> None:
    run_started = time.monotonic()
    bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    registry = _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    schema_path = _env("SCHEMA_PATH", "/app/schemas/playwright_event.avsc")
    request_topic = _env("YOUTUBE_TRANSCRIPT_REQUEST_TOPIC", "youtube.transcript.requests")
    result_topic = _env("YOUTUBE_TRANSCRIPT_RESULT_TOPIC", "youtube.transcript.results")
    state_path = _env("YOUTUBE_PIPELINE_STATE_DB", "/app/state/youtube-pipeline.sqlite")
    batch_size = _env_int("YOUTUBE_TRANSCRIPT_BATCH_SIZE", 10)
    cooldown = timedelta(
        seconds=_env_int("YOUTUBE_TRANSCRIPT_BLOCK_COOLDOWN_SECONDS", 21600, 60)
    )
    producer = EventProducer(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        schema_path=schema_path,
    )
    summary = {"event": "youtube_transcript_summary", "due": 0, "succeeded": 0, "failed": 0}
    with YouTubeStateStore(state_path) as state:
        _ingest_requests(state, bootstrap, registry, request_topic, batch_size)
        now = utc_now()
        if state.breaker_open("transcript", now):
            summary["circuit_open"] = True
            print(
                json.dumps(
                    finalize_worker_summary(
                        summary,
                        elapsed_seconds=time.monotonic() - run_started,
                        processed=0,
                    ),
                    sort_keys=True,
                )
            )
            return
        due = state.due_requests("transcript", now=now, limit=batch_size)
        summary["due"] = len(due)
        for row in due:
            attempted_at = utc_now()
            attempt_count = int(row.get("attempt_count") or 0) + 1
            request = json.loads(row["request_json"])
            languages = [request.get("language")] if request.get("language") else ["en"]
            result = fetch_transcript(
                row["video_id"],
                preferred_languages=languages,
                require_preferred_language=False,
                attempt_count=attempt_count,
            ).to_dict()
            status, terminal = _status(result, attempt_count)
            next_attempt = None if terminal else _retry_at(row, attempted_at, attempt_count)
            if next_attempt is None and not terminal:
                status = "permanent_error"
            state.record_request_result(
                "transcript",
                video_id=row["video_id"],
                status=status,
                attempted_at=attempted_at,
                next_attempt_at=next_attempt,
                result=result,
                error_class=result.get("error_code"),
                error_message=result.get("error_message"),
            )
            payload = result.get("payload") or {}
            producer.publish(
                result_topic,
                [
                    pipeline_event(
                        "youtube.transcript.result",
                        row["video_id"],
                        correlation_id=row["correlation_id"],
                        collected_at=attempted_at,
                        attempt_count=attempt_count,
                        transcript_status=status,
                        transcript_text=payload.get("text"),
                        transcript_segments_json=payload.get("segments_json"),
                        transcript_language=payload.get("language"),
                        transcript_language_code=payload.get("language_code"),
                        transcript_source=payload.get("source"),
                        transcript_is_generated=payload.get("is_generated"),
                        transcript_is_translated=payload.get("is_translated"),
                        transcript_segment_count=payload.get("segment_count"),
                        transcript_collected_at=attempted_at.isoformat(),
                        transcript_error_code=result.get("error_code"),
                        transcript_error_message=result.get("error_message"),
                        next_attempt_at=next_attempt.isoformat() if next_attempt else None,
                        payload_json=json.dumps(result, ensure_ascii=False, sort_keys=True),
                    )
                ],
            )
            if status == "available":
                summary["succeeded"] += 1
            else:
                summary["failed"] += 1
            if status == "ip_blocked":
                state.open_breaker(
                    "transcript",
                    now=attempted_at,
                    cooldown=cooldown,
                    reason=result.get("error_message") or status,
                )
                summary["circuit_open"] = True
                break
    processed = summary["succeeded"] + summary["failed"]
    print(
        json.dumps(
            finalize_worker_summary(
                summary,
                elapsed_seconds=time.monotonic() - run_started,
                processed=processed,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
