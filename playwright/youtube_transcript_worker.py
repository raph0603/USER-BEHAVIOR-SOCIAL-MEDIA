"""Independent, stateful YouTube transcript request worker."""

from __future__ import annotations

import json
import os
import time
from datetime import timedelta
from typing import Any

from common.transcripts import (
    TRANSCRIPT_AVAILABLE,
    TRANSCRIPT_BLOCKED,
    TRANSCRIPT_PERMANENT_ERROR,
    fetch_transcript,
    is_terminal_transcript_lifecycle,
    legacy_transcript_status,
    normalize_transcript_language_code,
    preferred_transcript_language_code,
    transcript_lifecycle_status,
)
from common.youtube_pipeline import finalize_worker_summary, parse_datetime, utc_now
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


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(_env(name, str(default))))
    except ValueError:
        return default


def _retry_at(row: dict, attempted_at, attempt_count: int, max_attempts: int):
    offsets = [6, 24, 72, 168]
    if attempt_count >= max_attempts:
        return None
    offset_index = min(attempt_count - 1, len(offsets) - 1)
    scheduled = attempted_at + timedelta(hours=offsets[offset_index])
    return max(scheduled, attempted_at + timedelta(minutes=5))


def _requested_language_code(request: dict) -> str:
    explicit = request.get("transcript_requested_language_code") or request.get(
        "requested_language_code"
    )
    if explicit:
        return normalize_transcript_language_code(explicit) or "en"
    return preferred_transcript_language_code(request.get("language"))


def _available_language_codes(payload: dict) -> list[str] | None:
    codes = []
    for item in payload.get("available_languages") or []:
        if not isinstance(item, dict):
            continue
        value = item.get("language_code") or item.get("language")
        if value:
            codes.append(str(value))
    return sorted(set(codes)) or None


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
            state.enqueue_transcript_request(
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
    max_attempts = _env_int("YOUTUBE_TRANSCRIPT_MAX_ATTEMPTS", 5)
    cooldown = timedelta(seconds=_env_int("YOUTUBE_TRANSCRIPT_BLOCK_COOLDOWN_SECONDS", 21600, 60))
    producer = EventProducer(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        schema_path=schema_path,
    )
    summary: dict[str, Any] = {
        "event": "youtube_transcript_summary",
        "due": 0,
        "succeeded": 0,
        "failed": 0,
        "outbox_redrained": 0,
    }
    with YouTubeStateStore(state_path) as state:
        summary["outbox_redrained"] = drain_outbox(
            state,
            producer,
            include_deferred=True,
        )
        _ingest_requests(state, bootstrap, registry, request_topic, batch_size)
        now = utc_now()
        if state.breaker_open("transcript", now):
            summary["circuit_open"] = True
            completed = finalize_worker_summary(
                summary,
                elapsed_seconds=time.monotonic() - run_started,
                processed=0,
            )
            state.record_worker_health(
                worker_name="youtube_transcript",
                observed_at=now,
                status="circuit_open",
                processed_count=0,
                success_count=0,
                error_count=0,
                latency_ms=(time.monotonic() - run_started) * 1000,
                circuit_open=True,
                details=completed,
            )
            print(
                json.dumps(
                    completed,
                    sort_keys=True,
                )
            )
            return
        due = state.due_transcript_requests(now=now, limit=batch_size)
        summary["due"] = len(due)
        for row in due:
            attempted_at = utc_now()
            attempt_count = int(row.get("attempt_count") or 0) + 1
            request = json.loads(row["request_json"])
            requested_language_code = row.get("requested_language_code") or (
                _requested_language_code(request)
            )
            requested_language = row.get("requested_language") or requested_language_code
            request_started = time.monotonic()
            result = fetch_transcript(
                row["video_id"],
                preferred_languages=[requested_language_code],
                require_preferred_language=True,
                attempt_count=attempt_count,
            ).to_dict()
            request_latency_ms = (time.monotonic() - request_started) * 1000
            payload = result.get("payload") or {}
            lifecycle_status = transcript_lifecycle_status(
                result.get("status"),
                error_code=result.get("error_code"),
                has_text=bool(str(payload.get("text") or "").strip()),
                attempt_count=attempt_count,
                max_attempts=max_attempts,
            )
            terminal = is_terminal_transcript_lifecycle(lifecycle_status)
            next_attempt = (
                None if terminal else _retry_at(row, attempted_at, attempt_count, max_attempts)
            )
            if next_attempt is None and not terminal:
                lifecycle_status = TRANSCRIPT_PERMANENT_ERROR
            compatibility_status = legacy_transcript_status(lifecycle_status)
            blocked_response = str(result.get("error_code") or "").lower() in {
                "ip_blocked",
                "request_blocked",
            }
            recovered_at = (
                attempted_at
                if lifecycle_status == TRANSCRIPT_AVAILABLE
                and row.get("transcript_lifecycle_status") != TRANSCRIPT_AVAILABLE
                and int(row.get("attempt_count") or 0) > 0
                else None
            )
            collected_at = parse_datetime(payload.get("collected_at"))
            available_languages = payload.get("available_languages")
            generation_type = (
                None
                if payload.get("is_generated") is None
                else ("automatic" if payload.get("is_generated") else "manual")
            )
            event = pipeline_event(
                "youtube.transcript.result",
                row["video_id"],
                correlation_id=row["correlation_id"],
                collected_at=attempted_at,
                attempt_count=attempt_count,
                transcript_lifecycle_status=lifecycle_status,
                transcript_status=compatibility_status,
                transcript_text=payload.get("text"),
                transcript_segments_json=(
                    json.dumps(
                        payload.get("segments"),
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    if payload.get("segments") is not None
                    else None
                ),
                transcript_requested_language=requested_language,
                transcript_requested_language_code=requested_language_code,
                transcript_obtained_language=payload.get("language"),
                transcript_obtained_language_code=payload.get("language_code"),
                transcript_language=payload.get("language"),
                transcript_language_code=payload.get("language_code"),
                transcript_source_language=payload.get("source_language"),
                transcript_source_language_code=payload.get("source_language_code"),
                transcript_available_languages=_available_language_codes(payload),
                transcript_available_languages_json=(
                    json.dumps(
                        available_languages,
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    if available_languages is not None
                    else None
                ),
                transcript_generation_type=generation_type,
                transcript_provider=payload.get("source") or "youtube_transcript_api",
                transcript_source=payload.get("source") or "youtube_transcript_api",
                transcript_is_generated=payload.get("is_generated"),
                transcript_is_translated=payload.get("is_translated"),
                transcript_selection_strategy=payload.get("selection_strategy"),
                transcript_segment_count=payload.get("segment_count"),
                transcript_covered_duration_seconds=payload.get("covered_duration_seconds"),
                transcript_attempt_count=attempt_count,
                transcript_last_attempt_at=attempted_at.isoformat(),
                transcript_next_attempt_at=(next_attempt.isoformat() if next_attempt else None),
                transcript_collected_at=(collected_at.isoformat() if collected_at else None),
                transcript_recovered_at=(recovered_at.isoformat() if recovered_at else None),
                transcript_content_version=payload.get("content_version"),
                transcript_error_code=result.get("error_code"),
                transcript_error_message=result.get("error_message"),
                last_attempt_at=attempted_at.isoformat(),
                next_attempt_at=next_attempt.isoformat() if next_attempt else None,
                payload_json=json.dumps(result, ensure_ascii=False, sort_keys=True),
            )
            with state.transaction():
                technical_failure = lifecycle_status in {
                    TRANSCRIPT_BLOCKED,
                    TRANSCRIPT_PERMANENT_ERROR,
                    "rate_limited",
                    "retryable_error",
                }
                state.record_api_usage(
                    endpoint="transcripts.fetch",
                    request_count=1,
                    resource_count=1,
                    success_count=0 if technical_failure else 1,
                    error_count=1 if technical_failure else 0,
                    quota_bucket="transcript",
                    observed_at=attempted_at,
                    provider="youtube-transcript-api",
                    priority="normal",
                    retry_count=max(0, attempt_count - 1),
                    latency_ms=request_latency_ms,
                    queue_depth=max(0, len(due) - summary["succeeded"] - summary["failed"] - 1),
                    circuit_open=(lifecycle_status == TRANSCRIPT_BLOCKED or blocked_response),
                    status="error" if technical_failure else "success",
                    error_code=result.get("error_code"),
                )
                state.record_transcript_result(
                    video_id=row["video_id"],
                    requested_language_code=requested_language_code,
                    lifecycle_status=lifecycle_status,
                    attempt_count=attempt_count,
                    attempted_at=attempted_at,
                    next_attempt_at=next_attempt,
                    result=result,
                    requested_language=requested_language,
                    obtained_language=payload.get("language"),
                    obtained_language_code=payload.get("language_code"),
                    available_languages=available_languages,
                    generation_type=generation_type,
                    is_generated=payload.get("is_generated"),
                    is_translated=payload.get("is_translated"),
                    provider=payload.get("source") or "youtube_transcript_api",
                    selection_strategy=payload.get("selection_strategy"),
                    collected_at=collected_at,
                    error_code=result.get("error_code"),
                    error_message=result.get("error_message"),
                    recovered_at=recovered_at,
                    content_version=payload.get("content_version"),
                )
                state.enqueue_outbox(
                    worker_name="youtube_transcript",
                    aggregate_id=row["video_id"],
                    topic=result_topic,
                    event=event,
                    created_at=attempted_at,
                )
                if lifecycle_status == TRANSCRIPT_BLOCKED or blocked_response:
                    state.open_breaker(
                        "transcript",
                        now=attempted_at,
                        cooldown=cooldown,
                        reason=result.get("error_message") or lifecycle_status,
                    )
            if lifecycle_status == TRANSCRIPT_AVAILABLE:
                summary["succeeded"] += 1
            else:
                summary["failed"] += 1
            drain_outbox(state, producer)
            if lifecycle_status == TRANSCRIPT_BLOCKED or blocked_response:
                summary["circuit_open"] = True
                break
        processed = summary["succeeded"] + summary["failed"]
        completed = finalize_worker_summary(
            summary,
            elapsed_seconds=time.monotonic() - run_started,
            processed=processed,
        )
        state.record_worker_health(
            worker_name="youtube_transcript",
            observed_at=utc_now(),
            status=(
                "circuit_open"
                if summary.get("circuit_open")
                else "partial"
                if summary["failed"]
                else "success"
                if processed
                else "idle"
            ),
            processed_count=processed,
            success_count=summary["succeeded"],
            error_count=summary["failed"],
            retry_count=summary["failed"],
            latency_ms=(time.monotonic() - run_started) * 1000,
            circuit_open=bool(summary.get("circuit_open")),
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
