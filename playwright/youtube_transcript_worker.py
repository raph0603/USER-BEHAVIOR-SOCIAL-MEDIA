"""Independent, stateful YouTube transcript request worker."""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from datetime import timedelta
from typing import Any

from common.transcripts import (
    TRANSCRIPT_AVAILABLE,
    TRANSCRIPT_PERMANENT_ERROR,
    TranscriptProviderChain,
    TranscriptRequest,
    YouTubeTranscriptProvider,
    is_terminal_transcript_lifecycle,
    legacy_transcript_status,
    normalize_transcript_language_code,
    preferred_transcript_language_code,
    transcript_lifecycle_status,
)
from common.gemini_transcripts import (
    GEMINI_PROVIDER,
    GeminiTranscriptConfig,
    GeminiTranscriptProvider,
)
from common.youtube_pipeline import finalize_worker_summary, parse_datetime, utc_now
from common.youtube_state import YouTubeStateStore
from youtube_pipeline_events import (
    EventConsumer,
    EventProducer,
    drain_outbox,
    pipeline_event,
)


_GEMINI_PRIORITY_POOL_LIMIT = 5000


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


def _gemini_candidate_minutes(
    row: dict[str, Any],
    *,
    max_duration_minutes: float,
) -> float | None:
    try:
        request = json.loads(row["request_json"])
        duration_minutes = float(request.get("duration_seconds")) / 60.0
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None
    availability = str(request.get("video_availability") or "public").strip().lower()
    if availability not in {"public", "available"}:
        return None
    if (
        not math.isfinite(duration_minutes)
        or duration_minutes <= 0
        or duration_minutes > max(0.0, float(max_duration_minutes))
    ):
        return None
    return duration_minutes


def prioritize_due_transcript_requests(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    max_duration_minutes: float,
    remaining_video_minutes: float,
    remaining_request_count: int,
    primary_circuit_open: bool,
) -> list[dict[str, Any]]:
    """Select the longest Gemini-eligible videos that fit the remaining budget."""

    batch_limit = max(1, int(limit))
    fallback_limit = min(batch_limit, max(0, int(remaining_request_count)))
    remaining = max(0.0, float(remaining_video_minutes))
    eligible = []
    for index, row in enumerate(rows):
        minutes = _gemini_candidate_minutes(
            row,
            max_duration_minutes=max_duration_minutes,
        )
        if minutes is not None:
            eligible.append((index, row, minutes))
    eligible.sort(key=lambda item: (-item[2], item[0]))

    selected: list[dict[str, Any]] = []
    selected_indexes: set[int] = set()
    for index, row, minutes in eligible:
        if len(selected) >= fallback_limit:
            break
        if minutes > remaining + 1e-9:
            continue
        selected.append(row)
        selected_indexes.add(index)
        remaining -= minutes

    if not primary_circuit_open:
        for index, row in enumerate(rows):
            if len(selected) >= batch_limit:
                break
            if index not in selected_indexes:
                selected.append(row)
    return selected


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
        if events:
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
        primary_circuit_open = state.breaker_open("transcript", now)
        gemini_config = GeminiTranscriptConfig.from_env()
        gemini_circuit_open = state.breaker_open("transcript_gemini", now)
        summary["primary_circuit_open"] = primary_circuit_open
        summary["gemini_circuit_open"] = gemini_circuit_open
        summary["gemini_fallbacks"] = 0
        summary["primary_attempts"] = 0
        summary["gemini_succeeded"] = 0
        summary["gemini_cache_hits"] = 0
        summary["gemini_video_minutes"] = 0.0
        summary["fallback_reasons"] = {}
        summary["gemini_errors"] = {}
        candidate_pool = state.due_transcript_requests(
            now=now,
            limit=max(batch_size, _GEMINI_PRIORITY_POOL_LIMIT),
        )
        summary["candidate_pool"] = len(candidate_pool)
        if gemini_config.enabled and not gemini_circuit_open:
            used_minutes = state.gemini_video_minutes_today(now)
            remaining_minutes = max(
                0.0,
                gemini_config.daily_video_minutes_budget - used_minutes,
            )
            used_requests = state.gemini_requests_current_quota_day(now)
            remaining_requests = max(
                0,
                gemini_config.daily_request_budget - used_requests,
            )
            due = prioritize_due_transcript_requests(
                candidate_pool,
                limit=batch_size,
                max_duration_minutes=gemini_config.max_duration_minutes,
                remaining_video_minutes=remaining_minutes,
                remaining_request_count=remaining_requests,
                primary_circuit_open=primary_circuit_open,
            )
            summary["selection_strategy"] = (
                "gemini_longest_budget_fit"
                if primary_circuit_open
                else "gemini_longest_budget_fit_then_oldest"
            )
            summary["gemini_budget_remaining_before_selection"] = remaining_minutes
            summary["gemini_requests_remaining_before_selection"] = remaining_requests
        else:
            due = candidate_pool[:batch_size]
            summary["selection_strategy"] = "oldest_due"
        summary["due"] = len(due)
        for row in due:
            attempted_at = utc_now()
            attempt_count = int(row.get("attempt_count") or 0) + 1
            request = json.loads(row["request_json"])
            requested_language_code = row.get("requested_language_code") or (
                _requested_language_code(request)
            )
            requested_language = row.get("requested_language") or requested_language_code
            provider_request = TranscriptRequest(
                video_id=row["video_id"],
                requested_language_code=requested_language_code,
                attempt_count=attempt_count,
                max_primary_attempts=max_attempts,
                duration_seconds=request.get("duration_seconds"),
                video_availability=request.get("video_availability"),
                source_content_version=request.get("transcript_source_content_version"),
            )
            gemini_provider = GeminiTranscriptProvider(
                gemini_config,
                cache_get=lambda key: state.cached_gemini_transcript(key),
                cache_put=lambda key, cached_payload, video_minutes: (
                    state.cache_gemini_transcript(
                        key,
                        cached_payload,
                        video_minutes,
                        source_content_version=provider_request.source_content_version,
                    )
                ),
                used_video_minutes=state.gemini_video_minutes_today,
                used_requests=state.gemini_requests_current_quota_day,
            )
            chain = TranscriptProviderChain(
                YouTubeTranscriptProvider(),
                gemini_provider,
                fallback_enabled=(gemini_config.enabled and not gemini_circuit_open),
            )
            request_started = time.monotonic()
            chain_result = chain.collect(
                provider_request,
                primary_circuit_open=primary_circuit_open,
            )
            result = chain_result.final_result.to_dict()
            primary_result = chain_result.primary_result.to_dict()
            fallback_result = (
                chain_result.fallback_result.to_dict()
                if chain_result.fallback_result is not None
                else None
            )
            request_latency_ms = (time.monotonic() - request_started) * 1000
            if not primary_circuit_open:
                summary["primary_attempts"] += 1
            if chain_result.used_fallback:
                summary["gemini_fallbacks"] += 1
                reason = chain_result.fallback_reason or "unknown"
                summary["fallback_reasons"][reason] = summary["fallback_reasons"].get(reason, 0) + 1
            if gemini_provider.last_cache_hit:
                summary["gemini_cache_hits"] += 1
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
            blocked_response = str(primary_result.get("error_code") or "").lower() in {
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
            generation_type = payload.get("generation_type_override") or (
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
                transcript_model=payload.get("model"),
                transcript_source=payload.get("source") or "youtube_transcript_api",
                transcript_is_generated=payload.get("is_generated"),
                transcript_is_translated=payload.get("is_translated"),
                transcript_selection_strategy=payload.get("selection_strategy"),
                transcript_fallback_reason=chain_result.fallback_reason,
                transcript_prompt_version=payload.get("prompt_version"),
                transcript_generated_by_model=payload.get("generated_by_model"),
                transcript_source_content_version=provider_request.source_content_version,
                transcript_primary_attempt_count=primary_result.get("attempt_count"),
                transcript_fallback_attempt_count=(
                    fallback_result.get("attempt_count") if fallback_result else 0
                ),
                transcript_primary_last_attempt_at=attempted_at.isoformat(),
                transcript_fallback_last_attempt_at=(
                    attempted_at.isoformat() if fallback_result else None
                ),
                transcript_primary_result_json=json.dumps(
                    primary_result, ensure_ascii=False, sort_keys=True
                ),
                transcript_fallback_result_json=(
                    json.dumps(fallback_result, ensure_ascii=False, sort_keys=True)
                    if fallback_result
                    else None
                ),
                transcript_warnings_json=(
                    json.dumps(payload.get("warnings"), ensure_ascii=False, sort_keys=True)
                    if payload.get("warnings") is not None
                    else None
                ),
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
                state.record_api_usage(
                    endpoint="transcripts.fetch",
                    request_count=0 if primary_circuit_open else 1,
                    resource_count=1,
                    success_count=(
                        1
                        if str((primary_result.get("payload") or {}).get("text") or "").strip()
                        else 0
                    ),
                    error_count=(
                        0
                        if str((primary_result.get("payload") or {}).get("text") or "").strip()
                        else 1
                    ),
                    quota_bucket="transcript",
                    observed_at=attempted_at,
                    provider="youtube_transcript_api",
                    priority="normal",
                    retry_count=max(0, attempt_count - 1),
                    latency_ms=request_latency_ms,
                    queue_depth=max(0, len(due) - summary["succeeded"] - summary["failed"] - 1),
                    circuit_open=(primary_circuit_open or blocked_response),
                    status=primary_result.get("status"),
                    error_code=primary_result.get("error_code"),
                )
                if fallback_result is not None:
                    fallback_payload = fallback_result.get("payload") or {}
                    fallback_success = bool(str(fallback_payload.get("text") or "").strip())
                    fallback_request_count = (
                        0
                        if gemini_provider.last_cache_hit
                        or int(fallback_result.get("attempt_count") or 0) == 0
                        else int(fallback_result.get("attempt_count") or 0)
                    )
                    video_minutes = (
                        gemini_provider.video_minutes(provider_request) * fallback_request_count
                        if fallback_request_count
                        else 0.0
                    )
                    summary["gemini_video_minutes"] += video_minutes
                    if fallback_success:
                        summary["gemini_succeeded"] += 1
                    elif fallback_result.get("error_code"):
                        gemini_error_code = str(fallback_result["error_code"])
                        summary["gemini_errors"][gemini_error_code] = (
                            summary["gemini_errors"].get(gemini_error_code, 0) + 1
                        )
                    used_minutes = state.gemini_video_minutes_today(attempted_at)
                    remaining_minutes = max(
                        0.0,
                        gemini_config.daily_video_minutes_budget - used_minutes - video_minutes,
                    )
                    summary["gemini_budget_remaining_minutes"] = remaining_minutes
                    state.record_api_usage(
                        endpoint="transcripts.generate_from_youtube_url",
                        request_count=fallback_request_count,
                        resource_count=1,
                        success_count=1 if fallback_success else 0,
                        error_count=0 if fallback_success else 1,
                        quota_bucket="transcript_fallback",
                        observed_at=attempted_at,
                        provider=GEMINI_PROVIDER,
                        priority="low",
                        cache_hit_count=int(gemini_provider.last_cache_hit),
                        cache_miss_count=fallback_request_count,
                        retry_count=max(0, int(fallback_result.get("attempt_count") or 0) - 1),
                        latency_ms=request_latency_ms,
                        queue_depth=max(
                            0,
                            len(due) - summary["succeeded"] - summary["failed"] - 1,
                        ),
                        circuit_open=gemini_circuit_open,
                        status=fallback_result.get("status"),
                        error_code=fallback_result.get("error_code"),
                        video_minutes=video_minutes,
                        daily_video_minutes_budget=(gemini_config.daily_video_minutes_budget),
                        remaining_video_minutes=remaining_minutes,
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
                    model=payload.get("model"),
                    fallback_reason=chain_result.fallback_reason,
                    prompt_version=payload.get("prompt_version"),
                    generated_by_model=payload.get("generated_by_model"),
                    source_content_version=provider_request.source_content_version,
                    primary_attempt_count=int(primary_result.get("attempt_count") or 0),
                    fallback_attempt_count=int((fallback_result or {}).get("attempt_count") or 0),
                    primary_last_attempt_at=attempted_at,
                    fallback_last_attempt_at=(
                        attempted_at if fallback_result is not None else None
                    ),
                    primary_result=primary_result,
                    fallback_result=fallback_result,
                )
                state.record_transcript_provider_attempt(
                    attempt_id=hashlib.sha256(
                        (
                            f"{row['video_id']}:{requested_language_code}:"
                            f"youtube_transcript_api:{attempt_count}"
                        ).encode("utf-8")
                    ).hexdigest(),
                    video_id=row["video_id"],
                    requested_language_code=requested_language_code,
                    provider="youtube_transcript_api",
                    model=None,
                    attempt_count=int(primary_result.get("attempt_count") or 0),
                    attempted_at=attempted_at,
                    latency_ms=request_latency_ms,
                    status=str(primary_result.get("status") or "failed"),
                    error_code=primary_result.get("error_code"),
                    fallback_reason=chain_result.fallback_reason,
                    result=primary_result,
                )
                if fallback_result is not None:
                    state.record_transcript_provider_attempt(
                        attempt_id=hashlib.sha256(
                            (
                                f"{row['video_id']}:{requested_language_code}:"
                                f"gemini:{attempt_count}"
                            ).encode("utf-8")
                        ).hexdigest(),
                        video_id=row["video_id"],
                        requested_language_code=requested_language_code,
                        provider=GEMINI_PROVIDER,
                        model=gemini_config.model,
                        attempt_count=int(fallback_result.get("attempt_count") or 0),
                        attempted_at=attempted_at,
                        latency_ms=request_latency_ms,
                        status=str(fallback_result.get("status") or "failed"),
                        error_code=fallback_result.get("error_code"),
                        fallback_reason=chain_result.fallback_reason,
                        result=fallback_result,
                    )
                state.enqueue_outbox(
                    worker_name="youtube_transcript",
                    aggregate_id=row["video_id"],
                    topic=result_topic,
                    event=event,
                    created_at=attempted_at,
                )
                if blocked_response:
                    state.open_breaker(
                        "transcript",
                        now=attempted_at,
                        cooldown=cooldown,
                        reason=primary_result.get("error_message") or "primary_blocked",
                    )
                gemini_error = str((fallback_result or {}).get("error_code") or "")
                if gemini_error in {
                    "gemini_rate_limited",
                    "gemini_model_unavailable",
                    "gemini_timeout",
                }:
                    state.open_breaker(
                        "transcript_gemini",
                        now=attempted_at,
                        cooldown=timedelta(seconds=gemini_config.cooldown_seconds),
                        reason=gemini_error,
                    )
                    gemini_circuit_open = True
                    summary["gemini_circuit_open"] = True
            if lifecycle_status == TRANSCRIPT_AVAILABLE:
                summary["succeeded"] += 1
            else:
                summary["failed"] += 1
            drain_outbox(state, producer)
            if blocked_response:
                summary["circuit_open"] = True
                primary_circuit_open = True
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
            cache_hit_count=summary["gemini_cache_hits"],
            cache_miss_count=max(0, summary["gemini_fallbacks"] - summary["gemini_cache_hits"]),
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
