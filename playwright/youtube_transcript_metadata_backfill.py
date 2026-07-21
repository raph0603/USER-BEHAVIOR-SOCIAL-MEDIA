"""Bounded YouTube metadata repair for queued historical transcript requests."""

from __future__ import annotations

import json
import math
import os
import re
import time
from collections import defaultdict
from typing import Any

from googleapiclient.discovery import build

from common.youtube_pipeline import utc_now
from common.youtube_state import YouTubeStateStore


_ISO_DURATION = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def parse_iso8601_duration(value: str | None) -> float | None:
    """Return seconds for a YouTube contentDetails ISO-8601 duration."""

    match = _ISO_DURATION.fullmatch(str(value or ""))
    if not match:
        return None
    parts = {name: float(raw or 0) for name, raw in match.groupdict().items()}
    return (
        parts["days"] * 86_400 + parts["hours"] * 3_600 + parts["minutes"] * 60 + parts["seconds"]
    )


def metadata_request_update(request: dict[str, Any], item: dict[str, Any] | None) -> dict:
    """Add bounded duration and availability fields without retaining API payloads."""

    updated = dict(request)
    if item is None:
        updated["video_availability"] = "unavailable"
        return updated
    duration = parse_iso8601_duration((item.get("contentDetails") or {}).get("duration"))
    updated["duration_seconds"] = duration if duration is not None else 0.0
    privacy = str((item.get("status") or {}).get("privacyStatus") or "").strip().lower()
    if privacy:
        updated["video_availability"] = privacy
    return updated


def _candidate_rows(state: YouTubeStateStore, limit: int) -> list[dict[str, Any]]:
    rows = state.connection.execute(
        """
        SELECT * FROM youtube_transcript_lifecycle
        WHERE (
          transcript_lifecycle_status NOT IN (
            'available', 'unavailable', 'disabled', 'permanent_error'
          ) OR error_code = 'gemini_duration_unknown'
        )
          AND json_extract(request_json, '$.duration_seconds') IS NULL
          AND lower(coalesce(
            json_extract(request_json, '$.video_availability'), 'public'
          )) IN ('public', 'available')
        ORDER BY first_seen_at, video_id, requested_language_code
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    return [dict(row) for row in rows]


def _persist_updates(
    state: YouTubeStateStore,
    rows: list[dict[str, Any]],
    items: dict[str, dict[str, Any]],
) -> tuple[int, int]:
    updated_count = 0
    unavailable_count = 0
    now = utc_now().isoformat()
    with state.transaction():
        for row in rows:
            item = items.get(row["video_id"])
            request = metadata_request_update(json.loads(row["request_json"]), item)
            serialized = json.dumps(request, ensure_ascii=False, sort_keys=True)
            if item is None:
                unavailable_count += 1
                state.connection.execute(
                    """
                    UPDATE youtube_transcript_lifecycle
                    SET request_json = ?,
                        transcript_lifecycle_status = 'unavailable',
                        transcript_status = 'not_available',
                        next_attempt_at = NULL,
                        error_code = 'youtube_video_not_returned',
                        error_message = 'YouTube videos.list did not return this video'
                    WHERE video_id = ? AND requested_language_code = ?
                    """,
                    (serialized, row["video_id"], row["requested_language_code"]),
                )
            else:
                repair_duration_error = row.get("error_code") == "gemini_duration_unknown"
                state.connection.execute(
                    """
                    UPDATE youtube_transcript_lifecycle
                    SET request_json = ?,
                        transcript_lifecycle_status = CASE WHEN ? THEN 'pending'
                          ELSE transcript_lifecycle_status END,
                        transcript_status = CASE WHEN ? THEN 'pending'
                          ELSE transcript_status END,
                        next_attempt_at = CASE WHEN ? THEN ? ELSE next_attempt_at END,
                        error_code = CASE WHEN ? THEN NULL ELSE error_code END,
                        error_message = CASE WHEN ? THEN NULL ELSE error_message END
                    WHERE video_id = ? AND requested_language_code = ?
                    """,
                    (
                        serialized,
                        repair_duration_error,
                        repair_duration_error,
                        repair_duration_error,
                        now,
                        repair_duration_error,
                        repair_duration_error,
                        row["video_id"],
                        row["requested_language_code"],
                    ),
                )
            updated_count += 1
    return updated_count, unavailable_count


def main() -> None:
    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is required for transcript metadata backfill")
    limit = _env_int("YOUTUBE_TRANSCRIPT_METADATA_BACKFILL_LIMIT", 500, minimum=1)
    state_path = os.getenv("YOUTUBE_PIPELINE_STATE_DB", "/app/state/youtube-pipeline.sqlite")
    started = time.monotonic()
    updated = 0
    unavailable = 0
    api_requests = 0

    with YouTubeStateStore(state_path) as state:
        candidates = _candidate_rows(state, limit)
        by_video: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in candidates:
            by_video[row["video_id"]].append(row)
        video_ids = sorted(by_video)
        requested_calls = math.ceil(len(video_ids) / 50)
        decision = state.quota_decision(
            endpoint="videos.list",
            workload="descriptive_metadata",
            requested_calls=requested_calls,
            now=utc_now(),
        )
        allowed_ids = video_ids[: decision.allowed_calls * 50]
        client = (
            build("youtube", "v3", developerKey=api_key, cache_discovery=False)
            if allowed_ids
            else None
        )
        for start in range(0, len(allowed_ids), 50):
            batch_ids = allowed_ids[start : start + 50]
            attempted_at = utc_now()
            request_started = time.monotonic()
            try:
                response = (
                    client.videos()
                    .list(
                        part="contentDetails,status",
                        id=",".join(batch_ids),
                        maxResults=50,
                    )
                    .execute()
                )
            except Exception:
                state.record_api_usage(
                    endpoint="videos.list",
                    request_count=1,
                    resource_count=len(batch_ids),
                    success_count=0,
                    error_count=len(batch_ids),
                    quota_bucket="transcript_metadata_backfill",
                    observed_at=attempted_at,
                    provider="youtube",
                    priority="low",
                    latency_ms=(time.monotonic() - request_started) * 1000,
                    status="error",
                    error_code="youtube_metadata_backfill_error",
                )
                raise
            items = {item["id"]: item for item in response.get("items", [])}
            batch_rows = [row for video_id in batch_ids for row in by_video[video_id]]
            batch_updated, batch_unavailable = _persist_updates(state, batch_rows, items)
            updated += batch_updated
            unavailable += batch_unavailable
            api_requests += 1
            state.record_api_usage(
                endpoint="videos.list",
                request_count=1,
                resource_count=len(batch_ids),
                success_count=len(items),
                error_count=max(0, len(batch_ids) - len(items)),
                quota_bucket="transcript_metadata_backfill",
                observed_at=attempted_at,
                provider="youtube",
                priority="low",
                latency_ms=(time.monotonic() - request_started) * 1000,
                status="partial" if len(items) < len(batch_ids) else "success",
            )

    print(
        "YouTube transcript metadata backfill: "
        f"selected={len(candidates)}, videos={len(video_ids)}, "
        f"api_requests={api_requests}, updated={updated}, unavailable={unavailable}, "
        f"throttled={max(0, len(video_ids) - len(allowed_ids))}, "
        f"elapsed_seconds={time.monotonic() - started:.3f}"
    )


if __name__ == "__main__":
    main()
