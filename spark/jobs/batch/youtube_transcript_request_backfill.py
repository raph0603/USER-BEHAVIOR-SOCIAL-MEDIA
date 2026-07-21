"""Seed retryable Silver YouTube transcript gaps into the persistent worker queue."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any


JOBS_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
for import_root in (JOBS_DIR, REPOSITORY_ROOT):
    import_path = str(import_root)
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

from common.youtube_pipeline import parse_datetime, utc_now  # noqa: E402
from common.youtube_state import YouTubeStateStore  # noqa: E402
from youtube_transcripts import (  # noqa: E402
    TRANSCRIPT_TABLE,
    _build_spark,
    _external_candidates,
    ensure_transcript_table,
)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def candidate_request(candidate: dict[str, Any]) -> dict[str, Any]:
    """Build the provider-neutral request persisted by the transcript worker."""

    video_id = str(candidate.get("root_content_id") or "").strip()
    language_code = str(candidate.get("requested_language_code") or "en").strip() or "en"
    published_at = parse_datetime(candidate.get("created_at"))
    return {
        "event_type": "youtube.transcript.requested",
        "video_id": video_id,
        "correlation_id": str(candidate.get("content_id") or video_id),
        "collected_at": utc_now().isoformat(),
        "published_at": published_at.isoformat() if published_at else None,
        "language": candidate.get("language"),
        "duration_seconds": candidate.get("duration_seconds"),
        "transcript_requested_language": str(candidate.get("requested_language") or language_code),
        "transcript_requested_language_code": language_code,
        "collection_status": "pending",
    }


def main() -> None:
    bucket = os.getenv("MINIO_BUCKET", "lakehouse")
    warehouse = f"s3a://{bucket}/warehouse"
    limit = _env_int("YOUTUBE_TRANSCRIPT_REQUEST_BACKFILL_LIMIT", 5000, minimum=1)
    max_attempts = _env_int("YOUTUBE_TRANSCRIPT_BACKFILL_MAX_ATTEMPTS", 5, minimum=1)
    retry_cooldown_seconds = _env_int(
        "YOUTUBE_TRANSCRIPT_BACKFILL_RETRY_COOLDOWN_SECONDS",
        3600,
    )
    state_path = os.getenv(
        "YOUTUBE_PIPELINE_STATE_DB",
        "/opt/spark/collector-state/youtube-pipeline.sqlite",
    )

    spark = _build_spark("youtube-transcript-request-backfill", warehouse)
    spark.sparkContext.setLogLevel("WARN")
    selected = 0
    inserted = 0
    invalid = 0
    try:
        ensure_transcript_table(spark)
        candidates = _external_candidates(
            spark.table("lakehouse.silver.events"),
            spark.table(TRANSCRIPT_TABLE),
            limit=limit,
            max_attempts=max_attempts,
            retry_cooldown_seconds=retry_cooldown_seconds,
        ).collect()
        selected = len(candidates)
        with YouTubeStateStore(state_path) as state:
            for candidate_row in candidates:
                candidate = candidate_row.asDict(recursive=True)
                request = candidate_request(candidate)
                video_id = request["video_id"]
                if not video_id:
                    invalid += 1
                    continue
                first_seen_at = parse_datetime(candidate.get("created_at")) or utc_now()
                inserted += int(
                    state.enqueue_transcript_request(
                        video_id=video_id,
                        correlation_id=request["correlation_id"],
                        first_seen_at=first_seen_at,
                        published_at=request.get("published_at"),
                        request=request,
                    )
                )
    finally:
        spark.stop()

    print(
        "YouTube transcript request backfill: "
        f"selected={selected}, inserted={inserted}, existing={selected - inserted - invalid}, "
        f"invalid={invalid}"
    )


if __name__ == "__main__":
    main()
