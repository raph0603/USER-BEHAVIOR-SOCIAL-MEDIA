"""Persist and retry YouTube transcript collection outcomes from Silver events.

The job first materializes transcript data already carried by collector events. It
then fetches only eligible missing or retryable outcomes and records every attempt,
including permanent unavailability and technical failures.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import parse_qs, urlparse

PYSPARK_IMPORT_ERROR: ModuleNotFoundError | None = None
try:
    from pyspark.sql import DataFrame, SparkSession, Window
    from pyspark.sql.functions import (
        col,
        coalesce,
        concat_ws,
        current_timestamp,
        length,
        lit,
        lower,
        regexp_extract,
        row_number,
        sha2,
        size,
        split,
        to_date,
        to_json,
        to_timestamp,
        trim,
        unix_timestamp,
        when,
    )
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ModuleNotFoundError as exc:
    if exc.name != "pyspark" and not str(exc.name or "").startswith("pyspark."):
        raise
    PYSPARK_IMPORT_ERROR = exc
    DataFrame = Any
    SparkSession = Any


JOBS_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
for import_root in (JOBS_DIR, REPOSITORY_ROOT):
    import_path = str(import_root)
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

from common.transcripts import (
    RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES,
    TERMINAL_TRANSCRIPT_LIFECYCLE_STATUSES,
    TRANSCRIPT_AVAILABLE,
    TRANSCRIPT_PERMANENT_ERROR,
    legacy_transcript_status,
    preferred_transcript_language_code,
    transcript_lifecycle_status,
)

TRANSCRIPT_TABLE = "lakehouse.silver.transcripts"
LEGACY_TERMINAL_TRANSCRIPT_STATUSES = frozenset({"success", "not_available", "disabled"})
LEGACY_RETRYABLE_TRANSCRIPT_STATUSES = frozenset({"pending", "partial", "rate_limited", "failed"})
TERMINAL_TRANSCRIPT_STATUSES = (
    TERMINAL_TRANSCRIPT_LIFECYCLE_STATUSES | LEGACY_TERMINAL_TRANSCRIPT_STATUSES
)
RETRYABLE_TRANSCRIPT_STATUSES = frozenset(
    RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES | LEGACY_RETRYABLE_TRANSCRIPT_STATUSES
)
KNOWN_TRANSCRIPT_STATUSES = TERMINAL_TRANSCRIPT_STATUSES | RETRYABLE_TRANSCRIPT_STATUSES

CREATE_TRANSCRIPTS_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.transcripts (
  video_id STRING,
  content_id STRING,
  transcript_status STRING,
  transcript_lifecycle_status STRING,
  requested_language STRING,
  requested_language_code STRING,
  obtained_language STRING,
  obtained_language_code STRING,
  language STRING,
  language_code STRING,
  transcript_text STRING,
  segments_json STRING,
  duration_seconds DOUBLE,
  covered_duration_seconds DOUBLE,
  word_count BIGINT,
  segment_count BIGINT,
  available_languages_json STRING,
  has_auto_captions BOOLEAN,
  is_generated BOOLEAN,
  is_translated BOOLEAN,
  generation_type STRING,
  source_language STRING,
  source_language_code STRING,
  transcript_source STRING,
  provider STRING,
  selection_strategy STRING,
  error_code STRING,
  error_message STRING,
  attempt_count BIGINT,
  last_attempt_at TIMESTAMP,
  next_attempt_at TIMESTAMP,
  collected_at TIMESTAMP,
  recovered_at TIMESTAMP,
  content_version STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  event_date DATE
)
USING iceberg
PARTITIONED BY (event_date)
"""

TRANSCRIPT_COLUMN_TYPES = {
    "video_id": "STRING",
    "content_id": "STRING",
    "transcript_status": "STRING",
    "transcript_lifecycle_status": "STRING",
    "requested_language": "STRING",
    "requested_language_code": "STRING",
    "obtained_language": "STRING",
    "obtained_language_code": "STRING",
    "language": "STRING",
    "language_code": "STRING",
    "transcript_text": "STRING",
    "segments_json": "STRING",
    "duration_seconds": "DOUBLE",
    "covered_duration_seconds": "DOUBLE",
    "word_count": "BIGINT",
    "segment_count": "BIGINT",
    "available_languages_json": "STRING",
    "has_auto_captions": "BOOLEAN",
    "is_generated": "BOOLEAN",
    "is_translated": "BOOLEAN",
    "generation_type": "STRING",
    "source_language": "STRING",
    "source_language_code": "STRING",
    "transcript_source": "STRING",
    "provider": "STRING",
    "selection_strategy": "STRING",
    "error_code": "STRING",
    "error_message": "STRING",
    "attempt_count": "BIGINT",
    "last_attempt_at": "TIMESTAMP",
    "next_attempt_at": "TIMESTAMP",
    "collected_at": "TIMESTAMP",
    "recovered_at": "TIMESTAMP",
    "content_version": "STRING",
    "created_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
    "event_date": "DATE",
}
TRANSCRIPT_COLUMNS = list(TRANSCRIPT_COLUMN_TYPES)

EVENT_OPTIONAL_COLUMNS = {
    "content_id": "STRING",
    "root_content_id": "STRING",
    "content_type": "STRING",
    "relation_type": "STRING",
    "conversation_id": "STRING",
    "platform_event_id": "STRING",
    "url": "STRING",
    "language": "STRING",
    "event_ts": "TIMESTAMP",
    "duration_seconds": "DOUBLE",
    "has_auto_captions": "BOOLEAN",
    "attempt_count": "BIGINT",
    "last_attempt_at": "TIMESTAMP",
    "transcript_status": "STRING",
    "transcript_lifecycle_status": "STRING",
    "transcript_text": "STRING",
    "transcript_segments_json": "STRING",
    "transcript_language": "STRING",
    "transcript_language_code": "STRING",
    "transcript_requested_language": "STRING",
    "transcript_requested_language_code": "STRING",
    "transcript_obtained_language": "STRING",
    "transcript_obtained_language_code": "STRING",
    "transcript_is_generated": "BOOLEAN",
    "transcript_is_translated": "BOOLEAN",
    "transcript_generation_type": "STRING",
    "transcript_provider": "STRING",
    "transcript_source_language": "STRING",
    "transcript_source_language_code": "STRING",
    "transcript_source": "STRING",
    "transcript_selection_strategy": "STRING",
    "transcript_segment_count": "BIGINT",
    "transcript_available_languages": "ARRAY<STRING>",
    "transcript_available_languages_json": "STRING",
    "transcript_covered_duration_seconds": "DOUBLE",
    "transcript_collected_at": "TIMESTAMP",
    "transcript_attempt_count": "BIGINT",
    "transcript_last_attempt_at": "TIMESTAMP",
    "transcript_next_attempt_at": "TIMESTAMP",
    "transcript_recovered_at": "TIMESTAMP",
    "transcript_content_version": "STRING",
    "transcript_error_code": "STRING",
    "transcript_error_message": "STRING",
}


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _env_bool(name: str, default: bool) -> bool:
    fallback = "true" if default else "false"
    value = _env(name, fallback).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _require_pyspark() -> None:
    if PYSPARK_IMPORT_ERROR is not None:
        raise RuntimeError(
            "PySpark is required to execute the transcript backfill"
        ) from PYSPARK_IMPORT_ERROR


def _transcript_schema():
    _require_pyspark()
    return StructType(
        [
            StructField("video_id", StringType(), True),
            StructField("content_id", StringType(), False),
            StructField("transcript_status", StringType(), False),
            StructField("transcript_lifecycle_status", StringType(), False),
            StructField("requested_language", StringType(), True),
            StructField("requested_language_code", StringType(), False),
            StructField("obtained_language", StringType(), True),
            StructField("obtained_language_code", StringType(), True),
            StructField("language", StringType(), True),
            StructField("language_code", StringType(), True),
            StructField("transcript_text", StringType(), True),
            StructField("segments_json", StringType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("covered_duration_seconds", DoubleType(), True),
            StructField("word_count", LongType(), True),
            StructField("segment_count", LongType(), True),
            StructField("available_languages_json", StringType(), True),
            StructField("has_auto_captions", BooleanType(), True),
            StructField("is_generated", BooleanType(), True),
            StructField("is_translated", BooleanType(), True),
            StructField("generation_type", StringType(), True),
            StructField("source_language", StringType(), True),
            StructField("source_language_code", StringType(), True),
            StructField("transcript_source", StringType(), True),
            StructField("provider", StringType(), True),
            StructField("selection_strategy", StringType(), True),
            StructField("error_code", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("attempt_count", LongType(), False),
            StructField("last_attempt_at", TimestampType(), True),
            StructField("next_attempt_at", TimestampType(), True),
            StructField("collected_at", TimestampType(), True),
            StructField("recovered_at", TimestampType(), True),
            StructField("content_version", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("event_date", DateType(), True),
        ]
    )


def _build_spark(app_name: str, warehouse: str) -> SparkSession:
    _require_pyspark()
    minio_endpoint = _env("MINIO_ENDPOINT", "http://minio:9000")
    access_key = _env("MINIO_ROOT_USER", "minioadmin")
    secret_key = _env("MINIO_ROOT_PASSWORD", "minioadmin")

    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def _video_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    if hostname in {"youtu.be", "www.youtu.be"}:
        return parsed.path.strip("/").split("/", 1)[0] or None
    if hostname.endswith("youtube.com"):
        query_id = parse_qs(parsed.query).get("v", [None])[0]
        if query_id:
            return query_id
        path_parts = [part for part in parsed.path.split("/") if part]
        if len(path_parts) >= 2 and path_parts[0] in {"shorts", "embed", "live"}:
            return path_parts[1]
    return None


def _extract_video_id(row: Mapping[str, Any]) -> str | None:
    from_url = _video_id_from_url(row.get("url"))
    if from_url:
        return from_url
    platform_id = row.get("conversation_id") or row.get("platform_event_id")
    if platform_id and re.fullmatch(r"[A-Za-z0-9_-]{6,}", str(platform_id)):
        return str(platform_id)
    return None


def _as_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "to_dict"):
        converted = value.to_dict()
        if isinstance(converted, Mapping):
            return dict(converted)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    return {
        name: getattr(value, name)
        for name in dir(value)
        if not name.startswith("_") and not callable(getattr(value, name, None))
    }


def _as_datetime(value: Any, fallback: datetime | None = None) -> datetime | None:
    if value is None:
        return fallback
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        if normalized:
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                return fallback
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
    return fallback


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _normalize_status(value: Any, *, has_text: bool = False) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in KNOWN_TRANSCRIPT_STATUSES:
        return normalized
    return "success" if has_text else "failed"


def is_retryable_transcript(
    status: str | None,
    transcript_text: str | None,
    attempt_count: int | None,
    max_attempts: int,
) -> bool:
    """Return whether a transcript outcome is eligible for another attempt."""

    normalized_status = str(status or "").strip().lower()
    if transcript_text and transcript_text.strip():
        normalized_status = "success"
    elif not normalized_status:
        normalized_status = "pending"
    return (
        normalized_status in RETRYABLE_TRANSCRIPT_STATUSES
        and int(attempt_count or 0) < max_attempts
    )


def candidate_sort_key(row: Mapping[str, Any]) -> tuple[int, datetime, str]:
    """Provide the driver-side equivalent of the deterministic Spark ordering."""

    oldest = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        int(row.get("existing_attempt_count") or 0),
        _as_datetime(row.get("existing_last_attempt_at"), oldest) or oldest,
        str(row.get("content_id") or ""),
    )


def _fetch_common_transcript(
    video_id: str,
    preferred_languages: Sequence[str],
    *,
    attempt_count: int,
):
    from common.transcripts import fetch_transcript

    return fetch_transcript(
        video_id,
        preferred_languages=preferred_languages,
        require_preferred_language=True,
        attempt_count=attempt_count,
    )


def _preferred_languages_for_candidate(candidate: Mapping[str, Any]) -> tuple[str, ...]:
    """Select Vietnamese only for Vietnamese videos, English for every other video."""

    explicit = candidate.get("requested_language_code") or candidate.get(
        "transcript_requested_language_code"
    )
    if explicit:
        return (str(explicit).strip().casefold().replace("_", "-"),)
    return (preferred_transcript_language_code(candidate.get("language")),)


def _classify_unexpected_exception(exc: Exception) -> tuple[str, str, str]:
    try:
        from common.transcripts import classify_transcript_error

        classification = classify_transcript_error(exc)
        return (
            classification.status,
            classification.error_code,
            classification.error_message or classification.error_code,
        )
    except Exception:
        pass

    error_code = type(exc).__name__
    error_message = (str(exc)[:1000] or error_code).replace("\n", " ")
    normalized = error_code.lower()
    if normalized == "transcriptsdisabled":
        return "disabled", error_code, error_message
    if normalized in {
        "notranscriptfound",
        "videounavailable",
        "agerestricted",
        "videounplayable",
        "invalidvideoid",
    }:
        return "not_available", error_code, error_message
    if normalized in {"ipblocked", "requestblocked", "toomanyrequests"} or "429" in str(exc):
        return "rate_limited", error_code, error_message
    return "failed", error_code, error_message


def _base_attempt_row(
    candidate: Mapping[str, Any],
    *,
    video_id: str | None,
    now: datetime,
    max_attempts: int = 5,
    retry_cooldown_seconds: int = 3600,
) -> dict[str, Any]:
    created_at = _as_datetime(candidate.get("created_at"), now) or now
    requested_language_code = _preferred_languages_for_candidate(candidate)[0]
    requested_language = candidate.get("requested_language") or requested_language_code
    attempt_count = int(candidate.get("existing_attempt_count") or 0) + 1
    lifecycle_status = transcript_lifecycle_status(
        "failed",
        attempt_count=attempt_count,
        max_attempts=max_attempts,
    )
    next_attempt_at = (
        now + timedelta(seconds=max(0, int(retry_cooldown_seconds)))
        if lifecycle_status in RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES
        else None
    )
    return {
        "video_id": video_id,
        "content_id": str(candidate["content_id"]),
        "transcript_status": legacy_transcript_status(lifecycle_status),
        "transcript_lifecycle_status": lifecycle_status,
        "requested_language": requested_language,
        "requested_language_code": requested_language_code,
        "obtained_language": None,
        "obtained_language_code": None,
        "language": candidate.get("language"),
        "language_code": None,
        "transcript_text": None,
        "segments_json": None,
        "duration_seconds": candidate.get("duration_seconds"),
        "covered_duration_seconds": None,
        "word_count": None,
        "segment_count": None,
        "available_languages_json": None,
        "has_auto_captions": None,
        "is_generated": None,
        "is_translated": None,
        "generation_type": None,
        "source_language": None,
        "source_language_code": None,
        "transcript_source": "youtube_transcript_api",
        "provider": "youtube_transcript_api",
        "selection_strategy": None,
        "error_code": None,
        "error_message": None,
        "attempt_count": attempt_count,
        "last_attempt_at": now,
        "next_attempt_at": next_attempt_at,
        "collected_at": None,
        "recovered_at": None,
        "content_version": None,
        "created_at": created_at,
        "updated_at": now,
        "event_date": created_at.date(),
    }


def _result_to_row(
    candidate: Mapping[str, Any],
    video_id: str,
    result: Any,
    *,
    now: datetime,
    max_attempts: int = 5,
    retry_cooldown_seconds: int = 3600,
) -> dict[str, Any]:
    row = _base_attempt_row(
        candidate,
        video_id=video_id,
        now=now,
        max_attempts=max_attempts,
        retry_cooldown_seconds=retry_cooldown_seconds,
    )
    result_map = _as_mapping(result)
    payload = _as_mapping(result_map.get("payload", getattr(result, "payload", None)))
    transcript_text = payload.get("text")
    if transcript_text is not None:
        transcript_text = str(transcript_text).strip() or None
    result_status = _normalize_status(
        result_map.get("status", getattr(result, "status", None)),
        has_text=bool(transcript_text),
    )
    error_code = result_map.get("error_code", getattr(result, "error_code", None))
    error_message = result_map.get(
        "error_message",
        getattr(result, "error_message", None),
    )
    if result_status == "success" and not transcript_text:
        result_status = "partial"
        error_code = error_code or "empty_transcript"
        error_message = error_message or "The selected transcript contained no text."

    lifecycle_status = transcript_lifecycle_status(
        result_status,
        error_code=error_code,
        has_text=bool(transcript_text),
        attempt_count=row["attempt_count"],
        max_attempts=max_attempts,
    )

    segments = payload.get("segments")
    available_languages = payload.get("available_languages")
    collected_at = _as_datetime(payload.get("collected_at"))
    completed_at = (
        _as_datetime(
            result_map.get("completed_at", getattr(result, "completed_at", None)),
            now,
        )
        or now
    )
    is_generated = payload.get("is_generated")
    recovered_at = (
        completed_at
        if lifecycle_status == TRANSCRIPT_AVAILABLE
        and candidate.get("existing_transcript_lifecycle_status")
        not in {None, TRANSCRIPT_AVAILABLE}
        and int(candidate.get("existing_attempt_count") or 0) > 0
        else None
    )
    next_attempt_at = (
        completed_at + timedelta(seconds=max(0, int(retry_cooldown_seconds)))
        if lifecycle_status in RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES
        else None
    )

    row.update(
        {
            "transcript_status": legacy_transcript_status(lifecycle_status),
            "transcript_lifecycle_status": lifecycle_status,
            "requested_language": payload.get("requested_language") or row["requested_language"],
            "requested_language_code": payload.get("requested_language_code")
            or row["requested_language_code"],
            "obtained_language": payload.get("language"),
            "obtained_language_code": payload.get("language_code"),
            "language": payload.get("language") or candidate.get("language"),
            "language_code": payload.get("language_code"),
            "transcript_text": transcript_text,
            "segments_json": _json_or_none(segments),
            "covered_duration_seconds": payload.get("covered_duration_seconds"),
            "word_count": payload.get("word_count"),
            "segment_count": payload.get("segment_count"),
            "available_languages_json": _json_or_none(available_languages),
            "has_auto_captions": is_generated,
            "is_generated": is_generated,
            "is_translated": payload.get("is_translated"),
            "generation_type": (
                None if is_generated is None else ("automatic" if is_generated else "manual")
            ),
            "source_language": payload.get("source_language"),
            "source_language_code": payload.get("source_language_code"),
            "transcript_source": payload.get("source") or "youtube_transcript_api",
            "provider": payload.get("source") or "youtube_transcript_api",
            "selection_strategy": payload.get("selection_strategy"),
            "error_code": error_code,
            "error_message": error_message,
            "last_attempt_at": completed_at,
            "next_attempt_at": next_attempt_at,
            "collected_at": (collected_at if lifecycle_status == TRANSCRIPT_AVAILABLE else None),
            "recovered_at": recovered_at,
            "content_version": payload.get("content_version"),
            "updated_at": completed_at,
        }
    )
    if row["word_count"] is None and transcript_text:
        row["word_count"] = len(transcript_text.split())
    if row["segment_count"] is None and segments is not None:
        row["segment_count"] = len(segments)
    return row


def _attempt_transcript_row(
    candidate: Mapping[str, Any],
    preferred_languages: Sequence[str],
    sleep_seconds: float,
    *,
    fetcher: Callable[..., Any] = _fetch_common_transcript,
    clock: Callable[[], datetime] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    max_attempts: int = 5,
    retry_cooldown_seconds: int = 3600,
) -> dict[str, Any]:
    now = (clock or (lambda: datetime.now(timezone.utc)))()
    video_id = _extract_video_id(candidate)
    try:
        if not video_id:
            row = _base_attempt_row(
                candidate,
                video_id=None,
                now=now,
                max_attempts=max_attempts,
                retry_cooldown_seconds=retry_cooldown_seconds,
            )
            row.update(
                {
                    "transcript_status": legacy_transcript_status(TRANSCRIPT_PERMANENT_ERROR),
                    "transcript_lifecycle_status": TRANSCRIPT_PERMANENT_ERROR,
                    "next_attempt_at": None,
                    "error_code": "missing_video_id",
                    "error_message": "A YouTube video ID could not be resolved.",
                }
            )
            return row
        attempt_count = int(candidate.get("existing_attempt_count") or 0) + 1
        result = fetcher(
            video_id,
            preferred_languages,
            attempt_count=attempt_count,
        )
        return _result_to_row(
            candidate,
            video_id,
            result,
            now=now,
            max_attempts=max_attempts,
            retry_cooldown_seconds=retry_cooldown_seconds,
        )
    except Exception as exc:
        status, error_code, error_message = _classify_unexpected_exception(exc)
        row = _base_attempt_row(
            candidate,
            video_id=video_id,
            now=now,
            max_attempts=max_attempts,
            retry_cooldown_seconds=retry_cooldown_seconds,
        )
        lifecycle_status = transcript_lifecycle_status(
            status,
            error_code=error_code,
            attempt_count=row["attempt_count"],
            max_attempts=max_attempts,
        )
        row.update(
            {
                "transcript_status": legacy_transcript_status(lifecycle_status),
                "transcript_lifecycle_status": lifecycle_status,
                "next_attempt_at": (
                    row["next_attempt_at"]
                    if lifecycle_status in RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES
                    else None
                ),
                "error_code": error_code,
                "error_message": error_message,
            }
        )
        return row
    finally:
        if sleep_seconds > 0:
            sleeper(sleep_seconds)


def _ensure_columns(spark: SparkSession, table: str, columns: Mapping[str, str]) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


def ensure_transcript_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(CREATE_TRANSCRIPTS_SQL)
    _ensure_columns(spark, TRANSCRIPT_TABLE, TRANSCRIPT_COLUMN_TYPES)
    spark.sql(
        f"""
        UPDATE {TRANSCRIPT_TABLE}
        SET transcript_lifecycle_status = COALESCE(
          transcript_lifecycle_status,
          CASE
            WHEN transcript_text IS NOT NULL AND LENGTH(TRIM(transcript_text)) > 0
              THEN 'available'
            WHEN LOWER(TRIM(transcript_status)) = 'success' THEN 'available'
            WHEN LOWER(TRIM(transcript_status)) IN ('not_available', 'not_found', 'age_restricted')
              THEN 'unavailable'
            WHEN LOWER(TRIM(transcript_status)) = 'disabled' THEN 'disabled'
            WHEN LOWER(TRIM(transcript_status)) = 'rate_limited' THEN 'rate_limited'
            WHEN LOWER(TRIM(transcript_status)) = 'ip_blocked' THEN 'blocked'
            WHEN LOWER(TRIM(transcript_status)) = 'permanent_error' THEN 'permanent_error'
            WHEN LOWER(TRIM(transcript_status)) = 'pending' THEN 'pending'
            ELSE 'retryable_error'
          END
        ),
        transcript_status = COALESCE(
          transcript_status,
          CASE
            WHEN transcript_text IS NOT NULL AND LENGTH(TRIM(transcript_text)) > 0
              THEN 'success'
            ELSE 'pending'
          END
        ),
        requested_language_code = COALESCE(
          requested_language_code,
          CASE
            WHEN LOWER(REPLACE(COALESCE(language, ''), '_', '-')) = 'vi'
              OR LOWER(REPLACE(COALESCE(language, ''), '_', '-')) LIKE 'vi-%'
              OR LOWER(COALESCE(language, '')) LIKE '%vietnam%'
              THEN 'vi'
            ELSE 'en'
          END
        ),
        requested_language = COALESCE(
          requested_language,
          CASE
            WHEN LOWER(REPLACE(COALESCE(language, ''), '_', '-')) = 'vi'
              OR LOWER(REPLACE(COALESCE(language, ''), '_', '-')) LIKE 'vi-%'
              OR LOWER(COALESCE(language, '')) LIKE '%vietnam%'
              THEN 'vi'
            ELSE 'en'
          END
        ),
        obtained_language = COALESCE(obtained_language, language),
        obtained_language_code = COALESCE(obtained_language_code, language_code),
        generation_type = COALESCE(
          generation_type,
          CASE
            WHEN is_generated = TRUE THEN 'automatic'
            WHEN is_generated = FALSE THEN 'manual'
          END
        ),
        provider = COALESCE(provider, transcript_source, 'youtube_transcript_api'),
        content_version = COALESCE(
          content_version,
          CASE
            WHEN transcript_text IS NOT NULL AND LENGTH(TRIM(transcript_text)) > 0
              THEN SHA2(CONCAT_WS(
                '\u001f', COALESCE(video_id, ''),
                LOWER(REPLACE(COALESCE(language_code, ''), '_', '-')),
                transcript_text
              ), 256)
          END
        ),
        attempt_count = COALESCE(attempt_count, 0),
        updated_at = COALESCE(updated_at, created_at)
        WHERE transcript_lifecycle_status IS NULL
           OR transcript_status IS NULL
           OR requested_language_code IS NULL
           OR requested_language IS NULL
           OR provider IS NULL
           OR (
             content_version IS NULL
             AND transcript_text IS NOT NULL
             AND LENGTH(TRIM(transcript_text)) > 0
           )
        """
    )


def _merge_transcript_dataframe(spark: SparkSession, dataframe: DataFrame) -> None:
    dataframe.select(*TRANSCRIPT_COLUMNS).createOrReplaceTempView("youtube_transcripts_upsert")
    spark.sql(
        f"""
        MERGE INTO {TRANSCRIPT_TABLE} AS t
        USING youtube_transcripts_upsert AS s
        ON t.content_id = s.content_id
          AND t.requested_language_code = s.requested_language_code
        WHEN MATCHED THEN UPDATE SET
          t.video_id = COALESCE(s.video_id, t.video_id),
          t.transcript_lifecycle_status = CASE
            WHEN s.transcript_lifecycle_status = 'available' THEN 'available'
            WHEN t.transcript_lifecycle_status IN (
              'available', 'unavailable', 'disabled', 'permanent_error'
            ) THEN t.transcript_lifecycle_status
            ELSE COALESCE(
              s.transcript_lifecycle_status,
              t.transcript_lifecycle_status
            )
          END,
          t.transcript_status = CASE
            WHEN s.transcript_status = 'success' THEN 'success'
            WHEN t.transcript_status IN ('success', 'not_available', 'disabled')
              THEN t.transcript_status
            ELSE COALESCE(s.transcript_status, t.transcript_status)
          END,
          t.requested_language = COALESCE(
            s.requested_language,
            t.requested_language
          ),
          t.requested_language_code = COALESCE(
            s.requested_language_code,
            t.requested_language_code
          ),
          t.obtained_language = COALESCE(
            s.obtained_language,
            t.obtained_language
          ),
          t.obtained_language_code = COALESCE(
            s.obtained_language_code,
            t.obtained_language_code
          ),
          t.language = COALESCE(s.language, t.language),
          t.language_code = COALESCE(s.language_code, t.language_code),
          t.transcript_text = COALESCE(s.transcript_text, t.transcript_text),
          t.segments_json = COALESCE(s.segments_json, t.segments_json),
          t.duration_seconds = COALESCE(s.duration_seconds, t.duration_seconds),
          t.covered_duration_seconds = COALESCE(
            s.covered_duration_seconds,
            t.covered_duration_seconds
          ),
          t.word_count = COALESCE(s.word_count, t.word_count),
          t.segment_count = COALESCE(s.segment_count, t.segment_count),
          t.available_languages_json = COALESCE(
            s.available_languages_json,
            t.available_languages_json
          ),
          t.has_auto_captions = COALESCE(s.has_auto_captions, t.has_auto_captions),
          t.is_generated = COALESCE(s.is_generated, t.is_generated),
          t.is_translated = COALESCE(s.is_translated, t.is_translated),
          t.generation_type = COALESCE(s.generation_type, t.generation_type),
          t.source_language = COALESCE(s.source_language, t.source_language),
          t.source_language_code = COALESCE(
            s.source_language_code,
            t.source_language_code
          ),
          t.transcript_source = COALESCE(s.transcript_source, t.transcript_source),
          t.provider = COALESCE(s.provider, t.provider),
          t.selection_strategy = COALESCE(s.selection_strategy, t.selection_strategy),
          t.error_code = CASE
            WHEN s.transcript_lifecycle_status = 'available' THEN NULL
            WHEN t.transcript_lifecycle_status = 'available' THEN t.error_code
            ELSE COALESCE(s.error_code, t.error_code)
          END,
          t.error_message = CASE
            WHEN s.transcript_lifecycle_status = 'available' THEN NULL
            WHEN t.transcript_lifecycle_status = 'available' THEN t.error_message
            ELSE COALESCE(s.error_message, t.error_message)
          END,
          t.attempt_count = GREATEST(
            COALESCE(s.attempt_count, 0),
            COALESCE(t.attempt_count, 0)
          ),
          t.last_attempt_at = COALESCE(s.last_attempt_at, t.last_attempt_at),
          t.next_attempt_at = s.next_attempt_at,
          t.collected_at = COALESCE(s.collected_at, t.collected_at),
          t.recovered_at = COALESCE(s.recovered_at, t.recovered_at),
          t.content_version = COALESCE(s.content_version, t.content_version),
          t.created_at = COALESCE(t.created_at, s.created_at),
          t.updated_at = COALESCE(s.updated_at, t.updated_at),
          t.event_date = COALESCE(t.event_date, s.event_date)
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(TRANSCRIPT_COLUMNS)})
          VALUES ({", ".join(f"s.{column}" for column in TRANSCRIPT_COLUMNS)})
        """
    )


def _merge_transcripts(spark: SparkSession, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    dataframe = spark.createDataFrame(
        [dict(row) for row in rows],
        schema=_transcript_schema(),
    )
    _merge_transcript_dataframe(spark, dataframe)


def _prepare_youtube_events(events: DataFrame) -> DataFrame:
    prepared = events
    for name, data_type in EVENT_OPTIONAL_COLUMNS.items():
        if name not in prepared.columns:
            prepared = prepared.withColumn(name, lit(None).cast(data_type))

    query_video_id = regexp_extract(col("url"), r"[?&]v=([^&]+)", 1)
    short_video_id = regexp_extract(col("url"), r"youtu\.be/([^?&#/]+)", 1)
    path_video_id = regexp_extract(
        col("url"),
        r"youtube\.com/(?:shorts|embed|live)/([^?&#/]+)",
        1,
    )
    derived_video_id = coalesce(
        col("conversation_id"),
        when(length(query_video_id) > 0, query_video_id),
        when(length(short_video_id) > 0, short_video_id),
        when(length(path_video_id) > 0, path_video_id),
        col("platform_event_id"),
    )
    derived_content_id = sha2(concat_ws(":", lit("youtube"), derived_video_id), 256)
    return (
        prepared.filter(lower(col("source")) == "youtube")
        .filter(
            (lower(coalesce(col("relation_type"), lit(""))) == "root")
            | (lower(coalesce(col("content_type"), lit(""))) == "youtube_video")
            | (
                col("relation_type").isNull()
                & col("content_type").isNull()
                & (
                    col("conversation_id").isNull()
                    | (col("conversation_id") == col("platform_event_id"))
                )
            )
        )
        .withColumn("resolved_video_id", derived_video_id)
        .withColumn("resolved_content_id", coalesce(col("content_id"), derived_content_id))
        .filter(col("resolved_content_id").isNotNull())
    )


def _embedded_transcript_dataframe(events: DataFrame) -> DataFrame:
    prepared = _prepare_youtube_events(events)
    transcript_text = when(
        length(trim(col("transcript_text"))) > 0,
        col("transcript_text"),
    ).otherwise(lit(None).cast("string"))
    event_status = lower(trim(col("transcript_status")))
    legacy_status = (
        when(transcript_text.isNotNull(), lit("success"))
        .when(event_status == "success", lit("partial"))
        .otherwise(event_status)
    )
    event_lifecycle = lower(trim(col("transcript_lifecycle_status")))
    lifecycle_status = (
        when(transcript_text.isNotNull(), lit("available"))
        .when(
            event_lifecycle.isin(
                *sorted(
                    TERMINAL_TRANSCRIPT_LIFECYCLE_STATUSES | RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES
                )
            ),
            event_lifecycle,
        )
        .when(event_status == "success", lit("retryable_error"))
        .when(event_status.isin("not_available", "not_found", "age_restricted"), lit("unavailable"))
        .when(event_status == "disabled", lit("disabled"))
        .when(event_status == "rate_limited", lit("rate_limited"))
        .when(event_status == "ip_blocked", lit("blocked"))
        .when(event_status == "permanent_error", lit("permanent_error"))
        .when(event_status == "pending", lit("pending"))
        .otherwise(lit("retryable_error"))
    )
    normalized_language = lower(
        regexp_extract(
            coalesce(
                col("transcript_requested_language_code"),
                col("language"),
                lit(""),
            ),
            r"^([A-Za-z]+)",
            1,
        )
    )
    requested_language_code = when(normalized_language == "vi", lit("vi")).otherwise(
        coalesce(col("transcript_requested_language_code"), lit("en"))
    )
    created_at = coalesce(col("event_ts"), current_timestamp())
    updated_at = coalesce(col("transcript_collected_at"), col("last_attempt_at"), created_at)
    embedded = (
        prepared.withColumn("embedded_transcript_text", transcript_text)
        .withColumn("embedded_transcript_status", legacy_status)
        .withColumn("embedded_transcript_lifecycle_status", lifecycle_status)
        .filter(
            col("embedded_transcript_text").isNotNull()
            | col("embedded_transcript_status").isin(*KNOWN_TRANSCRIPT_STATUSES)
            | col("embedded_transcript_lifecycle_status").isin(
                *sorted(
                    TERMINAL_TRANSCRIPT_LIFECYCLE_STATUSES | RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES
                )
            )
        )
        .select(
            col("resolved_video_id").alias("video_id"),
            col("resolved_content_id").alias("content_id"),
            col("embedded_transcript_status").alias("transcript_status"),
            col("embedded_transcript_lifecycle_status").alias("transcript_lifecycle_status"),
            coalesce(
                col("transcript_requested_language"),
                requested_language_code,
            ).alias("requested_language"),
            requested_language_code.alias("requested_language_code"),
            coalesce(
                col("transcript_obtained_language"),
                col("transcript_language"),
            ).alias("obtained_language"),
            coalesce(
                col("transcript_obtained_language_code"),
                col("transcript_language_code"),
            ).alias("obtained_language_code"),
            coalesce(col("transcript_language"), col("language")).alias("language"),
            col("transcript_language_code").alias("language_code"),
            col("embedded_transcript_text").alias("transcript_text"),
            col("transcript_segments_json").alias("segments_json"),
            col("duration_seconds"),
            col("transcript_covered_duration_seconds").alias("covered_duration_seconds"),
            when(
                col("embedded_transcript_text").isNotNull(),
                size(split(trim(col("embedded_transcript_text")), r"\s+")).cast("bigint"),
            )
            .otherwise(lit(None).cast("bigint"))
            .alias("word_count"),
            col("transcript_segment_count").cast("bigint").alias("segment_count"),
            coalesce(
                col("transcript_available_languages_json"),
                to_json(col("transcript_available_languages")),
            ).alias("available_languages_json"),
            coalesce(col("transcript_is_generated"), col("has_auto_captions")).alias(
                "has_auto_captions"
            ),
            col("transcript_is_generated").alias("is_generated"),
            col("transcript_is_translated").alias("is_translated"),
            col("transcript_generation_type").alias("generation_type"),
            col("transcript_source_language").alias("source_language"),
            col("transcript_source_language_code").alias("source_language_code"),
            col("transcript_source"),
            coalesce(
                col("transcript_provider"),
                col("transcript_source"),
                lit("youtube_transcript_api"),
            ).alias("provider"),
            col("transcript_selection_strategy").alias("selection_strategy"),
            col("transcript_error_code").alias("error_code"),
            col("transcript_error_message").alias("error_message"),
            coalesce(col("transcript_attempt_count"), col("attempt_count"), lit(1))
            .cast("bigint")
            .alias("attempt_count"),
            to_timestamp(coalesce(col("transcript_last_attempt_at"), updated_at)).alias(
                "last_attempt_at"
            ),
            to_timestamp(col("transcript_next_attempt_at")).alias("next_attempt_at"),
            to_timestamp(col("transcript_collected_at")).alias("collected_at"),
            to_timestamp(col("transcript_recovered_at")).alias("recovered_at"),
            col("transcript_content_version").alias("content_version"),
            created_at.alias("created_at"),
            to_timestamp(updated_at).alias("updated_at"),
            to_date(created_at).alias("event_date"),
        )
    )
    priority = when(col("transcript_lifecycle_status") == "available", lit(0)).otherwise(lit(1))
    window = Window.partitionBy("content_id", "requested_language_code").orderBy(
        priority.asc(),
        col("updated_at").desc_nulls_last(),
        col("video_id").asc_nulls_last(),
    )
    return (
        embedded.withColumn("transcript_row_number", row_number().over(window))
        .filter(col("transcript_row_number") == 1)
        .drop("transcript_row_number")
    )


def _external_candidates(
    events: DataFrame,
    transcripts: DataFrame,
    *,
    limit: int,
    max_attempts: int,
    retry_cooldown_seconds: int,
) -> DataFrame:
    prepared = _prepare_youtube_events(events)
    existing = transcripts.select(
        "content_id",
        "requested_language_code",
        col("transcript_status").alias("existing_transcript_status"),
        col("transcript_lifecycle_status").alias("existing_transcript_lifecycle_status"),
        col("transcript_text").alias("existing_transcript_text"),
        col("attempt_count").alias("existing_attempt_count"),
        col("last_attempt_at").alias("existing_last_attempt_at"),
    )
    selected_events = prepared.select(
        col("resolved_content_id").alias("content_id"),
        col("resolved_video_id").alias("root_content_id"),
        "conversation_id",
        "platform_event_id",
        "url",
        "language",
        "duration_seconds",
        col("event_ts").alias("created_at"),
    )
    selected_events = selected_events.withColumn(
        "requested_language_code",
        when(
            lower(regexp_extract(coalesce(col("language"), lit("")), r"^([A-Za-z]+)", 1)) == "vi",
            lit("vi"),
        ).otherwise(lit("en")),
    ).withColumn("requested_language", col("requested_language_code"))
    event_window = Window.partitionBy("content_id").orderBy(
        col("created_at").desc_nulls_last(),
        col("platform_event_id").asc_nulls_last(),
        col("url").asc_nulls_last(),
    )
    selected_events = (
        selected_events.withColumn("candidate_row_number", row_number().over(event_window))
        .filter(col("candidate_row_number") == 1)
        .drop("candidate_row_number")
    )
    candidates = (
        selected_events.join(existing, ["content_id", "requested_language_code"], "left")
        .withColumn(
            "effective_transcript_lifecycle_status",
            when(
                length(trim(col("existing_transcript_text"))) > 0,
                lit("available"),
            ).otherwise(
                coalesce(
                    lower(trim(col("existing_transcript_lifecycle_status"))),
                    when(
                        lower(trim(col("existing_transcript_status"))) == "success",
                        lit("available"),
                    )
                    .when(
                        lower(trim(col("existing_transcript_status"))).isin(
                            "not_available", "not_found", "age_restricted"
                        ),
                        lit("unavailable"),
                    )
                    .when(
                        lower(trim(col("existing_transcript_status"))) == "disabled",
                        lit("disabled"),
                    )
                    .otherwise(lit("pending")),
                )
            ),
        )
        .withColumn(
            "existing_attempt_count",
            coalesce(col("existing_attempt_count"), lit(0)).cast("bigint"),
        )
    )
    cooldown_elapsed = col("existing_last_attempt_at").isNull() | (
        unix_timestamp(current_timestamp()) - unix_timestamp(col("existing_last_attempt_at"))
        >= lit(retry_cooldown_seconds)
    )
    return (
        candidates.filter(
            col("effective_transcript_lifecycle_status").isin(
                *RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES
            )
            & (col("existing_attempt_count") < lit(max_attempts))
            & cooldown_elapsed
        )
        .orderBy(
            col("existing_attempt_count").asc(),
            col("existing_last_attempt_at").asc_nulls_first(),
            col("content_id").asc(),
        )
        .limit(limit)
    )


def main() -> None:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    warehouse = f"s3a://{bucket}/warehouse"
    limit = max(1, int(_env("YOUTUBE_TRANSCRIPT_BACKFILL_LIMIT", "500")))
    sleep_seconds = max(
        0.0,
        float(_env("YOUTUBE_TRANSCRIPT_BACKFILL_SLEEP_SECONDS", "0.25")),
    )
    max_attempts = max(
        1,
        int(_env("YOUTUBE_TRANSCRIPT_BACKFILL_MAX_ATTEMPTS", "5")),
    )
    retry_cooldown_seconds = max(
        0,
        int(_env("YOUTUBE_TRANSCRIPT_BACKFILL_RETRY_COOLDOWN_SECONDS", "3600")),
    )
    stop_on_rate_limit = _env_bool(
        "YOUTUBE_TRANSCRIPT_BACKFILL_STOP_ON_RATE_LIMIT",
        True,
    )
    fail_on_retryable = _env_bool(
        "YOUTUBE_TRANSCRIPT_BACKFILL_FAIL_ON_RETRYABLE",
        False,
    )

    spark = _build_spark("youtube-transcript-backfill", warehouse)
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_transcript_table(spark)
        events = spark.table("lakehouse.silver.events")

        embedded = _embedded_transcript_dataframe(events)
        embedded_count = embedded.count()
        if embedded_count:
            _merge_transcript_dataframe(spark, embedded)

        candidates = _external_candidates(
            events,
            spark.table(TRANSCRIPT_TABLE),
            limit=limit,
            max_attempts=max_attempts,
            retry_cooldown_seconds=retry_cooldown_seconds,
        ).collect()

        rows: list[dict[str, Any]] = []
        outcomes: dict[str, int] = {}
        for candidate in candidates:
            row = _attempt_transcript_row(
                candidate.asDict(recursive=True),
                _preferred_languages_for_candidate(candidate.asDict(recursive=True)),
                sleep_seconds,
                max_attempts=max_attempts,
                retry_cooldown_seconds=retry_cooldown_seconds,
            )
            rows.append(row)
            status = row["transcript_lifecycle_status"]
            outcomes[status] = outcomes.get(status, 0) + 1
            if status in {"rate_limited", "blocked"} and stop_on_rate_limit:
                break

        _merge_transcripts(spark, rows)
        print(
            "YouTube transcript backfill: "
            f"embedded={embedded_count}, selected={len(candidates)}, "
            f"attempted={len(rows)}, outcomes={outcomes}"
        )
        retryable_count = sum(
            count
            for status, count in outcomes.items()
            if status in RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES
        )
        if retryable_count and fail_on_retryable:
            raise RuntimeError(
                "Transcript attempts produced retryable outcomes; details were persisted."
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
