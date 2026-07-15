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
from datetime import datetime, timezone
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


TRANSCRIPT_TABLE = "lakehouse.silver.transcripts"
TERMINAL_TRANSCRIPT_STATUSES = frozenset({"success", "not_available", "disabled"})
RETRYABLE_TRANSCRIPT_STATUSES = frozenset(
    {"pending", "partial", "rate_limited", "failed"}
)
KNOWN_TRANSCRIPT_STATUSES = TERMINAL_TRANSCRIPT_STATUSES | RETRYABLE_TRANSCRIPT_STATUSES

CREATE_TRANSCRIPTS_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.transcripts (
  video_id STRING,
  content_id STRING,
  transcript_status STRING,
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
  source_language STRING,
  source_language_code STRING,
  transcript_source STRING,
  selection_strategy STRING,
  error_code STRING,
  error_message STRING,
  attempt_count BIGINT,
  last_attempt_at TIMESTAMP,
  collected_at TIMESTAMP,
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
    "source_language": "STRING",
    "source_language_code": "STRING",
    "transcript_source": "STRING",
    "selection_strategy": "STRING",
    "error_code": "STRING",
    "error_message": "STRING",
    "attempt_count": "BIGINT",
    "last_attempt_at": "TIMESTAMP",
    "collected_at": "TIMESTAMP",
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
    "transcript_text": "STRING",
    "transcript_segments_json": "STRING",
    "transcript_language": "STRING",
    "transcript_language_code": "STRING",
    "transcript_is_generated": "BOOLEAN",
    "transcript_is_translated": "BOOLEAN",
    "transcript_source_language": "STRING",
    "transcript_source_language_code": "STRING",
    "transcript_source": "STRING",
    "transcript_selection_strategy": "STRING",
    "transcript_segment_count": "BIGINT",
    "transcript_available_languages": "ARRAY<STRING>",
    "transcript_covered_duration_seconds": "DOUBLE",
    "transcript_collected_at": "TIMESTAMP",
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
        raise RuntimeError("PySpark is required to execute the transcript backfill") from PYSPARK_IMPORT_ERROR


def _transcript_schema():
    _require_pyspark()
    return StructType(
        [
            StructField("video_id", StringType(), True),
            StructField("content_id", StringType(), False),
            StructField("transcript_status", StringType(), False),
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
            StructField("source_language", StringType(), True),
            StructField("source_language_code", StringType(), True),
            StructField("transcript_source", StringType(), True),
            StructField("selection_strategy", StringType(), True),
            StructField("error_code", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("attempt_count", LongType(), False),
            StructField("last_attempt_at", TimestampType(), True),
            StructField("collected_at", TimestampType(), True),
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
    if is_dataclass(value):
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
        attempt_count=attempt_count,
    )


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
    if normalized in {"ipblocked", "requestblocked", "toomanyrequests"} or "429" in str(
        exc
    ):
        return "rate_limited", error_code, error_message
    return "failed", error_code, error_message


def _base_attempt_row(
    candidate: Mapping[str, Any],
    *,
    video_id: str | None,
    now: datetime,
) -> dict[str, Any]:
    created_at = _as_datetime(candidate.get("created_at"), now) or now
    return {
        "video_id": video_id,
        "content_id": str(candidate["content_id"]),
        "transcript_status": "failed",
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
        "source_language": None,
        "source_language_code": None,
        "transcript_source": "youtube_transcript_api",
        "selection_strategy": None,
        "error_code": None,
        "error_message": None,
        "attempt_count": int(candidate.get("existing_attempt_count") or 0) + 1,
        "last_attempt_at": now,
        "collected_at": None,
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
) -> dict[str, Any]:
    row = _base_attempt_row(candidate, video_id=video_id, now=now)
    result_map = _as_mapping(result)
    payload = _as_mapping(result_map.get("payload", getattr(result, "payload", None)))
    transcript_text = payload.get("text")
    if transcript_text is not None:
        transcript_text = str(transcript_text).strip() or None
    status = _normalize_status(
        result_map.get("status", getattr(result, "status", None)),
        has_text=bool(transcript_text),
    )
    error_code = result_map.get("error_code", getattr(result, "error_code", None))
    error_message = result_map.get(
        "error_message",
        getattr(result, "error_message", None),
    )
    if status == "success" and not transcript_text:
        status = "partial"
        error_code = error_code or "empty_transcript"
        error_message = error_message or "The selected transcript contained no text."

    segments = payload.get("segments")
    available_languages = payload.get("available_languages")
    collected_at = _as_datetime(payload.get("collected_at"))
    completed_at = _as_datetime(
        result_map.get("completed_at", getattr(result, "completed_at", None)),
        now,
    )
    is_generated = payload.get("is_generated")

    row.update(
        {
            "transcript_status": status,
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
            "source_language": payload.get("source_language"),
            "source_language_code": payload.get("source_language_code"),
            "transcript_source": payload.get("source") or "youtube_transcript_api",
            "selection_strategy": payload.get("selection_strategy"),
            "error_code": error_code,
            "error_message": error_message,
            "last_attempt_at": completed_at,
            "collected_at": collected_at if status in {"success", "partial"} else None,
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
) -> dict[str, Any]:
    now = (clock or (lambda: datetime.now(timezone.utc)))()
    video_id = _extract_video_id(candidate)
    try:
        if not video_id:
            row = _base_attempt_row(candidate, video_id=None, now=now)
            row.update(
                {
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
        return _result_to_row(candidate, video_id, result, now=now)
    except Exception as exc:
        status, error_code, error_message = _classify_unexpected_exception(exc)
        row = _base_attempt_row(candidate, video_id=video_id, now=now)
        row.update(
            {
                "transcript_status": status,
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


def _create_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(CREATE_TRANSCRIPTS_SQL)
    _ensure_columns(spark, TRANSCRIPT_TABLE, TRANSCRIPT_COLUMN_TYPES)
    spark.sql(
        f"""
        UPDATE {TRANSCRIPT_TABLE}
        SET transcript_status = CASE
          WHEN transcript_text IS NOT NULL AND LENGTH(TRIM(transcript_text)) > 0
            THEN 'success'
          ELSE 'pending'
        END,
        attempt_count = COALESCE(attempt_count, 0),
        updated_at = COALESCE(updated_at, created_at)
        WHERE transcript_status IS NULL
        """
    )


def _merge_transcript_dataframe(spark: SparkSession, dataframe: DataFrame) -> None:
    dataframe.select(*TRANSCRIPT_COLUMNS).createOrReplaceTempView(
        "youtube_transcripts_upsert"
    )
    spark.sql(
        f"""
        MERGE INTO {TRANSCRIPT_TABLE} AS t
        USING youtube_transcripts_upsert AS s
        ON t.content_id = s.content_id
        WHEN MATCHED THEN UPDATE SET
          t.video_id = COALESCE(s.video_id, t.video_id),
          t.transcript_status = CASE
            WHEN s.transcript_status = 'success' THEN 'success'
            WHEN t.transcript_status IN ('success', 'not_available', 'disabled')
              THEN t.transcript_status
            ELSE COALESCE(s.transcript_status, t.transcript_status)
          END,
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
          t.source_language = COALESCE(s.source_language, t.source_language),
          t.source_language_code = COALESCE(
            s.source_language_code,
            t.source_language_code
          ),
          t.transcript_source = COALESCE(s.transcript_source, t.transcript_source),
          t.selection_strategy = COALESCE(s.selection_strategy, t.selection_strategy),
          t.error_code = CASE
            WHEN s.transcript_status = 'success' THEN NULL
            WHEN t.transcript_status = 'success' THEN t.error_code
            ELSE COALESCE(s.error_code, t.error_code)
          END,
          t.error_message = CASE
            WHEN s.transcript_status = 'success' THEN NULL
            WHEN t.transcript_status = 'success' THEN t.error_message
            ELSE COALESCE(s.error_message, t.error_message)
          END,
          t.attempt_count = GREATEST(
            COALESCE(s.attempt_count, 0),
            COALESCE(t.attempt_count, 0)
          ),
          t.last_attempt_at = COALESCE(s.last_attempt_at, t.last_attempt_at),
          t.collected_at = COALESCE(s.collected_at, t.collected_at),
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
    status = (
        when(transcript_text.isNotNull(), lit("success"))
        .when(event_status == "success", lit("partial"))
        .otherwise(event_status)
    )
    created_at = coalesce(col("event_ts"), current_timestamp())
    updated_at = coalesce(col("transcript_collected_at"), col("last_attempt_at"), created_at)
    embedded = (
        prepared.withColumn("embedded_transcript_text", transcript_text)
        .withColumn("embedded_transcript_status", status)
        .filter(
            col("embedded_transcript_text").isNotNull()
            | col("embedded_transcript_status").isin(*KNOWN_TRANSCRIPT_STATUSES)
        )
        .select(
            col("resolved_video_id").alias("video_id"),
            col("resolved_content_id").alias("content_id"),
            col("embedded_transcript_status").alias("transcript_status"),
            coalesce(col("transcript_language"), col("language")).alias("language"),
            col("transcript_language_code").alias("language_code"),
            col("embedded_transcript_text").alias("transcript_text"),
            col("transcript_segments_json").alias("segments_json"),
            col("duration_seconds"),
            col("transcript_covered_duration_seconds").alias(
                "covered_duration_seconds"
            ),
            when(
                col("embedded_transcript_text").isNotNull(),
                size(split(trim(col("embedded_transcript_text")), r"\s+")).cast(
                    "bigint"
                ),
            )
            .otherwise(lit(None).cast("bigint"))
            .alias("word_count"),
            col("transcript_segment_count").cast("bigint").alias("segment_count"),
            to_json(col("transcript_available_languages")).alias(
                "available_languages_json"
            ),
            coalesce(col("transcript_is_generated"), col("has_auto_captions")).alias(
                "has_auto_captions"
            ),
            col("transcript_is_generated").alias("is_generated"),
            col("transcript_is_translated").alias("is_translated"),
            col("transcript_source_language").alias("source_language"),
            col("transcript_source_language_code").alias("source_language_code"),
            col("transcript_source"),
            col("transcript_selection_strategy").alias("selection_strategy"),
            col("transcript_error_code").alias("error_code"),
            col("transcript_error_message").alias("error_message"),
            coalesce(col("attempt_count"), lit(1)).cast("bigint").alias("attempt_count"),
            to_timestamp(updated_at).alias("last_attempt_at"),
            to_timestamp(col("transcript_collected_at")).alias("collected_at"),
            created_at.alias("created_at"),
            to_timestamp(updated_at).alias("updated_at"),
            to_date(created_at).alias("event_date"),
        )
    )
    priority = when(col("transcript_status") == "success", lit(0)).otherwise(lit(1))
    window = Window.partitionBy("content_id").orderBy(
        priority.asc(),
        col("updated_at").desc_nulls_last(),
        col("video_id").asc_nulls_last(),
    )
    return embedded.withColumn("transcript_row_number", row_number().over(window)).filter(
        col("transcript_row_number") == 1
    ).drop("transcript_row_number")


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
        col("transcript_status").alias("existing_transcript_status"),
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
        selected_events
        .join(existing, "content_id", "left")
        .withColumn(
            "effective_transcript_status",
            when(
                length(trim(col("existing_transcript_text"))) > 0,
                lit("success"),
            ).otherwise(
                coalesce(lower(trim(col("existing_transcript_status"))), lit("pending"))
            ),
        )
        .withColumn(
            "existing_attempt_count",
            coalesce(col("existing_attempt_count"), lit(0)).cast("bigint"),
        )
    )
    cooldown_elapsed = (
        col("existing_last_attempt_at").isNull()
        | (
            unix_timestamp(current_timestamp())
            - unix_timestamp(col("existing_last_attempt_at"))
            >= lit(retry_cooldown_seconds)
        )
    )
    return (
        candidates.filter(
            col("effective_transcript_status").isin(*RETRYABLE_TRANSCRIPT_STATUSES)
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
    languages = tuple(
        value.strip()
        for value in _env("YOUTUBE_TRANSCRIPT_LANGUAGES", "en,vi").split(",")
        if value.strip()
    ) or ("en",)
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
        _create_tables(spark)
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
                languages,
                sleep_seconds,
            )
            rows.append(row)
            status = row["transcript_status"]
            outcomes[status] = outcomes.get(status, 0) + 1
            if status == "rate_limited" and stop_on_rate_limit:
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
            if status in RETRYABLE_TRANSCRIPT_STATUSES
        )
        if retryable_count and fail_on_retryable:
            raise RuntimeError(
                "Transcript attempts produced retryable outcomes; details were persisted."
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
