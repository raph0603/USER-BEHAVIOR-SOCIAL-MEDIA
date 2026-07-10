"""
Backfill YouTube transcripts for videos already present in Silver events.

This job uses ``youtube-transcript-api`` directly, so it does not consume
YouTube Data API search quota. It materializes transcript rows into
``lakehouse.silver.transcripts`` and can be run before content analytics.
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
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
from youtube_transcript_api import YouTubeTranscriptApi


TRANSCRIPT_TABLE = "lakehouse.silver.transcripts"
CREATE_TRANSCRIPTS_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.transcripts (
  video_id STRING,
  content_id STRING,
  language STRING,
  transcript_text STRING,
  segments_json STRING,
  duration_seconds DOUBLE,
  word_count BIGINT,
  has_auto_captions BOOLEAN,
  created_at TIMESTAMP,
  event_date DATE
)
USING iceberg
PARTITIONED BY (event_date)
"""
TRANSCRIPT_COLUMNS = [
    "video_id",
    "content_id",
    "language",
    "transcript_text",
    "segments_json",
    "duration_seconds",
    "word_count",
    "has_auto_captions",
    "created_at",
    "event_date",
]
TRANSCRIPT_SCHEMA = StructType(
    [
        StructField("video_id", StringType(), False),
        StructField("content_id", StringType(), False),
        StructField("language", StringType(), True),
        StructField("transcript_text", StringType(), True),
        StructField("segments_json", StringType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("word_count", LongType(), True),
        StructField("has_auto_captions", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("event_date", DateType(), True),
    ]
)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _build_spark(app_name: str, warehouse: str) -> SparkSession:
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
    if parsed.hostname in {"youtu.be", "www.youtu.be"}:
        return parsed.path.strip("/").split("/", 1)[0] or None
    return parse_qs(parsed.query).get("v", [None])[0]


def _extract_video_id(row) -> str | None:
    from_url = _video_id_from_url(row["url"])
    if from_url:
        return from_url
    platform_id = row["conversation_id"] or row["platform_event_id"]
    if platform_id and re.fullmatch(r"[A-Za-z0-9_-]{6,}", platform_id):
        return platform_id
    return None


def _fetch_transcript(video_id: str, languages: list[str]):
    transcript = YouTubeTranscriptApi().fetch(video_id, languages=languages)
    segments = []
    for item in transcript:
        if isinstance(item, dict):
            segment = dict(item)
        else:
            segment = {
                "text": getattr(item, "text", str(item)),
                "start": getattr(item, "start", 0.0),
                "duration": getattr(item, "duration", 0.0),
            }
        if str(segment.get("text", "")).strip():
            segments.append(segment)
    return segments


def _language_from_segments(segments: list[dict], fallback: str | None) -> str | None:
    for segment in segments:
        language = segment.get("language") or segment.get("language_code")
        if language:
            return str(language)
    return fallback


def _transcript_row(row, languages: list[str], sleep_seconds: float):
    video_id = _extract_video_id(row)
    if not video_id:
        return None, "missing_video_id"
    try:
        segments = _fetch_transcript(video_id, languages)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    except Exception as exc:
        return None, type(exc).__name__

    transcript_text = " ".join(
        str(segment.get("text", "")).strip()
        for segment in segments
        if str(segment.get("text", "")).strip()
    )
    if not transcript_text:
        return None, "empty_transcript"

    created_at = row["created_at"] or datetime.now(timezone.utc)
    return (
        {
            "video_id": video_id,
            "content_id": row["content_id"],
            "language": _language_from_segments(segments, row["language"]),
            "transcript_text": transcript_text,
            "segments_json": json.dumps(segments, ensure_ascii=False),
            "duration_seconds": None,
            "word_count": len(transcript_text.split()),
            "has_auto_captions": None,
            "created_at": created_at,
            "event_date": created_at.date(),
        },
        None,
    )


def _ensure_columns(spark: SparkSession, table: str, columns: dict[str, str]) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


def _create_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(CREATE_TRANSCRIPTS_SQL)
    _ensure_columns(
        spark,
        TRANSCRIPT_TABLE,
        {
            "segments_json": "STRING",
            "duration_seconds": "DOUBLE",
            "has_auto_captions": "BOOLEAN",
        },
    )


def _merge_transcripts(spark: SparkSession, rows: list[dict]) -> None:
    if not rows:
        return

    dataframe = spark.createDataFrame(rows, schema=TRANSCRIPT_SCHEMA)
    dataframe.select(*TRANSCRIPT_COLUMNS).createOrReplaceTempView(
        "youtube_transcripts_upsert"
    )
    spark.sql(
        f"""
        MERGE INTO {TRANSCRIPT_TABLE} AS t
        USING youtube_transcripts_upsert AS s
        ON t.video_id = s.video_id AND t.content_id = s.content_id
        WHEN MATCHED THEN UPDATE SET
          t.language = COALESCE(s.language, t.language),
          t.transcript_text = COALESCE(s.transcript_text, t.transcript_text),
          t.segments_json = COALESCE(s.segments_json, t.segments_json),
          t.duration_seconds = COALESCE(s.duration_seconds, t.duration_seconds),
          t.word_count = COALESCE(s.word_count, t.word_count),
          t.has_auto_captions = COALESCE(s.has_auto_captions, t.has_auto_captions),
          t.created_at = COALESCE(s.created_at, t.created_at),
          t.event_date = COALESCE(s.event_date, t.event_date)
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(TRANSCRIPT_COLUMNS)})
          VALUES ({", ".join(f"s.{column}" for column in TRANSCRIPT_COLUMNS)})
        """
    )


def main() -> None:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    warehouse = f"s3a://{bucket}/warehouse"
    languages = [
        value.strip()
        for value in _env("YOUTUBE_TRANSCRIPT_LANGUAGES", "en,vi").split(",")
        if value.strip()
    ]
    limit = int(_env("YOUTUBE_TRANSCRIPT_BACKFILL_LIMIT", "500"))
    sleep_seconds = float(_env("YOUTUBE_TRANSCRIPT_BACKFILL_SLEEP_SECONDS", "0.25"))

    spark = _build_spark("youtube-transcript-backfill", warehouse)
    spark.sparkContext.setLogLevel("WARN")
    _create_tables(spark)

    events = spark.table("lakehouse.silver.events")
    if "conversation_id" not in events.columns:
        events = events.withColumn("conversation_id", lit(None).cast("string"))
    if "language" not in events.columns:
        events = events.withColumn("language", lit(None).cast("string"))

    existing = spark.table(TRANSCRIPT_TABLE).select("content_id").distinct()
    candidates = (
        events
        .filter("source = 'youtube'")
        .selectExpr(
            """
            sha2(
              concat_ws(
                ':',
                source,
                coalesce(
                  conversation_id,
                  CASE
                    WHEN regexp_extract(url, '[?&]v=([^&]+)', 1) != ''
                    THEN regexp_extract(url, '[?&]v=([^&]+)', 1)
                    ELSE NULL
                  END,
                  platform_event_id,
                  url
                )
              ),
              256
            ) AS content_id
            """,
            "conversation_id",
            "platform_event_id",
            "url",
            "language",
            "event_ts AS created_at",
        )
        .dropDuplicates(["content_id"])
        .join(existing, "content_id", "left_anti")
        .limit(limit)
        .collect()
    )

    rows = []
    failures: dict[str, int] = {}
    for candidate in candidates:
        row, reason = _transcript_row(candidate.asDict(), languages, sleep_seconds)
        if row is None:
            failures[reason or "unknown"] = failures.get(reason or "unknown", 0) + 1
        else:
            rows.append(row)

    _merge_transcripts(spark, rows)
    print(
        "YouTube transcript backfill: "
        f"candidates={len(candidates)}, inserted_or_updated={len(rows)}, "
        f"failures={failures}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
