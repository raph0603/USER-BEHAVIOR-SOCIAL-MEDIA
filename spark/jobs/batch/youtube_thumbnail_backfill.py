"""Backfill YouTube low-resolution thumbnail URLs without using API quota."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

PYSPARK_IMPORT_ERROR: ModuleNotFoundError | None = None
try:
    from pyspark.sql import DataFrame, SparkSession, Window
    from pyspark.sql.functions import (
        col,
        coalesce,
        concat,
        concat_ws,
        current_timestamp,
        length,
        lit,
        lower,
        regexp_extract,
        row_number,
        sha2,
        trim,
        when,
    )
except ModuleNotFoundError as exc:
    if exc.name != "pyspark" and not str(exc.name or "").startswith("pyspark."):
        raise
    PYSPARK_IMPORT_ERROR = exc
    DataFrame = Any
    SparkSession = Any
    Window = None


JOBS_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
for import_root in (JOBS_DIR, REPOSITORY_ROOT):
    import_path = str(import_root)
    if import_path not in sys.path:
        sys.path.insert(0, import_path)


EVENT_TABLE = "lakehouse.silver.events"


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _require_pyspark() -> None:
    if PYSPARK_IMPORT_ERROR is not None:
        raise RuntimeError("PySpark is required to execute thumbnail backfill") from (
            PYSPARK_IMPORT_ERROR
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


def low_resolution_thumbnail_url(video_id: str) -> str:
    """Return YouTube's public low-resolution thumbnail URL for a video id."""

    return f"https://img.youtube.com/vi/{video_id}/default.jpg"


def _ensure_columns(spark: SparkSession) -> None:
    current_columns = set(spark.table(EVENT_TABLE).columns)
    if "thumbnail_url" not in current_columns:
        spark.sql(f"ALTER TABLE {EVENT_TABLE} ADD COLUMN thumbnail_url STRING")
    if "updated_at" not in current_columns:
        spark.sql(f"ALTER TABLE {EVENT_TABLE} ADD COLUMN updated_at STRING")


def _prepare_youtube_events(events: DataFrame) -> DataFrame:
    prepared = events
    for name, data_type in {
        "content_id": "STRING",
        "root_content_id": "STRING",
        "content_type": "STRING",
        "relation_type": "STRING",
        "conversation_id": "STRING",
        "platform_event_id": "STRING",
        "url": "STRING",
        "thumbnail_url": "STRING",
        "event_ts": "TIMESTAMP",
    }.items():
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


def _thumbnail_candidates(events: DataFrame, *, limit: int) -> DataFrame:
    prepared = _prepare_youtube_events(events)
    candidates = (
        prepared.filter(
            col("thumbnail_url").isNull()
            | (length(trim(col("thumbnail_url"))) == 0)
        )
        .filter(length(trim(col("resolved_video_id"))) > 0)
        .select(
            col("resolved_content_id").alias("content_id"),
            col("resolved_video_id").alias("video_id"),
            "event_ts",
        )
    )
    window = Window.partitionBy("content_id").orderBy(
        col("event_ts").desc_nulls_last(),
        col("video_id").asc(),
    )
    return (
        candidates.withColumn("candidate_row_number", row_number().over(window))
        .filter(col("candidate_row_number") == 1)
        .drop("candidate_row_number")
        .orderBy(col("content_id").asc())
        .limit(limit)
    )


def _merge_thumbnail_dataframe(spark: SparkSession, dataframe: DataFrame) -> None:
    thumbnail_rows = dataframe.select(
        "content_id",
        concat(
            lit("https://img.youtube.com/vi/"),
            col("video_id"),
            lit("/default.jpg"),
        ).alias("thumbnail_url"),
        current_timestamp().alias("updated_at"),
    )
    thumbnail_rows.createOrReplaceTempView("youtube_thumbnail_upsert")
    spark.sql(
        f"""
        MERGE INTO {EVENT_TABLE} AS t
        USING youtube_thumbnail_upsert AS s
        ON t.source = 'youtube' AND t.content_id = s.content_id
        WHEN MATCHED THEN UPDATE SET
          t.thumbnail_url = CASE
            WHEN t.thumbnail_url IS NULL OR LENGTH(TRIM(t.thumbnail_url)) = 0
              THEN s.thumbnail_url
            ELSE t.thumbnail_url
          END,
          t.updated_at = CASE
            WHEN t.thumbnail_url IS NULL OR LENGTH(TRIM(t.thumbnail_url)) = 0
              THEN CAST(s.updated_at AS STRING)
            ELSE t.updated_at
          END
        """
    )


def main() -> None:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    warehouse = f"s3a://{bucket}/warehouse"
    limit = max(1, int(_env("YOUTUBE_THUMBNAIL_BACKFILL_LIMIT", "5000")))

    spark = _build_spark("youtube-thumbnail-backfill", warehouse)
    spark.sparkContext.setLogLevel("WARN")
    try:
        _ensure_columns(spark)
        candidates = _thumbnail_candidates(spark.table(EVENT_TABLE), limit=limit)
        selected_count = candidates.count()
        if selected_count:
            _merge_thumbnail_dataframe(spark, candidates)
        print(
            "YouTube thumbnail backfill: "
            f"selected={selected_count}, updated={selected_count}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
