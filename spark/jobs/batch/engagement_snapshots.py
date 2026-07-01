"""
Spark batch job: append engagement observations to
``lakehouse.silver.engagement_snapshots``.

Design intent
-------------
The engagement refresh flow (``apply_insight_updates.py``) updates the
*latest* engagement value on ``silver.events`` (MERGE / UPDATE).  This
job appends a **snapshot row** every time engagement is refreshed so that
time-horizon labels (T+1h, T+6h, T+24h, …) can be built later for
retraining.

The table is **append-only** — no row is ever modified after insertion.
The ``observed_at`` + ``platform_event_id`` / ``(source, url)`` triplet
uniquely identifies an observation.

Run modes
---------
PROCESSING_MODE=availableNow   – one-shot (Airflow / CI)
PROCESSING_MODE=continuous      – streaming micro-batch
"""

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    to_date,
    to_timestamp,
    unix_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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


# ---------------------------------------------------------------------------
# Schema of the insight-refresh JSONL files produced by insight_refresh.py
# (mirrored from apply_insight_updates.py)
# ---------------------------------------------------------------------------

INSIGHT_REFRESH_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("url", StringType(), False),
        StructField("event_ts", StringType(), False),
        StructField("source", StringType(), False),
        StructField("platform_event_id", StringType(), True),
        StructField("metadata_refreshed_at", StringType(), True),
        StructField("owner_channel_id", StringType(), True),
        StructField("collaborator_channel_ids", ArrayType(StringType()), True),
        StructField("like_count", LongType(), True),
        StructField("view_count", LongType(), True),
        StructField("comment_count", LongType(), True),
        StructField("reply_count", LongType(), True),
        StructField("retweet_count", LongType(), True),
        StructField("bookmark_count", LongType(), True),
        StructField("score", LongType(), True),
        StructField("follower_count", LongType(), True),
        StructField("subscriber_count", LongType(), True),
        StructField("subreddit_member_count", LongType(), True),
    ]
)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.engagement_snapshots (
  source               STRING    COMMENT 'Origin platform: youtube, x, reddit, playwright',
  platform_event_id    STRING    COMMENT 'Platform-native stable identifier',
  user_id              STRING    COMMENT 'Internal session/user identifier',
  url                  STRING    COMMENT 'Canonical URL of the post',
  created_at           TIMESTAMP COMMENT 'Original event creation timestamp',
  observed_at          TIMESTAMP COMMENT 'Timestamp when this engagement snapshot was taken',
  age_minutes          BIGINT    COMMENT 'Age of the post in minutes at observation time',
  like_count           BIGINT    COMMENT 'Like count at observation time',
  view_count           BIGINT    COMMENT 'View count at observation time',
  comment_count        BIGINT    COMMENT 'Comment count at observation time',
  reply_count          BIGINT    COMMENT 'Reply count at observation time',
  retweet_count        BIGINT    COMMENT 'Retweet / repost count at observation time',
  bookmark_count       BIGINT    COMMENT 'Bookmark count at observation time',
  score                BIGINT    COMMENT 'Reddit score (upvotes - downvotes) at observation time',
  snapshot_date        DATE      COMMENT 'Partition column derived from observed_at'
)
USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  'write.metadata.delete-after-commit.enabled' = 'false'
)
"""

_SNAPSHOT_COLUMNS = [
    "source",
    "platform_event_id",
    "user_id",
    "url",
    "created_at",
    "observed_at",
    "age_minutes",
    "like_count",
    "view_count",
    "comment_count",
    "reply_count",
    "retweet_count",
    "bookmark_count",
    "score",
    "snapshot_date",
]


# ---------------------------------------------------------------------------
# Snapshot derivation
# ---------------------------------------------------------------------------


def build_snapshots_from_updates(spark: SparkSession, updates: list[dict]):
    """
    Convert a list of insight-refresh dicts into a DataFrame of snapshot rows.

    Each update dict is produced by ``playwright/insight_refresh.py`` and
    contains the latest engagement values plus ``metadata_refreshed_at``.
    """
    if not updates:
        return None

    df = spark.createDataFrame(updates, schema=INSIGHT_REFRESH_SCHEMA)

    df = (
        df.withColumn("created_at", to_timestamp(col("event_ts")))
        .withColumn("observed_at", to_timestamp(col("metadata_refreshed_at")))
        .withColumn(
            "age_minutes",
            (
                (unix_timestamp(col("observed_at")) - unix_timestamp(col("created_at")))
                / 60
            ).cast("bigint"),
        )
        .withColumn("snapshot_date", to_date(col("observed_at")))
    )

    return df.select(*_SNAPSHOT_COLUMNS)


def _ensure_columns(spark: SparkSession, table: str, columns: dict) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    import json
    from pathlib import Path

    bucket = _env("MINIO_BUCKET", "lakehouse")
    warehouse = f"s3a://{bucket}/warehouse"
    spark = _build_spark("engagement-snapshots", warehouse)
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(_CREATE_TABLE_SQL)

    _ensure_columns(
        spark,
        "lakehouse.silver.engagement_snapshots",
        {
            "score": "BIGINT",
            "bookmark_count": "BIGINT",
        },
    )

    input_dir = Path(_env("INSIGHT_REFRESH_OUTPUT_DIR", "/opt/spark/insight-refresh"))
    input_files = [
        input_dir / f"{source}.jsonl" for source in ("youtube", "x", "reddit")
    ]

    updates = []
    for input_file in input_files:
        if not input_file.is_file():
            continue
        with input_file.open(encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    updates.append(json.loads(line))

    if not updates:
        print("No insight updates to snapshot")
        spark.stop()
        return

    snapshots_df = build_snapshots_from_updates(spark, updates)
    if snapshots_df is None or snapshots_df.rdd.isEmpty():
        print("No valid snapshots derived")
        spark.stop()
        return

    # Append-only: always insert, never update
    snapshots_df.writeTo("lakehouse.silver.engagement_snapshots").append()

    print(f"Appended {len(updates)} engagement snapshots")
    spark.stop()


if __name__ == "__main__":
    main()
