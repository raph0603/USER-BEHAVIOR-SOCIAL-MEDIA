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

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    lit,
    row_number,
    sha2,
    to_date,
    to_timestamp,
    unix_timestamp,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    IntegerType,
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
        StructField("last_metrics_refresh_at", StringType(), True),
        StructField("next_metrics_refresh_at", StringType(), True),
        StructField("metrics_refresh_count", IntegerType(), True),
        StructField("metrics_refresh_status", StringType(), True),
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
  observation_id       STRING    COMMENT 'Stable source, platform ID, observed-at identity',
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
  views_delta          BIGINT    COMMENT 'Non-negative views change since prior observation',
  likes_delta          BIGINT    COMMENT 'Non-negative likes change since prior observation',
  comments_delta       BIGINT    COMMENT 'Non-negative comments change since prior observation',
  hours_since_previous DOUBLE    COMMENT 'Elapsed hours since prior observation',
  views_per_hour       DOUBLE    COMMENT 'Views delta divided by elapsed hours',
  likes_per_hour       DOUBLE    COMMENT 'Likes delta divided by elapsed hours',
  comments_per_hour    DOUBLE    COMMENT 'Comments delta divided by elapsed hours',
  like_rate            DOUBLE    COMMENT 'Likes divided by views when views are positive',
  comment_rate         DOUBLE    COMMENT 'Comments divided by views when views are positive',
  engagement_rate      DOUBLE    COMMENT 'Likes plus comments divided by views',
  views_acceleration   DOUBLE    COMMENT 'Change in views per hour divided by elapsed hours',
  metrics_refresh_status STRING  COMMENT 'Outcome of the metrics observation',
  snapshot_date        DATE      COMMENT 'Partition column derived from observed_at'
)
USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  'write.metadata.delete-after-commit.enabled' = 'false'
)
"""

_SNAPSHOT_COLUMNS = [
    "observation_id",
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
    "views_delta",
    "likes_delta",
    "comments_delta",
    "hours_since_previous",
    "views_per_hour",
    "likes_per_hour",
    "comments_per_hour",
    "like_rate",
    "comment_rate",
    "engagement_rate",
    "views_acceleration",
    "metrics_refresh_status",
    "snapshot_date",
]


# ---------------------------------------------------------------------------
# Snapshot derivation
# ---------------------------------------------------------------------------


def _non_negative_delta(current: str, previous: str):
    return when(
        col(current).isNull()
        | col(previous).isNull()
        | (col(current) < col(previous)),
        lit(None).cast("bigint"),
    ).otherwise((col(current) - col(previous)).cast("bigint"))


def build_snapshots_from_updates(
    spark: SparkSession,
    updates: list[dict],
    previous_snapshots=None,
):
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
        .withColumn(
            "observation_id",
            sha2(
                concat_ws(
                    "\u001f",
                    coalesce(col("source"), lit("")),
                    coalesce(col("platform_event_id"), col("url"), lit("")),
                    coalesce(col("observed_at").cast("string"), lit("")),
                ),
                256,
            ),
        )
    )

    if previous_snapshots is None:
        for name, data_type in (
            ("previous_observed_at", "timestamp"),
            ("previous_view_count", "bigint"),
            ("previous_like_count", "bigint"),
            ("previous_comment_count", "bigint"),
            ("previous_views_per_hour", "double"),
        ):
            df = df.withColumn(name, lit(None).cast(data_type))
    else:
        previous = previous_snapshots.select(
            "source",
            "platform_event_id",
            col("observed_at").alias("previous_observed_at"),
            col("view_count").alias("previous_view_count"),
            col("like_count").alias("previous_like_count"),
            col("comment_count").alias("previous_comment_count"),
            col("views_per_hour").alias("previous_views_per_hour"),
        )
        df = df.join(previous, ["source", "platform_event_id"], "left")

    df = (
        df.withColumn(
            "hours_since_previous",
            when(
                col("previous_observed_at").isNotNull()
                & (col("observed_at") > col("previous_observed_at")),
                (
                    unix_timestamp(col("observed_at"))
                    - unix_timestamp(col("previous_observed_at"))
                )
                / lit(3600.0),
            ).cast("double"),
        )
        .withColumn(
            "views_delta",
            _non_negative_delta("view_count", "previous_view_count"),
        )
        .withColumn(
            "likes_delta",
            _non_negative_delta("like_count", "previous_like_count"),
        )
        .withColumn(
            "comments_delta",
            _non_negative_delta("comment_count", "previous_comment_count"),
        )
        .withColumn(
            "views_per_hour",
            when(
                (col("hours_since_previous") > 0) & col("views_delta").isNotNull(),
                col("views_delta") / col("hours_since_previous"),
            ).cast("double"),
        )
        .withColumn(
            "likes_per_hour",
            when(
                (col("hours_since_previous") > 0) & col("likes_delta").isNotNull(),
                col("likes_delta") / col("hours_since_previous"),
            ).cast("double"),
        )
        .withColumn(
            "comments_per_hour",
            when(
                (col("hours_since_previous") > 0)
                & col("comments_delta").isNotNull(),
                col("comments_delta") / col("hours_since_previous"),
            ).cast("double"),
        )
        .withColumn(
            "like_rate",
            when(
                (col("view_count") > 0) & col("like_count").isNotNull(),
                col("like_count") / col("view_count"),
            ).cast("double"),
        )
        .withColumn(
            "comment_rate",
            when(
                (col("view_count") > 0) & col("comment_count").isNotNull(),
                col("comment_count") / col("view_count"),
            ).cast("double"),
        )
        .withColumn(
            "engagement_rate",
            when(
                (col("view_count") > 0)
                & (col("like_count").isNotNull() | col("comment_count").isNotNull()),
                (
                    coalesce(col("like_count"), lit(0))
                    + coalesce(col("comment_count"), lit(0))
                )
                / col("view_count"),
            ).cast("double"),
        )
        .withColumn(
            "views_acceleration",
            when(
                (col("hours_since_previous") > 0)
                & col("views_per_hour").isNotNull()
                & col("previous_views_per_hour").isNotNull(),
                (col("views_per_hour") - col("previous_views_per_hour"))
                / col("hours_since_previous"),
            ).cast("double"),
        )
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
            "observation_id": "STRING",
            "score": "BIGINT",
            "bookmark_count": "BIGINT",
            "views_delta": "BIGINT",
            "likes_delta": "BIGINT",
            "comments_delta": "BIGINT",
            "hours_since_previous": "DOUBLE",
            "views_per_hour": "DOUBLE",
            "likes_per_hour": "DOUBLE",
            "comments_per_hour": "DOUBLE",
            "like_rate": "DOUBLE",
            "comment_rate": "DOUBLE",
            "engagement_rate": "DOUBLE",
            "views_acceleration": "DOUBLE",
            "metrics_refresh_status": "STRING",
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

    current_snapshots = spark.table("lakehouse.silver.engagement_snapshots")
    previous_snapshots = (
        current_snapshots.withColumn(
            "_previous_rank",
            row_number().over(
                Window.partitionBy("source", "platform_event_id").orderBy(
                    col("observed_at").desc()
                )
            ),
        )
        .filter(col("_previous_rank") == 1)
        .drop("_previous_rank")
    )
    snapshots_df = build_snapshots_from_updates(
        spark,
        updates,
        previous_snapshots=previous_snapshots,
    )
    if snapshots_df is None or snapshots_df.rdd.isEmpty():
        print("No valid snapshots derived")
        spark.stop()
        return

    snapshots_df = snapshots_df.dropDuplicates(["observation_id"])
    existing_ids = current_snapshots.filter(col("observation_id").isNotNull()).select(
        "observation_id"
    )
    snapshots_df = snapshots_df.join(existing_ids, ["observation_id"], "left_anti")
    if snapshots_df.rdd.isEmpty():
        print("No new engagement snapshots after idempotence check")
        spark.stop()
        return

    # Append-only: only unseen observations are inserted; no row is updated.
    snapshots_df.writeTo("lakehouse.silver.engagement_snapshots").append()

    print(f"Appended {len(updates)} engagement snapshots")
    spark.stop()


if __name__ == "__main__":
    main()
