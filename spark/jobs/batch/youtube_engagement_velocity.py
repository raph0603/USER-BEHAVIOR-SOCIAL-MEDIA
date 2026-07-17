"""Materialize latest YouTube velocity, acceleration, and virality signals."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    lit,
    log1p,
    row_number,
    to_date,
)


TABLE = "lakehouse.gold.youtube_engagement_velocity"


def _env(name: str, default: str) -> str:
    return os.getenv(name) or default


def _spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("youtube-engagement-velocity")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{bucket}/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", _env("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", _env("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", _env("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def build_latest_velocity(snapshots, threshold: float):
    latest = (
        snapshots.filter(col("source") == "youtube")
        .filter(col("platform_event_id").isNotNull())
        .withColumn(
            "_rank",
            row_number().over(
                Window.partitionBy("platform_event_id").orderBy(col("observed_at").desc())
            ),
        )
        .filter(col("_rank") == 1)
        .drop("_rank")
    )
    score = (
        log1p(coalesce(col("views_per_hour"), lit(0.0)))
        + coalesce(col("engagement_rate"), lit(0.0)) * lit(10.0)
        + coalesce(col("views_acceleration"), lit(0.0)) / lit(1000.0)
    )
    return (
        latest.withColumn("video_id", col("platform_event_id"))
        .withColumn("virality_score", score.cast("double"))
        .withColumn("is_viral", (score >= lit(threshold)).cast("boolean"))
        .withColumn("updated_at", current_timestamp())
        .withColumn("snapshot_date", to_date(col("observed_at")))
        .select(
            "video_id",
            "observed_at",
            "view_count",
            "like_count",
            "comment_count",
            "views_delta",
            "likes_delta",
            "comments_delta",
            "views_per_hour",
            "likes_per_hour",
            "comments_per_hour",
            "like_rate",
            "comment_rate",
            "engagement_rate",
            "views_acceleration",
            "virality_score",
            "is_viral",
            "updated_at",
            "snapshot_date",
        )
    )


def main() -> None:
    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          video_id STRING,
          observed_at TIMESTAMP,
          view_count BIGINT,
          like_count BIGINT,
          comment_count BIGINT,
          views_delta BIGINT,
          likes_delta BIGINT,
          comments_delta BIGINT,
          views_per_hour DOUBLE,
          likes_per_hour DOUBLE,
          comments_per_hour DOUBLE,
          like_rate DOUBLE,
          comment_rate DOUBLE,
          engagement_rate DOUBLE,
          views_acceleration DOUBLE,
          virality_score DOUBLE,
          is_viral BOOLEAN,
          updated_at TIMESTAMP,
          snapshot_date DATE
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )
    threshold = float(_env("YOUTUBE_VIRALITY_THRESHOLD", "8.0"))
    latest = build_latest_velocity(
        spark.table("lakehouse.silver.engagement_snapshots"), threshold
    )
    latest.createOrReplaceTempView("youtube_velocity_updates")
    spark.sql(
        f"""
        MERGE INTO {TABLE} AS target
        USING youtube_velocity_updates AS source
        ON target.video_id = source.video_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print(f"Updated {latest.count()} YouTube velocity records")
    spark.stop()


if __name__ == "__main__":
    main()
