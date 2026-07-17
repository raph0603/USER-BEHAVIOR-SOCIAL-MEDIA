"""Persist append-only YouTube API usage from collector state to Iceberg."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


USAGE_SCHEMA = StructType(
    [
        StructField("usage_id", LongType(), False),
        StructField("usage_date", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("request_count", IntegerType(), False),
        StructField("resource_count", IntegerType(), False),
        StructField("success_count", IntegerType(), False),
        StructField("error_count", IntegerType(), False),
        StructField("quota_bucket", StringType(), False),
        StructField("observed_at", StringType(), False),
    ]
)


def _env(name: str, default: str) -> str:
    return os.getenv(name) or default


def _spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("youtube-api-usage")
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


def load_usage(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT usage_id, usage_date, endpoint, request_count,
                   resource_count, success_count, error_count,
                   quota_bucket, observed_at
            FROM youtube_api_usage
            ORDER BY usage_id
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def main() -> None:
    state_path = Path(
        _env(
            "YOUTUBE_PIPELINE_STATE_DB",
            "/opt/spark/collector-state/youtube-pipeline.sqlite",
        )
    )
    rows = load_usage(state_path)
    if not rows:
        print("No YouTube API usage rows to persist")
        return
    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.monitoring")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.monitoring.youtube_api_usage (
          usage_id BIGINT,
          usage_date DATE,
          endpoint STRING,
          request_count INT,
          resource_count INT,
          success_count INT,
          error_count INT,
          quota_bucket STRING,
          observed_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (usage_date)
        """
    )
    incoming = (
        spark.createDataFrame(rows, schema=USAGE_SCHEMA)
        .withColumn("usage_date", to_date(col("usage_date")))
        .withColumn("observed_at", to_timestamp(col("observed_at")))
        .dropDuplicates(["usage_id"])
    )
    existing = spark.table("lakehouse.monitoring.youtube_api_usage").select("usage_id")
    new_rows = incoming.join(existing, ["usage_id"], "left_anti")
    if not new_rows.rdd.isEmpty():
        new_rows.writeTo("lakehouse.monitoring.youtube_api_usage").append()
        print(f"Appended {new_rows.count()} YouTube API usage rows")
    else:
        print("No new YouTube API usage rows")
    spark.stop()


if __name__ == "__main__":
    main()
