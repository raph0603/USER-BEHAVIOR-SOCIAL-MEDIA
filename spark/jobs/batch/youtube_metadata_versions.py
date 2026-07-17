"""Append changed descriptive YouTube metadata versions to Iceberg."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    from_json,
    get_json_object,
    lit,
    sha2,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import ArrayType, StringType


TABLE = "lakehouse.silver.youtube_metadata_versions"


def _env(name: str, default: str) -> str:
    return os.getenv(name) or default


def _spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("youtube-metadata-versions")
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


def build_versions(events):
    observed_at = to_timestamp(
        coalesce(col("metadata_collected_at"), col("collected_at"), col("timestamp"))
    )
    canonical = col("canonical_metadata")
    return (
        events.filter(col("source") == "youtube")
        .filter(col("metadata_hash").isNotNull())
        .filter(
            (col("event_type") == "youtube.metadata.changed")
            | col("previous_metadata_hash").isNull()
            | (col("previous_metadata_hash") != col("metadata_hash"))
        )
        .withColumn("observed_at", observed_at)
        .withColumn("valid_from", observed_at)
        .withColumn("snapshot_date", to_date(observed_at))
        .withColumn("title", get_json_object(canonical, "$.title"))
        .withColumn("description", get_json_object(canonical, "$.description"))
        .withColumn(
            "tags",
            from_json(get_json_object(canonical, "$.tags"), ArrayType(StringType())),
        )
        .withColumn(
            "categories",
            from_json(
                get_json_object(canonical, "$.categories"),
                ArrayType(StringType()),
            ),
        )
        .withColumn("chapters_json", get_json_object(canonical, "$.chapters"))
        .withColumn("thumbnails_json", get_json_object(canonical, "$.thumbnails"))
        .withColumn("subtitles_json", get_json_object(canonical, "$.subtitles"))
        .withColumn(
            "automatic_captions_json",
            get_json_object(canonical, "$.automatic_captions"),
        )
        .withColumn("availability", get_json_object(canonical, "$.availability"))
        .withColumn("live_status", get_json_object(canonical, "$.live_status"))
        .withColumn(
            "version_id",
            sha2(
                concat_ws(
                    "\u001f",
                    coalesce(col("video_id"), col("platform_event_id"), lit("")),
                    col("metadata_hash"),
                    col("observed_at").cast("string"),
                ),
                256,
            ),
        )
        .select(
            "version_id",
            coalesce(col("video_id"), col("platform_event_id")).alias("video_id"),
            "observed_at",
            "valid_from",
            "metadata_hash",
            "previous_metadata_hash",
            "changed_fields",
            "title",
            "description",
            "tags",
            "categories",
            "chapters_json",
            "thumbnails_json",
            "subtitles_json",
            "automatic_captions_json",
            "availability",
            "live_status",
            "metadata_source",
            "snapshot_date",
        )
    )


def main() -> None:
    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          version_id STRING,
          video_id STRING,
          observed_at TIMESTAMP,
          valid_from TIMESTAMP,
          metadata_hash STRING,
          previous_metadata_hash STRING,
          changed_fields ARRAY<STRING>,
          title STRING,
          description STRING,
          tags ARRAY<STRING>,
          categories ARRAY<STRING>,
          chapters_json STRING,
          thumbnails_json STRING,
          subtitles_json STRING,
          automatic_captions_json STRING,
          availability STRING,
          live_status STRING,
          metadata_source STRING,
          snapshot_date DATE
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )
    versions = build_versions(spark.table("lakehouse.silver.events")).dropDuplicates(
        ["version_id"]
    )
    existing = spark.table(TABLE).select("version_id")
    new_versions = versions.join(existing, ["version_id"], "left_anti")
    if not new_versions.rdd.isEmpty():
        count = new_versions.count()
        new_versions.writeTo(TABLE).append()
        print(f"Appended {count} YouTube metadata versions")
    else:
        print("No changed YouTube metadata versions to append")
    spark.stop()


if __name__ == "__main__":
    main()
