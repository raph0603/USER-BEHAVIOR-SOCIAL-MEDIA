import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)


METRIC_COLUMNS = (
    "like_count",
    "view_count",
    "follower_count",
    "subscriber_count",
    "subreddit_member_count",
)
AUTHOR_COLUMNS = (
    "owner_channel_id",
    "collaborator_channel_ids",
)
METADATA_COLUMNS = (
    "platform_event_id",
)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _build_spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("apply-insight-updates")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config(
            "spark.sql.catalog.lakehouse.warehouse",
            f"s3a://{bucket}/warehouse",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            _env("MINIO_ENDPOINT", "http://minio:9000"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            _env("MINIO_ROOT_USER", "minioadmin"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            _env("MINIO_ROOT_PASSWORD", "minioadmin"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


def _load_updates(path: Path) -> list[dict]:
    updates = []
    if not path.is_file():
        return updates
    with path.open(encoding="utf-8") as source:
        for line in source:
            if line.strip():
                updates.append(json.loads(line))
    return updates


def _merge_updates(spark: SparkSession, table: str) -> None:
    assignments = ",\n".join(
        f"t.{column} = COALESCE(s.{column}, t.{column})"
        for column in (*METADATA_COLUMNS, *AUTHOR_COLUMNS, *METRIC_COLUMNS)
    )
    assignments = (
        f"{assignments},\n"
        "t.metadata_refreshed_at = COALESCE("
        "to_timestamp(s.metadata_refreshed_at), "
        "t.metadata_refreshed_at"
        ")"
    )
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING insight_updates AS s
        ON t.source = s.source
           AND (
             (
               s.platform_event_id IS NOT NULL
               AND t.platform_event_id = s.platform_event_id
             )
             OR (
               s.platform_event_id IS NULL
               AND t.user_id = s.user_id
               AND t.url = s.url
               AND t.event_ts = to_timestamp(s.event_ts)
             )
           )
        WHEN MATCHED THEN UPDATE SET
          {assignments}
        """
    )


def main() -> None:
    input_dir = Path(
        _env("INSIGHT_REFRESH_OUTPUT_DIR", "/opt/spark/insight-refresh")
    )
    input_files = [
        input_dir / f"{source}.jsonl"
        for source in ("youtube", "x", "reddit")
    ]
    updates = [
        update
        for input_file in input_files
        for update in _load_updates(input_file)
    ]
    if not updates:
        print("No insight updates to apply")
        return

    schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("url", StringType(), False),
            StructField("event_ts", StringType(), False),
            StructField("source", StringType(), False),
            StructField("platform_event_id", StringType(), True),
            StructField("metadata_refreshed_at", StringType(), True),
            StructField("owner_channel_id", StringType(), True),
            StructField(
                "collaborator_channel_ids",
                ArrayType(StringType()),
                True,
            ),
            *[
                StructField(column, LongType(), True)
                for column in METRIC_COLUMNS
            ],
        ]
    )
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.createDataFrame(updates, schema=schema).dropDuplicates(
        ["source", "platform_event_id", "user_id", "url", "event_ts"]
    ).createOrReplaceTempView("insight_updates")

    for table in ("lakehouse.bronze.events", "lakehouse.silver.events"):
        current_columns = set(spark.table(table).columns)
        if "platform_event_id" not in current_columns:
            spark.sql(
                f"ALTER TABLE {table} ADD COLUMN platform_event_id STRING"
            )
        if "metadata_refreshed_at" not in current_columns:
            spark.sql(
                f"ALTER TABLE {table} ADD COLUMN metadata_refreshed_at TIMESTAMP"
            )
        if "owner_channel_id" not in current_columns:
            spark.sql(
                f"ALTER TABLE {table} ADD COLUMN owner_channel_id STRING"
            )
        if "collaborator_channel_ids" not in current_columns:
            spark.sql(
                f"ALTER TABLE {table} ADD COLUMN "
                "collaborator_channel_ids ARRAY<STRING>"
            )
        _merge_updates(spark, table)

    print(f"Applied {len(updates)} insight updates to Bronze and Silver")
    spark.stop()


if __name__ == "__main__":
    main()
