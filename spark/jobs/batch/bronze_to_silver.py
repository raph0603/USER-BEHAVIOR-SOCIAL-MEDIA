"""Apply Bronze event-log rows to Silver without relying on Kafka delivery."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from event_contract import BRONZE_COLUMNS
from pipeline.silver_merge import apply_events_to_silver, ensure_silver_tables


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


def main() -> None:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    spark = _build_spark("bronze-event-log-to-silver", f"s3a://{bucket}/warehouse")
    spark.sparkContext.setLogLevel("WARN")
    ensure_silver_tables(spark)

    source_table = _env("BRONZE_EVENT_LOG_TABLE", "lakehouse.bronze.event_log")
    source_stream = spark.readStream.format("iceberg").load(source_table).select(
        *BRONZE_COLUMNS
    )
    run_id = _env("PIPELINE_RUN_ID", "standalone-direct")

    def _foreach_batch(df: DataFrame, epoch_id: int) -> None:
        result = apply_events_to_silver(df, epoch_id=epoch_id, run_id=run_id)
        print(
            json.dumps(
                {
                    "event": "silver_direct_apply_complete",
                    "epoch_id": epoch_id,
                    "input_events": result.input_events,
                    "already_applied": result.already_applied,
                    "newly_applied": result.newly_applied,
                    "current_rows_merged": result.current_rows_merged,
                },
                sort_keys=True,
            )
        )

    checkpoint = _env(
        "SILVER_DIRECT_CHECKPOINT",
        f"s3a://{bucket}/checkpoints/silver/events/direct/event_log_v1",
    )
    writer = (
        source_stream.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(_foreach_batch)
    )
    if _env("PROCESSING_MODE", "continuous").lower() == "availablenow":
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(
            processingTime=_env("PROCESSING_TRIGGER", "30 seconds")
        ).start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
