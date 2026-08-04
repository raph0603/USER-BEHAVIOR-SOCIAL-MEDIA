"""Idempotently replay one committed X journal event into Bronze current and Silver."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from event_contract import BRONZE_COLUMNS
from pipeline.silver_merge import apply_events_to_silver, ensure_silver_tables
from streaming.kafka_to_iceberg_bronze import _merge_current_projection


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _build_spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("replay-single-x-lineage-event")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
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
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform-event-id", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    args = parser.parse_args()

    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        event = (
            spark.table("lakehouse.bronze.event_log")
            .filter(
                (col("source") == "x")
                & (col("platform_event_id") == args.platform_event_id)
            )
            .select(*BRONZE_COLUMNS)
            .limit(2)
        )
        event_count = event.count()
        if event_count != 1:
            raise RuntimeError(
                "Expected exactly one committed X journal event for "
                f"{args.platform_event_id}; found {event_count}"
            )

        projected = _merge_current_projection(event, epoch_id=0)
        ensure_silver_tables(spark)
        applied = apply_events_to_silver(
            event,
            epoch_id=0,
            run_id=args.pipeline_run_id,
        )

        current_count = (
            spark.table("lakehouse.bronze.events")
            .filter(
                (col("source") == "x")
                & (col("platform_event_id") == args.platform_event_id)
            )
            .limit(2)
            .count()
        )
        silver_count = (
            spark.table("lakehouse.silver.events")
            .filter(
                (col("source") == "x")
                & (col("platform_event_id") == args.platform_event_id)
            )
            .limit(2)
            .count()
        )
        if current_count != 1 or silver_count != 1:
            raise RuntimeError(
                "Exact journal replay did not produce one Bronze current and "
                f"one Silver row: bronze={current_count}, silver={silver_count}"
            )

        print(
            json.dumps(
                {
                    "status": "PASS",
                    "platform_event_id": args.platform_event_id,
                    "journal_rows": event_count,
                    "bronze_current_rows": current_count,
                    "projection_input_rows": projected,
                    "silver_rows": silver_count,
                    "silver_newly_applied": applied.newly_applied,
                    "silver_already_applied": applied.already_applied,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
