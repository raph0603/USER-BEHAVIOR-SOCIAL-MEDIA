"""Additively migrate reliability tables, transcript lifecycle, and historical rows."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    lit,
    sha2,
    struct,
    to_json,
    trim,
    when,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from batch.youtube_transcripts import (
    TRANSCRIPT_TABLE,
    ensure_transcript_table,
)
from event_contract import BRONZE_COLUMNS, BRONZE_EVENT_LOG_COLUMNS
from pipeline.silver_merge import (
    APPLIED_EVENT_COLUMNS,
    APPLIED_EVENTS_TABLE,
    SILVER_TABLE,
    ensure_silver_tables,
)
from streaming.kafka_to_iceberg_bronze import (
    CURRENT_TABLE,
    EVENT_LOG_TABLE,
    _ensure_tables as ensure_bronze_tables,
    _merge_insert_only,
)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _build_spark(warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName("migrate-pipeline-reliability")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", _env("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", _env("MINIO_ROOT_USER", "minioadmin"))
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            _env("MINIO_ROOT_PASSWORD", "minioadmin"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def _latest_snapshot_id(spark: SparkSession, table: str) -> int | None:
    try:
        row = spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()
    except Exception as exc:  # metadata tables vary across Iceberg versions
        print(
            json.dumps(
                {
                    "level": "warning",
                    "event": "snapshot_id_unavailable",
                    "table": table,
                    "error_class": type(exc).__name__,
                },
                sort_keys=True,
            )
        )
        return None
    return int(row["snapshot_id"]) if row else None


def _historical_journal_rows(spark: SparkSession) -> DataFrame:
    bronze = spark.table(CURRENT_TABLE)
    serialized = to_json(struct(*[col(name) for name in BRONZE_COLUMNS]))
    fingerprint = coalesce(col("payload_fingerprint"), sha2(serialized, 256))
    synthetic_id = sha2(
        concat_ws(
            "\u001f",
            lit("historical_v1"),
            coalesce(col("source"), lit("")),
            coalesce(col("event_id"), lit("")),
            coalesce(col("platform_event_id"), lit("")),
            coalesce(col("user_id"), lit("")),
            coalesce(col("url"), lit("")),
            coalesce(col("timestamp"), lit("")),
            fingerprint,
        ),
        256,
    )
    return (
        bronze.withColumn("payload_fingerprint", fingerprint)
        .withColumn(
            "event_id",
            when(
                trim(coalesce(col("event_id"), lit(""))).rlike("^[0-9a-fA-F]{64}$"),
                col("event_id"),
            ).otherwise(synthetic_id),
        )
        .withColumn("kafka_topic", lit(None).cast("string"))
        .withColumn("kafka_partition", lit(None).cast("int"))
        .withColumn("kafka_offset", lit(None).cast("long"))
        .withColumn("kafka_timestamp", lit(None).cast("timestamp"))
        .withColumn("bronze_epoch_id", lit(-1).cast("long"))
        .withColumn("bronze_run_id", lit("historical-backfill-v1"))
        .withColumn("ingested_at", current_timestamp())
        .select(*BRONZE_EVENT_LOG_COLUMNS)
        .dropDuplicates(["event_id"])
    )


def _backfill_applied_events(spark: SparkSession) -> int:
    historical = spark.table(EVENT_LOG_TABLE).filter(col("bronze_epoch_id") == -1).alias("journal")
    silver = spark.table(SILVER_TABLE).alias("silver")
    matches = historical.join(
        silver,
        (col("journal.source") == col("silver.source"))
        & (
            (
                col("journal.platform_event_id").isNotNull()
                & (col("journal.platform_event_id") == col("silver.platform_event_id"))
            )
            | (
                col("journal.platform_event_id").isNull()
                & (col("journal.user_id") == col("silver.user_id"))
                & (col("journal.url") == col("silver.url"))
                & (col("journal.event_ts") == col("silver.event_ts"))
            )
        ),
        "inner",
    )
    applied = (
        matches.select(
            col("journal.event_id").alias("event_id"),
            col("journal.source").alias("source"),
            col("journal.platform_event_id").alias("platform_event_id"),
            col("silver.event_date").alias("event_date"),
            col("journal.payload_fingerprint").alias("payload_fingerprint"),
        )
        .withColumn("applied_at", current_timestamp())
        .withColumn("silver_epoch_id", lit(-1).cast("long"))
        .withColumn("silver_run_id", lit("historical-backfill-v1"))
        .select(*APPLIED_EVENT_COLUMNS)
        .dropDuplicates(["event_id"])
    )
    candidates = applied.count()
    if candidates:
        _merge_insert_only(
            applied,
            table=APPLIED_EVENTS_TABLE,
            identity_column="event_id",
            columns=APPLIED_EVENT_COLUMNS,
            view_name="historical_applied_events",
        )
    return candidates


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    bucket = _env("MINIO_BUCKET", "lakehouse")
    spark = _build_spark(f"s3a://{bucket}/warehouse")
    spark.sparkContext.setLogLevel("WARN")
    bronze_exists = spark.catalog.tableExists(CURRENT_TABLE)
    silver_exists = spark.catalog.tableExists(SILVER_TABLE)
    transcript_exists = spark.catalog.tableExists(TRANSCRIPT_TABLE)
    before = {
        "mode": "apply" if args.apply else "dry-run",
        "bronze_rows": spark.table(CURRENT_TABLE).count() if bronze_exists else 0,
        "silver_rows": spark.table(SILVER_TABLE).count() if silver_exists else 0,
        "event_log_exists": spark.catalog.tableExists(EVENT_LOG_TABLE),
        "applied_events_exists": spark.catalog.tableExists(APPLIED_EVENTS_TABLE),
        "transcript_table_exists": transcript_exists,
        "transcript_rows": (spark.table(TRANSCRIPT_TABLE).count() if transcript_exists else 0),
        "bronze_snapshot_id": (
            _latest_snapshot_id(spark, CURRENT_TABLE) if bronze_exists else None
        ),
        "silver_snapshot_id": (_latest_snapshot_id(spark, SILVER_TABLE) if silver_exists else None),
    }
    if args.dry_run:
        print(
            json.dumps(
                {
                    **before,
                    "would_backfill_historical_rows": before["bronze_rows"],
                    "would_migrate_transcript_lifecycle_rows": before["transcript_rows"],
                },
                sort_keys=True,
            )
        )
        return 0

    ensure_bronze_tables(spark)
    ensure_silver_tables(spark)
    ensure_transcript_table(spark)
    journal_rows = _historical_journal_rows(spark)
    journal_candidates = journal_rows.count()
    if journal_candidates:
        _merge_insert_only(
            journal_rows,
            table=EVENT_LOG_TABLE,
            identity_column="event_id",
            columns=BRONZE_EVENT_LOG_COLUMNS,
            view_name="historical_bronze_event_log",
        )
    applied_candidates = _backfill_applied_events(spark)
    result = {
        **before,
        "journal_candidates": journal_candidates,
        "applied_candidates": applied_candidates,
        "event_log_rows_after": spark.table(EVENT_LOG_TABLE).count(),
        "applied_events_rows_after": spark.table(APPLIED_EVENTS_TABLE).count(),
        "transcript_rows_after": spark.table(TRANSCRIPT_TABLE).count(),
        "transcript_lifecycle_rows_after": spark.table(TRANSCRIPT_TABLE)
        .filter("transcript_lifecycle_status IS NOT NULL")
        .count(),
        "historical_limit": (
            "one deterministic synthetic event per current Bronze row; "
            "pre-journal history cannot be reconstructed"
        ),
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
