"""Check or repair the Bronze event-log to Silver applied-event boundary."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, min as spark_min

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from event_contract import BRONZE_COLUMNS
from pipeline.reconciliation import ReconciliationReport, reconciliation_epoch_id
from pipeline.silver_merge import (
    APPLIED_EVENTS_TABLE,
    apply_events_to_silver,
    ensure_silver_tables,
)


EVENT_LOG_TABLE = "lakehouse.bronze.event_log"


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _build_spark(warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName("reconcile-bronze-silver")
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
        .config("spark.sql.shuffle.partitions", _env("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )


def _duplicate_id_count(frame: DataFrame) -> int:
    duplicates = (
        frame.groupBy("event_id").agg(count("*").alias("row_count")).filter(col("row_count") > 1)
    )
    return sum(int(row["row_count"]) - 1 for row in duplicates.collect())


def _missing_events(spark: SparkSession) -> DataFrame:
    event_log = spark.table(EVENT_LOG_TABLE)
    applied_ids = spark.table(APPLIED_EVENTS_TABLE).select("event_id")
    return event_log.join(applied_ids, ["event_id"], "left_anti")


def build_report(
    spark: SparkSession,
    *,
    mode: str,
    repaired_events: int = 0,
) -> ReconciliationReport:
    event_log = spark.table(EVENT_LOG_TABLE)
    applied = spark.table(APPLIED_EVENTS_TABLE)
    missing = _missing_events(spark)
    orphan_applied = applied.select("event_id").join(
        event_log.select("event_id"), ["event_id"], "left_anti"
    )
    missing_count = missing.count()
    oldest_age: float | None = None
    if missing_count:
        oldest = missing.agg(spark_min("ingested_at").alias("oldest")).first()["oldest"]
        if oldest is not None:
            if oldest.tzinfo is None:
                oldest = oldest.replace(tzinfo=timezone.utc)
            oldest_age = max(
                0.0,
                (datetime.now(timezone.utc) - oldest.astimezone(timezone.utc)).total_seconds(),
            )
    by_source = {
        str(row["source"] or "unknown"): int(row["count"])
        for row in missing.groupBy("source").count().collect()
    }
    return ReconciliationReport(
        mode=mode,
        event_log_events=event_log.count(),
        applied_events=applied.count(),
        missing_events=missing_count,
        duplicate_event_log_ids=_duplicate_id_count(event_log),
        duplicate_applied_ids=_duplicate_id_count(applied),
        orphan_applied_events=orphan_applied.count(),
        oldest_missing_age_seconds=(round(oldest_age, 3) if oldest_age is not None else None),
        missing_by_source=by_source,
        repaired_events=repaired_events,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check or repair committed Bronze events missing from Silver"
    )
    parser.add_argument("--mode", choices=("check", "repair"), required=True)
    parser.add_argument(
        "--repair-limit",
        type=int,
        default=int(_env("RECONCILIATION_REPAIR_LIMIT", "100000")),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.repair_limit <= 0:
        raise ValueError("--repair-limit must be greater than zero")
    bucket = _env("MINIO_BUCKET", "lakehouse")
    spark = _build_spark(f"s3a://{bucket}/warehouse")
    spark.sparkContext.setLogLevel("WARN")
    ensure_silver_tables(spark)
    if not spark.catalog.tableExists(EVENT_LOG_TABLE):
        raise RuntimeError(f"{EVENT_LOG_TABLE} does not exist; run the additive migration first")

    repaired_events = 0
    if args.mode == "repair":
        missing = _missing_events(spark).orderBy("ingested_at", "event_id").limit(args.repair_limit)
        run_id = _env(
            "PIPELINE_RUN_ID",
            f"reconcile-{datetime.now(timezone.utc).isoformat()}",
        )
        result = apply_events_to_silver(
            missing.select(*BRONZE_COLUMNS),
            epoch_id=reconciliation_epoch_id(run_id),
            run_id=run_id,
        )
        repaired_events = result.newly_applied

    report = build_report(
        spark,
        mode=args.mode,
        repaired_events=repaired_events,
    )
    print(json.dumps(report.to_dict(), sort_keys=True))
    return 0 if report.is_clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
