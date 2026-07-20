"""Audit and additively migrate engagement snapshot observation identities."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    lit,
    lower,
    row_number,
    sha2,
    struct,
    to_json,
    when,
)


TABLE = "lakehouse.silver.engagement_snapshots"
ADDITIVE_COLUMNS = {
    "observation_id": "STRING",
    "metrics_refresh_status": "STRING",
    "metrics_error_code": "STRING",
    "producer_name": "STRING",
    "producer_run_id": "STRING",
    "collection_method": "STRING",
    "api_endpoint": "STRING",
    "payload_fingerprint": "STRING",
    "coverage_json": "STRING",
    "like_count_available": "BOOLEAN",
    "view_count_available": "BOOLEAN",
    "comment_count_available": "BOOLEAN",
    "reply_count_available": "BOOLEAN",
    "retweet_count_available": "BOOLEAN",
    "bookmark_count_available": "BOOLEAN",
    "score_available": "BOOLEAN",
}


def _env(name: str, default: str) -> str:
    return os.getenv(name) or default


def _spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("migrate-engagement-snapshots")
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


def _with_effective_observation_id(snapshots: DataFrame) -> DataFrame:
    existing = (
        col("observation_id") if "observation_id" in snapshots.columns else lit(None).cast("string")
    )
    derived = sha2(
        concat_ws(
            "\u001f",
            coalesce(col("source"), lit("")),
            coalesce(col("platform_event_id"), col("url"), lit("")),
            coalesce(col("observed_at").cast("string"), lit("")),
        ),
        256,
    )
    valid_existing = when(existing.rlike("^[0-9a-fA-F]{64}$"), lower(existing))
    return snapshots.withColumn(
        "_effective_observation_id",
        coalesce(valid_existing, derived),
    )


def audit_snapshots(spark: SparkSession) -> tuple[dict, DataFrame | None]:
    if not spark.catalog.tableExists(TABLE):
        return {
            "table_exists": False,
            "rows": 0,
            "missing_observation_ids": 0,
            "invalid_observation_ids": 0,
            "duplicate_observation_ids": 0,
        }, None

    snapshots = spark.table(TABLE)
    candidate = _with_effective_observation_id(snapshots)
    if "observation_id" in snapshots.columns:
        missing = snapshots.filter(col("observation_id").isNull()).count()
        invalid = snapshots.filter(
            col("observation_id").isNotNull() & ~col("observation_id").rlike("^[0-9a-fA-F]{64}$")
        ).count()
    else:
        missing = snapshots.count()
        invalid = 0
    duplicates = (
        candidate.groupBy("_effective_observation_id").count().filter(col("count") > 1).count()
    )
    return {
        "table_exists": True,
        "rows": snapshots.count(),
        "missing_observation_ids": missing,
        "invalid_observation_ids": invalid,
        "duplicate_observation_ids": duplicates,
    }, candidate


def _ensure_columns(spark: SparkSession) -> None:
    current = set(spark.table(TABLE).columns)
    for name, data_type in ADDITIVE_COLUMNS.items():
        if name not in current:
            spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN {name} {data_type}")


def _backfill_in_place(candidate: DataFrame) -> None:
    updates = candidate.filter(
        col("observation_id").isNull()
        | (lower(col("observation_id")) != col("_effective_observation_id"))
    ).select(
        "source",
        "platform_event_id",
        "url",
        "observed_at",
        col("observation_id").alias("previous_observation_id"),
        col("_effective_observation_id").alias("observation_id"),
    )
    if updates.rdd.isEmpty():
        return
    updates.createOrReplaceTempView("engagement_snapshot_identity_updates")
    candidate.sparkSession.sql(
        f"""
        MERGE INTO {TABLE} AS target
        USING engagement_snapshot_identity_updates AS source
        ON target.source <=> source.source
          AND target.platform_event_id <=> source.platform_event_id
          AND target.url <=> source.url
          AND target.observed_at <=> source.observed_at
          AND target.observation_id <=> source.previous_observation_id
        WHEN MATCHED THEN UPDATE SET
          target.observation_id = source.observation_id
        """
    )


def _validated_staging_switch(candidate: DataFrame) -> tuple[str, str]:
    spark = candidate.sparkSession
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    staging = f"lakehouse.silver.engagement_snapshots_staging_{suffix}"
    backup = f"lakehouse.silver.engagement_snapshots_backup_{suffix}"
    source_columns = [name for name in candidate.columns if not name.startswith("_")]
    row_fingerprint = sha2(to_json(struct(*[col(name) for name in source_columns])), 256)
    staged = (
        candidate.withColumn("_row_fingerprint", row_fingerprint)
        .withColumn(
            "_rank",
            row_number().over(
                Window.partitionBy("_effective_observation_id").orderBy(
                    col("_row_fingerprint").desc()
                )
            ),
        )
        .filter(col("_rank") == 1)
        .withColumn("observation_id", col("_effective_observation_id"))
        .select(*source_columns)
    )
    expected_rows = staged.count()
    staged.writeTo(staging).using("iceberg").partitionedBy(col("snapshot_date")).create()
    staged_table = spark.table(staging)
    duplicate_count = (
        staged_table.groupBy("observation_id").count().filter(col("count") > 1).count()
    )
    if (
        staged_table.count() != expected_rows
        or staged_table.filter(col("observation_id").isNull()).count()
        or duplicate_count
    ):
        raise RuntimeError(f"Staging validation failed; preserved table for inspection: {staging}")

    spark.sql(f"ALTER TABLE {TABLE} RENAME TO {backup}")
    try:
        spark.sql(f"ALTER TABLE {staging} RENAME TO {TABLE}")
    except Exception:
        spark.sql(f"ALTER TABLE {backup} RENAME TO {TABLE}")
        raise
    return staging, backup


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        audit, candidate = audit_snapshots(spark)
        result = {"mode": "apply" if args.apply else "dry-run", **audit}
        if args.dry_run or candidate is None:
            print(json.dumps(result, sort_keys=True))
            return 0

        _ensure_columns(spark)
        audit, candidate = audit_snapshots(spark)
        if audit["duplicate_observation_ids"]:
            _, backup = _validated_staging_switch(candidate)
            result["migration_strategy"] = "validated_staging_switch"
            result["backup_table"] = backup
        else:
            _backfill_in_place(candidate)
            result["migration_strategy"] = "in_place_backfill"
        after, _ = audit_snapshots(spark)
        result["after"] = after
        if (
            after["missing_observation_ids"]
            or after["invalid_observation_ids"]
            or after["duplicate_observation_ids"]
        ):
            raise RuntimeError("Engagement snapshot identity migration is incomplete")
        print(json.dumps(result, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
