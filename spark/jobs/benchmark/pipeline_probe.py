"""Read-only benchmark probe for isolated Iceberg warehouses.

The probe never creates, deletes, or mutates a table. It emits one prefixed
JSON document so the host-side benchmark runner can distinguish it from Spark
logs.
"""

from __future__ import annotations

import json
import os
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum


TABLES = {
    "bronze_event_log": "lakehouse.bronze.event_log",
    "bronze_events": "lakehouse.bronze.events",
    "bronze_ingress_dlq": "lakehouse.bronze.ingress_dlq",
    "silver_events": "lakehouse.silver.events",
    "silver_applied_events": "lakehouse.silver.applied_events",
    "silver_contents": "lakehouse.silver.contents",
    "silver_interactions": "lakehouse.silver.interactions",
    "silver_engagement_snapshots": "lakehouse.silver.engagement_snapshots",
    "gold_content_stats": "lakehouse.gold.content_stats",
    "gold_user_evolution": "lakehouse.gold.user_evolution",
}


def _env(name: str, default: str) -> str:
    return os.getenv(name) or default


def _spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("pipeline-benchmark-probe")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{bucket}/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", _env("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", _env("MINIO_ROOT_USER", "minioadmin"))
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            _env("MINIO_ROOT_PASSWORD", "minioadmin"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )


def _scalar(spark: SparkSession, query: str, field: str) -> Any:
    row = spark.sql(query).first()
    return None if row is None else row[field]


def _metadata(spark: SparkSession, table: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "snapshot_id": None,
        "snapshot_count": None,
        "data_files": None,
        "data_bytes": None,
        "manifest_files": None,
        "manifest_bytes": None,
    }
    try:
        snapshots = spark.table(f"{table}.snapshots")
        result["snapshot_count"] = snapshots.count()
        current = snapshots.orderBy(col("committed_at").desc()).first()
        result["snapshot_id"] = None if current is None else int(current["snapshot_id"])
    except Exception as exc:  # Metadata support differs across Iceberg versions.
        result["snapshot_error"] = type(exc).__name__
    try:
        files = spark.table(f"{table}.files")
        totals = files.agg(
            count("*").alias("data_files"),
            spark_sum("file_size_in_bytes").alias("data_bytes"),
        ).first()
        result["data_files"] = int(totals["data_files"] or 0)
        result["data_bytes"] = int(totals["data_bytes"] or 0)
    except Exception as exc:
        result["files_error"] = type(exc).__name__
    try:
        manifests = spark.table(f"{table}.manifests")
        totals = manifests.agg(
            count("*").alias("manifest_files"),
            spark_sum("length").alias("manifest_bytes"),
        ).first()
        result["manifest_files"] = int(totals["manifest_files"] or 0)
        result["manifest_bytes"] = int(totals["manifest_bytes"] or 0)
    except Exception as exc:
        result["manifests_error"] = type(exc).__name__
    return result


def main() -> None:
    spark = _spark()
    spark.sparkContext.setLogLevel("ERROR")
    tables: dict[str, Any] = {}
    for name, table in TABLES.items():
        if not spark.catalog.tableExists(table):
            tables[name] = {"exists": False, "rows": None, "metadata": None}
            continue
        tables[name] = {
            "exists": True,
            "rows": spark.table(table).count(),
            "metadata": _metadata(spark, table),
        }

    reconciliation: dict[str, Any] | None = None
    if tables["bronze_event_log"]["exists"] and tables["silver_applied_events"]["exists"]:
        bronze = spark.table(TABLES["bronze_event_log"])
        proofs = spark.table(TABLES["silver_applied_events"])
        bronze_ids = bronze.select("event_id")
        proof_ids = proofs.select("event_id")
        reconciliation = {
            "bronze_committed": bronze.count(),
            "silver_application_proofs": proofs.count(),
            "missing_application_proofs": bronze_ids.join(
                proof_ids, ["event_id"], "left_anti"
            ).count(),
            "duplicate_bronze_event_ids": _scalar(
                spark,
                f"SELECT COALESCE(SUM(n - 1), 0) AS n FROM "
                f"(SELECT COUNT(*) AS n FROM {TABLES['bronze_event_log']} "
                "GROUP BY event_id HAVING COUNT(*) > 1)",
                "n",
            ),
            "duplicate_application_proofs": _scalar(
                spark,
                f"SELECT COALESCE(SUM(n - 1), 0) AS n FROM "
                f"(SELECT COUNT(*) AS n FROM {TABLES['silver_applied_events']} "
                "GROUP BY event_id HAVING COUNT(*) > 1)",
                "n",
            ),
            "orphan_application_proofs": proof_ids.join(
                bronze_ids, ["event_id"], "left_anti"
            ).count(),
        }
        reconciliation = {key: int(value or 0) for key, value in reconciliation.items()}
        reconciliation["passed"] = not any(
            reconciliation[key]
            for key in (
                "missing_application_proofs",
                "duplicate_bronze_event_ids",
                "duplicate_application_proofs",
                "orphan_application_proofs",
            )
        )

    payload = {
        "probe_schema_version": "pipeline-system-probe-v1",
        "warehouse_bucket": _env("MINIO_BUCKET", "lakehouse"),
        "spark": {
            "version": spark.version,
            "master": spark.sparkContext.master,
            "default_parallelism": spark.sparkContext.defaultParallelism,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        },
        "tables": tables,
        "reconciliation": reconciliation,
    }
    print("BENCHMARK_PROBE_JSON=" + json.dumps(payload, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
