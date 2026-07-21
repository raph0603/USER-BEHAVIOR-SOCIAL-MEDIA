"""Persist external API usage and pipeline health from SQLite to Iceberg."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


LEGACY_USAGE_TABLE = "lakehouse.monitoring.youtube_api_usage"
EXTERNAL_USAGE_TABLE = "lakehouse.monitoring.external_api_usage"
PIPELINE_HEALTH_TABLE = "lakehouse.monitoring.pipeline_health"
EVENT_LOG_TABLE = "lakehouse.bronze.event_log"
APPLIED_EVENTS_TABLE = "lakehouse.silver.applied_events"

ENDPOINT_QUOTA_UNITS = {
    "search.list": 100,
    "videos.list": 1,
    "channels.list": 1,
    "commentThreads.list": 1,
}

LEGACY_USAGE_COLUMNS = (
    "usage_id",
    "usage_date",
    "endpoint",
    "request_count",
    "resource_count",
    "success_count",
    "error_count",
    "quota_bucket",
    "observed_at",
)

USAGE_SCHEMA = StructType(
    [
        StructField("external_usage_id", StringType(), False),
        StructField("source_usage_id", LongType(), False),
        StructField("usage_date", StringType(), False),
        StructField("provider", StringType(), False),
        StructField("operation", StringType(), False),
        StructField("request_count", IntegerType(), False),
        StructField("resource_count", IntegerType(), False),
        StructField("success_count", IntegerType(), False),
        StructField("error_count", IntegerType(), False),
        StructField("quota_bucket", StringType(), False),
        StructField("quota_units", IntegerType(), False),
        StructField("quota_cost_per_request", IntegerType(), False),
        StructField("daily_budget_units", IntegerType(), True),
        StructField("reserved_units", IntegerType(), True),
        StructField("remaining_units", IntegerType(), True),
        StructField("reserve_remaining_units", IntegerType(), True),
        StructField("video_minutes", DoubleType(), False),
        StructField("daily_video_minutes_budget", DoubleType(), True),
        StructField("remaining_video_minutes", DoubleType(), True),
        StructField("priority", StringType(), True),
        StructField("cache_hit_count", IntegerType(), False),
        StructField("cache_miss_count", IntegerType(), False),
        StructField("retry_count", IntegerType(), False),
        StructField("latency_ms", DoubleType(), True),
        StructField("queue_depth", IntegerType(), True),
        StructField("oldest_queue_age_seconds", DoubleType(), True),
        StructField("circuit_open", BooleanType(), False),
        StructField("status", StringType(), False),
        StructField("error_code", StringType(), True),
        StructField("producer_run_id", StringType(), False),
        StructField("observed_at", StringType(), False),
    ]
)

HEALTH_SCHEMA = StructType(
    [
        StructField("health_id", StringType(), False),
        StructField("observed_at", StringType(), False),
        StructField("producer_run_id", StringType(), False),
        StructField("component", StringType(), False),
        StructField("status", StringType(), False),
        StructField("processed_count", IntegerType(), False),
        StructField("success_count", IntegerType(), False),
        StructField("error_count", IntegerType(), False),
        StructField("retry_count", IntegerType(), False),
        StructField("cache_hit_count", IntegerType(), False),
        StructField("cache_miss_count", IntegerType(), False),
        StructField("latency_ms", DoubleType(), True),
        StructField("queue_depth", IntegerType(), True),
        StructField("oldest_queue_age_seconds", DoubleType(), True),
        StructField("circuit_open", BooleanType(), False),
        StructField("bronze_lag_seconds", DoubleType(), True),
        StructField("silver_lag_seconds", DoubleType(), True),
        StructField("bronze_silver_gap", LongType(), True),
        StructField("details_json", StringType(), True),
    ]
)


def _env(name: str, default: str) -> str:
    return os.getenv(name) or default


def _positive_seconds(name: str, default: float) -> float:
    try:
        value = float(_env(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _stable_id(*parts: object) -> str:
    payload = "\x1f".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _optional_integer(value: Any) -> int | None:
    return None if value is None else _integer(value)


def _optional_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        is not None
    )


def _read_sqlite_rows(path: Path, table: str) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        if not _table_exists(connection, table):
            return []
        rows = connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def load_usage(path: Path) -> list[dict[str, Any]]:
    """Load both current and pre-migration SQLite usage rows."""

    normalized: list[dict[str, Any]] = []
    for row in _read_sqlite_rows(path, "youtube_api_usage"):
        usage_id = _integer(row.get("usage_id"))
        endpoint = str(row.get("operation") or row.get("endpoint") or "unknown")
        request_count = max(0, _integer(row.get("request_count")))
        unit_cost = max(
            0,
            _integer(
                row.get("quota_cost_per_request"),
                ENDPOINT_QUOTA_UNITS.get(endpoint, 0),
            ),
        )
        quota_units = max(
            0,
            _integer(row.get("quota_units"), request_count * unit_cost),
        )
        observed_at = str(row.get("observed_at") or "")
        provider = str(row.get("provider") or "youtube")
        status = str(
            row.get("status") or ("error" if _integer(row.get("error_count")) else "success")
        )
        normalized.append(
            {
                "external_usage_id": _stable_id(
                    "external-api-usage",
                    usage_id,
                    provider,
                    endpoint,
                    observed_at,
                ),
                "source_usage_id": usage_id,
                "usage_date": str(row.get("usage_date") or observed_at[:10]),
                "provider": provider,
                "operation": endpoint,
                "request_count": request_count,
                "resource_count": max(0, _integer(row.get("resource_count"))),
                "success_count": max(0, _integer(row.get("success_count"))),
                "error_count": max(0, _integer(row.get("error_count"))),
                "quota_bucket": str(row.get("quota_bucket") or "legacy"),
                "quota_units": quota_units,
                "quota_cost_per_request": unit_cost,
                "daily_budget_units": _optional_integer(row.get("daily_budget_units")),
                "reserved_units": _optional_integer(row.get("reserved_units")),
                "remaining_units": _optional_integer(row.get("remaining_units")),
                "reserve_remaining_units": _optional_integer(row.get("reserve_remaining_units")),
                "video_minutes": max(0.0, _optional_float(row.get("video_minutes")) or 0.0),
                "daily_video_minutes_budget": _optional_float(
                    row.get("daily_video_minutes_budget")
                ),
                "remaining_video_minutes": _optional_float(
                    row.get("remaining_video_minutes")
                ),
                "priority": row.get("priority"),
                "cache_hit_count": max(0, _integer(row.get("cache_hit_count"))),
                "cache_miss_count": max(0, _integer(row.get("cache_miss_count"))),
                "retry_count": max(0, _integer(row.get("retry_count"))),
                "latency_ms": _optional_float(row.get("latency_ms")),
                "queue_depth": _optional_integer(row.get("queue_depth")),
                "oldest_queue_age_seconds": _optional_float(row.get("oldest_queue_age_seconds")),
                "circuit_open": bool(row.get("circuit_open")),
                "status": status,
                "error_code": row.get("error_code"),
                "producer_run_id": str(row.get("producer_run_id") or "legacy"),
                "observed_at": observed_at,
            }
        )
    return normalized


def load_worker_health(path: Path) -> list[dict[str, Any]]:
    """Load durable worker summaries without requiring the newest schema."""

    normalized: list[dict[str, Any]] = []
    queue_warning_seconds = _positive_seconds(
        "PIPELINE_QUEUE_WARNING_AGE_SECONDS",
        900,
    )
    for row in _read_sqlite_rows(path, "youtube_worker_health"):
        observed_at = str(row.get("observed_at") or "")
        run_id = str(row.get("producer_run_id") or "legacy")
        component = str(row.get("worker_name") or "youtube_worker")
        oldest_queue_age = _optional_float(row.get("oldest_queue_age_seconds"))
        status = str(row.get("status") or "unknown")
        if (
            oldest_queue_age is not None
            and oldest_queue_age > queue_warning_seconds
            and status in {"idle", "success"}
        ):
            status = "warning"
        normalized.append(
            {
                "health_id": _stable_id(
                    "worker-health",
                    row.get("health_id"),
                    run_id,
                    component,
                    observed_at,
                ),
                "observed_at": observed_at,
                "producer_run_id": run_id,
                "component": component,
                "status": status,
                "processed_count": max(0, _integer(row.get("processed_count"))),
                "success_count": max(0, _integer(row.get("success_count"))),
                "error_count": max(0, _integer(row.get("error_count"))),
                "retry_count": max(0, _integer(row.get("retry_count"))),
                "cache_hit_count": max(0, _integer(row.get("cache_hit_count"))),
                "cache_miss_count": max(0, _integer(row.get("cache_miss_count"))),
                "latency_ms": _optional_float(row.get("latency_ms")),
                "queue_depth": _optional_integer(row.get("queue_depth")),
                "oldest_queue_age_seconds": oldest_queue_age,
                "circuit_open": bool(row.get("circuit_open")),
                "bronze_lag_seconds": None,
                "silver_lag_seconds": None,
                "bronze_silver_gap": None,
                "details_json": row.get("details_json"),
            }
        )
    return normalized


def _spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("external-api-and-pipeline-health")
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


def _ensure_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.monitoring")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {LEGACY_USAGE_TABLE} (
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
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {EXTERNAL_USAGE_TABLE} (
          external_usage_id STRING,
          source_usage_id BIGINT,
          usage_date DATE,
          provider STRING,
          operation STRING,
          request_count INT,
          resource_count INT,
          success_count INT,
          error_count INT,
          quota_bucket STRING,
          quota_units INT,
          quota_cost_per_request INT,
          daily_budget_units INT,
          reserved_units INT,
          remaining_units INT,
          reserve_remaining_units INT,
          video_minutes DOUBLE,
          daily_video_minutes_budget DOUBLE,
          remaining_video_minutes DOUBLE,
          priority STRING,
          cache_hit_count INT,
          cache_miss_count INT,
          retry_count INT,
          latency_ms DOUBLE,
          queue_depth INT,
          oldest_queue_age_seconds DOUBLE,
          circuit_open BOOLEAN,
          status STRING,
          error_code STRING,
          producer_run_id STRING,
          observed_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(observed_at))
        """
    )
    current_usage_columns = set(spark.table(EXTERNAL_USAGE_TABLE).columns)
    for name, data_type in {
        "video_minutes": "DOUBLE",
        "daily_video_minutes_budget": "DOUBLE",
        "remaining_video_minutes": "DOUBLE",
    }.items():
        if name not in current_usage_columns:
            spark.sql(f"ALTER TABLE {EXTERNAL_USAGE_TABLE} ADD COLUMN {name} {data_type}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {PIPELINE_HEALTH_TABLE} (
          health_id STRING,
          observed_at TIMESTAMP,
          producer_run_id STRING,
          component STRING,
          status STRING,
          processed_count INT,
          success_count INT,
          error_count INT,
          retry_count INT,
          cache_hit_count INT,
          cache_miss_count INT,
          latency_ms DOUBLE,
          queue_depth INT,
          oldest_queue_age_seconds DOUBLE,
          circuit_open BOOLEAN,
          bronze_lag_seconds DOUBLE,
          silver_lag_seconds DOUBLE,
          bronze_silver_gap BIGINT,
          details_json STRING
        ) USING iceberg
        PARTITIONED BY (days(observed_at))
        """
    )


def _age_seconds(observed_at: datetime, value: datetime | None) -> float | None:
    if value is None:
        return None
    normalized = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    return max(0.0, (observed_at - normalized.astimezone(timezone.utc)).total_seconds())


def build_boundary_health(spark: SparkSession, observed_at: datetime) -> dict[str, Any]:
    """Measure durable Bronze/Silver freshness and the unapplied-event gap."""

    run_id = _env("PIPELINE_RUN_ID", "standalone")
    missing_tables = [
        table
        for table in (EVENT_LOG_TABLE, APPLIED_EVENTS_TABLE)
        if not spark.catalog.tableExists(table)
    ]
    if missing_tables:
        details: dict[str, Any] = {"missing_tables": missing_tables}
        return {
            "health_id": _stable_id("pipeline-boundary", run_id, observed_at.isoformat()),
            "observed_at": observed_at.isoformat(),
            "producer_run_id": run_id,
            "component": "bronze_silver_boundary",
            "status": "error",
            "processed_count": 0,
            "success_count": 0,
            "error_count": len(missing_tables),
            "retry_count": 0,
            "cache_hit_count": 0,
            "cache_miss_count": 0,
            "latency_ms": None,
            "queue_depth": None,
            "oldest_queue_age_seconds": None,
            "circuit_open": False,
            "bronze_lag_seconds": None,
            "silver_lag_seconds": None,
            "bronze_silver_gap": None,
            "details_json": json.dumps(details, sort_keys=True),
        }

    event_log = spark.table(EVENT_LOG_TABLE)
    applied = spark.table(APPLIED_EVENTS_TABLE)
    bronze_stats = event_log.agg({"ingested_at": "max"}).first()
    silver_stats = applied.agg({"applied_at": "max"}).first()
    bronze_latest = bronze_stats[0] if bronze_stats else None
    silver_latest = silver_stats[0] if silver_stats else None
    gap = (
        event_log.select("event_id")
        .join(applied.select("event_id"), ["event_id"], "left_anti")
        .count()
    )
    bronze_lag = _age_seconds(observed_at, bronze_latest)
    silver_lag = _age_seconds(observed_at, silver_latest)
    bronze_threshold = _positive_seconds("PIPELINE_BRONZE_LAG_WARNING_SECONDS", 600)
    silver_threshold = _positive_seconds("PIPELINE_SILVER_LAG_WARNING_SECONDS", 900)
    empty = bronze_latest is None and silver_latest is None
    warning = (
        empty
        or (bronze_lag is not None and bronze_lag > bronze_threshold)
        or (silver_lag is not None and silver_lag > silver_threshold)
    )
    status = "error" if gap else "warning" if warning else "success"
    details = {
        "bronze_latest_at": bronze_latest,
        "silver_latest_at": silver_latest,
        "bronze_lag_warning_seconds": bronze_threshold,
        "silver_lag_warning_seconds": silver_threshold,
        "empty_boundary": empty,
    }
    return {
        "health_id": _stable_id("pipeline-boundary", run_id, observed_at.isoformat()),
        "observed_at": observed_at.isoformat(),
        "producer_run_id": run_id,
        "component": "bronze_silver_boundary",
        "status": status,
        "processed_count": 0,
        "success_count": 0 if gap else 1,
        "error_count": 1 if gap else 0,
        "retry_count": 0,
        "cache_hit_count": 0,
        "cache_miss_count": 0,
        "latency_ms": None,
        "queue_depth": None,
        "oldest_queue_age_seconds": None,
        "circuit_open": False,
        "bronze_lag_seconds": bronze_lag,
        "silver_lag_seconds": silver_lag,
        "bronze_silver_gap": gap,
        "details_json": json.dumps(details, sort_keys=True, default=str),
    }


def _merge_insert_only(frame: DataFrame, *, table: str, key: str, view: str) -> int:
    rows = frame.count()
    if not rows:
        return 0
    target_columns = frame.sparkSession.table(table).columns
    frame.select(*target_columns).createOrReplaceTempView(view)
    frame.sparkSession.sql(
        f"""
        MERGE INTO {table} AS target
        USING {view} AS source
        ON target.{key} = source.{key}
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return rows


def _usage_frame(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame | None:
    if not rows:
        return None
    return (
        spark.createDataFrame(rows, schema=USAGE_SCHEMA)
        .withColumn("usage_date", to_date(col("usage_date")))
        .withColumn("observed_at", to_timestamp(col("observed_at")))
        .dropDuplicates(["external_usage_id"])
    )


def _health_frame(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    return (
        spark.createDataFrame(rows, schema=HEALTH_SCHEMA)
        .withColumn("observed_at", to_timestamp(col("observed_at")))
        .dropDuplicates(["health_id"])
    )


def main() -> None:
    state_path = Path(
        _env(
            "YOUTUBE_PIPELINE_STATE_DB",
            "/opt/spark/collector-state/youtube-pipeline.sqlite",
        )
    )
    usage_rows = load_usage(state_path)
    health_rows = load_worker_health(state_path)
    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")
    _ensure_tables(spark)

    usage = _usage_frame(spark, usage_rows)
    usage_count = 0
    if usage is not None:
        usage_count = _merge_insert_only(
            usage,
            table=EXTERNAL_USAGE_TABLE,
            key="external_usage_id",
            view="incoming_external_api_usage",
        )
        legacy = usage.selectExpr(
            "source_usage_id AS usage_id",
            "usage_date",
            "operation AS endpoint",
            "request_count",
            "resource_count",
            "success_count",
            "error_count",
            "quota_bucket",
            "observed_at",
        )
        _merge_insert_only(
            legacy,
            table=LEGACY_USAGE_TABLE,
            key="usage_id",
            view="incoming_youtube_api_usage",
        )

    observed_at = datetime.now(timezone.utc)
    health_rows.append(build_boundary_health(spark, observed_at))
    health_count = _merge_insert_only(
        _health_frame(spark, health_rows),
        table=PIPELINE_HEALTH_TABLE,
        key="health_id",
        view="incoming_pipeline_health",
    )
    print(
        json.dumps(
            {
                "event": "pipeline_monitoring_persisted",
                "external_usage_rows": usage_count,
                "pipeline_health_rows": health_count,
            },
            sort_keys=True,
        )
    )
    spark.stop()


if __name__ == "__main__":
    main()
