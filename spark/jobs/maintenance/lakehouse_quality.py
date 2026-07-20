"""Evaluate configurable Iceberg quality rules and persist every result."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, max as spark_max, sum as spark_sum, when
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.quality_rules import (
    QualityConfig,
    QualityProfile,
    QualityRule,
    canonical_json,
    empty_outcome,
    find_orphan_files,
    load_quality_config,
    parse_threshold_overrides,
    result_causes_failure,
    stable_result_id,
)


RESULTS_TABLE = "lakehouse.monitoring.data_quality_results"
DEFAULT_RULES_PATH = Path(__file__).resolve().parents[1] / "config" / "lakehouse_quality_rules.json"
RESULT_COLUMNS = (
    "result_id",
    "observed_at",
    "producer_run_id",
    "profile",
    "rules_schema_version",
    "rule_id",
    "rule_kind",
    "severity",
    "status",
    "table_name",
    "metric_name",
    "metric_value",
    "threshold_json",
    "message",
    "details_json",
)
RESULT_SCHEMA = StructType(
    [
        StructField("result_id", StringType(), False),
        StructField("observed_at", TimestampType(), False),
        StructField("producer_run_id", StringType(), False),
        StructField("profile", StringType(), False),
        StructField("rules_schema_version", StringType(), False),
        StructField("rule_id", StringType(), False),
        StructField("rule_kind", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("table_name", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("threshold_json", StringType(), False),
        StructField("message", StringType(), False),
        StructField("details_json", StringType(), False),
    ]
)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("lakehouse-configurable-quality")
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
        .config("spark.sql.shuffle.partitions", _env("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run configurable schema, integrity, freshness, and orphan-file checks"
    )
    parser.add_argument(
        "--rules",
        type=Path,
        default=Path(_env("LAKEHOUSE_QUALITY_RULES_PATH", str(DEFAULT_RULES_PATH))),
    )
    parser.add_argument(
        "--profile",
        default=_env("LAKEHOUSE_QUALITY_PROFILE", "standard"),
    )
    parser.add_argument(
        "--threshold-overrides",
        default=os.getenv("LAKEHOUSE_QUALITY_THRESHOLD_OVERRIDES"),
        help="Inline JSON or a JSON file path mapping rule IDs to threshold overrides",
    )
    parser.add_argument(
        "--fail-on",
        default=os.getenv("LAKEHOUSE_QUALITY_FAIL_ON"),
        help="Comma-separated severities that make the command fail; use 'none' for report-only",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--observed-at")
    report_output = os.getenv("LAKEHOUSE_QUALITY_REPORT_PATH")
    parser.add_argument(
        "--report-output",
        type=Path,
        default=Path(report_output) if report_output else None,
    )
    return parser


def _parse_observed_at(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_fail_on(value: str | None) -> set[str] | None:
    if value is None:
        return None
    tokens = {token.strip().lower() for token in value.split(",") if token.strip()}
    if tokens == {"none"}:
        return set()
    if "none" in tokens:
        raise ValueError("--fail-on 'none' cannot be combined with severities")
    return tokens


@dataclass(frozen=True)
class Evaluation:
    status: str
    severity: str
    table_name: str | None
    metric_name: str | None
    metric_value: float | None
    message: str
    details: dict[str, Any]


class QualityEvaluator:
    def __init__(
        self,
        spark: SparkSession,
        *,
        profile: QualityProfile,
        observed_at: datetime,
    ) -> None:
        self.spark = spark
        self.profile = profile
        self.observed_at = observed_at
        self._frames: dict[str, DataFrame] = {}
        self._counts: dict[str, int] = {}

    def _frame(self, table: str) -> DataFrame:
        if not self.spark.catalog.tableExists(table):
            raise LookupError(f"Required table does not exist: {table}")
        if table not in self._frames:
            self._frames[table] = self.spark.table(table)
        return self._frames[table]

    def _count(self, table: str) -> int:
        if table not in self._counts:
            self._counts[table] = self._frame(table).count()
        return self._counts[table]

    @staticmethod
    def _require_columns(frame: DataFrame, columns: list[str]) -> None:
        missing = sorted(set(columns) - set(frame.columns))
        if missing:
            raise LookupError(f"Required columns are missing: {missing}")

    def _empty_evaluation(
        self,
        *,
        table: str,
        metric_name: str,
        message: str,
        metric_value: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> Evaluation:
        status, severity = empty_outcome(self.profile)
        return Evaluation(
            status,
            severity,
            table,
            metric_name,
            metric_value,
            message,
            details or {"row_count": 0},
        )

    @staticmethod
    def _threshold_evaluation(
        *,
        passed: bool,
        rule: QualityRule,
        table: str | None,
        metric_name: str,
        metric_value: float,
        passed_message: str,
        failed_message: str,
        details: dict[str, Any],
    ) -> Evaluation:
        return Evaluation(
            "passed" if passed else "failed",
            rule.severity,
            table,
            metric_name,
            metric_value,
            passed_message if passed else failed_message,
            details,
        )

    def evaluate(self, rule: QualityRule) -> Evaluation:
        handler = getattr(self, f"_evaluate_{rule.kind}")
        return handler(rule)

    def _evaluate_schema(self, rule: QualityRule) -> Evaluation:
        table = str(rule.options["table"])
        frame = self._frame(table)
        actual = {
            field.name: field.dataType.simpleString().lower() for field in frame.schema.fields
        }
        required = dict(rule.options["required_columns"])
        missing = sorted(set(required) - set(actual))
        mismatches = {
            column: {"expected": expected, "actual": actual[column]}
            for column, expected in required.items()
            if column in actual and expected not in {"*", actual[column]}
        }
        errors = len(missing) + len(mismatches)
        return self._threshold_evaluation(
            passed=errors == 0,
            rule=rule,
            table=table,
            metric_name="schema_errors",
            metric_value=float(errors),
            passed_message=f"Schema contract satisfied for {table}",
            failed_message=f"Schema contract failed for {table}",
            details={"missing_columns": missing, "type_mismatches": mismatches},
        )

    def _evaluate_unique_key(self, rule: QualityRule) -> Evaluation:
        table = str(rule.options["table"])
        frame = self._frame(table)
        columns = list(rule.options["columns"])
        self._require_columns(frame, columns)
        duplicate_row = (
            frame.groupBy(*columns)
            .count()
            .filter(col("count") > 1)
            .agg(spark_sum(col("count") - lit(1)).alias("duplicates"))
            .first()
        )
        duplicates = int((duplicate_row["duplicates"] if duplicate_row else None) or 0)
        maximum = int(rule.options["max_duplicates"])
        return self._threshold_evaluation(
            passed=duplicates <= maximum,
            rule=rule,
            table=table,
            metric_name="duplicate_rows",
            metric_value=float(duplicates),
            passed_message=f"Business key is unique in {table}",
            failed_message=f"Business key contains {duplicates} duplicate rows in {table}",
            details={"columns": columns, "max_duplicates": maximum},
        )

    def _evaluate_partitions(self, rule: QualityRule) -> Evaluation:
        table = str(rule.options["table"])
        self._frame(table)
        partition_count = self.spark.table(f"{table}.partitions").count()
        minimum = int(rule.options["min_partitions"])
        if partition_count == 0:
            return self._empty_evaluation(
                table=table,
                metric_name="partition_count",
                message=f"No populated partitions are visible for {table}",
                metric_value=0.0,
            )
        return self._threshold_evaluation(
            passed=partition_count >= minimum,
            rule=rule,
            table=table,
            metric_name="partition_count",
            metric_value=float(partition_count),
            passed_message=f"Partition count satisfied for {table}",
            failed_message=f"Partition count is below {minimum} for {table}",
            details={"min_partitions": minimum},
        )

    def _evaluate_table_empty(self, rule: QualityRule) -> Evaluation:
        table = str(rule.options["table"])
        row_count = self._count(table)
        minimum = int(rule.options["min_rows"])
        if row_count == 0:
            return self._empty_evaluation(
                table=table,
                metric_name="row_count",
                message=f"Table {table} is empty",
                metric_value=0.0,
            )
        return self._threshold_evaluation(
            passed=row_count >= minimum,
            rule=rule,
            table=table,
            metric_name="row_count",
            metric_value=float(row_count),
            passed_message=f"Table {table} contains {row_count} rows",
            failed_message=f"Table {table} contains fewer than {minimum} rows",
            details={"min_rows": minimum},
        )

    def _evaluate_completeness(self, rule: QualityRule) -> Evaluation:
        table = str(rule.options["table"])
        frame = self._frame(table)
        columns = list(rule.options["columns"])
        self._require_columns(frame, columns)
        total = self._count(table)
        if total == 0:
            return self._empty_evaluation(
                table=table,
                metric_name="minimum_completeness_rate",
                message=f"Completeness cannot be measured because {table} is empty",
            )
        aggregate = frame.agg(
            *[
                spark_sum(when(col(column).isNotNull(), 1).otherwise(0)).alias(column)
                for column in columns
            ]
        ).first()
        if aggregate is None:
            raise RuntimeError(f"Completeness aggregation returned no result for {table}")
        rates = {column: float(aggregate[column] or 0) / total for column in columns}
        minimum_rate = min(rates.values())
        threshold = float(rule.options["min_rate"])
        return self._threshold_evaluation(
            passed=minimum_rate >= threshold,
            rule=rule,
            table=table,
            metric_name="minimum_completeness_rate",
            metric_value=minimum_rate,
            passed_message=f"Completeness threshold satisfied for {table}",
            failed_message=f"Completeness fell below {threshold:.3f} for {table}",
            details={"column_rates": rates, "row_count": total},
        )

    def _evaluate_volume_ratio(self, rule: QualityRule) -> Evaluation:
        upstream = str(rule.options["upstream_table"])
        downstream = str(rule.options["downstream_table"])
        upstream_count = self._count(upstream)
        downstream_count = self._count(downstream)
        table_name = f"{upstream}->{downstream}"
        if upstream_count == 0:
            return self._empty_evaluation(
                table=table_name,
                metric_name="downstream_upstream_ratio",
                message=f"Volume ratio cannot be measured because {upstream} is empty",
                details={
                    "upstream_count": upstream_count,
                    "downstream_count": downstream_count,
                },
            )
        ratio = downstream_count / float(upstream_count)
        minimum = float(rule.options["min_ratio"])
        maximum = float(rule.options["max_ratio"])
        return self._threshold_evaluation(
            passed=minimum <= ratio <= maximum,
            rule=rule,
            table=table_name,
            metric_name="downstream_upstream_ratio",
            metric_value=ratio,
            passed_message="Inter-stage volume ratio is within bounds",
            failed_message="Inter-stage volume ratio is outside configured bounds",
            details={
                "upstream_count": upstream_count,
                "downstream_count": downstream_count,
                "min_ratio": minimum,
                "max_ratio": maximum,
            },
        )

    def _evaluate_freshness(self, rule: QualityRule) -> Evaluation:
        table = str(rule.options["table"])
        frame = self._frame(table)
        timestamp_column = str(rule.options["timestamp_column"])
        self._require_columns(frame, [timestamp_column])
        latest_row = frame.agg(
            spark_max(col(timestamp_column).cast("timestamp")).alias("latest")
        ).first()
        latest = latest_row["latest"] if latest_row else None
        if latest is None:
            row_count = self._count(table)
            return self._empty_evaluation(
                table=table,
                metric_name="freshness_age_minutes",
                message=f"No timestamp is available to measure freshness for {table}",
                details={
                    "row_count": row_count,
                    "timestamp_column": timestamp_column,
                },
            )
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age_minutes = max(
            0.0,
            (self.observed_at - latest.astimezone(timezone.utc)).total_seconds() / 60.0,
        )
        maximum = float(rule.options["max_age_minutes"])
        return self._threshold_evaluation(
            passed=age_minutes <= maximum,
            rule=rule,
            table=table,
            metric_name="freshness_age_minutes",
            metric_value=age_minutes,
            passed_message=f"Freshness threshold satisfied for {table}",
            failed_message=f"Freshness exceeded {maximum:g} minutes for {table}",
            details={"latest_timestamp": latest.isoformat(), "max_age_minutes": maximum},
        )

    def _table_location(self, table: str) -> str:
        rows = self.spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        for row in rows:
            values = row.asDict(recursive=True)
            if str(values.get("col_name") or "").strip().lower() == "location":
                location = str(values.get("data_type") or "").strip()
                if location:
                    return location
        raise RuntimeError(f"Iceberg table location is unavailable for {table}")

    def _referenced_files(self, table: str, *, max_objects: int) -> set[str]:
        referenced: set[str] = set()
        metadata_frames = [
            self.spark.table(f"{table}.all_data_files").select("file_path"),
            self.spark.table(f"{table}.all_delete_files").select("file_path"),
        ]
        for frame in metadata_frames:
            for row in frame.toLocalIterator():
                value = row["file_path"]
                if value:
                    referenced.add(str(value))
                if len(referenced) > max_objects:
                    raise RuntimeError(
                        f"Referenced-file count exceeded configured max_objects={max_objects}"
                    )
        return referenced

    def _listed_parquet_files(self, location: str, *, max_objects: int) -> set[str]:
        jvm = getattr(self.spark, "_jvm")
        jsc = getattr(self.spark, "_jsc")
        root = jvm.org.apache.hadoop.fs.Path(location)
        filesystem = root.getFileSystem(jsc.hadoopConfiguration())
        iterator = filesystem.listFiles(root, True)
        objects: set[str] = set()
        while iterator.hasNext():
            path = str(iterator.next().getPath().toString())
            if path.lower().endswith(".parquet"):
                objects.add(path)
                if len(objects) > max_objects:
                    raise RuntimeError(
                        f"Object count exceeded configured max_objects={max_objects}"
                    )
        return objects

    def _evaluate_orphan_files(self, rule: QualityRule) -> Evaluation:
        table = str(rule.options["table"])
        self._frame(table)
        max_objects = int(rule.options["max_objects"])
        location = self._table_location(table)
        referenced = self._referenced_files(table, max_objects=max_objects)
        objects = self._listed_parquet_files(location, max_objects=max_objects)
        orphans = find_orphan_files(objects, referenced)
        maximum = int(rule.options["max_orphans"])
        sample_limit = int(rule.options["sample_limit"])
        return self._threshold_evaluation(
            passed=len(orphans) <= maximum,
            rule=rule,
            table=table,
            metric_name="orphan_file_count",
            metric_value=float(len(orphans)),
            passed_message=f"No excess orphan data files detected for {table}",
            failed_message=f"Detected {len(orphans)} orphan data files for {table}",
            details={
                "table_location": location,
                "listed_parquet_files": len(objects),
                "referenced_files": len(referenced),
                "orphan_samples": orphans[:sample_limit],
                "deletion_performed": False,
            },
        )


def ensure_quality_results_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.monitoring")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
          result_id STRING,
          observed_at TIMESTAMP,
          producer_run_id STRING,
          profile STRING,
          rules_schema_version STRING,
          rule_id STRING,
          rule_kind STRING,
          severity STRING,
          status STRING,
          table_name STRING,
          metric_name STRING,
          metric_value DOUBLE,
          threshold_json STRING,
          message STRING,
          details_json STRING
        ) USING iceberg
        PARTITIONED BY (days(observed_at))
        """
    )


def _persist_results(spark: SparkSession, rows: list[dict[str, Any]]) -> None:
    frame = spark.createDataFrame(rows, schema=RESULT_SCHEMA).select(*RESULT_COLUMNS)
    frame.createOrReplaceTempView("incoming_data_quality_results")
    columns = ", ".join(RESULT_COLUMNS)
    values = ", ".join(f"source.{column}" for column in RESULT_COLUMNS)
    spark.sql(
        f"""
        MERGE INTO {RESULTS_TABLE} AS target
        USING incoming_data_quality_results AS source
        ON target.result_id = source.result_id
        WHEN NOT MATCHED THEN
          INSERT ({columns}) VALUES ({values})
        """
    )


def _evaluate_rules(
    spark: SparkSession,
    config: QualityConfig,
    *,
    run_id: str,
    observed_at: datetime,
) -> list[dict[str, Any]]:
    evaluator = QualityEvaluator(spark, profile=config.profile, observed_at=observed_at)
    rows = []
    for rule in config.rules:
        try:
            evaluation = evaluator.evaluate(rule)
        except Exception as exc:
            table_name = str(
                rule.options.get("table")
                or f"{rule.options.get('upstream_table')}->{rule.options.get('downstream_table')}"
            )
            evaluation = Evaluation(
                "failed",
                rule.severity,
                table_name,
                None,
                None,
                f"Rule evaluation raised {type(exc).__name__}",
                {"exception_type": type(exc).__name__},
            )
        rows.append(
            {
                "result_id": stable_result_id(run_id, config.profile.name, rule.rule_id),
                "observed_at": observed_at,
                "producer_run_id": run_id,
                "profile": config.profile.name,
                "rules_schema_version": config.schema_version,
                "rule_id": rule.rule_id,
                "rule_kind": rule.kind,
                "severity": evaluation.severity,
                "status": evaluation.status,
                "table_name": evaluation.table_name,
                "metric_name": evaluation.metric_name,
                "metric_value": evaluation.metric_value,
                "threshold_json": canonical_json(rule.threshold_options()),
                "message": evaluation.message,
                "details_json": canonical_json(evaluation.details),
            }
        )
    return rows


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    observed_at = _parse_observed_at(args.observed_at)
    run_id = args.run_id or os.getenv("PIPELINE_RUN_ID") or f"quality-{observed_at:%Y%m%dT%H%M%SZ}"
    config = load_quality_config(
        args.rules,
        profile_name=args.profile,
        threshold_overrides=parse_threshold_overrides(args.threshold_overrides),
        fail_severities=_parse_fail_on(args.fail_on),
    )

    spark = _spark()
    try:
        spark.sparkContext.setLogLevel("WARN")
        ensure_quality_results_table(spark)
        rows = _evaluate_rules(spark, config, run_id=run_id, observed_at=observed_at)
        _persist_results(spark, rows)
    finally:
        spark.stop()

    failures = [
        row for row in rows if result_causes_failure(row["status"], row["severity"], config.profile)
    ]
    counts = {
        status: sum(1 for row in rows if row["status"] == status)
        for status in ("passed", "anomaly", "failed")
    }
    report = {
        "run_id": run_id,
        "observed_at": observed_at.isoformat(),
        "profile": config.profile.name,
        "rules_schema_version": config.schema_version,
        "counts": counts,
        "failed_rule_ids": [row["rule_id"] for row in failures],
        "results": [
            {
                "rule_id": row["rule_id"],
                "severity": row["severity"],
                "status": row["status"],
                "metric_value": row["metric_value"],
                "message": row["message"],
            }
            for row in rows
        ],
    }
    if args.report_output:
        _write_report(args.report_output, report)
    print(json.dumps(report, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
