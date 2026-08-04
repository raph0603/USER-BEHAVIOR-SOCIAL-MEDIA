"""Export and validate one real X event across RAW, Bronze, Silver, and Gold."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.x_lineage import expected_clean_text, redaction_summary, sensitive_values


BRONZE_TABLES = (
    "lakehouse.bronze.event_log",
    "lakehouse.bronze.events",
)
SILVER_TABLES = (
    "lakehouse.silver.events",
    "lakehouse.silver.applied_events",
    "lakehouse.silver.contents",
    "lakehouse.silver.engagement_snapshots",
    "lakehouse.silver.post_features",
)
GOLD_TABLES = (
    "lakehouse.gold.content_stats",
    "lakehouse.gold.user_evolution",
    "lakehouse.gold.model_predictions",
    "lakehouse.gold.training_examples",
)
MANDATORY_TABLES = {
    "lakehouse.bronze.event_log",
    "lakehouse.bronze.events",
    "lakehouse.silver.events",
    "lakehouse.silver.applied_events",
    "lakehouse.silver.contents",
    "lakehouse.silver.post_features",
    "lakehouse.gold.content_stats",
    "lakehouse.gold.user_evolution",
}


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _build_spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("export-single-x-lineage")
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
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(_json_value(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _or_filter(frame: DataFrame, identifiers: dict[str, set[str]]) -> DataFrame:
    condition = None
    for column_name, values in identifiers.items():
        if column_name not in frame.columns or not values:
            continue
        candidate = col(column_name).cast("string").isin(*sorted(values))
        condition = candidate if condition is None else condition | candidate
    if condition is None:
        return frame.filter(lit(False))
    if "source" in frame.columns:
        condition = condition & (col("source") == "x")
    return frame.filter(condition)


def _collect_table(
    spark: SparkSession,
    table: str,
    identifiers: dict[str, set[str]],
    *,
    warnings: list[str],
    errors: list[str],
) -> list[dict[str, Any]]:
    if not spark.catalog.tableExists(table):
        message = f"table_absent:{table}"
        (errors if table in MANDATORY_TABLES else warnings).append(message)
        return []

    frame = spark.table(table)
    if table == "lakehouse.gold.content_stats":
        content_ids = identifiers.get("content_id") or set()
        matched = (
            frame.filter(
                (col("source") == "x")
                & col("content_id").cast("string").isin(*sorted(content_ids))
            )
            if content_ids
            else frame.filter(lit(False))
        )
    elif table == "lakehouse.gold.user_evolution":
        condition = col("source") == "x"
        user_hashes = identifiers.get("user_id_hash") or set()
        event_dates = identifiers.get("event_date") or set()
        if user_hashes:
            condition = condition & col("user_id_hash").cast("string").isin(
                *sorted(user_hashes)
            )
        if event_dates:
            condition = condition & col("event_date").cast("string").isin(
                *sorted(event_dates)
            )
        matched = frame.filter(condition) if user_hashes and event_dates else frame.filter(lit(False))
    else:
        matched = _or_filter(frame, identifiers)
    rows = [
        _json_value(row.asDict(recursive=True))
        for row in matched.limit(2).collect()
    ]
    if len(rows) > 1:
        errors.append(f"multiple_rows_for_single_event:{table}:{len(rows)}+")
        return rows[:1]
    if not rows and table in MANDATORY_TABLES:
        errors.append(f"mandatory_row_absent:{table}")
    return rows


def _snapshot_id(spark: SparkSession, table: str) -> int | None:
    if not spark.catalog.tableExists(table):
        return None
    try:
        row = spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()
    except Exception:
        return None
    return int(row["snapshot_id"]) if row and row["snapshot_id"] is not None else None


def _first(table_rows: dict[str, list[dict[str, Any]]], table: str) -> dict[str, Any]:
    rows = table_rows.get(table) or []
    return rows[0] if rows else {}


def _lineage(platform_event_id: str) -> dict[str, Any]:
    return {
        "source": "x",
        "platform_event_id": platform_event_id,
        "stages": [
            {
                "stage": "raw",
                "input": "X browser collector",
                "output": "raw.json",
                "transformations": [],
            },
            {
                "stage": "clean",
                "input": "raw.json",
                "output": "x.clean.events",
                "transformations": [
                    "@username → <USER>",
                    "email → <EMAIL>",
                    "phone → <PHONE>",
                    "IP → <IP>",
                    "URL → <URL>",
                    "source identity → salted SHA-256",
                    "Bronze-facing JSON payloads → privacy-cleaned JSON",
                ],
            },
            {
                "stage": "bronze",
                "input": "x.clean.events",
                "output": [
                    "lakehouse.bronze.event_log",
                    "lakehouse.bronze.events",
                ],
                "transformations": [
                    "append immutable event_id to event_log",
                    "merge current projection by source and platform_event_id",
                    "align incoming temporal strings to the existing projection schema",
                ],
            },
            {
                "stage": "silver",
                "input": "lakehouse.bronze.event_log",
                "output": list(SILVER_TABLES),
                "transformations": [
                    "idempotent current-state merge",
                    "materialize root content without a fabricated interaction",
                    "derive engagement observation and deterministic post features",
                ],
            },
            {
                "stage": "gold",
                "input": "Silver analytical tables",
                "output": [
                    "lakehouse.gold.content_stats",
                    "lakehouse.gold.user_evolution",
                ],
                "transformations": [
                    "aggregate interaction set and latest observed engagement",
                    "aggregate anonymized author activity by source and event date",
                ],
            },
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--checkpoint-version", required=True)
    args = parser.parse_args()

    raw_path = Path(args.raw)
    if not raw_path.is_file():
        raise FileNotFoundError(f"RAW capture is missing: {raw_path}")
    raw_document = json.loads(raw_path.read_text(encoding="utf-8"))
    raw_event = raw_document.get("event")
    if not isinstance(raw_event, dict) or raw_event.get("source") != "x":
        raise ValueError("RAW capture must contain one source=x event")
    platform_event_id = str(raw_event.get("platform_event_id") or "").strip()
    if not platform_event_id:
        raise ValueError("RAW capture has no platform_event_id")

    output_dir = Path(args.output_root) / platform_event_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_raw_path = output_dir / "raw.json"
    if raw_path.resolve() != output_raw_path.resolve():
        output_raw_path.write_bytes(raw_path.read_bytes())

    warnings: list[str] = []
    errors: list[str] = []
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        identifiers: dict[str, set[str]] = {
            "platform_event_id": {platform_event_id},
            "platform_content_id": {platform_event_id},
            "conversation_id": {platform_event_id},
            "event_id": {str(raw_event.get("event_id") or "")},
            "content_id": {str(raw_event.get("content_id") or "")},
            "root_content_id": {str(raw_event.get("root_content_id") or "")},
            "url": {str(raw_event.get("url") or "")},
        }
        identifiers = {
            column_name: {value for value in values if value}
            for column_name, values in identifiers.items()
        }

        bronze: dict[str, list[dict[str, Any]]] = {}
        for table in BRONZE_TABLES:
            bronze[table] = _collect_table(
                spark, table, identifiers, warnings=warnings, errors=errors
            )
        bronze_log = _first(bronze, "lakehouse.bronze.event_log")
        bronze_current = _first(bronze, "lakehouse.bronze.events")
        for key in ("event_id", "content_id", "root_content_id", "url"):
            for row in (bronze_log, bronze_current):
                if row.get(key):
                    identifiers.setdefault(key, set()).add(str(row[key]))

        silver: dict[str, list[dict[str, Any]]] = {}
        for table in SILVER_TABLES:
            silver[table] = _collect_table(
                spark, table, identifiers, warnings=warnings, errors=errors
            )
            for row in silver[table]:
                for key in (
                    "event_id",
                    "content_id",
                    "root_content_id",
                    "conversation_id",
                    "url",
                ):
                    if row.get(key):
                        identifiers.setdefault(key, set()).add(str(row[key]))

        content = _first(silver, "lakehouse.silver.contents")
        if content:
            if content.get("author_id_hash"):
                identifiers.setdefault("user_id_hash", set()).add(
                    str(content["author_id_hash"])
                )
            if content.get("event_date"):
                identifiers.setdefault("event_date", set()).add(str(content["event_date"]))
            if content.get("source") != "x":
                errors.append("silver_contents_source_is_not_x")
            if content.get("depth") != 0:
                errors.append("silver_contents_depth_is_not_zero")
            if content.get("parent_content_id") is not None:
                errors.append("silver_contents_parent_is_not_null")
            if content.get("root_content_id") != content.get("content_id"):
                errors.append("silver_contents_root_does_not_equal_content_id")
            if str(content.get("conversation_id")) != platform_event_id:
                errors.append("silver_contents_conversation_id_mismatch")

        gold: dict[str, list[dict[str, Any]]] = {}
        for table in GOLD_TABLES:
            gold[table] = _collect_table(
                spark, table, identifiers, warnings=warnings, errors=errors
            )

        cleaned = bronze_current or bronze_log
        before_text = raw_event.get("raw_text") or raw_event.get("title")
        actual_clean_text = cleaned.get("clean_text")
        summary = redaction_summary(before_text)
        expected = expected_clean_text(before_text)
        if actual_clean_text != expected:
            errors.append("spark_clean_text_differs_from_reference_policy")
        for label, count in summary.items():
            token = {
                "user_count": "<USER>",
                "email_count": "<EMAIL>",
                "phone_count": "<PHONE>",
                "ip_count": "<IP>",
                "url_count": "<URL>",
            }[label]
            if count and str(actual_clean_text or "").count(token) < count:
                errors.append(f"missing_privacy_token:{token}")
        text_for_model = str(cleaned.get("text_for_model") or "")
        for token in ("<USER>", "<EMAIL>", "<PHONE>", "<IP>", "<URL>"):
            if token in str(actual_clean_text or "") and token not in text_for_model:
                errors.append(f"model_text_lost_token:{token}")

        protected_payload = json.dumps(
            {"bronze": bronze, "silver": silver}, ensure_ascii=False, default=str
        )
        for category, values in sensitive_values(before_text).items():
            for value in values:
                if category == "url" and value == raw_event.get("url"):
                    continue
                if value and value in protected_payload:
                    errors.append(f"sensitive_value_survived:{category}")

        if not silver.get("lakehouse.silver.engagement_snapshots"):
            warnings.append("no_engagement_snapshot:source_metrics_were_not_observed")
        for optional_table in (
            "lakehouse.gold.model_predictions",
            "lakehouse.gold.training_examples",
        ):
            if not gold.get(optional_table):
                warnings.append(f"no_optional_row:{optional_table}")

        clean_document = {
            "before": {
                "title": raw_event.get("title"),
                "raw_text": before_text,
                "url": raw_event.get("url"),
                "x_account": raw_event.get("x_account"),
            },
            "after": {
                "title": cleaned.get("title"),
                "raw_text": cleaned.get("raw_text"),
                "clean_text": actual_clean_text,
                "text_for_model": cleaned.get("text_for_model"),
                "url": cleaned.get("url"),
                "x_account": cleaned.get("x_account"),
            },
            "redaction_summary": summary,
        }
        gold_document = {
            table: [
                {
                    **row,
                    "_lineage_reason": (
                        "content_id matches the Silver root content"
                        if table.endswith("content_stats")
                        else "anonymized author and event_date match the Silver root content"
                        if table.endswith("user_evolution")
                        else "optional table row matched an event lineage identifier"
                    ),
                }
                for row in rows
            ]
            for table, rows in gold.items()
        }

        _write_json(output_dir / "clean.json", clean_document)
        _write_json(output_dir / "bronze.json", bronze)
        _write_json(output_dir / "silver.json", silver)
        _write_json(output_dir / "gold.json", gold_document)
        _write_json(output_dir / "lineage.json", _lineage(platform_event_id))

        all_tables = (*BRONZE_TABLES, *SILVER_TABLES, *GOLD_TABLES)
        table_rows = {**bronze, **silver, **gold}
        manifest = {
            "status": "PASS" if not errors else "FAIL",
            "source": "x",
            "platform_event_id": platform_event_id,
            "event_id": cleaned.get("event_id"),
            "content_id": content.get("content_id"),
            "producer_run_id": raw_document.get("capture_metadata", {}).get(
                "producer_run_id"
            ),
            "pipeline_run_id": args.pipeline_run_id,
            "capture_timestamp": raw_document.get("capture_metadata", {}).get(
                "captured_at"
            ),
            "publication_timestamp": raw_event.get("published_at")
            or raw_event.get("timestamp"),
            "kafka_topics": {
                "raw": raw_document.get("capture_metadata", {}).get("kafka_topic"),
                "clean": "x.clean.events",
                "bronze_handoff": "lakehouse.bronze.for_silver",
            },
            "kafka_coordinates": {
                "topic": bronze_log.get("kafka_topic"),
                "partition": bronze_log.get("kafka_partition"),
                "offset": bronze_log.get("kafka_offset"),
            },
            "iceberg_tables_scanned": list(all_tables),
            "matched_row_counts": {
                table: len(table_rows.get(table) or []) for table in all_tables
            },
            "iceberg_snapshot_ids": {
                table: _snapshot_id(spark, table) for table in all_tables
            },
            "checkpoint_versions": {
                "clean": args.checkpoint_version,
                "bronze": args.checkpoint_version,
                "silver": args.checkpoint_version,
            },
            "files_generated": [
                "raw.json",
                "clean.json",
                "bronze.json",
                "silver.json",
                "gold.json",
                "lineage.json",
                "manifest.json",
                "manifest.sha256",
                "logs/collector.log",
                "logs/privacy-cleaning.log",
                "logs/bronze.log",
                "logs/silver.log",
                "logs/gold.log",
                "logs/export.log",
            ],
            "sha256": {},
            "warnings": sorted(set(warnings)),
            "errors": sorted(set(errors)),
        }
        _write_json(output_dir / "manifest.json", manifest)
    finally:
        spark.stop()

    if errors:
        print(json.dumps({"status": "FAIL", "errors": sorted(set(errors))}, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "status": "PASS",
                "platform_event_id": platform_event_id,
                "output": str(output_dir),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
