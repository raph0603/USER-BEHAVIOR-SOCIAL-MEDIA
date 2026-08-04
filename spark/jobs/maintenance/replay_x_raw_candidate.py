"""Replay one exact real X RAW Kafka event through the privacy gateway."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from pyspark.sql.functions import col, from_json, lit, struct, to_json

JOBS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(JOBS_ROOT))
sys.path.insert(0, str(JOBS_ROOT / "pipeline"))

from common.x_lineage import redaction_summary
from collector_stream_pipeline import protect_event
from event_contract import EVENT_COLUMNS, spark_struct_type
from maintenance.inspect_x_transformation_candidates import (
    _build_spark,
    _read_unique_raw_x_events,
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
        json.dumps(_json_value(payload), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _read_existing_clean(spark, platform_event_id: str):
    kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
        .option("subscribe", "x.clean.events")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "true")
        .load()
    )
    return (
        kafka.select(
            col("topic").alias("_kafka_topic"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            from_json(col("value").cast("string"), spark_struct_type()).alias("data"),
        )
        .select(
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "data.*",
        )
        .filter(
            (col("source") == "x")
            & (col("platform_event_id").cast("string") == platform_event_id)
        )
        .orderBy(col("_kafka_offset").desc(), col("_kafka_partition").desc())
        .limit(1)
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform-event-id", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--republish-clean", action="store_true")
    args = parser.parse_args()

    platform_event_id = str(args.platform_event_id).strip()
    output_dir = Path(args.output_root) / platform_event_id
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        candidate = _read_unique_raw_x_events(spark).filter(
            col("platform_event_id").cast("string") == platform_event_id
        )
        if candidate.limit(2).count() != 1:
            raise RuntimeError(
                f"Expected exactly one unique RAW X event for {platform_event_id}"
            )

        raw_row = candidate.first().asDict(recursive=True)
        event = {name: _json_value(raw_row.get(name)) for name in EVENT_COLUMNS}
        raw_document = {
            "capture_metadata": {
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "producer_name": "existing_x_raw_kafka_replay",
                "producer_run_id": args.pipeline_run_id,
                "kafka_topic": raw_row["_kafka_topic"],
                "kafka_partition": raw_row["_kafka_partition"],
                "kafka_offset": raw_row["_kafka_offset"],
                "capture_stage": "existing_before_privacy_cleaning",
                "event_type": "real_existing",
            },
            "event": event,
        }
        _write_json(output_dir / "raw.json", raw_document)

        existing_clean = _read_existing_clean(spark, platform_event_id)
        existing_count = existing_clean.count()
        published = False
        if existing_count and not args.republish_clean:
            clean_row = existing_clean.first().asDict(recursive=True)
        else:
            raw_event_frame = candidate.select(*EVENT_COLUMNS)
            protected = protect_event(
                raw_event_frame,
                platform="x",
                privacy_hash_salt=os.getenv("PRIVACY_HASH_SALT", "dev-privacy-salt"),
            )
            (
                protected.select(
                    col("user_id").cast("string").alias("key"),
                    to_json(
                        struct(*EVENT_COLUMNS, lit("clean").alias("stage"))
                    ).alias("value"),
                )
                .write.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                )
                .option("topic", "x.clean.events")
                .save()
            )
            published = True
            clean_row = protected.first().asDict(recursive=True)

        before_text = event.get("raw_text") or event.get("title")
        clean_document = {
            "before": {
                "title": event.get("title"),
                "raw_text": before_text,
                "url": event.get("url"),
                "x_account": event.get("x_account"),
            },
            "after": {
                "title": clean_row.get("title"),
                "raw_text": clean_row.get("raw_text"),
                "clean_text": clean_row.get("clean_text"),
                "text_for_model": clean_row.get("text_for_model"),
                "url": clean_row.get("url"),
                "x_account": clean_row.get("x_account"),
            },
            "redaction_summary": redaction_summary(before_text),
            "clean_kafka_republished": published,
        }
        _write_json(output_dir / "clean.json", clean_document)
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "event_type": "real_existing",
                    "platform_event_id": platform_event_id,
                    "clean_kafka_republished": published,
                    "redaction_summary": clean_document["redaction_summary"],
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
