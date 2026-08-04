"""Rank real X events that can demonstrate visible privacy transformations."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

JOBS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(JOBS_ROOT))
sys.path.insert(0, str(JOBS_ROOT / "pipeline"))

from common.x_lineage import expected_clean_text, redaction_summary
from collector_stream_pipeline import _decode_confluent_avro

_HASHTAG_RE = re.compile(r"(?<!\w)#[\w_]+", re.UNICODE)
_EMOJI_RE = re.compile(
    "["
    "\U0001f300-\U0001f5ff"
    "\U0001f600-\U0001f64f"
    "\U0001f680-\U0001f6ff"
    "\U0001f700-\U0001f77f"
    "\U0001f780-\U0001f7ff"
    "\U0001f800-\U0001f8ff"
    "\U0001f900-\U0001f9ff"
    "\U0001fa00-\U0001faff"
    "\u2600-\u26ff"
    "\u2700-\u27bf"
    "]",
)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _build_spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("inspect-x-transformation-candidates")
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
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _read_unique_raw_x_events(spark: SparkSession):
    kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", _env("KAFKA_BOOTSTRAP", "kafka:9092"))
        .option("subscribe", "x.raw.events")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "true")
        .load()
    )
    metadata = kafka.select(
        col("topic").alias("_kafka_topic"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        col("value"),
    )
    decoded = _decode_confluent_avro(
        metadata,
        _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
        {"x.raw.events": "x.raw.events-value"},
    ).filter((col("source") == "x") & col("platform_event_id").isNotNull())
    latest = Window.partitionBy("platform_event_id").orderBy(
        col("_kafka_offset").desc(),
        col("_kafka_partition").desc(),
    )
    return decoded.withColumn("_rank", row_number().over(latest)).filter(col("_rank") == 1)


def _score_candidate(
    raw: dict[str, Any],
    features: dict[str, Any] | None,
    *,
    bronze_available: bool,
) -> dict[str, Any]:
    raw_text = str(raw.get("raw_text") or raw.get("title") or "")
    redactions = redaction_summary(raw_text)
    features = features or {}
    token_counts = {
        "user_count": int(features.get("mention_token_count") or redactions["user_count"]),
        "email_count": int(features.get("email_token_count") or redactions["email_count"]),
        "url_count": int(features.get("url_token_count") or redactions["url_count"]),
        "phone_count": int(features.get("phone_token_count") or redactions["phone_count"]),
        "ip_count": int(features.get("ip_token_count") or redactions["ip_count"]),
    }
    hashtag_count = int(
        features.get("hashtag_count") or len(_HASHTAG_RE.findall(raw_text))
    )
    emoji_count = int(features.get("emoji_count") or len(_EMOJI_RE.findall(raw_text)))
    score = (
        3 * int(token_counts["user_count"] > 0)
        + 3 * int(token_counts["email_count"] > 0)
        + 3 * int(token_counts["url_count"] > 0)
        + 2 * int(token_counts["phone_count"] > 0)
        + 2 * int(token_counts["ip_count"] > 0)
        + int(hashtag_count > 0)
        + int(emoji_count > 0)
    )
    reference_clean = expected_clean_text(raw_text)
    reference_model = reference_clean.lower()
    for token in ("USER", "EMAIL", "PHONE", "IP", "URL"):
        reference_model = reference_model.replace(f"<{token.lower()}>", f"<{token}>")
    observed_types = sum(int(value > 0) for value in token_counts.values())
    observed_types += int(hashtag_count > 0)
    observed_types += int(emoji_count > 0)
    return {
        "platform_event_id": str(raw["platform_event_id"]),
        "x_account": raw.get("x_account"),
        "score": score,
        "transformation_type_count": observed_types,
        "total_signal_count": (
            sum(token_counts.values()) + hashtag_count + emoji_count
        ),
        "raw_redaction_counts": redactions,
        "silver_token_counts": token_counts,
        "hashtag_count": hashtag_count,
        "emoji_count": emoji_count,
        "lowercase_normalization": reference_clean != reference_model,
        "raw_available": True,
        "bronze_available": bronze_available,
        "silver_available": bool(features),
        "kafka": {
            "topic": raw.get("_kafka_topic"),
            "partition": raw.get("_kafka_partition"),
            "offset": raw.get("_kafka_offset"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--exclude-account", action="append", default=[])
    args = parser.parse_args()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw_frame = _read_unique_raw_x_events(spark)
        raw_rows = {
            str(row["platform_event_id"]): _json_value(row.asDict(recursive=True))
            for row in raw_frame.select(
                "_kafka_topic",
                "_kafka_partition",
                "_kafka_offset",
                "platform_event_id",
                "x_account",
                "raw_text",
                "title",
            ).collect()
        }
        features_frame = spark.table("lakehouse.silver.post_features").filter(
            col("source") == "x"
        )
        feature_rows = {
            str(row["platform_event_id"]): _json_value(row.asDict(recursive=True))
            for row in features_frame.select(
                "platform_event_id",
                "mention_token_count",
                "email_token_count",
                "url_token_count",
                "phone_token_count",
                "ip_token_count",
                "hashtag_count",
                "emoji_count",
            ).collect()
        }
        bronze_ids = {
            str(row["platform_event_id"])
            for row in spark.table("lakehouse.bronze.event_log")
            .filter((col("source") == "x") & col("platform_event_id").isNotNull())
            .select("platform_event_id")
            .distinct()
            .collect()
        }
        candidates = [
            _score_candidate(
                raw,
                feature_rows.get(event_id),
                bronze_available=event_id in bronze_ids,
            )
            for event_id, raw in raw_rows.items()
            if str(raw.get("x_account") or "") not in set(args.exclude_account)
        ]
        candidates.sort(
            key=lambda item: (
                -int(item["score"]),
                -int(item["transformation_type_count"]),
                -int(item["total_signal_count"]),
                str(item["platform_event_id"]),
            )
        )
        token_candidate_counts = {
            name: sum(
                int(candidate["silver_token_counts"][name] > 0)
                for candidate in candidates
            )
            for name in (
                "user_count",
                "email_count",
                "url_count",
                "phone_count",
                "ip_count",
            )
        }
        token_candidate_counts.update(
            {
                "hashtag_count": sum(int(item["hashtag_count"] > 0) for item in candidates),
                "emoji_count": sum(int(item["emoji_count"] > 0) for item in candidates),
            }
        )
        payload = {
            "event_type": "real_existing",
            "raw_events_inspected": len(raw_rows),
            "events_with_bronze_and_silver": sum(
                int(item["bronze_available"] and item["silver_available"])
                for item in candidates
            ),
            "candidate_counts": token_candidate_counts,
            "top_candidates": candidates[:20],
        }
        output.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(
            json.dumps(
                {
                    "raw_events_inspected": len(raw_rows),
                    "events_with_bronze_and_silver": sum(
                        int(item["bronze_available"] and item["silver_available"])
                        for item in candidates
                    ),
                    "eligible_candidates": sum(
                        int(
                            candidate["score"] > 0
                            and sum(
                                int(value > 0)
                                for value in candidate["silver_token_counts"].values()
                            )
                            > 0
                        )
                        for candidate in candidates
                    ),
                    "best_platform_event_id": (
                        candidates[0]["platform_event_id"] if candidates else None
                    ),
                    "best_score": candidates[0]["score"] if candidates else None,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
