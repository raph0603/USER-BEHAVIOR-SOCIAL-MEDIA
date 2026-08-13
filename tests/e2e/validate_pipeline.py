"""Validate deterministic Iceberg state at each pipeline E2E checkpoint."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


EXPECTED_EVENT_COUNT = 15
EXPECTED_CURRENT_COUNT = 7
EXPECTED_SNAPSHOT_COUNT = 6


def _env(name: str, default: str) -> str:
    return os.getenv(name) or default


def _spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("pipeline-reliability-e2e-validation")
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
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _count(frame: DataFrame) -> int:
    return int(frame.count())


def _distinct_count(frame: DataFrame, *columns: str) -> int:
    return _count(frame.select(*columns).dropDuplicates(list(columns)))


def _table(spark: SparkSession, name: str) -> DataFrame:
    _require(spark.catalog.tableExists(name), f"required table does not exist: {name}")
    return spark.table(name)


def _validate_bronze(spark: SparkSession, *, expected_dlq_rows: int, projected: bool) -> None:
    event_log = _table(spark, "lakehouse.bronze.event_log")
    dlq = _table(spark, "lakehouse.bronze.ingress_dlq")
    current = _table(spark, "lakehouse.bronze.events")

    _require(_count(event_log) == EXPECTED_EVENT_COUNT, "Bronze journal row count changed")
    _require(
        _distinct_count(event_log, "event_id") == EXPECTED_EVENT_COUNT,
        "Bronze journal contains duplicate event IDs",
    )
    _require(_count(dlq) == expected_dlq_rows, "unexpected Bronze DLQ row count")
    _require(
        _distinct_count(dlq, "dlq_id") == expected_dlq_rows,
        "Bronze DLQ identity is not unique",
    )
    for row in dlq.select("category", "protected_payload").collect():
        protected = str(row["protected_payload"])
        _require(row["category"] == "malformed_json", "unexpected DLQ category")
        _require('"redacted":true' in protected, "DLQ payload is not protected")
        _require("must-never-be-persisted" not in protected, "DLQ leaked rejected content")
    expected_current = EXPECTED_CURRENT_COUNT if projected else 0
    _require(_count(current) == expected_current, "Bronze projection violated commit ordering")


def _validate_silver(spark: SparkSession) -> None:
    applied = _table(spark, "lakehouse.silver.applied_events")
    current = _table(spark, "lakehouse.silver.events")
    _require(_count(applied) == EXPECTED_EVENT_COUNT, "Silver applied-event count changed")
    _require(
        _distinct_count(applied, "event_id") == EXPECTED_EVENT_COUNT,
        "Silver applied-event IDs are not unique",
    )
    _require(_count(current) == EXPECTED_CURRENT_COUNT, "Silver current projection count changed")


def _validate_analytics_and_ml(spark: SparkSession) -> dict[str, Any]:
    snapshots = _table(spark, "lakehouse.silver.engagement_snapshots")
    transcripts = _table(spark, "lakehouse.silver.transcripts")
    interactions = _table(spark, "lakehouse.silver.interactions")
    contents = _table(spark, "lakehouse.silver.contents")
    stats = _table(spark, "lakehouse.gold.content_stats")
    post_features = _table(spark, "lakehouse.silver.post_features")
    examples = _table(spark, "lakehouse.gold.training_examples")
    manifests = _table(spark, "lakehouse.gold.dataset_manifests")

    _require(_count(snapshots) == EXPECTED_SNAPSHOT_COUNT, "snapshot replay created duplicates")
    _require(
        _distinct_count(snapshots, "observation_id") == EXPECTED_SNAPSHOT_COUNT,
        "snapshot observation IDs are not unique",
    )
    youtube_snapshots = snapshots.filter(col("source") == "youtube")
    _require(_count(youtube_snapshots) == 4, "expected two snapshots for each YouTube video")
    per_video = {
        str(row["platform_event_id"]): int(row["count"])
        for row in youtube_snapshots.groupBy("platform_event_id").count().collect()
    }
    _require(per_video == {"video-en": 2, "video-vi": 2}, "YouTube history was collapsed")

    en_views = [
        row["view_count"]
        for row in youtube_snapshots.filter(col("platform_event_id") == "video-en")
        .orderBy("snapshot_at")
        .select("view_count")
        .collect()
    ]
    _require(en_views == [5, 0], "counter decrease was not retained as an observation")
    known_zero = youtube_snapshots.filter(
        (col("platform_event_id") == "video-en")
        & (col("view_count") == 0)
        & (col("view_count_available") == True)  # noqa: E712
    )
    unknown = youtube_snapshots.filter(
        (col("platform_event_id") == "video-vi")
        & col("view_count").isNull()
        & (col("view_count_available") == False)  # noqa: E712
    )
    _require(_count(known_zero) == 1, "known zero view count was not preserved")
    _require(_count(unknown) == 2, "unknown view count was coerced or lost")

    lifecycle = {
        (str(row["video_id"]), str(row["requested_language_code"])): str(
            row["transcript_lifecycle_status"]
        )
        for row in transcripts.select(
            "video_id", "requested_language_code", "transcript_lifecycle_status"
        ).collect()
    }
    _require(
        lifecycle
        == {
            ("video-en", "en"): "available",
            ("video-vi", "vi"): "available",
            ("video-private", "en"): "unavailable",
        },
        "transcript language or lifecycle state leaked between videos",
    )
    transcript_provenance = {
        str(row["video_id"]): row.asDict(recursive=True)
        for row in transcripts.select(
            "video_id",
            "provider",
            "generation_type",
            "model",
            "fallback_reason",
            "generated_by_model",
            "primary_attempt_count",
            "fallback_attempt_count",
        ).collect()
    }
    _require(
        transcript_provenance["video-en"]["provider"] == "youtube_transcript_api"
        and transcript_provenance["video-en"]["generation_type"] == "manual"
        and transcript_provenance["video-en"]["generated_by_model"] is not True,
        "primary transcript provenance changed",
    )
    _require(
        transcript_provenance["video-vi"]["provider"] == "gemini"
        and transcript_provenance["video-vi"]["generation_type"] == "model_generated"
        and transcript_provenance["video-vi"]["model"] == "gemini-3.5-flash"
        and transcript_provenance["video-vi"]["fallback_reason"]
        == "no_transcript_found"
        and transcript_provenance["video-vi"]["generated_by_model"] is True
        and transcript_provenance["video-vi"]["primary_attempt_count"] == 1
        and transcript_provenance["video-vi"]["fallback_attempt_count"] == 1,
        "Gemini fallback provenance is incomplete",
    )
    _require(
        transcript_provenance["video-private"]["provider"]
        == "youtube_transcript_api"
        and transcript_provenance["video-private"]["fallback_attempt_count"] == 0,
        "ineligible private video unexpectedly used fallback",
    )

    reddit_root = hashlib.sha256(b"pipeline-e2e:content:reddit-post-1").hexdigest()
    x_root = hashlib.sha256(b"pipeline-e2e:content:x-post-1001").hexdigest()
    relation_rows = {
        str(row["source"]): row.asDict(recursive=True)
        for row in interactions.select(
            "source", "parent_content_id", "root_content_id", "relation_type"
        ).collect()
    }
    _require(set(relation_rows) == {"reddit", "x"}, "Reddit/X interactions are incomplete")
    _require(
        relation_rows["reddit"]["root_content_id"] == reddit_root
        and relation_rows["reddit"]["parent_content_id"] == reddit_root,
        "Reddit thread relation is broken",
    )
    _require(
        relation_rows["x"]["root_content_id"] == x_root
        and relation_rows["x"]["parent_content_id"] == x_root,
        "X conversation relation is broken",
    )

    _require(_count(contents) == 5, "entity-level content materialization changed")
    _require(_count(stats) == 5, "content analytics materialization changed")
    youtube_contents = contents.filter(col("source") == "youtube")
    _require(
        _count(
            youtube_contents.filter(
                col("last_discovered_at").isNotNull() & col("last_enriched_at").isNotNull()
            )
        )
        == 2,
        "YouTube discovery/enrichment freshness is incomplete",
    )
    _require(_count(post_features) == EXPECTED_CURRENT_COUNT, "post feature source is incomplete")

    _require(_count(examples) == 2, "official training dataset should contain two videos")
    _require(_distinct_count(examples, "example_id") == 2, "training examples are duplicated")
    _require(
        _distinct_count(examples, "dataset_version") == 1,
        "identical source snapshots produced multiple dataset versions",
    )
    labels = {str(row["label_value"]) for row in examples.select("label_value").collect()}
    _require(labels == {"viral", "not_viral"}, "deterministic label distribution changed")
    _require(
        _count(
            examples.filter(
                (col("audience_available") == False)  # noqa: E712
                & col("audience_count").isNull()
            )
        )
        == 2,
        "official Gold exposed audience without pre-publication history",
    )
    _require(_count(manifests) == 1, "dataset manifest is not deterministic")
    manifest = manifests.select("dataset_version", "example_count", "filters_json").first()
    dataset_row = examples.select("dataset_version").first()
    if manifest is None or dataset_row is None:
        raise AssertionError("dataset manifest or examples unexpectedly empty")
    dataset_version = dataset_row["dataset_version"]
    _require(manifest["dataset_version"] == dataset_version, "manifest version mismatch")
    _require(int(manifest["example_count"]) == 2, "manifest example count mismatch")
    filters = json.loads(str(manifest["filters_json"]))
    _require(
        filters.get("audience_feature_policy") == "excluded_no_prepublication_history",
        "manifest did not record the official audience exclusion policy",
    )

    return {
        "contents": _count(contents),
        "dataset_version": str(dataset_version),
        "interactions": _count(interactions),
        "snapshots": _count(snapshots),
        "training_examples": _count(examples),
        "transcripts": _count(transcripts),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=("after-failure", "after-bronze", "after-silver", "analytics", "replay"),
        required=True,
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.phase == "after-failure":
            _validate_bronze(spark, expected_dlq_rows=1, projected=False)
            summary: dict[str, Any] = {"phase": args.phase}
        elif args.phase == "after-bronze":
            _validate_bronze(spark, expected_dlq_rows=1, projected=True)
            summary = {"phase": args.phase}
        elif args.phase == "after-silver":
            _validate_bronze(spark, expected_dlq_rows=1, projected=True)
            _validate_silver(spark)
            summary = {"phase": args.phase}
        elif args.phase == "analytics":
            _validate_bronze(spark, expected_dlq_rows=1, projected=True)
            _validate_silver(spark)
            summary = {"phase": args.phase, **_validate_analytics_and_ml(spark)}
        else:
            _validate_bronze(spark, expected_dlq_rows=2, projected=True)
            _validate_silver(spark)
            summary = {"phase": args.phase, **_validate_analytics_and_ml(spark)}
        print(json.dumps(summary, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
