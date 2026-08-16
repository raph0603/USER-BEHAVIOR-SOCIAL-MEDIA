"""Build and export a versioned training dataset from pinned Iceberg snapshots."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

import numpy as np
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    expr,
    greatest,
    length,
    lit,
    log1p,
    max as spark_max,
    min as spark_min,
    row_number,
    sha2,
    sqrt,
    struct,
    sum as spark_sum,
    to_date,
    to_json,
    trim,
    when,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.dataset_manifest import DatasetIdentity, canonical_json, missing_rate
from pipeline.gold_schemas import (
    DATASET_MANIFESTS_COLUMNS,
    TRAINING_EXAMPLES_COLUMNS,
    TRAINING_EXAMPLES_SCHEMA_VERSION,
    create_gold_tables,
)
from pipeline.virality_contract import (
    ENGAGEMENT_SCORE_VERSION,
    OBSERVATION_SELECTION_POLICY,
    PLATFORM_REFERENCE_POLICY,
    QUANTILE_METHOD,
    TRAINING_REFERENCE_POLICY,
    ViralityContract,
    build_contract,
)


POST_FEATURES_TABLE = "lakehouse.silver.post_features"
ENGAGEMENT_SNAPSHOTS_TABLE = "lakehouse.silver.engagement_snapshots"
TRAINING_EXAMPLES_TABLE = "lakehouse.gold.training_examples"
DATASET_MANIFESTS_TABLE = "lakehouse.gold.dataset_manifests"
DATASET_BUILDER_REVISION = "prepublication-feature-contract-v3"
AUDIENCE_FEATURE_POLICY = "excluded_no_prepublication_history"

METRICS_BY_SOURCE = {
    "youtube": ("view_count", "like_count", "comment_count"),
    "x": (
        "like_count",
        "view_count",
        "retweet_count",
        "reply_count",
        "bookmark_count",
    ),
    "reddit": ("score", "comment_count"),
}
ALL_METRICS = tuple(
    sorted({metric for metrics in METRICS_BY_SOURCE.values() for metric in metrics})
)
AUDIENCE_BY_SOURCE = {
    "youtube": "subscriber_count",
    "x": "follower_count",
    "reddit": "subreddit_member_count",
}
VALID_HORIZONS = {1: "T+1h", 6: "T+6h", 24: "T+24h", 72: "T+72h", 168: "T+7d"}
DATASET_VERSION_PATTERN = re.compile(
    rf"^dataset-{re.escape(TRAINING_EXAMPLES_SCHEMA_VERSION)}-[a-f0-9]{{20}}$"
)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("build-versioned-training-dataset")
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build or export an exact lakehouse training dataset version"
    )
    parser.add_argument(
        "--dataset-version",
        default=_env("ML_DATASET_VERSION", "auto"),
        help="Use 'auto' to build from pinned current snapshots, or export an exact version",
    )
    parser.add_argument(
        "--label-horizon-hours",
        type=int,
        default=int(_env("ML_LABEL_HORIZON_HOURS", "24")),
    )
    parser.add_argument(
        "--label-tolerance-hours",
        type=int,
        default=int(_env("ML_LABEL_TOLERANCE_HOURS", "24")),
    )
    parser.add_argument(
        "--viral-quantile",
        type=float,
        default=float(_env("ML_VIRAL_QUANTILE", "0.75")),
    )
    parser.add_argument(
        "--virality-policy",
        choices=(PLATFORM_REFERENCE_POLICY, TRAINING_REFERENCE_POLICY),
        default=_env("ML_VIRALITY_POLICY", TRAINING_REFERENCE_POLICY),
    )
    parser.add_argument(
        "--virality-contract",
        type=Path,
        help="Frozen contract to apply; required for platform_reference_quantile",
    )
    parser.add_argument(
        "--virality-contract-output",
        type=Path,
        help="Optional additional path for the generated/resolved immutable contract",
    )
    parser.add_argument(
        "--min-reference-examples-per-platform",
        type=int,
        help="Explicit minimum reference population required for every included platform",
    )
    parser.add_argument(
        "--holdout-fraction",
        type=float,
        default=float(_env("ML_HOLDOUT_FRACTION", "0.2")),
    )
    parser.add_argument(
        "--split-seed",
        type=int,
        default=int(_env("ML_SPLIT_SEED", "42")),
    )
    parser.add_argument(
        "--min-text-chars",
        type=int,
        default=int(_env("ML_MIN_TEXT_CHARS", "3")),
    )
    parser.add_argument(
        "--post-features-snapshot-id",
        type=int,
        default=int(_env("ML_POST_FEATURES_SNAPSHOT_ID", "0")),
        help="Exact post_features Iceberg snapshot; 0 resolves and locks the latest snapshot once.",
    )
    parser.add_argument(
        "--engagement-snapshots-snapshot-id",
        type=int,
        default=int(_env("ML_ENGAGEMENT_SNAPSHOTS_SNAPSHOT_ID", "0")),
        help=(
            "Exact engagement_snapshots Iceberg snapshot; 0 resolves and locks the latest "
            "snapshot once."
        ),
    )
    parser.add_argument(
        "--training-examples-snapshot-id",
        type=int,
        default=int(_env("ML_TRAINING_EXAMPLES_SNAPSHOT_ID", "0")),
        help=(
            "Exact Gold training_examples Iceberg snapshot used when exporting an existing "
            "dataset version. Auto builds capture the post-merge Gold snapshot themselves."
        ),
    )
    parser.add_argument(
        "--export-root",
        type=Path,
        default=Path(_env("ML_DATASET_EXPORT_ROOT", "/opt/spark/balancing/ml")),
    )
    parser.add_argument("--manifest-output", type=Path)
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if args.dataset_version != "auto" and not DATASET_VERSION_PATTERN.fullmatch(
        args.dataset_version
    ):
        raise ValueError("--dataset-version must be 'auto' or a deterministic lakehouse version")
    if args.label_horizon_hours not in VALID_HORIZONS:
        raise ValueError(
            "--label-horizon-hours must be one of "
            + ", ".join(str(value) for value in sorted(VALID_HORIZONS))
        )
    if args.label_tolerance_hours < 0:
        raise ValueError("--label-tolerance-hours must be non-negative")
    if not 0 < args.viral_quantile < 1:
        raise ValueError("--viral-quantile must be between zero and one")
    if args.min_text_chars < 1:
        raise ValueError("--min-text-chars must be greater than zero")
    if not 0 < args.holdout_fraction < 1:
        raise ValueError("--holdout-fraction must be between zero and one")
    if args.dataset_version == "auto" and args.virality_policy == PLATFORM_REFERENCE_POLICY:
        if args.virality_contract is None:
            raise ValueError(
                "--virality-contract is required for platform_reference_quantile; "
                "evaluation data must never estimate its own thresholds"
            )
    if args.dataset_version == "auto" and args.virality_policy == TRAINING_REFERENCE_POLICY:
        if args.min_reference_examples_per_platform is None:
            raise ValueError(
                "--min-reference-examples-per-platform must be explicitly configured "
                "for training_reference_quantile"
            )
    if (
        args.min_reference_examples_per_platform is not None
        and args.min_reference_examples_per_platform <= 0
    ):
        raise ValueError("--min-reference-examples-per-platform must be greater than zero")
    requested_snapshots = (
        args.post_features_snapshot_id,
        args.engagement_snapshots_snapshot_id,
    )
    if any(value < 0 for value in requested_snapshots):
        raise ValueError("Iceberg snapshot IDs must be zero or positive")
    if bool(requested_snapshots[0]) != bool(requested_snapshots[1]):
        raise ValueError("Both source snapshot IDs must be supplied together")
    if args.dataset_version != "auto" and any(requested_snapshots):
        raise ValueError("Explicit source snapshots require --dataset-version auto")
    if args.training_examples_snapshot_id < 0:
        raise ValueError("The Gold training snapshot ID must be zero or positive")
    if args.dataset_version == "auto" and args.training_examples_snapshot_id:
        raise ValueError("An explicit Gold snapshot requires an exact --dataset-version")
    if args.dataset_version != "auto" and not args.training_examples_snapshot_id:
        raise ValueError(
            "Exporting an existing dataset version requires --training-examples-snapshot-id"
        )


def _latest_snapshot_id(spark: SparkSession, table: str) -> int:
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"Required official table does not exist: {table}")
    row = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()
    if row is None or row["snapshot_id"] is None:
        raise RuntimeError(f"Required official table has no Iceberg snapshot: {table}")
    return int(row["snapshot_id"])


def _read_snapshot(spark: SparkSession, table: str, snapshot_id: int) -> DataFrame:
    return spark.read.format("iceberg").option("snapshot-id", str(snapshot_id)).load(table)


def _with_optional_columns(
    dataframe: DataFrame,
    columns: dict[str, str],
) -> DataFrame:
    result = dataframe
    current = set(dataframe.columns)
    for name, data_type in columns.items():
        if name not in current:
            result = result.withColumn(name, lit(None).cast(data_type))
    return result


def _available(dataframe: DataFrame, metric: str):
    flag = f"{metric}_available"
    if flag in dataframe.columns:
        return coalesce(col(flag).cast("boolean"), col(metric).isNotNull())
    return col(metric).isNotNull()


def _metric_expressions(dataframe: DataFrame):
    observed_terms = []
    score_terms = []
    for metric in ALL_METRICS:
        sources = [source for source, metrics in METRICS_BY_SOURCE.items() if metric in metrics]
        valid = (
            col("source").isin(*sources) & _available(dataframe, metric) & col(metric).isNotNull()
        )
        observed_terms.append(when(valid, lit(1)).otherwise(lit(0)))
        score_terms.append(
            when(
                valid,
                log1p(greatest(col(metric).cast("double"), lit(0.0))),
            ).otherwise(lit(0.0))
        )
    observed = reduce(lambda left, right: left + right, observed_terms)
    score_total = reduce(lambda left, right: left + right, score_terms)
    expected = lit(0)
    for source, metrics in METRICS_BY_SOURCE.items():
        expected = when(col("source") == source, lit(len(metrics))).otherwise(expected)
    return observed, score_total, expected


def _audience_expressions(dataframe: DataFrame):
    available = lit(False)
    value = lit(None).cast("bigint")
    for source, metric in reversed(tuple(AUDIENCE_BY_SOURCE.items())):
        valid = (col("source") == source) & _available(dataframe, metric) & col(metric).isNotNull()
        available = when(valid, lit(True)).otherwise(available)
        value = when(
            valid,
            greatest(col(metric).cast("bigint"), lit(0).cast("bigint")),
        ).otherwise(value)
    return value, available


def build_scored_examples(
    post_features: DataFrame,
    snapshots: DataFrame,
    *,
    label_horizon_hours: int,
    label_tolerance_hours: int,
    min_text_chars: int,
) -> DataFrame:
    """Build fixed-horizon engagement scores without consulting evaluation composition."""

    feature_types = {
        "source": "string",
        "platform_event_id": "string",
        "event_ts": "timestamp",
        "author_hash": "string",
        "text_for_model": "string",
        "feature_version": "string",
    }
    snapshot_types = {
        "source": "string",
        "platform_event_id": "string",
        "observation_id": "string",
        "observed_at": "timestamp",
        "provenance_json": "string",
        "coverage_json": "string",
        "metrics_refresh_status": "string",
        **{metric: "bigint" for metric in ALL_METRICS},
        **{f"{metric}_available": "boolean" for metric in ALL_METRICS},
        **{metric: "bigint" for metric in AUDIENCE_BY_SOURCE.values()},
        **{f"{metric}_available": "boolean" for metric in AUDIENCE_BY_SOURCE.values()},
    }
    features = _with_optional_columns(post_features, feature_types).filter(
        col("source").isin(*METRICS_BY_SOURCE)
        & col("platform_event_id").isNotNull()
        & col("event_ts").isNotNull()
        & col("text_for_model").isNotNull()
        & (length(trim(col("text_for_model"))) >= min_text_chars)
    )
    feature_order = Window.partitionBy("source", "platform_event_id").orderBy(
        col("event_ts").asc(),
        sha2(
            concat_ws(
                "\u001f",
                coalesce(col("author_hash"), lit("")),
                col("text_for_model"),
                coalesce(col("feature_version"), lit("")),
            ),
            256,
        ).asc(),
    )
    features = (
        features.withColumn("_feature_rank", row_number().over(feature_order))
        .filter(col("_feature_rank") == 1)
        .drop("_feature_rank")
    )
    features = features.withColumn(
        "_target_observed_at",
        col("event_ts") + expr(f"INTERVAL {int(label_horizon_hours)} HOURS"),
    )
    observations = _with_optional_columns(snapshots, snapshot_types)
    snapshot_tiebreaker_columns = [
        "source",
        "platform_event_id",
        "observation_id",
        "observed_at",
        "provenance_json",
        "coverage_json",
        "metrics_refresh_status",
        *ALL_METRICS,
        *[f"{metric}_available" for metric in ALL_METRICS],
        *AUDIENCE_BY_SOURCE.values(),
        *[f"{metric}_available" for metric in AUDIENCE_BY_SOURCE.values()],
    ]
    snapshot_select = [
        col("source").alias("_snapshot_source"),
        col("platform_event_id").alias("_snapshot_platform_event_id"),
        col("observation_id"),
        col("observed_at"),
        col("provenance_json"),
        col("coverage_json"),
        col("metrics_refresh_status"),
        *[col(metric) for metric in ALL_METRICS],
        *[col(f"{metric}_available") for metric in ALL_METRICS],
        *[col(metric) for metric in AUDIENCE_BY_SOURCE.values()],
        *[col(f"{metric}_available") for metric in AUDIENCE_BY_SOURCE.values()],
        sha2(
            to_json(struct(*[col(name) for name in snapshot_tiebreaker_columns])),
            256,
        ).alias("_snapshot_tiebreaker"),
    ]
    observations = observations.select(*snapshot_select)

    candidates = (
        features.alias("features")
        .join(
            observations.alias("snapshots"),
            (col("features.source") == col("snapshots._snapshot_source"))
            & (col("features.platform_event_id") == col("snapshots._snapshot_platform_event_id")),
            "inner",
        )
        .filter(col("snapshots.observed_at") >= col("features._target_observed_at"))
        .filter(
            col("snapshots.observed_at")
            <= col("features._target_observed_at")
            + expr(f"INTERVAL {int(label_tolerance_hours)} HOURS")
        )
    )
    observation_order = Window.partitionBy(
        col("features.source"),
        col("features.platform_event_id"),
        col("features.event_ts"),
    ).orderBy(
        col("snapshots.observed_at").asc(),
        col("snapshots.observation_id").asc_nulls_last(),
        col("snapshots._snapshot_tiebreaker").asc(),
    )
    selected = (
        candidates.withColumn("_observation_rank", row_number().over(observation_order))
        .filter(col("_observation_rank") == 1)
        .select(
            col("features.source").alias("source"),
            col("features.platform_event_id").alias("platform_event_id"),
            col("features.event_ts").alias("event_ts"),
            col("features.author_hash").alias("author_hash"),
            col("features.text_for_model").alias("text_for_model"),
            col("features.feature_version").alias("feature_version"),
            col("snapshots.observation_id").alias("observation_id"),
            col("snapshots.observed_at").alias("label_observed_at"),
            col("snapshots.provenance_json").alias("provenance_json"),
            col("snapshots.coverage_json").alias("coverage_json"),
            col("snapshots.metrics_refresh_status").alias("metrics_refresh_status"),
            *[col(f"snapshots.{metric}").alias(metric) for metric in ALL_METRICS],
            *[
                col(f"snapshots.{metric}_available").alias(f"{metric}_available")
                for metric in ALL_METRICS
            ],
            *[col(f"snapshots.{metric}").alias(metric) for metric in AUDIENCE_BY_SOURCE.values()],
            *[
                col(f"snapshots.{metric}_available").alias(f"{metric}_available")
                for metric in AUDIENCE_BY_SOURCE.values()
            ],
        )
    )
    selected = selected.withColumn(
        "observation_id",
        coalesce(
            col("observation_id"),
            sha2(
                concat_ws(
                    "\u001f",
                    col("source"),
                    col("platform_event_id"),
                    col("label_observed_at").cast("string"),
                ),
                256,
            ),
        ),
    )
    observed, score_total, expected = _metric_expressions(selected)
    # The collected audience values are not guaranteed to predate publication.
    # A viral post may already have increased them by collection time, so exposing
    # them to an official model would leak future outcome information. Keep the Gold
    # contract nullable until a timestamped reputation history can satisfy
    # audience_observed_at <= event_ts.
    audience_count = lit(None).cast("bigint")
    audience_available = lit(False)
    label_horizon = VALID_HORIZONS[label_horizon_hours]
    scored = (
        selected.withColumn("engagement_observed_metrics", observed.cast("int"))
        .withColumn(
            "engagement_coverage",
            when(expected > 0, observed.cast("double") / expected.cast("double")),
        )
        .withColumn(
            "engagement_score",
            when(observed > 0, score_total / sqrt(observed.cast("double"))),
        )
        .withColumn("audience_count", audience_count)
        .withColumn("audience_available", audience_available)
        .filter(col("engagement_observed_metrics") > 0)
        .withColumn("label_horizon", lit(label_horizon))
        .withColumn(
            "example_id",
            sha2(
                concat_ws(
                    "\u001f",
                    col("source"),
                    col("platform_event_id"),
                    col("observation_id"),
                    lit(label_horizon),
                    coalesce(col("feature_version"), lit("unknown")),
                ),
                256,
            ),
        )
    )
    context_fields = [
        "observation_id",
        "label_observed_at",
        "metrics_refresh_status",
        "engagement_observed_metrics",
        "engagement_coverage",
        "audience_count",
        "audience_available",
        "provenance_json",
        "coverage_json",
        *ALL_METRICS,
        *[f"{metric}_available" for metric in ALL_METRICS],
    ]
    return scored.withColumn(
        "context_feature_snapshot",
        to_json(struct(*[col(name) for name in context_fields])),
    )


def assign_split(scored: DataFrame, *, holdout_fraction: float, split_seed: int) -> DataFrame:
    """Reproduce GroupShuffleSplit semantics from stable author/example identities."""

    with_groups = scored.withColumn("_split_group", coalesce(col("author_hash"), col("example_id")))
    groups = sorted(
        row["_split_group"] for row in with_groups.select("_split_group").distinct().collect()
    )
    if len(groups) < 2:
        raise RuntimeError("At least two author groups are required for a train/holdout split")
    holdout_count = int(math.ceil(len(groups) * holdout_fraction))
    permutation = np.random.RandomState(split_seed).permutation(len(groups))
    holdout = {groups[int(index)] for index in permutation[:holdout_count]}
    assignments = [(group, "holdout" if group in holdout else "train") for group in groups]
    assignment_frame = scored.sparkSession.createDataFrame(
        assignments, schema="_split_group string, split_name string"
    )
    return with_groups.join(assignment_frame, "_split_group", "inner").drop("_split_group")


def reference_scores(scored: DataFrame) -> dict[str, list[float]]:
    """Collect only the explicitly materialized training partition for contract estimation."""

    result: dict[str, list[float]] = {}
    rows = (
        scored.filter(col("split_name") == "train")
        .select("source", "engagement_score")
        .toLocalIterator()
    )
    for row in rows:
        result.setdefault(str(row["source"]), []).append(float(row["engagement_score"]))
    return result


def apply_virality_contract(
    scored: DataFrame,
    contract: ViralityContract,
    *,
    dataset_version: str,
) -> DataFrame:
    """Apply absolute platform thresholds; no rank, target rate, or tie breaker exists."""

    observed_platforms = {
        str(row["source"]) for row in scored.select("source").distinct().collect()
    }
    missing = sorted(observed_platforms - set(contract.thresholds))
    if missing:
        raise RuntimeError(
            "Frozen virality contract has no threshold for platform(s): " + ", ".join(missing)
        )
    label_expression = None
    for platform in sorted(contract.thresholds):
        threshold = float(contract.thresholds[platform]["value"])
        platform_label = when(col("engagement_score") >= lit(threshold), lit("viral")).otherwise(
            lit("not_viral")
        )
        condition = col("source") == platform
        label_expression = (
            when(condition, platform_label)
            if label_expression is None
            else label_expression.when(condition, platform_label)
        )
    return (
        scored.withColumn("label_value", label_expression)
        .withColumn("dataset_version", lit(dataset_version))
        .withColumn("virality_policy", lit(contract.policy))
        .withColumn("virality_contract_fingerprint", lit(contract.fingerprint))
        .withColumn("schema_version", lit(TRAINING_EXAMPLES_SCHEMA_VERSION))
        .withColumn("example_date", to_date(col("event_ts")))
        .select(*TRAINING_EXAMPLES_COLUMNS)
    )


def _merge_examples(examples: DataFrame) -> None:
    examples.createOrReplaceTempView("incoming_training_examples")
    columns = ", ".join(TRAINING_EXAMPLES_COLUMNS)
    values = ", ".join(f"source.{name}" for name in TRAINING_EXAMPLES_COLUMNS)
    examples.sparkSession.sql(
        f"""
        MERGE INTO {TRAINING_EXAMPLES_TABLE} AS target
        USING incoming_training_examples AS source
        ON target.dataset_version = source.dataset_version
           AND target.example_id = source.example_id
        WHEN NOT MATCHED THEN
          INSERT ({columns}) VALUES ({values})
        """
    )


def _manifest_for(
    examples: DataFrame,
    identity: DatasetIdentity,
    contract: ViralityContract,
) -> dict:
    example_count = examples.count()
    if example_count == 0:
        raise RuntimeError("No eligible official training examples were produced")
    period = examples.agg(
        spark_min("event_ts").alias("period_start"),
        spark_max("event_ts").alias("period_end"),
    ).first()
    missing_counts = examples.agg(
        *[
            spark_sum(when(col(name).isNull(), 1).otherwise(0)).alias(name)
            for name in (
                "author_hash",
                "audience_count",
                "observation_id",
                "text_for_model",
            )
        ]
    ).first()
    if period is None or missing_counts is None:
        raise RuntimeError("Training example aggregates unexpectedly returned no row")
    missing_rates = {
        name: missing_rate(int(missing_counts[name] or 0), example_count)
        for name in missing_counts.asDict()
    }
    distribution = [
        row.asDict(recursive=True)
        for row in examples.groupBy("source", "label_value")
        .count()
        .orderBy("source", "label_value")
        .collect()
    ]
    created_at = datetime.now(timezone.utc)
    snapshots = {
        table: int(snapshot_id) for table, snapshot_id in sorted(identity.source_snapshots.items())
    }
    return {
        "dataset_version": identity.dataset_version,
        "schema_version": identity.schema_version,
        "period_start": period["period_start"],
        "period_end": period["period_end"],
        "source_tables_json": canonical_json({"tables": sorted(snapshots)}),
        "iceberg_snapshots_json": canonical_json(snapshots),
        "filters_json": canonical_json(dict(identity.filters)),
        "example_count": example_count,
        "missing_rates_json": canonical_json(missing_rates),
        "distributions_json": canonical_json({"source_label": distribution}),
        "labeling_json": canonical_json(
            {
                "target": "virality",
                "policy": contract.policy,
                "virality_contract_fingerprint": contract.fingerprint,
                "quantile": contract.payload["quantile"],
                "quantile_method": contract.payload["quantile_method"],
                "engagement": contract.payload["engagement"],
                "thresholds": contract.payload["thresholds"],
            }
        ),
        "virality_policy": contract.policy,
        "virality_contract_fingerprint": contract.fingerprint,
        "dataset_fingerprint": identity.fingerprint,
        "created_at": created_at,
    }


def _merge_manifest(spark: SparkSession, manifest: dict) -> None:
    spark.createDataFrame([manifest]).select(*DATASET_MANIFESTS_COLUMNS).createOrReplaceTempView(
        "incoming_dataset_manifest"
    )
    columns = ", ".join(DATASET_MANIFESTS_COLUMNS)
    values = ", ".join(f"source.{name}" for name in DATASET_MANIFESTS_COLUMNS)
    spark.sql(
        f"""
        MERGE INTO {DATASET_MANIFESTS_TABLE} AS target
        USING incoming_dataset_manifest AS source
        ON target.dataset_version = source.dataset_version
        WHEN NOT MATCHED THEN
          INSERT ({columns}) VALUES ({values})
        """
    )


def _existing_manifest(spark: SparkSession, dataset_version: str) -> dict:
    row = (
        spark.table(DATASET_MANIFESTS_TABLE)
        .filter(col("dataset_version") == dataset_version)
        .limit(1)
        .first()
    )
    if row is None:
        raise RuntimeError(f"Unknown lakehouse dataset version: {dataset_version}")
    return row.asDict(recursive=True)


def _export_dataset(
    examples: DataFrame,
    manifest: dict,
    *,
    training_snapshot_id: int,
    export_root: Path,
    manifest_output: Path,
) -> None:
    version = str(manifest["dataset_version"])
    expected_count = int(manifest["example_count"])
    actual_count = examples.count()
    if actual_count != expected_count:
        raise RuntimeError(
            f"Dataset {version} contains {actual_count} rows; manifest expects {expected_count}"
        )
    dataset_path = export_root / "datasets" / version
    examples.orderBy("example_id").coalesce(1).write.mode("overwrite").parquet(str(dataset_path))
    manifest_output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        **manifest,
        "created_at": str(manifest.get("created_at")),
        "period_start": str(manifest.get("period_start")),
        "period_end": str(manifest.get("period_end")),
        "dataset_relative_path": os.path.relpath(dataset_path, manifest_output.parent),
        "format": "parquet",
        "official_input": True,
        "training_table": TRAINING_EXAMPLES_TABLE,
        "training_snapshot_id": training_snapshot_id,
    }
    payload["labeling"] = json.loads(str(manifest["labeling_json"]))
    payload["labeling"]["contract_relative_path"] = os.path.relpath(
        export_root / "virality-contracts" / f"{manifest['virality_contract_fingerprint']}.json",
        manifest_output.parent,
    )
    manifest_output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _print_contract_summary(contract: ViralityContract, examples: DataFrame) -> None:
    applications = {
        str(row["source"]): {
            "count": int(row["count"]),
            "positive_count": int(row["positive_count"] or 0),
        }
        for row in examples.groupBy("source")
        .agg(
            spark_sum(lit(1)).alias("count"),
            spark_sum(when(col("label_value") == "viral", 1).otherwise(0)).alias("positive_count"),
        )
        .collect()
    }
    print("Virality reference contract")
    print("---------------------------")
    print(f"Policy: {contract.policy}")
    print(f"Quantile: {contract.payload['quantile']} ({QUANTILE_METHOD})")
    print(
        f"Horizon: {contract.payload['engagement']['horizon_hours']}h | "
        f"Tolerance: {contract.payload['engagement']['tolerance_hours']}h"
    )
    for platform, threshold in sorted(contract.thresholds.items()):
        application = applications.get(platform, {"count": 0, "positive_count": 0})
        rate = application["positive_count"] / application["count"] if application["count"] else 0.0
        print(
            f"{platform}: reference_rows={threshold['reference_count']} "
            f"virality_engagement_threshold={threshold['value']:.12g} "
            f"applied_positive_count={application['positive_count']} "
            f"applied_positive_rate={rate:.6f}"
        )
    print(f"Contract fingerprint: {contract.fingerprint}")
    print("Status: VALID")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _validate_args(args)
    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")
    create_gold_tables(spark)
    manifest_output = args.manifest_output or args.export_root / "current.json"
    resolved_contract = None

    if args.dataset_version != "auto":
        manifest = _existing_manifest(spark, args.dataset_version)
        training_snapshot_id = args.training_examples_snapshot_id
        examples = _read_snapshot(
            spark,
            TRAINING_EXAMPLES_TABLE,
            training_snapshot_id,
        ).filter(col("dataset_version") == args.dataset_version)
        if examples.limit(1).count() == 0:
            raise RuntimeError(
                f"Dataset manifest exists but examples are missing: {args.dataset_version}"
            )
    else:
        snapshots = {
            POST_FEATURES_TABLE: args.post_features_snapshot_id
            or _latest_snapshot_id(spark, POST_FEATURES_TABLE),
            ENGAGEMENT_SNAPSHOTS_TABLE: args.engagement_snapshots_snapshot_id
            or _latest_snapshot_id(spark, ENGAGEMENT_SNAPSHOTS_TABLE),
        }
        scoring_filters = {
            "audience_feature_policy": AUDIENCE_FEATURE_POLICY,
            "dataset_builder_revision": DATASET_BUILDER_REVISION,
            "label_horizon_hours": args.label_horizon_hours,
            "label_tolerance_hours": args.label_tolerance_hours,
            "min_text_chars": args.min_text_chars,
            "required_observed_metrics": 1,
            "engagement_score_version": ENGAGEMENT_SCORE_VERSION,
            "observation_selection_policy": OBSERVATION_SELECTION_POLICY,
            "holdout_fraction": args.holdout_fraction,
            "split_seed": args.split_seed,
        }
        reference_identity = DatasetIdentity(
            schema_version=TRAINING_EXAMPLES_SCHEMA_VERSION,
            source_snapshots=snapshots,
            filters=scoring_filters,
        )
        scored = assign_split(
            build_scored_examples(
                _read_snapshot(spark, POST_FEATURES_TABLE, snapshots[POST_FEATURES_TABLE]),
                _read_snapshot(
                    spark,
                    ENGAGEMENT_SNAPSHOTS_TABLE,
                    snapshots[ENGAGEMENT_SNAPSHOTS_TABLE],
                ),
                label_horizon_hours=args.label_horizon_hours,
                label_tolerance_hours=args.label_tolerance_hours,
                min_text_chars=args.min_text_chars,
            ),
            holdout_fraction=args.holdout_fraction,
            split_seed=args.split_seed,
        ).cache()
        if args.virality_policy == PLATFORM_REFERENCE_POLICY:
            contract = ViralityContract.load(args.virality_contract)
            if contract.policy != PLATFORM_REFERENCE_POLICY:
                raise ValueError("External official contract must use platform_reference_quantile")
            engagement = contract.payload["engagement"]
            if (
                int(engagement["horizon_hours"]) != args.label_horizon_hours
                or int(engagement["tolerance_hours"]) != args.label_tolerance_hours
                or engagement["engagement_score_version"] != ENGAGEMENT_SCORE_VERSION
            ):
                raise ValueError(
                    "Frozen virality contract is incompatible with the requested engagement contract"
                )
        else:
            contract = build_contract(
                reference_scores(scored),
                policy=TRAINING_REFERENCE_POLICY,
                quantile=args.viral_quantile,
                reference={
                    "reference_population": "deterministic_training_partition",
                    "construction_fingerprint": reference_identity.fingerprint,
                    "source_snapshots": snapshots,
                    "holdout_excluded": True,
                    "holdout_fraction": args.holdout_fraction,
                    "split_seed": args.split_seed,
                },
                horizon_hours=args.label_horizon_hours,
                tolerance_hours=args.label_tolerance_hours,
                eligibility_filters=scoring_filters,
                min_reference_examples_per_platform=(args.min_reference_examples_per_platform),
            )
        filters = {
            **scoring_filters,
            "label_policy": contract.policy,
            "quantile": contract.payload["quantile"],
            "quantile_method": QUANTILE_METHOD,
            "virality_contract_fingerprint": contract.fingerprint,
        }
        identity = DatasetIdentity(
            schema_version=TRAINING_EXAMPLES_SCHEMA_VERSION,
            source_snapshots=snapshots,
            filters=filters,
        )
        contract_path = args.export_root / "virality-contracts" / f"{contract.fingerprint}.json"
        contract.write(contract_path)
        if args.virality_contract_output:
            contract.write(args.virality_contract_output)
        resolved_contract = contract
        examples = apply_virality_contract(
            scored,
            contract,
            dataset_version=identity.dataset_version,
        ).cache()
        scored.unpersist()
        try:
            manifest = _manifest_for(examples, identity, contract)
            _merge_examples(examples)
            training_snapshot_id = _latest_snapshot_id(spark, TRAINING_EXAMPLES_TABLE)
            _merge_manifest(spark, manifest)
            manifest = _existing_manifest(spark, identity.dataset_version)
        finally:
            examples.unpersist()
        examples = _read_snapshot(
            spark,
            TRAINING_EXAMPLES_TABLE,
            training_snapshot_id,
        ).filter(col("dataset_version") == identity.dataset_version)

    _export_dataset(
        examples,
        manifest,
        training_snapshot_id=training_snapshot_id,
        export_root=args.export_root,
        manifest_output=manifest_output,
    )
    if resolved_contract is not None:
        _print_contract_summary(resolved_contract, examples)
    print(
        json.dumps(
            {
                "dataset_version": manifest["dataset_version"],
                "example_count": int(manifest["example_count"]),
                "training_snapshot_id": training_snapshot_id,
                "manifest_output": str(manifest_output),
                "virality_policy": manifest["virality_policy"],
                "virality_contract_fingerprint": manifest["virality_contract_fingerprint"],
            },
            sort_keys=True,
        )
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
