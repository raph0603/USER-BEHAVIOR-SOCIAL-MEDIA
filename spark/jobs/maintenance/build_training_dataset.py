"""Build and export a versioned training dataset from pinned Iceberg snapshots."""

from __future__ import annotations

import argparse
from importlib import metadata
import json
import os
import platform
import re
import sys
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    ceil,
    coalesce,
    col,
    concat_ws,
    count,
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

JOBS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
for import_root in (JOBS_ROOT, REPOSITORY_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))
from common.reproducibility import (
    file_sha256,
    fingerprint,
    manifest_sha256,
    normalize_container_digest,
)
from pipeline.dataset_manifest import DatasetIdentity, canonical_json, missing_rate
from pipeline.gold_schemas import (
    DATASET_MANIFESTS_COLUMNS,
    TRAINING_EXAMPLES_COLUMNS,
    TRAINING_EXAMPLES_SCHEMA_VERSION,
    create_gold_tables,
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


def _distribution_version(name: str) -> str | None:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def _build_environment(spark: SparkSession) -> dict:
    """Capture the runtime that materialized and exported the Gold dataset."""

    lock_path = Path(os.getenv("SPARK_DEPENDENCY_LOCK", "/tmp/requirements.txt"))
    if not lock_path.is_file():
        raise FileNotFoundError(f"Spark dependency lock is missing: {lock_path}")
    digest = normalize_container_digest(os.getenv("ML_CONTAINER_IMAGE_DIGEST"))
    environment = {
        "schema_version": "dataset-build-environment-v1",
        "code": {
            "git_commit": os.getenv("SOURCE_GIT_COMMIT") or None,
            "git_dirty": (os.getenv("SOURCE_GIT_DIRTY", "").strip().lower() == "true"),
        },
        "runtime": {
            "python": platform.python_version(),
            "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
            "spark": spark.version,
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "dependencies": {
            name.replace("-", "_"): _distribution_version(name)
            for name in ("pyspark", "pandas", "numpy", "pyarrow")
        },
        "dependency_lock": {
            "path": "spark/requirements.txt",
            "sha256": file_sha256(lock_path),
        },
        "container": {
            "image": os.getenv("ML_CONTAINER_IMAGE") or None,
            "digest": digest,
            "digest_available": digest is not None,
            "executor_image": os.getenv("SPARK_WORKER_IMAGE") or None,
            "executor_digest": normalize_container_digest(os.getenv("SPARK_WORKER_IMAGE_DIGEST")),
        },
    }
    environment["environment_fingerprint"] = fingerprint(environment)
    return environment


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


def build_training_examples(
    post_features: DataFrame,
    snapshots: DataFrame,
    *,
    dataset_version: str,
    label_horizon_hours: int,
    label_tolerance_hours: int,
    viral_quantile: float,
    min_text_chars: int,
) -> DataFrame:
    """Derive one coverage-aware, deterministic label per eligible content row."""

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
    rank_window = Window.partitionBy("source").orderBy(
        col("engagement_score").desc(),
        col("example_id").asc(),
    )
    count_window = Window.partitionBy("source")
    labeled = (
        scored.withColumn("_label_rank", row_number().over(rank_window))
        .withColumn("_source_count", count(lit(1)).over(count_window))
        .withColumn(
            "_viral_target",
            greatest(
                lit(1),
                ceil(col("_source_count") * lit(1.0 - viral_quantile)),
            ),
        )
        .withColumn(
            "label_value",
            when(col("_label_rank") <= col("_viral_target"), lit("viral")).otherwise(
                lit("not_viral")
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
    return (
        labeled.withColumn(
            "context_feature_snapshot",
            to_json(struct(*[col(name) for name in context_fields])),
        )
        .withColumn("dataset_version", lit(dataset_version))
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
    build_environment: dict,
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
        "manifest_schema_version": "dataset-manifest-v2",
        "created_at": str(manifest.get("created_at")),
        "period_start": str(manifest.get("period_start")),
        "period_end": str(manifest.get("period_end")),
        "dataset_relative_path": os.path.relpath(dataset_path, manifest_output.parent),
        "format": "parquet",
        "official_input": True,
        "training_table": TRAINING_EXAMPLES_TABLE,
        "training_snapshot_id": training_snapshot_id,
        "gold_table": TRAINING_EXAMPLES_TABLE,
        "gold_snapshot_id": int(training_snapshot_id),
        "build_environment": build_environment,
        "build_environment_fingerprint": build_environment["environment_fingerprint"],
    }
    payload["manifest_sha256"] = manifest_sha256(payload)
    manifest_output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _validate_args(args)
    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")
    create_gold_tables(spark)
    manifest_output = args.manifest_output or args.export_root / "current.json"

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
        filters = {
            "audience_feature_policy": AUDIENCE_FEATURE_POLICY,
            "dataset_builder_revision": DATASET_BUILDER_REVISION,
            "label_horizon_hours": args.label_horizon_hours,
            "label_strategy": "coverage_aware_source_rank_v1",
            "label_tolerance_hours": args.label_tolerance_hours,
            "min_text_chars": args.min_text_chars,
            "required_observed_metrics": 1,
            "viral_quantile": args.viral_quantile,
        }
        identity = DatasetIdentity(
            schema_version=TRAINING_EXAMPLES_SCHEMA_VERSION,
            source_snapshots=snapshots,
            filters=filters,
        )
        examples = build_training_examples(
            _read_snapshot(spark, POST_FEATURES_TABLE, snapshots[POST_FEATURES_TABLE]),
            _read_snapshot(
                spark,
                ENGAGEMENT_SNAPSHOTS_TABLE,
                snapshots[ENGAGEMENT_SNAPSHOTS_TABLE],
            ),
            dataset_version=identity.dataset_version,
            label_horizon_hours=args.label_horizon_hours,
            label_tolerance_hours=args.label_tolerance_hours,
            viral_quantile=args.viral_quantile,
            min_text_chars=args.min_text_chars,
        ).cache()
        try:
            manifest = _manifest_for(examples, identity)
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
        build_environment=_build_environment(spark),
    )
    print(
        json.dumps(
            {
                "dataset_version": manifest["dataset_version"],
                "example_count": int(manifest["example_count"]),
                "training_snapshot_id": training_snapshot_id,
                "manifest_output": str(manifest_output),
            },
            sort_keys=True,
        )
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
