import json
import os
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    coalesce,
    current_timestamp,
    greatest,
    lit,
    row_number,
    sha2,
    when,
)


METRICS_BY_SOURCE = {
    "youtube": ("view_count", "like_count", "comment_count"),
    "x": (
        "like_count",
        "reply_count",
        "view_count",
        "retweet_count",
        "bookmark_count",
    ),
    "reddit": ("score", "comment_count"),
}
METRIC_COLUMNS = tuple(
    sorted({metric for metrics in METRICS_BY_SOURCE.values() for metric in metrics})
)
DERIVED_DIMENSIONS = {
    "engagement_band",
    "comment_type",
}
ALLOWED_DIMENSIONS = {
    "source",
    "event_date",
    *DERIVED_DIMENSIONS,
}
DEFAULT_DIMENSIONS = ("source",)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(_env(name, str(default))))
    except ValueError:
        return default


def _parse_dimensions(raw_value: str) -> list[str]:
    dimensions = [
        dimension.strip() for dimension in raw_value.split(",") if dimension.strip()
    ] or list(DEFAULT_DIMENSIONS)
    unknown = sorted(set(dimensions) - ALLOWED_DIMENSIONS)
    if unknown:
        raise ValueError(
            "Unsupported BALANCE_DIMENSIONS values: "
            + ", ".join(unknown)
            + ". Allowed values: "
            + ", ".join(sorted(ALLOWED_DIMENSIONS))
        )
    return dimensions


def _build_spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("build-balanced-dataset")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config(
            "spark.sql.catalog.lakehouse.warehouse",
            f"s3a://{bucket}/warehouse",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            _env("MINIO_ENDPOINT", "http://minio:9000"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            _env("MINIO_ROOT_USER", "minioadmin"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            _env("MINIO_ROOT_PASSWORD", "minioadmin"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


def _ensure_columns(spark: SparkSession, table: str, columns: dict[str, str]) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


def _distribution(dataframe, dimensions: list[str]) -> list[dict]:
    return [
        row.asDict(recursive=True)
        for row in (dataframe.groupBy(*dimensions).count().orderBy(*dimensions).collect())
    ]


def _metric_available(dataframe, metric: str):
    availability_column = f"{metric}_available"
    if availability_column in dataframe.columns:
        return coalesce(
            col(availability_column).cast("boolean"),
            col(metric).isNotNull(),
        )
    return col(metric).isNotNull()


def _engagement_expressions(dataframe):
    totals = []
    observations = []
    expected = lit(0)
    for source, metrics in METRICS_BY_SOURCE.items():
        expected = when(col("source") == source, lit(len(metrics))).otherwise(expected)
        for metric in metrics:
            valid = (
                (col("source") == source)
                & _metric_available(dataframe, metric)
                & col(metric).isNotNull()
            )
            totals.append(
                when(
                    valid,
                    greatest(col(metric).cast("long"), lit(0).cast("long")),
                ).otherwise(lit(0).cast("long"))
            )
            observations.append(when(valid, lit(1)).otherwise(lit(0)))
    total = reduce(lambda left, right: left + right, totals)
    observed = reduce(lambda left, right: left + right, observations)
    return total, observed, expected


def main() -> None:
    source_table = _env("BALANCE_SOURCE_TABLE", "lakehouse.silver.events")
    output_table = _env(
        "BALANCE_OUTPUT_TABLE",
        "lakehouse.silver.balanced_events",
    )
    report_path = Path(_env("BALANCE_REPORT_PATH", "/opt/spark/balancing/report.json"))
    seed = _env_int("BALANCE_SEED", 42, minimum=0)
    requested_target = _env_int("BALANCE_TARGET_PER_GROUP", 0, minimum=0)
    dimensions = _parse_dimensions(_env("BALANCE_DIMENSIONS", ",".join(DEFAULT_DIMENSIONS)))

    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    _ensure_columns(
        spark,
        source_table,
        {
            "platform_event_id": "STRING",
            "metadata_refreshed_at": "TIMESTAMP",
            **{metric: "BIGINT" for metric in METRIC_COLUMNS},
            **{f"{metric}_available": "BOOLEAN" for metric in METRIC_COLUMNS},
        },
    )

    source = spark.table(source_table).filter(col("error").isNull())
    metric_total, observed_metrics, expected_metrics = _engagement_expressions(source)
    reply_available = _metric_available(source, "reply_count") & col("reply_count").isNotNull()
    prepared = (
        source.dropDuplicates(["source", "platform_event_id", "user_id", "url", "event_ts"])
        .withColumn("_engagement_total", metric_total)
        .withColumn("engagement_observed_metrics", observed_metrics.cast("int"))
        .withColumn(
            "engagement_coverage",
            when(
                expected_metrics > 0,
                observed_metrics.cast("double") / expected_metrics.cast("double"),
            ),
        )
        .withColumn(
            "engagement_band",
            when(col("engagement_observed_metrics") == 0, lit("unknown"))
            .when(col("_engagement_total") == 0, lit("none"))
            .when(col("_engagement_total") < 10, lit("low"))
            .when(col("_engagement_total") < 100, lit("medium"))
            .otherwise(lit("high")),
        )
        .withColumn(
            "comment_type",
            when(~reply_available, lit("unknown"))
            .when(col("reply_count") > 0, lit("has_replies"))
            .otherwise(lit("no_replies")),
        )
    )

    group_counts = prepared.groupBy(*dimensions).count()
    collected_counts = [row.asDict(recursive=True) for row in group_counts.collect()]
    total_before = prepared.count()
    if not collected_counts:
        report = {
            "source_table": source_table,
            "output_table": output_table,
            "seed": seed,
            "dimensions": dimensions,
            "requested_target_per_group": requested_target,
            "effective_target_per_group": 0,
            "total_before": total_before,
            "total_after": 0,
            "distribution_before": [],
            "distribution_after": [],
            "constraints": ["No eligible rows were available."],
        }
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        spark.stop()
        return

    minimum_group_size = min(row["count"] for row in collected_counts)
    effective_target = (
        minimum_group_size if requested_target == 0 else min(requested_target, minimum_group_size)
    )
    constraints = []
    if requested_target and requested_target > minimum_group_size:
        constraints.append(
            "Requested target exceeds the smallest group; "
            f"using {minimum_group_size} rows per group."
        )

    sample_key = sha2(
        concat_ws(
            ":",
            lit(str(seed)),
            col("source"),
            coalesce(col("platform_event_id"), lit("")),
            coalesce(col("url"), lit("")),
            coalesce(col("user_id"), lit("")),
            col("event_ts").cast("string"),
        ),
        256,
    )
    ranked = prepared.withColumn(
        "_balance_rank",
        row_number().over(Window.partitionBy(*dimensions).orderBy(sample_key)),
    )
    balanced = (
        ranked.filter(col("_balance_rank") <= effective_target)
        .withColumn("balance_group", concat_ws("|", *[col(d) for d in dimensions]))
        .withColumn("balance_seed", lit(seed))
        .withColumn("balanced_at", current_timestamp())
        .drop("_engagement_total")
    )

    balanced.writeTo(output_table).using("iceberg").createOrReplace()

    total_after = balanced.count()
    report = {
        "source_table": source_table,
        "output_table": output_table,
        "seed": seed,
        "dimensions": dimensions,
        "requested_target_per_group": requested_target,
        "effective_target_per_group": effective_target,
        "total_before": total_before,
        "total_after": total_after,
        "distribution_before": _distribution(prepared, dimensions),
        "distribution_after": _distribution(balanced, dimensions),
        "constraints": constraints,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {total_after} balanced rows to {output_table}; report: {report_path}")
    spark.stop()


if __name__ == "__main__":
    main()
