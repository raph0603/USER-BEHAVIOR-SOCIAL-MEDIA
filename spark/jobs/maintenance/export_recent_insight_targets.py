import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, current_timestamp, expr, row_number


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(_env(name, str(default))))
    except ValueError:
        return default


def _build_spark() -> SparkSession:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    return (
        SparkSession.builder.appName("export-recent-insight-targets")
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


def main() -> None:
    lookback_days = _env_int("INSIGHT_REFRESH_LOOKBACK_DAYS", 15)
    youtube_max_age_days = _env_int("YOUTUBE_METRICS_MAX_AGE_DAYS", 3650)
    max_per_source = _env_int("INSIGHT_REFRESH_MAX_PER_SOURCE", 100)
    output_path = Path(
        _env(
            "INSIGHT_REFRESH_TARGETS_PATH",
            "/opt/spark/insight-refresh/targets.jsonl",
        )
    )

    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    _ensure_columns(
        spark,
        "lakehouse.silver.events",
        {
            "platform_event_id": "STRING",
            "metadata_refreshed_at": "TIMESTAMP",
            "last_metrics_refresh_at": "TIMESTAMP",
            "next_metrics_refresh_at": "TIMESTAMP",
            "metrics_refresh_count": "INT",
            "metrics_refresh_status": "STRING",
        },
    )

    recent = (
        spark.table("lakehouse.silver.events")
        .filter(col("source").isin("youtube", "x", "reddit"))
        .filter(
            (
                (col("source") == "youtube")
                & (
                    col("event_ts")
                    >= current_timestamp()
                    - expr(f"INTERVAL {youtube_max_age_days} DAYS")
                )
            )
            | (
                (col("source") != "youtube")
                & (
                    col("event_ts")
                    >= current_timestamp() - expr(f"INTERVAL {lookback_days} DAYS")
                )
            )
        )
        .filter(
            (col("source") != "youtube")
            | col("next_metrics_refresh_at").isNull()
            | (col("next_metrics_refresh_at") <= current_timestamp())
        )
        .filter(col("url").isNotNull())
        .dropDuplicates(
            ["source", "platform_event_id", "user_id", "url", "event_ts"]
        )
        .withColumn(
            "_rank",
            row_number().over(
                Window.partitionBy("source").orderBy(
                    col("next_metrics_refresh_at").asc_nulls_first(),
                    col("event_ts").desc(),
                )
            ),
        )
        .filter(col("_rank") <= max_per_source)
        .select(
            "user_id",
            "url",
            "title",
            "platform_event_id",
            col("event_ts").cast("string").alias("event_ts"),
            "source",
            col("metadata_refreshed_at").cast("string").alias("metadata_refreshed_at"),
            col("last_metrics_refresh_at").cast("string").alias("last_metrics_refresh_at"),
            col("next_metrics_refresh_at").cast("string").alias("next_metrics_refresh_at"),
            "metrics_refresh_count",
            "metrics_refresh_status",
            "like_count",
            "view_count",
            "comment_count",
        )
        .orderBy("source", col("event_ts").desc())
    )

    rows = [row.asDict(recursive=True) for row in recent.collect()]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=True) + "\n")
    temporary_path.replace(output_path)

    counts: dict[str, int] = {}
    for row in rows:
        counts[row["source"]] = counts.get(row["source"], 0) + 1
    print(
        f"Exported {len(rows)} insight targets from the last "
        f"{lookback_days} days to {output_path}: {counts}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
