"""Apply committed Bronze journal events from Kafka to Silver exactly once."""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, from_json, lit, sha2, trim, when

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from event_contract import spark_struct_type
from pipeline.reliability import fail_on_data_loss_option
from pipeline.silver_merge import apply_events_to_silver, ensure_silver_tables


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _trigger(writer, mode: str, interval: str):
    if mode == "available_now":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=interval)


def _build_spark(app_name: str, warehouse: str) -> SparkSession:
    minio_endpoint = _env("MINIO_ENDPOINT", "http://minio:9000")
    access_key = _env("MINIO_ROOT_USER", "minioadmin")
    secret_key = _env("MINIO_ROOT_PASSWORD", "minioadmin")

    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", _env("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.default.parallelism", _env("SPARK_DEFAULT_PARALLELISM", "4"))
        .getOrCreate()
    )


def main() -> None:
    kafka_bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topics = _env("SILVER_KAFKA_TOPICS", "lakehouse.bronze.for_silver")
    bucket = _env("MINIO_BUCKET", "lakehouse")
    spark = _build_spark("bronze-to-silver-from-kafka", f"s3a://{bucket}/warehouse")
    spark.sparkContext.setLogLevel("WARN")
    ensure_silver_tables(spark)

    fail_on_data_loss = fail_on_data_loss_option(
        os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "true"),
        allow_data_loss=os.getenv("ALLOW_KAFKA_DATA_LOSS", "false"),
    )
    if fail_on_data_loss == "false":
        print(
            json.dumps(
                {
                    "level": "warning",
                    "event": "kafka_data_loss_override",
                    "stage": "silver",
                    "topics": kafka_topics.split(","),
                },
                sort_keys=True,
            )
        )

    schema = spark_struct_type(
        ("metadata_refreshed_at", "timestamp"),
        ("event_ts", "timestamp"),
    )
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topics)
        .option("startingOffsets", _env("SILVER_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", fail_on_data_loss)
        .load()
    )
    parsed = raw.select(
        col("value").cast("string").alias("_raw_value"),
        from_json(col("value").cast("string"), schema).alias("data"),
    ).select("_raw_value", "data.*")
    events = (
        parsed.withColumn(
            "payload_fingerprint",
            coalesce(col("payload_fingerprint"), sha2(col("_raw_value"), 256)),
        )
        .withColumn(
            "event_id",
            when(
                trim(coalesce(col("event_id"), lit(""))).rlike("^[0-9a-fA-F]{64}$"),
                col("event_id"),
            ).otherwise(
                sha2(
                    concat_ws(
                        "\u001f",
                        lit("v1"),
                        coalesce(col("source"), lit("")),
                        coalesce(col("event_id"), lit("")),
                        coalesce(col("platform_event_id"), lit("")),
                        coalesce(col("user_id"), lit("")),
                        coalesce(col("url"), lit("")),
                        coalesce(col("timestamp"), lit("")),
                        coalesce(col("event_type"), lit("")),
                        coalesce(col("event_version"), lit("")),
                        coalesce(col("collected_at"), lit("")),
                        col("payload_fingerprint"),
                    ),
                    256,
                )
            ),
        )
        .drop("_raw_value")
    )

    run_id = _env("PIPELINE_RUN_ID", "standalone")

    def _foreach_batch(df: DataFrame, epoch_id: int) -> None:
        result = apply_events_to_silver(df, epoch_id=epoch_id, run_id=run_id)
        print(
            json.dumps(
                {
                    "event": "silver_apply_complete",
                    "epoch_id": epoch_id,
                    "input_events": result.input_events,
                    "already_applied": result.already_applied,
                    "newly_applied": result.newly_applied,
                    "current_rows_merged": result.current_rows_merged,
                },
                sort_keys=True,
            )
        )

    checkpoint_key = re.sub(r"[^a-zA-Z0-9._-]+", "_", kafka_topics)
    checkpoint_version = _env("SILVER_CHECKPOINT_VERSION", "applied_events_v1")
    checkpoint = (
        f"s3a://{bucket}/checkpoints/silver/events/kafka/{checkpoint_version}/{checkpoint_key}"
    )
    writer = (
        events.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(_foreach_batch)
    )
    query = _trigger(
        writer,
        _env("SILVER_TRIGGER_MODE", "processing_time").lower(),
        _env("PROCESSING_TRIGGER", "30 seconds"),
    ).start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
