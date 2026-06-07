import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, concat_ws, expr, lit, regexp_replace, sha2, to_timestamp, when
from pyspark.sql.functions import struct, to_json


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _build_spark(app_name: str, warehouse: str) -> SparkSession:
    minio_endpoint = _env("MINIO_ENDPOINT", "http://minio:9000")
    access_key = _env("MINIO_ROOT_USER", "minioadmin")
    secret_key = _env("MINIO_ROOT_PASSWORD", "minioadmin")

    spark = (
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
        .getOrCreate()
    )

    return spark


def _redact_text(column):
    redacted = regexp_replace(
        column,
        r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
        "[REDACTED_EMAIL]",
    )
    redacted = regexp_replace(
        redacted,
        r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)(?!\d)",
        "[REDACTED_PHONE]",
    )
    return regexp_replace(
        redacted,
        r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
        "[REDACTED_IP]",
    )


def _apply_privacy_gateway(events: DataFrame, hash_salt: str) -> DataFrame:
    hashed_user_id = when(
        col("user_id").isNull(),
        lit(None),
    ).otherwise(sha2(concat_ws(":", lit(hash_salt), col("user_id")), 256))

    sanitized_url = regexp_replace(col("url"), r"([?#]).*$", "")

    return events.select(
        hashed_user_id.alias("user_id"),
        sanitized_url.alias("url"),
        _redact_text(col("title")).alias("title"),
        col("timestamp"),
        col("source"),
        _redact_text(col("error")).alias("error"),
    )


def main() -> None:
    kafka_bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topics = _env(
        "KAFKA_TOPIC",
        "youtube.raw.events,x.raw.events,reddit.raw.events",
    )
    schema_path = _env("SCHEMA_PATH", "/opt/spark/schemas/playwright_event.avsc")
    bucket = _env("MINIO_BUCKET", "lakehouse")
    privacy_hash_salt = _env("PRIVACY_HASH_SALT", "dev-privacy-salt")

    warehouse = f"s3a://{bucket}/warehouse"
    checkpoint_key = kafka_topics.replace(",", "__")
    checkpoint = f"s3a://{bucket}/checkpoints/bronze/events/{checkpoint_key}"

    spark = _build_spark("kafka-to-iceberg-bronze", warehouse)

    schema = Path(schema_path).read_text(encoding="utf-8")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.events (
          user_id STRING,
          url STRING,
          title STRING,
          timestamp STRING,
          source STRING,
          error STRING,
          event_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
        """
    )

    starting_offsets = _env("KAFKA_STARTING_OFFSETS", "earliest")
    fail_on_data_loss = _env("KAFKA_FAIL_ON_DATA_LOSS", "false")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topics)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", fail_on_data_loss)
        .load()
    )

    decoded = (
        raw.withColumn("avro_value", expr("substring(value, 6, length(value) - 5)"))
        .select(from_avro(col("avro_value"), schema).alias("data"))
        .select("data.*")
    )
    privacy_safe = _apply_privacy_gateway(decoded, privacy_hash_salt)
    enriched = privacy_safe.withColumn("event_ts", to_timestamp(col("timestamp")))

    # trigger configuration for Bronze micro-batches
    bronze_trigger = _env("BRONZE_TRIGGER", "10 seconds")

    # write to Iceberg Bronze table (micro-batched)
    iceberg_query = (
        enriched.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=bronze_trigger)
        .toTable("lakehouse.bronze.events")
    )

    # Optionally publish a sanitized JSON representation to a Kafka topic for downstream jobs
    kafka_out_topic = _env("BRONZE_KAFKA_OUT_TOPIC", "lakehouse.bronze.for_silver")
    kafka_out_checkpoint = (
        f"s3a://{bucket}/checkpoints/bronze/to_kafka/{checkpoint_key}"
    )

    kafka_payload = enriched.select(to_json(struct("user_id", "url", "title", "timestamp", "source", "error", "event_ts")).alias("value"))

    kafka_query = (
        kafka_payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", kafka_out_topic)
        .option("checkpointLocation", kafka_out_checkpoint)
        .trigger(processingTime=bronze_trigger)
        .start()
    )

    # keep both streams running
    iceberg_query.awaitTermination()
    kafka_query.awaitTermination()


if __name__ == "__main__":
    main()
