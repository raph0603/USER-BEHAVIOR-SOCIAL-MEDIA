import os
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    expr,
    from_json,
    lit,
    regexp_replace,
    sha2,
    struct,
    to_json,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from cleaning import clean_text, invalid_reason


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _trigger(writer, mode: str, interval: str):
    if mode == "available_now":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=interval)


def _build_spark(app_name: str) -> SparkSession:
    minio_endpoint = _env("MINIO_ENDPOINT", "http://minio:9000")
    access_key = _env("MINIO_ROOT_USER", "minioadmin")
    secret_key = _env("MINIO_ROOT_PASSWORD", "minioadmin")

    return (
        SparkSession.builder.appName(app_name)
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
    platform = _env("PLATFORM", "").strip().lower()
    if platform not in {"youtube", "x", "reddit"}:
        raise ValueError(
            f"Unsupported PLATFORM={platform!r}; expected youtube, x or reddit"
        )

    kafka_bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    source_topic = _env(
        "COLLECTOR_SOURCE_TOPIC",
        f"{platform}.raw.events",
    )
    clean_topic = _env("CLEAN_KAFKA_TOPIC", f"{platform}.clean.events")
    dlq_topic = _env("DLQ_KAFKA_TOPIC", f"{platform}.dlq.events")
    schema_path = _env("SCHEMA_PATH", "/opt/spark/schemas/playwright_event.avsc")
    value_format = _env("CLEAN_SOURCE_VALUE_FORMAT", "avro").lower()
    bucket = _env("MINIO_BUCKET", "lakehouse")
    privacy_hash_salt = _env("PRIVACY_HASH_SALT", "dev-privacy-salt")
    starting_offsets = _env("CLEAN_STARTING_OFFSETS", "earliest")
    trigger_interval = _env("CLEAN_TRIGGER", "10 seconds")
    trigger_mode = _env("CLEAN_TRIGGER_MODE", "processing_time").lower()
    checkpoint_version = _env("CLEAN_CHECKPOINT_VERSION", "pre_bronze_v3")
    checkpoint_key = re.sub(r"[^a-zA-Z0-9._-]+", "_", source_topic)

    spark = _build_spark(f"collector-event-cleaning-{platform}")
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", source_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    metadata = raw.select(
        col("topic").alias("_kafka_topic"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        col("value"),
    )
    if value_format == "avro":
        schema = Path(schema_path).read_text(encoding="utf-8")
        decoded = (
            metadata.withColumn(
                "avro_value",
                expr("substring(value, 6, length(value) - 5)"),
            )
            .select(
                "_kafka_topic",
                "_kafka_partition",
                "_kafka_offset",
                from_avro(col("avro_value"), schema).alias("data"),
            )
            .select("_kafka_topic", "_kafka_partition", "_kafka_offset", "data.*")
        )
    elif value_format == "json":
        event_schema = StructType(
            [
                StructField("user_id", StringType()),
                StructField("url", StringType()),
                StructField("title", StringType()),
                StructField("timestamp", StringType()),
                StructField("source", StringType()),
                StructField("error", StringType()),
                StructField("platform_event_id", StringType()),
                StructField("owner_channel_id", StringType()),
                StructField(
                    "collaborator_channel_ids",
                    ArrayType(StringType()),
                ),
                StructField("like_count", LongType()),
                StructField("comment_count", LongType()),
                StructField("reply_count", LongType()),
                StructField("view_count", LongType()),
                StructField("retweet_count", LongType()),
                StructField("bookmark_count", LongType()),
                StructField("score", LongType()),
            ]
        )
        decoded = metadata.select(
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            from_json(col("value").cast("string"), event_schema).alias("data"),
        ).select("_kafka_topic", "_kafka_partition", "_kafka_offset", "data.*")
    else:
        raise ValueError(
            f"Unsupported CLEAN_SOURCE_VALUE_FORMAT={value_format!r}; "
            "expected avro or json"
        )

    decoded = decoded.filter(col("source") == lit(platform))
    protected = (
        decoded.withColumn(
            "user_id",
            when(col("user_id").isNull(), lit(None)).otherwise(
                sha2(
                    concat_ws(":", lit(privacy_hash_salt), col("user_id")),
                    256,
                )
            ),
        )
        .withColumn("url", regexp_replace(col("url"), r"#.*$", ""))
        .withColumn("title", clean_text(col("title")))
        .withColumn("error", clean_text(col("error")))
    )

    reason = (
        when(protected["user_id"].isNull(), lit("missing_user_id"))
        .when(protected["url"].isNull(), lit("missing_url"))
        .when(protected["timestamp"].isNull(), lit("missing_timestamp"))
        .when(protected["error"].isNotNull(), lit("collector_error"))
        .otherwise(invalid_reason(protected["title"]))
    )
    validated = protected.withColumn("_invalid_reason", reason)

    clean_payload = validated.filter(col("_invalid_reason").isNull()).select(
        col("user_id").cast("string").alias("key"),
        to_json(
            struct(
                "user_id",
                "url",
                "title",
                "timestamp",
                "source",
                "error",
                "platform_event_id",
                "owner_channel_id",
                "collaborator_channel_ids",
                "like_count",
                "comment_count",
                "reply_count",
                "view_count",
                "retweet_count",
                "bookmark_count",
                "score",
                lit("clean").alias("stage"),
            )
        ).alias("value"),
    )

    dlq_payload = validated.filter(col("_invalid_reason").isNotNull()).select(
        to_json(
            struct(
                col("_invalid_reason").alias("reason"),
                "user_id",
                "url",
                "title",
                "timestamp",
                "source",
                "error",
                "platform_event_id",
                "owner_channel_id",
                "collaborator_channel_ids",
                "like_count",
                "comment_count",
                "reply_count",
                "view_count",
                "retweet_count",
                "bookmark_count",
                "score",
                "_kafka_topic",
                "_kafka_partition",
                "_kafka_offset",
                current_timestamp().alias("failed_at"),
            )
        ).alias("value")
    )

    clean_writer = (
        clean_payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", clean_topic)
        .option(
            "checkpointLocation",
            f"s3a://{bucket}/checkpoints/clean/{checkpoint_version}/"
            f"{platform}/{checkpoint_key}/valid",
        )
        .outputMode("append")
    )
    clean_query = _trigger(
        clean_writer,
        trigger_mode,
        trigger_interval,
    ).start()

    dlq_writer = (
        dlq_payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", dlq_topic)
        .option(
            "checkpointLocation",
            f"s3a://{bucket}/checkpoints/clean/{checkpoint_version}/"
            f"{platform}/{checkpoint_key}/dlq",
        )
        .outputMode("append")
    )
    dlq_query = _trigger(
        dlq_writer,
        trigger_mode,
        trigger_interval,
    ).start()

    clean_query.awaitTermination()
    dlq_query.awaitTermination()


if __name__ == "__main__":
    main()
