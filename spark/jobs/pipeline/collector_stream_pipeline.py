import os
import re
import sys
import json
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col,
    coalesce,
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
from cleaning import clean_text, invalid_reason, prepare_text_for_model

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from event_contract import EVENT_COLUMNS, spark_struct_type


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


def _registered_avro_schemas(registry_url: str, subject: str) -> list[tuple[int, str]]:
    """Load every writer schema so historical Confluent records decode safely."""

    subject_url = quote(subject, safe="")
    with urlopen(
        f"{registry_url.rstrip('/')}/subjects/{subject_url}/versions",
        timeout=30,
    ) as response:
        versions = json.load(response)

    schemas = []
    for version in versions:
        with urlopen(
            f"{registry_url.rstrip('/')}/subjects/{subject_url}/versions/{version}",
            timeout=30,
        ) as response:
            registered = json.load(response)
        schemas.append((int(registered["id"]), registered["schema"]))
    if not schemas:
        raise RuntimeError(f"No Avro writer schemas are registered for {subject}")
    return schemas


def _decode_confluent_avro(metadata, registry_url: str, subject: str):
    """Decode each record with the writer schema identified by its wire header."""

    framed = (
        metadata.withColumn("_schema_id", expr("conv(hex(substring(value, 2, 4)), 16, 10)").cast("int"))
        .withColumn("_avro_value", expr("substring(value, 6, length(value) - 5)"))
    )
    registered_schemas = _registered_avro_schemas(registry_url, subject)
    known_schema_ids = [schema_id for schema_id, _ in registered_schemas]
    decoded = None
    for schema_id, writer_schema in registered_schemas:
        branch = (
            framed.filter(col("_schema_id") == lit(schema_id))
            .select(
                "_kafka_topic",
                "_kafka_partition",
                "_kafka_offset",
                from_avro(
                    col("_avro_value"),
                    writer_schema,
                    {"mode": "PERMISSIVE"},
                ).alias("data"),
            )
            .select("_kafka_topic", "_kafka_partition", "_kafka_offset", "data.*")
            .withColumn("_decode_error", lit(None).cast("string"))
        )
        decoded = branch if decoded is None else decoded.unionByName(
            branch,
            allowMissingColumns=True,
        )
    unknown = framed.filter(~col("_schema_id").isin(known_schema_ids)).select(
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        *[lit(None).alias(name) for name in EVENT_COLUMNS],
        expr(
            "concat('unregistered_schema_id:', cast(_schema_id as string))"
        ).alias("_decode_error"),
    )
    return decoded.unionByName(unknown, allowMissingColumns=True)


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
    schema_registry_url = _env(
        "SCHEMA_REGISTRY_URL",
        "http://schema-registry:8081",
    )
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
        Path(schema_path).read_text(encoding="utf-8")
        decoded = _decode_confluent_avro(
            metadata,
            schema_registry_url,
            f"{source_topic}-value",
        )
    elif value_format == "json":
        event_schema = spark_struct_type()
        decoded = metadata.select(
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            from_json(col("value").cast("string"), event_schema).alias("data"),
        ).select("_kafka_topic", "_kafka_partition", "_kafka_offset", "data.*").withColumn(
            "_decode_error",
            lit(None).cast("string"),
        )
    else:
        raise ValueError(
            f"Unsupported CLEAN_SOURCE_VALUE_FORMAT={value_format!r}; "
            "expected avro or json"
        )

    decoded = decoded.filter(
        (col("source") == lit(platform)) | col("_decode_error").isNotNull()
    )
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
        .withColumn("raw_text", coalesce(col("raw_text"), col("title")))
        .withColumn("clean_text", clean_text(col("raw_text")))
        .withColumn("text_for_model", prepare_text_for_model(col("clean_text")))
        .withColumn("title", clean_text(col("title")))
        .withColumn("error", clean_text(col("error")))
    )

    reason = (
        when(
            protected["_decode_error"].isNotNull(),
            lit("unregistered_writer_schema"),
        )
        .when(protected["user_id"].isNull(), lit("missing_user_id"))
        .when(protected["url"].isNull(), lit("missing_url"))
        .when(protected["timestamp"].isNull(), lit("missing_timestamp"))
        .when(protected["error"].isNotNull(), lit("collector_error"))
        .otherwise(invalid_reason(protected["title"]))
    )
    validated = protected.withColumn("_invalid_reason", reason)

    clean_payload = validated.filter(col("_invalid_reason").isNull()).select(
        col("user_id").cast("string").alias("key"),
        to_json(
            struct(*EVENT_COLUMNS, lit("clean").alias("stage"))
        ).alias("value"),
    )

    dlq_payload = validated.filter(col("_invalid_reason").isNotNull()).select(
        to_json(
            struct(
                col("_invalid_reason").alias("reason"),
                *EVENT_COLUMNS,
                "_kafka_topic",
                "_kafka_partition",
                "_kafka_offset",
                "_decode_error",
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
