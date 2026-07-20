"""Persist immutable Bronze events before projecting or handing them to Silver."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_json,
    length,
    lit,
    row_number,
    sha2,
    struct,
    to_json,
    to_timestamp,
    trim,
    when,
)
from pyspark.storagelevel import StorageLevel

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from event_contract import (
    BRONZE_COLUMNS,
    BRONZE_DLQ_COLUMNS,
    BRONZE_DLQ_TYPES,
    BRONZE_EVENT_LOG_COLUMNS,
    BRONZE_EVENT_LOG_METADATA_TYPES,
    ICEBERG_TYPES,
    create_table_columns,
    merge_assignment,
    spark_struct_type,
)
from pipeline.reliability import fail_on_data_loss_option


EVENT_LOG_TABLE = "lakehouse.bronze.event_log"
CURRENT_TABLE = "lakehouse.bronze.events"
INGRESS_DLQ_TABLE = "lakehouse.bronze.ingress_dlq"


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


def _ensure_columns(spark: SparkSession, table: str, columns: dict[str, str]) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


def _merge_insert_only(
    frame: DataFrame,
    *,
    table: str,
    identity_column: str,
    columns: tuple[str, ...],
    view_name: str,
) -> None:
    """Atomically insert identities that are not already committed."""

    frame.createOrReplaceTempView(view_name)
    rendered_columns = ", ".join(columns)
    rendered_values = ", ".join(f"s.{name}" for name in columns)
    frame.sparkSession.sql(
        f"""
        MERGE INTO {table} AS t
        USING {view_name} AS s
        ON t.{identity_column} = s.{identity_column}
        WHEN NOT MATCHED THEN
          INSERT ({rendered_columns}) VALUES ({rendered_values})
        """
    )


def _latest_projection(events: DataFrame) -> DataFrame:
    """Choose one deterministic current-state update per business identity."""

    business_id = coalesce(
        col("platform_event_id"),
        sha2(concat_ws("\u001f", col("user_id"), col("url"), col("event_ts")), 256),
    )
    ordering = coalesce(
        to_timestamp(col("updated_at")),
        to_timestamp(col("collected_at")),
        to_timestamp(col("timestamp")),
        col("event_ts"),
    )
    window = Window.partitionBy("source", "_business_id").orderBy(
        ordering.desc_nulls_last(), col("event_id").desc()
    )
    return (
        events.withColumn("_business_id", business_id)
        .withColumn("_projection_rank", row_number().over(window))
        .filter(col("_projection_rank") == 1)
        .drop("_business_id", "_projection_rank")
    )


def _merge_current_projection(events: DataFrame, *, epoch_id: int) -> int:
    projection = _latest_projection(events).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        row_count = projection.count()
        if row_count == 0:
            return 0
        view_name = f"bronze_projection_{epoch_id}"
        projection.createOrReplaceTempView(view_name)
        columns = list(BRONZE_COLUMNS)
        updates = ",\n                  ".join(
            merge_assignment(column)
            for column in columns
            if column not in {"source", "platform_event_id"}
        )
        rendered_columns = ", ".join(columns)
        projection.sparkSession.sql(
            f"""
            MERGE INTO lakehouse.bronze.events AS t
            USING {view_name} AS s
            ON t.source = s.source
               AND (
                 (s.platform_event_id IS NOT NULL
                  AND t.platform_event_id = s.platform_event_id)
                 OR
                 (s.platform_event_id IS NULL
                  AND t.user_id = s.user_id
                  AND t.url = s.url
                  AND t.event_ts = s.event_ts)
               )
            WHEN MATCHED THEN UPDATE SET
              t.platform_event_id = COALESCE(s.platform_event_id, t.platform_event_id),
              {updates}
            WHEN NOT MATCHED THEN
              INSERT ({rendered_columns})
              VALUES ({', '.join(f's.{name}' for name in columns)})
            """
        )
        return row_count
    finally:
        projection.unpersist()


def _ensure_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {EVENT_LOG_TABLE} (
          {create_table_columns(BRONZE_EVENT_LOG_COLUMNS)}
        )
        USING iceberg
        PARTITIONED BY (days(ingested_at))
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CURRENT_TABLE} (
          {create_table_columns(BRONZE_COLUMNS)}
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {INGRESS_DLQ_TABLE} (
          {create_table_columns(BRONZE_DLQ_COLUMNS)}
        )
        USING iceberg
        PARTITIONED BY (days(failed_at))
        """
    )
    _ensure_columns(
        spark,
        EVENT_LOG_TABLE,
        {
            column: ICEBERG_TYPES[column]
            for column in BRONZE_EVENT_LOG_COLUMNS
        },
    )
    _ensure_columns(
        spark,
        CURRENT_TABLE,
        {column: ICEBERG_TYPES[column] for column in BRONZE_COLUMNS},
    )
    _ensure_columns(
        spark,
        INGRESS_DLQ_TABLE,
        {column: ICEBERG_TYPES[column] for column in BRONZE_DLQ_COLUMNS},
    )


def main() -> None:
    kafka_bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topics = _env(
        "KAFKA_TOPIC",
        "youtube.clean.events,x.clean.events,reddit.clean.events",
    )
    value_format = _env("KAFKA_VALUE_FORMAT", "json").lower()
    if value_format != "json":
        raise ValueError(
            f"Unsupported KAFKA_VALUE_FORMAT={value_format!r}; expected json"
        )

    bucket = _env("MINIO_BUCKET", "lakehouse")
    warehouse = f"s3a://{bucket}/warehouse"
    checkpoint_key = kafka_topics.replace(",", "__")
    checkpoint_version = _env("BRONZE_CHECKPOINT_VERSION", "event_log_v1")
    checkpoint = (
        f"s3a://{bucket}/checkpoints/bronze/events/"
        f"{checkpoint_version}/{checkpoint_key}"
    )

    spark = _build_spark("kafka-to-iceberg-bronze", warehouse)
    spark.sparkContext.setLogLevel("WARN")
    _ensure_tables(spark)

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
                    "stage": "bronze",
                    "topics": kafka_topics.split(","),
                },
                sort_keys=True,
            )
        )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topics)
        .option("startingOffsets", _env("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", fail_on_data_loss)
        .load()
    )

    event_schema = spark_struct_type(("stage", "string"))
    metadata = raw.select(
        col("topic").alias("_kafka_topic"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        col("timestamp").alias("_kafka_timestamp"),
        col("value").cast("string").alias("_raw_value"),
    )
    decoded = metadata.withColumn(
        "_data", from_json(col("_raw_value"), event_schema)
    ).select(
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
        "_raw_value",
        col("_data").isNull().alias("_decode_failed"),
        "_data.*",
    )
    prepared = (
        decoded.withColumn(
            "_invalid_category",
            when(
                col("_decode_failed") | col("stage").isNull(),
                lit("malformed_json"),
            )
            .when(col("stage") != lit("clean"), lit("invalid_stage"))
            .when(
                col("source").isNull()
                | col("user_id").isNull()
                | col("url").isNull()
                | col("timestamp").isNull(),
                lit("missing_required_fields"),
            )
            .otherwise(lit(None).cast("string")),
        )
        .withColumn(
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
    )

    kafka_out_topic = _env("BRONZE_KAFKA_OUT_TOPIC", "lakehouse.bronze.for_silver")
    dlq_out_topic = _env("BRONZE_INGRESS_DLQ_TOPIC", "lakehouse.bronze.ingress.dlq")
    bronze_run_id = _env("PIPELINE_RUN_ID", "standalone")

    def _process_batch(df: DataFrame, epoch_id: int) -> None:
        cached = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            input_rows = cached.count()
            if input_rows == 0:
                print(f"Bronze epoch {epoch_id}: no input rows")
                return

            invalid = cached.filter(col("_invalid_category").isNotNull())
            invalid_rows = invalid.count()
            if invalid_rows:
                dlq = (
                    invalid.withColumn(
                        "dlq_id",
                        sha2(
                            concat_ws(
                                "\u001f",
                                col("_kafka_topic"),
                                col("_kafka_partition"),
                                col("_kafka_offset"),
                                col("payload_fingerprint"),
                            ),
                            256,
                        ),
                    )
                    .withColumn("kafka_topic", col("_kafka_topic"))
                    .withColumn("kafka_partition", col("_kafka_partition"))
                    .withColumn("kafka_offset", col("_kafka_offset"))
                    .withColumn("kafka_timestamp", col("_kafka_timestamp"))
                    .withColumn("category", col("_invalid_category"))
                    .withColumn(
                        "protected_payload",
                        to_json(
                            struct(
                                lit(True).alias("redacted"),
                                length(col("_raw_value").cast("binary")).alias(
                                    "byte_length"
                                ),
                                col("payload_fingerprint").alias("sha256"),
                            )
                        ),
                    )
                    .withColumn("failed_at", current_timestamp())
                    .withColumn("bronze_epoch_id", lit(epoch_id).cast("long"))
                    .withColumn("bronze_run_id", lit(bronze_run_id))
                    .select(*BRONZE_DLQ_COLUMNS)
                    .dropDuplicates(["dlq_id"])
                )
                _merge_insert_only(
                    dlq,
                    table=INGRESS_DLQ_TABLE,
                    identity_column="dlq_id",
                    columns=BRONZE_DLQ_COLUMNS,
                    view_name=f"bronze_dlq_{epoch_id}",
                )
                committed_dlq = spark.table(INGRESS_DLQ_TABLE).join(
                    dlq.select("dlq_id"), ["dlq_id"], "inner"
                )
                (
                    committed_dlq.select(
                        to_json(struct(*BRONZE_DLQ_COLUMNS)).alias("value")
                    )
                    .write.format("kafka")
                    .option("kafka.bootstrap.servers", kafka_bootstrap)
                    .option("topic", dlq_out_topic)
                    .save()
                )

            valid = cached.filter(col("_invalid_category").isNull())
            valid_rows = valid.count()
            if valid_rows == 0:
                print(
                    f"Bronze epoch {epoch_id}: persisted {invalid_rows} invalid rows; "
                    "no valid events"
                )
                return

            journal_rows = (
                valid.withColumn(
                    "metadata_refreshed_at",
                    to_timestamp(col("metadata_collected_at")),
                )
                .withColumn("storage_status", lit("success"))
                .withColumn(
                    "event_ts",
                    coalesce(
                        to_timestamp(col("published_at")),
                        to_timestamp(col("timestamp")),
                    ),
                )
                .withColumn("kafka_topic", col("_kafka_topic"))
                .withColumn("kafka_partition", col("_kafka_partition"))
                .withColumn("kafka_offset", col("_kafka_offset"))
                .withColumn("kafka_timestamp", col("_kafka_timestamp"))
                .withColumn("bronze_epoch_id", lit(epoch_id).cast("long"))
                .withColumn("bronze_run_id", lit(bronze_run_id))
                .withColumn("ingested_at", current_timestamp())
                .select(*BRONZE_EVENT_LOG_COLUMNS)
                .dropDuplicates(["event_id"])
            )
            _merge_insert_only(
                journal_rows,
                table=EVENT_LOG_TABLE,
                identity_column="event_id",
                columns=BRONZE_EVENT_LOG_COLUMNS,
                view_name=f"bronze_event_log_{epoch_id}",
            )

            batch_ids = journal_rows.select("event_id").dropDuplicates(["event_id"])
            committed = spark.table(EVENT_LOG_TABLE).join(
                batch_ids, ["event_id"], "inner"
            ).select(*BRONZE_COLUMNS)
            projected_rows = _merge_current_projection(committed, epoch_id=epoch_id)

            committed_after_projection = spark.table(EVENT_LOG_TABLE).join(
                batch_ids, ["event_id"], "inner"
            ).select(*BRONZE_COLUMNS)
            (
                committed_after_projection.select(
                    col("event_id").cast("string").alias("key"),
                    to_json(struct(*BRONZE_COLUMNS)).alias("value"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap)
                .option("topic", kafka_out_topic)
                .save()
            )
            committed_count = batch_ids.count()
            print(
                f"Bronze epoch {epoch_id}: journaled {committed_count} events, "
                f"projected {projected_rows}, handed off after commit, and "
                f"persisted {invalid_rows} invalid rows from {input_rows} Kafka rows"
            )
        finally:
            cached.unpersist()

    writer = (
        prepared.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(_process_batch)
    )
    query = _trigger(
        writer,
        _env("BRONZE_TRIGGER_MODE", "processing_time").lower(),
        _env("BRONZE_TRIGGER", "10 seconds"),
    ).start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
