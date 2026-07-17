import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, lit, struct, to_json, to_timestamp
from pyspark.storagelevel import StorageLevel

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from event_contract import (
    BRONZE_COLUMNS,
    ICEBERG_TYPES,
    create_table_columns,
    merge_assignment,
    spark_struct_type,
)


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
        .config("spark.sql.shuffle.partitions", _env("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.default.parallelism", _env("SPARK_DEFAULT_PARALLELISM", "4"))
        .getOrCreate()
    )

    return spark


def _ensure_columns(spark: SparkSession, table: str, columns: dict[str, str]) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


def main() -> None:
    kafka_bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topics = _env(
        "KAFKA_TOPIC",
        "youtube.clean.events,x.clean.events,reddit.clean.events",
    )
    value_format = _env("KAFKA_VALUE_FORMAT", "json").lower()
    bucket = _env("MINIO_BUCKET", "lakehouse")

    warehouse = f"s3a://{bucket}/warehouse"
    checkpoint_key = kafka_topics.replace(",", "__")
    checkpoint_version = _env("BRONZE_CHECKPOINT_VERSION", "post_clean_v1")
    checkpoint = (
        f"s3a://{bucket}/checkpoints/bronze/events/"
        f"{checkpoint_version}/{checkpoint_key}"
    )

    spark = _build_spark("kafka-to-iceberg-bronze", warehouse)
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.events (
          {create_table_columns(BRONZE_COLUMNS)}
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
        """
    )
    _ensure_columns(
        spark,
        "lakehouse.bronze.events",
        {column: ICEBERG_TYPES[column] for column in BRONZE_COLUMNS},
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

    if value_format == "json":
        event_schema = spark_struct_type(("stage", "string"))
        decoded = raw.select(
            from_json(col("value").cast("string"), event_schema).alias("data")
        ).select("data.*").filter(col("stage") == lit("clean"))
    else:
        raise ValueError(
            f"Unsupported KAFKA_VALUE_FORMAT={value_format!r}; expected json"
        )
    enriched = (
        decoded.withColumn(
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
        .select(*BRONZE_COLUMNS)
    )
    bronze_trigger = _env("BRONZE_TRIGGER", "10 seconds")
    trigger_mode = _env("BRONZE_TRIGGER_MODE", "processing_time").lower()

    bronze_columns = list(BRONZE_COLUMNS)
    kafka_out_topic = _env("BRONZE_KAFKA_OUT_TOPIC", "lakehouse.bronze.for_silver")
    merge_updates = ",\n                  ".join(
        merge_assignment(column)
        for column in bronze_columns
        if column not in {"source", "platform_event_id"}
    )

    def _merge_bronze(df, epoch_id: int):
        cached = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            input_rows = cached.count()
            if input_rows == 0:
                print(f"Bronze epoch {epoch_id}: no clean input rows")
                return

            batch_df = cached.dropDuplicates(
                ["source", "platform_event_id", "user_id", "url", "event_ts"]
            )
            deduplicated_rows = batch_df.count()
            temp_view = f"bronze_microbatch_{epoch_id}"
            batch_df.createOrReplaceTempView(temp_view)
            cols = ", ".join(bronze_columns)
            batch_df.sparkSession.sql(
                f"""
                MERGE INTO lakehouse.bronze.events AS t
                USING {temp_view} AS s
                ON t.source = s.source
                   AND (
                     (
                       s.platform_event_id IS NOT NULL
                       AND t.platform_event_id = s.platform_event_id
                     )
                     OR (
                       s.platform_event_id IS NULL
                       AND t.user_id = s.user_id
                       AND t.url = s.url
                       AND t.event_ts = s.event_ts
                     )
                   )
                WHEN MATCHED THEN UPDATE SET
                  t.platform_event_id = COALESCE(
                    s.platform_event_id,
                    t.platform_event_id
                  ),
                  {merge_updates}
                WHEN NOT MATCHED THEN
                  INSERT ({cols})
                  VALUES ({', '.join([f's.{name}' for name in bronze_columns])})
                """
            )
            (
                batch_df.select(to_json(struct(*bronze_columns)).alias("value"))
                .write.format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap)
                .option("topic", kafka_out_topic)
                .save()
            )
            print(
                f"Bronze epoch {epoch_id}: merged {deduplicated_rows} "
                "and handed the committed batch to Silver from "
                f"{input_rows} Kafka rows"
            )
        finally:
            cached.unpersist()

    iceberg_writer = (
        enriched.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(_merge_bronze)
    )
    iceberg_query = _trigger(
        iceberg_writer,
        trigger_mode,
        bronze_trigger,
    ).start()

    iceberg_query.awaitTermination()


if __name__ == "__main__":
    main()
