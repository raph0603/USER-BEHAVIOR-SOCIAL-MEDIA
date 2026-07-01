import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp
from pyspark.sql.functions import struct, to_json
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
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
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.events (
          user_id STRING,
          url STRING,
          title STRING,
          raw_text STRING,
          clean_text STRING,
          text_for_model STRING,
          timestamp STRING,
          source STRING,
          error STRING,
          platform_event_id STRING,
          metadata_refreshed_at TIMESTAMP,
          owner_channel_id STRING,
          collaborator_channel_ids ARRAY<STRING>,
          like_count BIGINT,
          view_count BIGINT,
          comment_count BIGINT,
          reply_count BIGINT,
          retweet_count BIGINT,
          bookmark_count BIGINT,
          score BIGINT,
          follower_count BIGINT,
          subscriber_count BIGINT,
          subreddit_member_count BIGINT,
          event_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
        """
    )
    _ensure_columns(
        spark,
        "lakehouse.bronze.events",
        {
            "raw_text": "STRING",
            "clean_text": "STRING",
            "text_for_model": "STRING",
            "owner_channel_id": "STRING",
            "platform_event_id": "STRING",
            "metadata_refreshed_at": "TIMESTAMP",
            "collaborator_channel_ids": "ARRAY<STRING>",
            "like_count": "BIGINT",
            "view_count": "BIGINT",
            "comment_count": "BIGINT",
            "reply_count": "BIGINT",
            "retweet_count": "BIGINT",
            "bookmark_count": "BIGINT",
            "score": "BIGINT",
            "follower_count": "BIGINT",
            "subscriber_count": "BIGINT",
            "subreddit_member_count": "BIGINT",
        },
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
        event_schema = StructType(
            [
                StructField("user_id", StringType()),
                StructField("url", StringType()),
                StructField("title", StringType()),
                StructField("raw_text", StringType()),
                StructField("clean_text", StringType()),
                StructField("text_for_model", StringType()),
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
                StructField("view_count", LongType()),
                StructField("comment_count", LongType()),
                StructField("reply_count", LongType()),
                StructField("retweet_count", LongType()),
                StructField("bookmark_count", LongType()),
                StructField("score", LongType()),
                StructField("follower_count", LongType()),
                StructField("subscriber_count", LongType()),
                StructField("subreddit_member_count", LongType()),
                StructField("stage", StringType()),
            ]
        )
        decoded = raw.select(
            from_json(col("value").cast("string"), event_schema).alias("data")
        ).select("data.*").filter(col("stage") == lit("clean"))
    else:
        raise ValueError(
            f"Unsupported KAFKA_VALUE_FORMAT={value_format!r}; expected json"
        )
    enriched = decoded.withColumn(
        "metadata_refreshed_at",
        lit(None).cast("timestamp"),
    ).select(
        "user_id",
        "url",
        "title",
        "raw_text",
        "clean_text",
        "text_for_model",
        "timestamp",
        "source",
        "error",
        "platform_event_id",
        "metadata_refreshed_at",
        "owner_channel_id",
        "collaborator_channel_ids",
        "like_count",
        "view_count",
        "comment_count",
        "reply_count",
        "retweet_count",
        "bookmark_count",
        "score",
        "follower_count",
        "subscriber_count",
        "subreddit_member_count",
    ).withColumn("event_ts", to_timestamp(col("timestamp")))
    bronze_trigger = _env("BRONZE_TRIGGER", "10 seconds")
    trigger_mode = _env("BRONZE_TRIGGER_MODE", "processing_time").lower()

    bronze_columns = [
        "user_id",
        "url",
        "title",
        "raw_text",
        "clean_text",
        "text_for_model",
        "timestamp",
        "source",
        "error",
        "platform_event_id",
        "metadata_refreshed_at",
        "owner_channel_id",
        "collaborator_channel_ids",
        "like_count",
        "view_count",
        "comment_count",
        "reply_count",
        "retweet_count",
        "bookmark_count",
        "score",
        "follower_count",
        "subscriber_count",
        "subreddit_member_count",
        "event_ts",
    ]

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
                  t.title = s.title,
                  t.raw_text = s.raw_text,
                  t.clean_text = s.clean_text,
                  t.text_for_model = s.text_for_model,
                  t.timestamp = s.timestamp,
                  t.source = s.source,
                  t.error = s.error,
                  t.platform_event_id = COALESCE(
                    s.platform_event_id,
                    t.platform_event_id
                  ),
                  t.metadata_refreshed_at = COALESCE(
                    s.metadata_refreshed_at,
                    t.metadata_refreshed_at
                  ),
                  t.owner_channel_id = COALESCE(
                    s.owner_channel_id,
                    t.owner_channel_id
                  ),
                  t.collaborator_channel_ids = COALESCE(
                    s.collaborator_channel_ids,
                    t.collaborator_channel_ids
                  ),
                  t.like_count = COALESCE(s.like_count, t.like_count),
                  t.view_count = COALESCE(s.view_count, t.view_count),
                  t.comment_count = COALESCE(s.comment_count, t.comment_count),
                  t.reply_count = COALESCE(s.reply_count, t.reply_count),
                  t.retweet_count = COALESCE(s.retweet_count, t.retweet_count),
                  t.bookmark_count = COALESCE(s.bookmark_count, t.bookmark_count),
                  t.score = COALESCE(s.score, t.score),
                  t.follower_count = COALESCE(s.follower_count, t.follower_count),
                  t.subscriber_count = COALESCE(s.subscriber_count, t.subscriber_count),
                  t.subreddit_member_count = COALESCE(s.subreddit_member_count, t.subreddit_member_count)
                WHEN NOT MATCHED THEN
                  INSERT ({cols})
                  VALUES ({', '.join([f's.{name}' for name in bronze_columns])})
                """
            )
            print(
                f"Bronze epoch {epoch_id}: merged {deduplicated_rows} "
                f"deduplicated clean rows from {input_rows} Kafka rows"
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

    kafka_out_topic = _env("BRONZE_KAFKA_OUT_TOPIC", "lakehouse.bronze.for_silver")
    kafka_out_checkpoint = (
        f"s3a://{bucket}/checkpoints/bronze/to_kafka/"
        f"{checkpoint_version}/{checkpoint_key}"
    )

    kafka_payload = enriched.select(
        to_json(struct(*bronze_columns)).alias("value")
    )

    kafka_writer = (
        kafka_payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", kafka_out_topic)
        .option("checkpointLocation", kafka_out_checkpoint)
    )
    kafka_query = _trigger(
        kafka_writer,
        trigger_mode,
        bronze_trigger,
    ).start()

    iceberg_query.awaitTermination()
    kafka_query.awaitTermination()


if __name__ == "__main__":
    main()
