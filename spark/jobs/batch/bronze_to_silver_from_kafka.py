import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, to_date, to_timestamp
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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
        "SILVER_KAFKA_TOPICS",
        "lakehouse.bronze.for_silver",
    )
    starting_offsets = _env("SILVER_STARTING_OFFSETS", "earliest")
    trigger_mode = _env("SILVER_TRIGGER_MODE", "processing_time").lower()
    trigger_interval = _env("PROCESSING_TRIGGER", "30 seconds")
    bucket = _env("MINIO_BUCKET", "lakehouse")

    warehouse = f"s3a://{bucket}/warehouse"

    spark = _build_spark("bronze-to-silver-from-kafka", warehouse)
    spark.sparkContext.setLogLevel("WARN")

    silver_table = "lakehouse.silver.events"
    silver_columns = [
        "user_id",
        "url",
        "title",
        "raw_text",
        "clean_text",
        "text_for_model",
        "event_ts",
        "source",
        "error",
        "platform_event_id",
        "metadata_refreshed_at",
        "owner_channel_id",
        "subreddit",
        "x_account",
        "youtube_channel_name",
        "language",
        "parent_interaction_id",
        "conversation_id",
        "transcript_text",
        "transcript_segments_json",
        "duration_seconds",
        "has_auto_captions",
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
        "event_date",
    ]

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.events (
          user_id STRING,
          url STRING,
          title STRING,
          raw_text STRING,
          clean_text STRING,
          text_for_model STRING,
          event_ts TIMESTAMP,
          source STRING,
          error STRING,
          platform_event_id STRING,
          metadata_refreshed_at TIMESTAMP,
          owner_channel_id STRING,
          subreddit STRING,
          x_account STRING,
          youtube_channel_name STRING,
          language STRING,
          parent_interaction_id STRING,
          conversation_id STRING,
          transcript_text STRING,
          transcript_segments_json STRING,
          duration_seconds DOUBLE,
          has_auto_captions BOOLEAN,
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
          event_date DATE
        )
        USING iceberg
        PARTITIONED BY (event_date)
        """
    )
    _ensure_columns(
        spark,
        silver_table,
        {
            "raw_text": "STRING",
            "clean_text": "STRING",
            "text_for_model": "STRING",
            "owner_channel_id": "STRING",
            "platform_event_id": "STRING",
            "metadata_refreshed_at": "TIMESTAMP",
            "collaborator_channel_ids": "ARRAY<STRING>",
            "subreddit": "STRING",
            "x_account": "STRING",
            "youtube_channel_name": "STRING",
            "language": "STRING",
            "parent_interaction_id": "STRING",
            "conversation_id": "STRING",
            "transcript_text": "STRING",
            "transcript_segments_json": "STRING",
            "duration_seconds": "DOUBLE",
            "has_auto_captions": "BOOLEAN",
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

    schema = StructType(
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
            StructField("metadata_refreshed_at", TimestampType()),
            StructField("owner_channel_id", StringType()),
            StructField("subreddit", StringType()),
            StructField("x_account", StringType()),
            StructField("youtube_channel_name", StringType()),
            StructField("language", StringType()),
            StructField("parent_interaction_id", StringType()),
            StructField("conversation_id", StringType()),
            StructField("transcript_text", StringType()),
            StructField("transcript_segments_json", StringType()),
            StructField("duration_seconds", DoubleType()),
            StructField("has_auto_captions", BooleanType()),
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
            StructField("event_ts", TimestampType()),
        ]
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topics)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    updates = (
        parsed.withColumn(
            "event_ts",
            coalesce(to_timestamp(col("event_ts")), to_timestamp(col("timestamp"))),
        )
        .withColumn("event_date", to_date(col("event_ts")))
        .select(*silver_columns)
    )

    def _foreach_batch(df, epoch_id: int):
        cached = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            input_rows = cached.count()
            if input_rows == 0:
                print(f"Silver epoch {epoch_id}: no input rows")
                return

            batch_df = cached.dropDuplicates(
                ["source", "platform_event_id", "user_id", "url", "event_ts"]
            )
            deduplicated_rows = batch_df.count()
            temp_view = f"microbatch_{epoch_id}"
            batch_df.createOrReplaceTempView(temp_view)
            batch_spark = batch_df.sparkSession
            cols = ", ".join(silver_columns)
            merge_sql = f"""
            MERGE INTO {silver_table} AS t
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
              t.subreddit = COALESCE(s.subreddit, t.subreddit),
              t.x_account = COALESCE(s.x_account, t.x_account),
              t.youtube_channel_name = COALESCE(s.youtube_channel_name, t.youtube_channel_name),
              t.language = COALESCE(s.language, t.language),
              t.parent_interaction_id = COALESCE(s.parent_interaction_id, t.parent_interaction_id),
              t.conversation_id = COALESCE(s.conversation_id, t.conversation_id),
              t.transcript_text = COALESCE(s.transcript_text, t.transcript_text),
              t.transcript_segments_json = COALESCE(s.transcript_segments_json, t.transcript_segments_json),
              t.duration_seconds = COALESCE(s.duration_seconds, t.duration_seconds),
              t.has_auto_captions = COALESCE(s.has_auto_captions, t.has_auto_captions),
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
              INSERT ({cols}) VALUES ({', '.join([f's.{c}' for c in silver_columns])})
            """
            batch_spark.sql(merge_sql)
            print(
                f"Silver epoch {epoch_id}: merged {deduplicated_rows} "
                f"deduplicated rows from {input_rows} Kafka rows"
            )
        finally:
            cached.unpersist()

    checkpoint_key = re.sub(r"[^a-zA-Z0-9._-]+", "_", kafka_topics)
    checkpoint_version = _env("SILVER_CHECKPOINT_VERSION", "v2")
    checkpoint = (
        f"s3a://{bucket}/checkpoints/silver/events/kafka/"
        f"{checkpoint_version}/{checkpoint_key}"
    )

    writer = (
        updates.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(_foreach_batch)
    )
    query = _trigger(writer, trigger_mode, trigger_interval).start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
