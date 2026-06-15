import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, to_date, to_timestamp
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import (
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
        "event_ts",
        "source",
        "error",
        "like_count",
        "comment_count",
        "reply_count",
        "view_count",
        "retweet_count",
        "bookmark_count",
        "score",
        "event_date",
    ]

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.events (
          user_id STRING,
          url STRING,
          title STRING,
          event_ts TIMESTAMP,
          source STRING,
          error STRING,
          like_count BIGINT,
          comment_count BIGINT,
          reply_count BIGINT,
          view_count BIGINT,
          retweet_count BIGINT,
          bookmark_count BIGINT,
          score BIGINT,
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
            "like_count": "BIGINT",
            "comment_count": "BIGINT",
            "reply_count": "BIGINT",
            "view_count": "BIGINT",
            "retweet_count": "BIGINT",
            "bookmark_count": "BIGINT",
            "score": "BIGINT",
        },
    )

    # define schema for the JSON payload produced by Bronze
    schema = StructType(
        [
            StructField("user_id", StringType()),
            StructField("url", StringType()),
            StructField("title", StringType()),
            StructField("timestamp", StringType()),
            StructField("source", StringType()),
            StructField("error", StringType()),
            StructField("like_count", LongType()),
            StructField("comment_count", LongType()),
            StructField("reply_count", LongType()),
            StructField("view_count", LongType()),
            StructField("retweet_count", LongType()),
            StructField("bookmark_count", LongType()),
            StructField("score", LongType()),
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

            batch_df = cached.dropDuplicates(["user_id", "url", "event_ts"])
            deduplicated_rows = batch_df.count()
            temp_view = f"microbatch_{epoch_id}"
            batch_df.createOrReplaceTempView(temp_view)
            batch_spark = batch_df.sparkSession
            cols = ", ".join(silver_columns)
            merge_sql = f"""
            MERGE INTO {silver_table} AS t
            USING {temp_view} AS s
            ON t.user_id = s.user_id
               AND t.url = s.url
               AND t.event_ts = s.event_ts
            WHEN MATCHED THEN UPDATE SET
              t.title = s.title,
              t.source = s.source,
              t.error = s.error,
              t.like_count = COALESCE(s.like_count, t.like_count),
              t.comment_count = COALESCE(s.comment_count, t.comment_count),
              t.reply_count = COALESCE(s.reply_count, t.reply_count),
              t.view_count = COALESCE(s.view_count, t.view_count),
              t.retweet_count = COALESCE(s.retweet_count, t.retweet_count),
              t.bookmark_count = COALESCE(
                s.bookmark_count,
                t.bookmark_count
              ),
              t.score = COALESCE(s.score, t.score)
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
