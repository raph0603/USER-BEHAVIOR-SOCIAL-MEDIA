import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp


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


def _ensure_columns(spark: SparkSession, table: str, columns: dict[str, str]) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


def main() -> None:
    bucket = _env("MINIO_BUCKET", "lakehouse")

    warehouse = f"s3a://{bucket}/warehouse"

    spark = _build_spark("bronze-to-silver", warehouse)

    workers_env = os.getenv("SPARK_WORKER_COUNT")
    cores_env = os.getenv("SPARK_WORKER_CORES")
    try:
        workers = int(workers_env) if workers_env else None
    except ValueError:
        workers = None
    try:
        cores_per_worker = int(cores_env) if cores_env else None
    except ValueError:
        cores_per_worker = None

    default_partitions = 200
    if workers and cores_per_worker:
        default_partitions = max(200, workers * cores_per_worker * 2)

    shuffle_env = os.getenv("SPARK_SHUFFLE_PARTITIONS")
    if shuffle_env and shuffle_env.strip():
        try:
            shuffle_partitions = int(shuffle_env)
        except ValueError:
            shuffle_partitions = default_partitions
    else:
        shuffle_partitions = default_partitions

    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

    bronze_table = "lakehouse.bronze.events"
    silver_table = "lakehouse.silver.events"
    silver_columns = [
        "user_id",
        "url",
        "title",
        "event_ts",
        "source",
        "error",
        "platform_event_id",
        "metadata_refreshed_at",
        "owner_channel_id",
        "collaborator_channel_ids",
        "like_count",
        "view_count",
        "follower_count",
        "subscriber_count",
        "subreddit_member_count",
        "event_date",
    ]

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    _ensure_columns(
        spark,
        bronze_table,
        {
            "platform_event_id": "STRING",
            "metadata_refreshed_at": "TIMESTAMP",
        },
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.events (
          user_id STRING,
          url STRING,
          title STRING,
          event_ts TIMESTAMP,
          source STRING,
          error STRING,
          platform_event_id STRING,
          metadata_refreshed_at TIMESTAMP,
          owner_channel_id STRING,
          collaborator_channel_ids ARRAY<STRING>,
          like_count BIGINT,
          view_count BIGINT,
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
            "owner_channel_id": "STRING",
            "platform_event_id": "STRING",
            "metadata_refreshed_at": "TIMESTAMP",
            "collaborator_channel_ids": "ARRAY<STRING>",
            "like_count": "BIGINT",
            "view_count": "BIGINT",
            "follower_count": "BIGINT",
            "subscriber_count": "BIGINT",
            "subreddit_member_count": "BIGINT",
        },
    )

    current_columns = set(spark.table(silver_table).columns)
    for legacy_column in (
        "event_id",
        "pipeline_run_id",
        "ingested_at",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    ):
        if legacy_column in current_columns:
            spark.sql(f"ALTER TABLE {silver_table} DROP COLUMN {legacy_column}")

    checkpoint = f"s3a://{bucket}/checkpoints/silver/events/incremental"

    processing_mode = _env("PROCESSING_MODE", "continuous")
    trigger_interval = _env("PROCESSING_TRIGGER", "30 seconds")

    source_stream = (
        spark.readStream.format("iceberg")
        .load(bronze_table)
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("event_date", to_date(col("event_ts")))
        .select(*silver_columns)
    )

    def _foreach_batch(df, epoch_id: int):
        if df.rdd.isEmpty():
            return

        batch_df = df.dropDuplicates(
            ["source", "platform_event_id", "user_id", "url", "event_ts"]
        )
        temp_view = f"microbatch_{epoch_id}"
        batch_df.createOrReplaceTempView(temp_view)
        batch_spark = batch_df.sparkSession

        cols = ", ".join(silver_columns)
        merge_sql = f"""
        MERGE INTO {silver_table} AS t
        USING {temp_view} AS s
        ON t.event_date = s.event_date
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
          t.follower_count = COALESCE(s.follower_count, t.follower_count),
          t.subscriber_count = COALESCE(s.subscriber_count, t.subscriber_count),
          t.subreddit_member_count = COALESCE(s.subreddit_member_count, t.subreddit_member_count)
        WHEN NOT MATCHED THEN
          INSERT ({cols}) VALUES ({', '.join([f's.{c}' for c in silver_columns])})
        """

        batch_spark.sql(merge_sql)

    if processing_mode == "availableNow":
        checkpoint = f"s3a://{bucket}/checkpoints/silver/events/incremental"
        updates = (
            source_stream.dropDuplicates(
                ["source", "platform_event_id", "user_id", "url", "event_ts"]
            )
        )

        query = (
            updates.writeStream.outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(silver_table)
        )

        query.awaitTermination()
    else:
        checkpoint_rt = f"s3a://{bucket}/checkpoints/silver/events/realtime"

        query = (
            source_stream.writeStream
            .outputMode("append")
            .option("checkpointLocation", checkpoint_rt)
            .trigger(processingTime=trigger_interval)
            .foreachBatch(_foreach_batch)
            .start()
        )

        query.awaitTermination()


if __name__ == "__main__":
    main()
