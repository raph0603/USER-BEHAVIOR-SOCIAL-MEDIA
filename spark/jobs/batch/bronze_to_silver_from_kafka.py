import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


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


def main() -> None:
    kafka_bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topic = _env("BRONZE_KAFKA_OUT_TOPIC", "lakehouse.bronze.for_silver")
    starting_offsets = _env("SILVER_STARTING_OFFSETS", "earliest")
    bucket = _env("MINIO_BUCKET", "lakehouse")

    warehouse = f"s3a://{bucket}/warehouse"

    spark = _build_spark("bronze-to-silver-from-kafka", warehouse)

    silver_table = "lakehouse.silver.events"
    silver_columns = [
        "user_id",
        "url",
        "title",
        "event_ts",
        "source",
        "error",
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
          event_date DATE
        )
        USING iceberg
        PARTITIONED BY (event_date)
        """
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
            StructField("event_ts", TimestampType()),
        ]
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    updates = (
        parsed.withColumn("event_ts", to_timestamp(col("event_ts")))
        .withColumn("event_date", to_date(col("event_ts")))
        .select(*silver_columns)
    )

    def _foreach_batch(df, epoch_id: int):
        if df.rdd.isEmpty():
            return
        batch_df = df.dropDuplicates(["user_id", "url", "event_ts"])
        temp_view = f"microbatch_{epoch_id}"
        batch_df.createOrReplaceTempView(temp_view)
        batch_spark = batch_df.sparkSession
        cols = ", ".join(silver_columns)
        merge_sql = f"""
        MERGE INTO {silver_table} AS t
        USING {temp_view} AS s
        ON t.event_date = s.event_date
           AND t.user_id = s.user_id
           AND t.url = s.url
           AND t.event_ts = s.event_ts
        WHEN NOT MATCHED THEN
          INSERT ({cols}) VALUES ({', '.join([f's.{c}' for c in silver_columns])})
        """
        batch_spark.sql(merge_sql)

    checkpoint = f"s3a://{bucket}/checkpoints/silver/events/kafka"

    query = (
        updates.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=_env("PROCESSING_TRIGGER", "30 seconds"))
        .foreachBatch(_foreach_batch)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
