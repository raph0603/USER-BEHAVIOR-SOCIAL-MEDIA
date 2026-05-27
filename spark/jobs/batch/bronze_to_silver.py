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


def main() -> None:
    bucket = _env("MINIO_BUCKET", "lakehouse")

    warehouse = f"s3a://{bucket}/warehouse"

    spark = _build_spark("bronze-to-silver", warehouse)

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

    bronze = spark.table("lakehouse.bronze.events")
    updates = (
        bronze.withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("event_date", to_date(col("event_ts")))
        .dropDuplicates(["user_id", "url", "event_ts"])
        .select(
            "user_id",
            "url",
            "title",
            "event_ts",
            "source",
            "error",
            "event_date",
        )
    )

    updates.createOrReplaceTempView("silver_updates")

    spark.sql(
        """
        MERGE INTO lakehouse.silver.events t
        USING silver_updates s
        ON t.user_id = s.user_id
          AND t.url = s.url
          AND t.event_ts = s.event_ts
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


if __name__ == "__main__":
    main()
