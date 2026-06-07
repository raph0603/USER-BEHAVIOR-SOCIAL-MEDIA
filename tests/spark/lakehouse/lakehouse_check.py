import os
import sys

from pyspark.sql import SparkSession


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
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )

    return spark


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: lakehouse_check.py <table> <min_count>")
        return 2

    table = sys.argv[1]
    min_count = int(sys.argv[2])

    bucket = _env("MINIO_BUCKET", "lakehouse")
    warehouse = f"s3a://{bucket}/warehouse"

    spark = _build_spark("lakehouse-check", warehouse)
    count = spark.table(table).limit(min_count).count()

    print(f"Table {table} has at least {count} rows")

    if count < min_count:
        print(f"Expected at least {min_count} rows")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
