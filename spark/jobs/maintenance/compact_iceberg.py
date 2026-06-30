import os
import re

from pyspark.sql import SparkSession


TABLE_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$")


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def _env_int(name: str, default: int, minimum: int) -> int:
    raw_value = _env(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _build_spark(warehouse: str) -> SparkSession:
    minio_endpoint = _env("MINIO_ENDPOINT", "http://minio:9000")
    access_key = _env("MINIO_ROOT_USER", "minioadmin")
    secret_key = _env("MINIO_ROOT_PASSWORD", "minioadmin")

    return (
        SparkSession.builder.appName("iceberg-parquet-compaction")
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
        .config(
            "spark.sql.shuffle.partitions",
            _env("COMPACTION_SHUFFLE_PARTITIONS", "4"),
        )
        .getOrCreate()
    )


def _file_stats(spark: SparkSession, table: str) -> tuple[int, int, int]:
    row = spark.sql(
        f"""
        SELECT
          COUNT(*) AS file_count,
          COALESCE(SUM(file_size_in_bytes), 0) AS total_bytes,
          COALESCE(SUM(record_count), 0) AS record_count
        FROM {table}.files
        """
    ).first()
    return (
        int(row["file_count"]),
        int(row["total_bytes"]),
        int(row["record_count"]),
    )


def _compact_table(
    spark: SparkSession,
    table: str,
    target_file_size_bytes: int,
    min_input_files: int,
) -> None:
    if not TABLE_PATTERN.fullmatch(table):
        raise ValueError(f"Invalid Iceberg table identifier: {table!r}")

    if not spark.catalog.tableExists(table):
        print(f"Compaction skipped for {table}: table does not exist")
        return

    before_count, before_bytes, before_records = _file_stats(spark, table)
    print(
        f"Compaction candidate {table}: files={before_count}, "
        f"bytes={before_bytes}, records={before_records}, "
        f"target_bytes={target_file_size_bytes}"
    )

    result = spark.sql(
        f"""
        CALL lakehouse.system.rewrite_data_files(
          table => '{table.removeprefix("lakehouse.")}',
          strategy => 'binpack',
          options => map(
            'target-file-size-bytes', '{target_file_size_bytes}',
            'min-input-files', '{min_input_files}',
            'rewrite-all', 'false',
            'partial-progress.enabled', 'false'
          )
        )
        """
    ).first()

    after_count, after_bytes, after_records = _file_stats(spark, table)
    if after_records != before_records:
        raise RuntimeError(
            f"Record count changed during compaction for {table}: "
            f"before={before_records}, after={after_records}"
        )
    if int(result["failed_data_files_count"]) != 0:
        raise RuntimeError(
            f"Compaction reported failed files for {table}: "
            f"{int(result['failed_data_files_count'])}"
        )

    print(
        f"Compaction result {table}: "
        f"rewritten={int(result['rewritten_data_files_count'])}, "
        f"added={int(result['added_data_files_count'])}, "
        f"rewritten_bytes={int(result['rewritten_bytes_count'])}, "
        f"failed={int(result['failed_data_files_count'])}, "
        f"files_before={before_count}, files_after={after_count}, "
        f"bytes_before={before_bytes}, bytes_after={after_bytes}, "
        f"records_before={before_records}, records_after={after_records}"
    )


def main() -> None:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    tables = [
        table.strip()
        for table in _env(
            "COMPACTION_TABLES",
            "lakehouse.bronze.events,lakehouse.silver.events",
        ).split(",")
        if table.strip()
    ]
    if not tables:
        raise ValueError("COMPACTION_TABLES must contain at least one table")

    target_size_mb = _env_int("COMPACTION_TARGET_FILE_SIZE_MB", 128, 1)
    min_input_files = _env_int("COMPACTION_MIN_INPUT_FILES", 2, 2)
    target_file_size_bytes = target_size_mb * 1024 * 1024

    spark = _build_spark(f"s3a://{bucket}/warehouse")
    spark.sparkContext.setLogLevel("WARN")
    try:
        for table in tables:
            _compact_table(
                spark,
                table,
                target_file_size_bytes,
                min_input_files,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
