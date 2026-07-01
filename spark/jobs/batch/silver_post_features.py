"""
Spark batch job: build (or refresh) ``lakehouse.silver.post_features``.

This table is the model-input layer for the classification pipeline.
It is deliberately separate from ``lakehouse.silver.events``, which
remains the monitoring-friendly cleaned-event table.

Run modes
---------
PROCESSING_MODE=availableNow  – one-shot batch (default in CI / Airflow)
PROCESSING_MODE=continuous     – streaming micro-batch (long-running process)
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    length,
    regexp_replace,
    size,
    split,
    to_date,
    to_timestamp,
    trim,
    when,
)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


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
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Feature extraction helpers
# ---------------------------------------------------------------------------

_HASHTAG_PATTERN = r"#\w+"
_MENTION_PATTERN = r"@\w+"
_URL_PATTERN = r"(https?://\S+|www\.\S+)"
# Basic emoji block ranges (BMP + supplementary)
_EMOJI_PATTERN = r"[\U0001F300-\U0001FAFF\u2600-\u27BF\uFE00-\uFE0F]"


def compute_post_features(df):
    """Derive text-based features from a DataFrame that has a ``text_for_model`` column."""
    text = col("text_for_model")
    raw = col("raw_text")

    # Text length features
    char_len = length(text)
    word_len = size(split(trim(text), r"\s+"))

    # Structural signal features derived from raw_text (before PII scrub / lowercasing)
    hashtag_count = size(split(raw, _HASHTAG_PATTERN)) - 1
    mention_count = size(split(raw, _MENTION_PATTERN)) - 1
    url_count = size(split(raw, _URL_PATTERN)) - 1

    # Question marker: does the cleaned text contain a "?"
    has_question = when(text.contains("?"), 1).otherwise(0)

    # Emoji count: count occurrences in raw text
    # We split on non-emoji sequences and measure how many emoji-containing tokens exist.
    # size(split()) - 1 gives the number of delimiters found.
    emoji_count = size(split(raw, _EMOJI_PATTERN)) - 1

    return (
        df.withColumn("text_len_chars", char_len)
        .withColumn("text_len_words", word_len)
        .withColumn("has_question", has_question)
        .withColumn("hashtag_count", hashtag_count)
        .withColumn("mention_count", mention_count)
        .withColumn("url_count", url_count)
        .withColumn("emoji_count", emoji_count)
    )


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.post_features (
  source                STRING   COMMENT 'Origin platform: youtube, x, reddit, playwright',
  platform_event_id     STRING   COMMENT 'Platform-native stable identifier',
  user_id               STRING   COMMENT 'Internal user/session identifier',
  author_hash           STRING   COMMENT 'SHA-256 of author handle — privacy-safe',
  url                   STRING   COMMENT 'Canonical URL of the post',
  event_ts              TIMESTAMP COMMENT 'Event timestamp (UTC)',
  event_date            DATE     COMMENT 'Partition column derived from event_ts',
  text_for_model        STRING   COMMENT 'Lowercased, cleaned text ready for model input',
  clean_text            STRING   COMMENT 'Cleaned text (before lowercasing)',
  text_len_chars        INT      COMMENT 'Character length of text_for_model',
  text_len_words        INT      COMMENT 'Word count of text_for_model',
  has_question          INT      COMMENT '1 if text contains a question mark, else 0',
  hashtag_count         INT      COMMENT 'Number of #hashtags in raw_text',
  mention_count         INT      COMMENT 'Number of @mentions in raw_text',
  url_count             INT      COMMENT 'Number of URLs in raw_text',
  emoji_count           INT      COMMENT 'Number of emoji characters in raw_text',
  feature_version       STRING   COMMENT 'Schema/feature-set version for reproducibility'
)
USING iceberg
PARTITIONED BY (event_date)
"""

_FEATURE_VERSION = "v1"

_UPSERT_COLUMNS = [
    "source",
    "platform_event_id",
    "user_id",
    "author_hash",
    "url",
    "event_ts",
    "event_date",
    "text_for_model",
    "clean_text",
    "text_len_chars",
    "text_len_words",
    "has_question",
    "hashtag_count",
    "mention_count",
    "url_count",
    "emoji_count",
    "feature_version",
]


def _ensure_columns(spark: SparkSession, table: str, columns: dict) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


def main() -> None:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    warehouse = f"s3a://{bucket}/warehouse"
    spark = _build_spark("silver-post-features", warehouse)

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

    silver_table = "lakehouse.silver.events"
    features_table = "lakehouse.silver.post_features"

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(_CREATE_TABLE_SQL)

    _ensure_columns(
        spark,
        features_table,
        {
            "author_hash": "STRING",
            "emoji_count": "INT",
            "feature_version": "STRING",
        },
    )

    processing_mode = _env("PROCESSING_MODE", "continuous")
    trigger_interval = _env("PROCESSING_TRIGGER", "30 seconds")
    checkpoint = f"s3a://{bucket}/checkpoints/silver/post_features"

    source_stream = (
        spark.readStream.format("iceberg")
        .load(silver_table)
        .filter(col("text_for_model").isNotNull())
        .withColumn("event_date", to_date(col("event_ts")))
    )

    # Add features
    source_stream = compute_post_features(source_stream)
    source_stream = source_stream.withColumn("feature_version", col("source").cast("string"))
    # Overwrite feature_version with constant after we used col("source") for the cast trick
    from pyspark.sql.functions import lit
    source_stream = source_stream.withColumn("feature_version", lit(_FEATURE_VERSION))

    source_stream = source_stream.select(*_UPSERT_COLUMNS)

    def _foreach_batch(df, epoch_id: int):
        if df.rdd.isEmpty():
            return

        batch_df = df.dropDuplicates(
            ["source", "platform_event_id", "user_id", "url", "event_ts"]
        )
        temp_view = f"post_features_batch_{epoch_id}"
        batch_df.createOrReplaceTempView(temp_view)
        batch_spark = batch_df.sparkSession

        cols = ", ".join(_UPSERT_COLUMNS)
        s_cols = ", ".join([f"s.{c}" for c in _UPSERT_COLUMNS])

        update_set = ",\n".join(
            f"t.{c} = s.{c}" for c in _UPSERT_COLUMNS if c not in ("event_date",)
        )

        merge_sql = f"""
        MERGE INTO {features_table} AS t
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
          {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({cols}) VALUES ({s_cols})
        """
        batch_spark.sql(merge_sql)

    if processing_mode == "availableNow":
        query = (
            source_stream.writeStream.outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(features_table)
        )
        query.awaitTermination()
    else:
        query = (
            source_stream.writeStream.outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=trigger_interval)
            .foreachBatch(_foreach_batch)
            .start()
        )
        query.awaitTermination()


if __name__ == "__main__":
    main()
