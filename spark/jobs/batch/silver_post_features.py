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
    array_distinct,
    col,
    coalesce,
    expr,
    length,
    lit,
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
# Basic emoji block ranges (BMP + supplementary). Spark evaluates this pattern
# with ``java.util.regex.Pattern``, whose supplementary-code-point syntax is
# ``\x{...}`` rather than Python's ``\U........`` escape.
_EMOJI_PATTERN = r"[\x{1F300}-\x{1FAFF}\u2600-\u27BF\uFE00-\uFE0F]"


def _occurrence_count(column, literal_value: str):
    return (
        (length(column) - length(regexp_replace(column, literal_value, ""))) / len(literal_value)
    ).cast("int")


def compute_post_features(df):
    """Derive text-based features from a DataFrame that has a ``text_for_model`` column."""
    text = col("text_for_model")
    cleaned = (
        coalesce(col("clean_text"), text)
        if "clean_text" in df.columns
        else coalesce(col("raw_text"), text)
    )

    char_len = length(text)
    tokens = expr("filter(split(trim(text_for_model), '\\\\s+'), token -> token <> '')")
    word_len = when(text.isNull(), lit(None).cast("int")).otherwise(size(tokens))
    sentence_count = when(text.isNull(), lit(None).cast("int")).otherwise(
        expr(
            "size(filter(split(trim(text_for_model), '[.!?]+'), "
            "sentence -> trim(sentence) <> ''))"
        )
    )
    line_count = (
        when(text.isNull(), lit(None).cast("int"))
        .when(length(text) == 0, lit(0))
        .otherwise(size(split(text, r"\r\n|\r|\n")))
    )

    hashtag_count = size(split(cleaned, _HASHTAG_PATTERN)) - 1
    legacy_mentions = size(split(cleaned, _MENTION_PATTERN)) - 1
    legacy_urls = size(split(cleaned, _URL_PATTERN)) - 1
    emoji_count = size(split(cleaned, _EMOJI_PATTERN)) - 1
    question_mark_count = length(cleaned) - length(regexp_replace(cleaned, r"\?", ""))
    exclamation_mark_count = length(cleaned) - length(regexp_replace(cleaned, "!", ""))

    mention_token_count = _occurrence_count(text, "<USER>")
    email_token_count = _occurrence_count(text, "<EMAIL>")
    phone_token_count = _occurrence_count(text, "<PHONE>")
    ip_token_count = _occurrence_count(text, "<IP>")
    url_token_count = _occurrence_count(text, "<URL>")

    alphabetic_count = length(regexp_replace(cleaned, r"[^A-Za-zÀ-ÖØ-öø-ÿ]", ""))
    uppercase_count = length(regexp_replace(cleaned, r"[^A-ZÀ-ÖØ-Þ]", ""))
    uppercase_ratio = when(
        alphabetic_count > 0,
        uppercase_count.cast("double") / alphabetic_count.cast("double"),
    ).otherwise(lit(None).cast("double"))
    digit_count = length(regexp_replace(cleaned, r"\D", ""))
    digit_ratio = when(
        length(cleaned) > 0,
        digit_count.cast("double") / length(cleaned).cast("double"),
    ).otherwise(lit(None).cast("double"))
    lexical_diversity = when(
        word_len > 0,
        size(array_distinct(tokens)).cast("double") / word_len.cast("double"),
    ).otherwise(lit(None).cast("double"))

    return (
        df.withColumn("text_len_chars", char_len)
        .withColumn("text_len_words", word_len)
        .withColumn("character_count", char_len)
        .withColumn("word_count", word_len)
        .withColumn("sentence_count", sentence_count)
        .withColumn("line_count", line_count)
        .withColumn("mention_token_count", mention_token_count)
        .withColumn("email_token_count", email_token_count)
        .withColumn("phone_token_count", phone_token_count)
        .withColumn("ip_token_count", ip_token_count)
        .withColumn("url_token_count", url_token_count)
        .withColumn("mention_count", legacy_mentions)
        .withColumn("url_count", legacy_urls)
        .withColumn("hashtag_count", hashtag_count)
        .withColumn("emoji_count", emoji_count)
        .withColumn("question_mark_count", question_mark_count)
        .withColumn("exclamation_mark_count", exclamation_mark_count)
        .withColumn("has_question", when(question_mark_count > 0, 1).otherwise(0))
        .withColumn("uppercase_character_ratio", uppercase_ratio)
        .withColumn("digit_character_ratio", digit_ratio)
        .withColumn("lexical_diversity", lexical_diversity)
    )


def with_author_hash(df):
    """Keep an explicit author hash, falling back to the privacy-safe user ID."""

    if "author_hash" not in df.columns:
        return df.withColumn("author_hash", col("user_id"))
    return df.withColumn("author_hash", coalesce(col("author_hash"), col("user_id")))


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
  character_count       INT      COMMENT 'Character count of cleaned model text',
  word_count            INT      COMMENT 'Whitespace-delimited token count',
  sentence_count        INT      COMMENT 'Non-empty segments delimited by sentence punctuation',
  line_count            INT      COMMENT 'Line count after privacy cleaning',
  has_question          INT      COMMENT '1 if text contains a question mark, else 0',
  hashtag_count         INT      COMMENT 'Number of #hashtags in raw_text',
  mention_count         INT      COMMENT 'Legacy count of literal @mentions when present',
  url_count             INT      COMMENT 'Legacy count of literal URLs when present',
  emoji_count           INT      COMMENT 'Number of emoji characters in raw_text',
  mention_token_count   INT      COMMENT 'Number of <USER> tokens after privacy cleaning',
  email_token_count     INT      COMMENT 'Number of <EMAIL> tokens after privacy cleaning',
  phone_token_count     INT      COMMENT 'Number of <PHONE> tokens after privacy cleaning',
  ip_token_count        INT      COMMENT 'Number of <IP> tokens after privacy cleaning',
  url_token_count       INT      COMMENT 'Number of <URL> tokens after privacy cleaning',
  question_mark_count   INT      COMMENT 'Number of question marks',
  exclamation_mark_count INT     COMMENT 'Number of exclamation marks',
  uppercase_character_ratio DOUBLE COMMENT 'Uppercase alphabetic characters / alphabetic characters',
  digit_character_ratio DOUBLE   COMMENT 'Digit characters / all characters',
  lexical_diversity     DOUBLE   COMMENT 'Unique whitespace tokens / all whitespace tokens',
  like_count            BIGINT,
  view_count            BIGINT,
  reply_count           BIGINT,
  retweet_count         BIGINT,
  bookmark_count        BIGINT,
  follower_count        BIGINT,
  like_count_available  BOOLEAN,
  view_count_available  BOOLEAN,
  reply_count_available BOOLEAN,
  retweet_count_available BOOLEAN,
  bookmark_count_available BOOLEAN,
  follower_count_available BOOLEAN,
  feature_version       STRING   COMMENT 'Schema/feature-set version for reproducibility'
)
USING iceberg
PARTITIONED BY (event_date)
"""

_FEATURE_VERSION = "v2"

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
    "character_count",
    "word_count",
    "sentence_count",
    "line_count",
    "has_question",
    "hashtag_count",
    "mention_count",
    "url_count",
    "emoji_count",
    "mention_token_count",
    "email_token_count",
    "phone_token_count",
    "ip_token_count",
    "url_token_count",
    "question_mark_count",
    "exclamation_mark_count",
    "uppercase_character_ratio",
    "digit_character_ratio",
    "lexical_diversity",
    "like_count",
    "view_count",
    "reply_count",
    "retweet_count",
    "bookmark_count",
    "follower_count",
    "like_count_available",
    "view_count_available",
    "reply_count_available",
    "retweet_count_available",
    "bookmark_count_available",
    "follower_count_available",
    "feature_version",
]


def prepare_post_features(df):
    """Build the deterministic feature projection from the current Silver state."""

    prepared = df
    for metric in (
        "like_count",
        "view_count",
        "reply_count",
        "retweet_count",
        "bookmark_count",
        "follower_count",
    ):
        if metric not in prepared.columns:
            prepared = prepared.withColumn(metric, lit(None).cast("bigint"))
        availability = f"{metric}_available"
        if availability not in prepared.columns:
            prepared = prepared.withColumn(
                availability,
                col(metric).isNotNull(),
            )
    prepared = (
        prepared.filter(col("text_for_model").isNotNull())
        .withColumn("event_date", to_date(col("event_ts")))
        .transform(with_author_hash)
        .transform(compute_post_features)
        .withColumn("feature_version", lit(_FEATURE_VERSION))
    )
    return prepared.select(*_UPSERT_COLUMNS)


def merge_post_features(df, features_table: str, batch_id: int) -> None:
    """Idempotently materialize one feature batch into the Iceberg target."""

    if df.rdd.isEmpty():
        return

    batch_df = df.dropDuplicates(["source", "platform_event_id", "user_id", "url", "event_ts"])
    temp_view = f"post_features_batch_{batch_id}"
    batch_df.createOrReplaceTempView(temp_view)
    batch_spark = batch_df.sparkSession

    columns = ", ".join(_UPSERT_COLUMNS)
    source_columns = ", ".join(f"s.{name}" for name in _UPSERT_COLUMNS)
    update_set = ",\n".join(f"t.{name} = s.{name}" for name in _UPSERT_COLUMNS)

    merge_sql = f"""
    MERGE INTO {features_table} AS t
    USING {temp_view} AS s
    ON t.source <=> s.source
       AND (
         (
           s.platform_event_id IS NOT NULL
           AND t.platform_event_id = s.platform_event_id
         )
         OR (
           s.platform_event_id IS NULL
           AND t.platform_event_id IS NULL
           AND t.user_id <=> s.user_id
           AND t.url <=> s.url
           AND t.event_ts <=> s.event_ts
         )
       )
    WHEN MATCHED THEN UPDATE SET
      {update_set}
    WHEN NOT MATCHED THEN
      INSERT ({columns}) VALUES ({source_columns})
    """
    batch_spark.sql(merge_sql)


def refresh_post_features(
    spark: SparkSession,
    silver_table: str,
    features_table: str,
    batch_id: int,
) -> None:
    """Read a consistent Silver snapshot and merge its feature projection."""

    merge_post_features(
        prepare_post_features(spark.table(silver_table)),
        features_table,
        batch_id,
    )


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
            "character_count": "INT",
            "word_count": "INT",
            "sentence_count": "INT",
            "line_count": "INT",
            "mention_token_count": "INT",
            "email_token_count": "INT",
            "phone_token_count": "INT",
            "ip_token_count": "INT",
            "url_token_count": "INT",
            "question_mark_count": "INT",
            "exclamation_mark_count": "INT",
            "uppercase_character_ratio": "DOUBLE",
            "digit_character_ratio": "DOUBLE",
            "lexical_diversity": "DOUBLE",
            "like_count": "BIGINT",
            "view_count": "BIGINT",
            "reply_count": "BIGINT",
            "retweet_count": "BIGINT",
            "bookmark_count": "BIGINT",
            "follower_count": "BIGINT",
            "like_count_available": "BOOLEAN",
            "view_count_available": "BOOLEAN",
            "reply_count_available": "BOOLEAN",
            "retweet_count_available": "BOOLEAN",
            "bookmark_count_available": "BOOLEAN",
            "follower_count_available": "BOOLEAN",
        },
    )

    processing_mode = _env("PROCESSING_MODE", "availableNow")
    trigger_interval = _env("PROCESSING_TRIGGER", "30 seconds")
    checkpoint = f"s3a://{bucket}/checkpoints/silver/post_features"

    normalized_mode = processing_mode.replace("_", "").lower()
    if normalized_mode == "availablenow":
        # ``silver.events`` is a mutable Iceberg current-state table. Reading it
        # as an append-only stream rejects MERGE overwrite snapshots (or silently
        # skips them when configured to do so). A batch snapshot plus MERGE is
        # both safe after Silver updates and idempotent across Airflow retries.
        refresh_post_features(spark, silver_table, features_table, batch_id=0)
    elif normalized_mode == "continuous":
        # A rate stream only schedules refreshes; each callback reads a fresh,
        # consistent batch snapshot of the mutable Silver table. This preserves
        # continuous compatibility without skipping Iceberg overwrite snapshots.
        refresh_post_features(spark, silver_table, features_table, batch_id=0)

        def _refresh_on_tick(_heartbeat_df, epoch_id: int) -> None:
            refresh_post_features(
                spark,
                silver_table,
                features_table,
                batch_id=epoch_id + 1,
            )

        heartbeat_stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        query = (
            heartbeat_stream.writeStream.outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=trigger_interval)
            .foreachBatch(_refresh_on_tick)
            .start()
        )
        query.awaitTermination()
    else:
        raise ValueError(
            f"PROCESSING_MODE must be 'availableNow' or 'continuous'; received {processing_mode!r}"
        )


if __name__ == "__main__":
    main()
