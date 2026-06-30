import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, when, lit, current_timestamp,
)
from pyspark.sql.types import StringType

# Sibling modules (same directory, added to sys.path by spark-submit)
from config import (
    KAFKA_BOOTSTRAP,
    TOPIC_RAW_YOUTUBE,
    TOPIC_RAW_REDDIT,
    TOPIC_RAW_X,
    TOPIC_CLEAN,
    TOPIC_DLQ,
    CHECKPOINT_DIR,
)
from schemas import YOUTUBE_SCHEMA, REDDIT_SCHEMA, X_SCHEMA
from cleaning import clean_text, invalid_reason


# --- Per-platform spec ---------------------------------------------------
# One streaming job handles any platform; pick it with the PLATFORM env var.
#   schema    : how to parse the raw JSON
#   raw_topic : Kafka topic to read from
#   text_col  : field holding the raw text we clean
#   key_col   : field used as Kafka message key on clean.posts
#   required  : if any is null after parse -> route to DLQ
#   drop_cols : raw-PII fields parsed for validation but NOT forwarded downstream
PLATFORMS = {
    "youtube": {
        "schema": YOUTUBE_SCHEMA,
        "raw_topic": TOPIC_RAW_YOUTUBE,
        "text_col": "text",
        "key_col": "comment_id",
        "required": [
            "video_id", "thread_id", "comment_id",
            "is_reply", "text", "comment_published_at",
        ],
        "drop_cols": [],
    },
    "reddit": {
        "schema": REDDIT_SCHEMA,
        "raw_topic": TOPIC_RAW_REDDIT,
        "text_col": "comment_text",
        "key_col": "comment_id",
        "required": ["post_url", "comment_id", "comment_text", "created_iso"],
        "drop_cols": ["author"],
    },
    "x": {
        "schema": X_SCHEMA,
        "raw_topic": TOPIC_RAW_X,
        "text_col": "tweet_text",
        "key_col": "status_id",
        "required": ["status_id", "tweet_text", "tweet_time_iso"],
        "drop_cols": ["screen_name", "display_name"],
    },
}


def get_platform() -> str:
    platform = os.getenv("PLATFORM", "youtube").strip().lower()
    if platform not in PLATFORMS:
        raise ValueError(
            f"Unknown PLATFORM={platform!r}; expected one of {list(PLATFORMS)}"
        )
    return platform


def build_spark(platform: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"data-pipeline-{platform}")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# --- Section 1: read raw bytes from Kafka ---
def read_raw(spark, raw_topic):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", raw_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


# --- Section 2: parse JSON with PERMISSIVE mode ---
def parse_json(raw_df, schema):
    schema_with_corrupt = schema.add("_corrupt_record", StringType(), nullable=True)
    return raw_df.select(
        col("value").cast("string").alias("_raw_value"),
        from_json(
            col("value").cast("string"),
            schema_with_corrupt,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ).alias("_parsed"),
    ).select("_raw_value", "_parsed.*")


# --- Section 3: tag each row valid / invalid (with reason) ---
def tag_invalid(parsed_df, required_fields):
    reason = when(col("_corrupt_record").isNotNull(), lit("json_parse_failed"))
    for field in required_fields:
        reason = reason.when(col(field).isNull(), lit(f"missing_{field}"))
    reason = reason.otherwise(lit(None).cast("string"))
    return parsed_df.withColumn("_invalid_reason", reason)


# --- Section 4: clean text + final validity ---
def clean_and_validate(tagged_df, text_col):
    cleaned = tagged_df.withColumn(
        "text_clean",
        when(col("_invalid_reason").isNull(), clean_text(col(text_col))).otherwise(lit(None)),
    )
    cleaned = cleaned.withColumn(
        "_invalid_reason",
        when(col("_invalid_reason").isNotNull(), col("_invalid_reason"))
        .otherwise(invalid_reason(col("text_clean"))),
    )
    return cleaned


# --- Section 5: split into clean stream + DLQ stream, write to Kafka ---
def start_clean_sink(cleaned_df, spec, platform):
    schema = spec["schema"]
    drop_cols = set(spec["drop_cols"])
    # all original fields except dropped PII, then text_clean + source
    output_cols = [f.name for f in schema.fields if f.name not in drop_cols]

    ok = cleaned_df.filter(col("_invalid_reason").isNull())
    payload = ok.select(
        col(spec["key_col"]).cast("string").alias("key"),
        to_json(struct(
            *[col(c) for c in output_cols],
            col("text_clean"),
            lit(platform).alias("source"),
        )).alias("value"),
    )
    return (
        payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_CLEAN)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{platform}/clean")
        .outputMode("append")
        .start()
    )


def start_dlq_sink(cleaned_df, platform):
    bad = cleaned_df.filter(col("_invalid_reason").isNotNull())
    payload = bad.select(
        to_json(struct(
            col("_raw_value").alias("raw"),
            col("_invalid_reason").alias("reason"),
            lit(platform).alias("source"),
            current_timestamp().alias("failed_at"),
        )).alias("value")
    )
    return (
        payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_DLQ)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{platform}/dlq")
        .outputMode("append")
        .start()
    )


def main() -> None:
    platform = get_platform()
    spec = PLATFORMS[platform]

    spark = build_spark(platform)
    spark.sparkContext.setLogLevel("WARN")

    raw = read_raw(spark, spec["raw_topic"])
    parsed = parse_json(raw, spec["schema"])
    tagged = tag_invalid(parsed, spec["required"])
    cleaned = clean_and_validate(tagged, spec["text_col"])

    start_clean_sink(cleaned, spec, platform)
    start_dlq_sink(cleaned, platform)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
