from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, when, lit, current_timestamp,
)
from pyspark.sql.types import StringType

# Sibling modules (same directory, added to sys.path by spark-submit)
from config import (
    KAFKA_BOOTSTRAP,
    TOPIC_RAW_YOUTUBE,
    TOPIC_CLEAN,
    TOPIC_DLQ,
    CHECKPOINT_DIR,
)
from schemas import YOUTUBE_SCHEMA
from cleaning import clean_text, invalid_reason


# Required fields — if any is null after JSON parse, route to DLQ.
REQUIRED_FIELDS = [
    "video_id",
    "thread_id",
    "comment_id",
    "is_reply",
    "text",
    "comment_published_at",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("data-pipeline-youtube")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# --- Section 1: read raw bytes from Kafka ---
def read_raw(spark: SparkSession):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_RAW_YOUTUBE)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


# --- Section 2: parse JSON with PERMISSIVE mode ---
# PERMISSIVE adds a special column for malformed records so we can DLQ them.
def parse_json(raw_df):
    schema_with_corrupt = YOUTUBE_SCHEMA.add("_corrupt_record", StringType(), nullable=True)

    return raw_df.select(
        col("value").cast("string").alias("_raw_value"),
        from_json(
            col("value").cast("string"),
            schema_with_corrupt,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ).alias("_parsed"),
    ).select("_raw_value", "_parsed.*")


# --- Section 3: tag each row valid / invalid (with reason) ---
def tag_invalid(parsed_df):
    reason = when(col("_corrupt_record").isNotNull(), lit("json_parse_failed"))
    for field in REQUIRED_FIELDS:
        reason = reason.when(col(field).isNull(), lit(f"missing_{field}"))
    reason = reason.otherwise(lit(None).cast("string"))

    return parsed_df.withColumn("_invalid_reason", reason)


# --- Section 4: clean text + final validity ---
def clean_and_validate(tagged_df):
    # Only attempt to clean rows that passed schema validation.
    cleaned = tagged_df.withColumn(
        "text_clean",
        when(col("_invalid_reason").isNull(), clean_text(col("text"))).otherwise(lit(None)),
    )

    # If cleaning produced empty/too-short/too-long text, override the reason.
    cleaned = cleaned.withColumn(
        "_invalid_reason",
        when(col("_invalid_reason").isNotNull(), col("_invalid_reason"))
        .otherwise(invalid_reason(col("text_clean"))),
    )
    return cleaned


# --- Section 5: split into clean stream + DLQ stream, write to Kafka ---
# Output columns for clean topic (all 22 original fields + text_clean).
CLEAN_OUTPUT_COLS = [f.name for f in YOUTUBE_SCHEMA.fields] + ["text_clean"]


def start_clean_sink(cleaned_df):
    ok = cleaned_df.filter(col("_invalid_reason").isNull())
    payload = ok.select(
        col("comment_id").cast("string").alias("key"),
        to_json(struct(*[col(c) for c in CLEAN_OUTPUT_COLS])).alias("value"),
    )
    return (
        payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_CLEAN)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/clean")
        .outputMode("append")
        .start()
    )


def start_dlq_sink(cleaned_df):
    bad = cleaned_df.filter(col("_invalid_reason").isNotNull())
    payload = bad.select(
        to_json(struct(
            col("_raw_value").alias("raw"),
            col("_invalid_reason").alias("reason"),
            current_timestamp().alias("failed_at"),
        )).alias("value")
    )
    return (
        payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_DLQ)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq")
        .outputMode("append")
        .start()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_raw(spark)
    parsed = parse_json(raw)
    tagged = tag_invalid(parsed)
    cleaned = clean_and_validate(tagged)

    start_clean_sink(cleaned)
    start_dlq_sink(cleaned)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()