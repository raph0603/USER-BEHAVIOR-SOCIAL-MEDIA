from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, desc
from pyspark.sql.types import StructType, StructField, StringType

from config import KAFKA_BOOTSTRAP, TOPIC_DLQ

# Envelope written by start_dlq_sink() in stream_pipeline.py
DLQ_SCHEMA = StructType([
    StructField("raw",       StringType(), nullable=True),
    StructField("reason",    StringType(), nullable=True),
    StructField("failed_at", StringType(), nullable=True),
])


def main() -> None:
    spark = SparkSession.builder.appName("dlq-report").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Batch read: from the very beginning to the latest offset, then stop.
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_DLQ)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), DLQ_SCHEMA).alias("d")
    ).select("d.*")

    total = parsed.count()
    print(f"\n===== DLQ REPORT =====")
    print(f"total failed records: {total}\n")

    (
        parsed.groupBy("reason")
        .agg(count("*").alias("n"))
        .orderBy(desc("n"))
        .show(truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()