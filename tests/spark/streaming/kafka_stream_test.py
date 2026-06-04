import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main() -> None:
    kafka_topic = os.getenv("KAFKA_TEST_TOPIC", "spark-test-topic")

    spark = SparkSession.builder.appName("kafka-stream-test").getOrCreate()

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    lines = df.select(col("value").cast("string").alias("message"))

    query = (
        lines.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
