from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main() -> None:
    spark = SparkSession.builder.appName("kafka-stream-test").getOrCreate()

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "test-topic")
        .option("startingOffsets", "latest")
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
