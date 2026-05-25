#!/usr/bin/env bash
set -euo pipefail

run_kafka=false
run_spark=false
run_spark_kafka=false

if [[ $# -eq 0 ]]; then
  run_kafka=true
  run_spark=true
  run_spark_kafka=true
else
  for arg in "$@"; do
    case "$arg" in
      --kafka) run_kafka=true ;;
      --spark) run_spark=true ;;
      --spark-kafka) run_spark_kafka=true ;;
      *) echo "Unknown option: $arg"; exit 1 ;;
    esac
  done
fi

echo "Starting services..."
docker-compose up -d >/dev/null

if [[ "$run_kafka" == true ]]; then
  echo "Kafka test: create/list topic"
  docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic test-topic --partitions 2 --replication-factor 1 --bootstrap-server kafka:9092
  docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092
  echo "Kafdrop: http://localhost:9000"
fi

if [[ "$run_spark" == true ]]; then
  echo "Spark test: SparkPi"
  docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.2.jar 10
  echo "Spark UI: http://localhost:8080"
fi

if [[ "$run_spark_kafka" == true ]]; then
  echo "Spark + Kafka test: start streaming job"
  docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 /opt/spark/tests/streaming/kafka_stream_test.py > /opt/spark/tests/streaming/kafka_stream_test.log 2>&1 &"

  sleep 5

  message="test-$(date +%Y%m%d-%H%M%S)"
  echo "$message" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic test-topic

  sleep 5

  echo "Stream log (tail):"
  docker exec spark-master /bin/bash -lc "tail -n 50 /opt/spark/tests/streaming/kafka_stream_test.log"
fi
