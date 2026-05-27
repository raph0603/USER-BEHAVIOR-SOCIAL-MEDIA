#!/usr/bin/env bash
set -euo pipefail

run_kafka=false
run_spark=false
run_spark_kafka=false
run_lakehouse=false
do_clean=false
log_dir="./tests/logs"
run_id="$(date +%Y%m%d-%H%M%S)"
main_log="${log_dir}/test_stack_${run_id}.log"
spark_test_topic="spark-test-topic"
failed=false
producer_ok=false

mark_failed() {
  failed=true
  echo "$1"
}

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
      --lakehouse) run_lakehouse=true ;;
      --clean) do_clean=true ;;
      *) echo "Unknown option: $arg"; exit 1 ;;
    esac
  done
fi

echo "Starting services..."
mkdir -p "$log_dir"
if [[ "$do_clean" == true ]]; then
  rm -rf ./data/minio
fi
docker compose up -d minio minio-init kafka schema-registry kafdrop spark-master spark-worker-1 spark-worker-2 >/dev/null
exec > >(tee -a "$main_log") 2>&1
echo "Run log: $main_log"

if [[ "$run_kafka" == true ]]; then
  echo "Kafka test: create/list topic"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic test-topic --partitions 2 --replication-factor 1 --bootstrap-server kafka:9092 | tee -a "${log_dir}/kafka_${run_id}.log"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092 | tee -a "${log_dir}/kafka_${run_id}.log"
  echo "Kafdrop: http://localhost:9002"
fi

if [[ "$run_spark" == true ]]; then
  echo "Spark test: SparkPi"
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.2.jar 10 | tee -a "${log_dir}/sparkpi_${run_id}.log"
  echo "Spark UI: http://localhost:8080"
fi

if [[ "$run_spark_kafka" == true ]]; then
  echo "Spark + Kafka test: create isolated topic"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic "$spark_test_topic" --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092 | tee -a "${log_dir}/spark_kafka_${run_id}.log"

  echo "Spark + Kafka test: start streaming job"
  docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=1 --conf spark.executor.cores=1 /opt/spark/tests/streaming/kafka_stream_test.py > /opt/spark/tests/streaming/kafka_stream_test.log 2>&1 &"

  sleep 5

  message="test-$(date +%Y%m%d-%H%M%S)"
  echo "$message" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic "$spark_test_topic"

  sleep 5

  echo "Stream log (tail):"
  docker exec spark-master /bin/bash -lc "tail -n 50 /opt/spark/tests/streaming/kafka_stream_test.log" | tee -a "${log_dir}/spark_kafka_${run_id}.log"
fi

if [[ "$run_lakehouse" == true ]]; then
  echo "Producer check: run bounded synthetic producer"
  if docker compose run --rm -e PRODUCER_MODE=synthetic -e PRODUCER_MAX_EVENTS=20 -e EVENT_INTERVAL_SEC=0 playwright-producer >/dev/null; then
    for i in {1..6}; do
      if docker exec kafka /bin/bash -lc "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-topic --from-beginning --max-messages 1 --timeout-ms 3000" >/dev/null; then
        producer_ok=true
        break
      fi
      sleep 2
    done
  else
    mark_failed "Producer run: FAILED"
  fi
  if [[ "$producer_ok" != true ]]; then
    mark_failed "Producer check: FAILED"
  fi
fi

if [[ "$run_lakehouse" == true ]]; then
  if [[ "$producer_ok" != true ]]; then
    echo "Skipping Lakehouse: producer did not emit messages."
  else
  echo "Lakehouse test: ensure MinIO bucket"
  for i in {1..10}; do
    if docker compose run --rm minio-init >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done

  echo "Lakehouse test: start Bronze streaming job"
  docker exec spark-master /bin/bash -lc "mkdir -p /opt/spark/tests/lakehouse && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/streaming/kafka_to_iceberg_bronze.py > /opt/spark/tests/lakehouse/bronze_stream.log 2>&1 & echo $! > /opt/spark/tests/lakehouse/bronze_stream.pid"

  echo "Waiting for Bronze data..."
  sleep 30

  echo "Lakehouse test: check Bronze"
  bronze_ok=false
  for i in {1..6}; do
    if docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/tests/lakehouse/lakehouse_check.py lakehouse.bronze.events 1" | tee -a "${log_dir}/lakehouse_bronze_${run_id}.log"; then
      bronze_ok=true
      break
    fi
    sleep 10
  done

  if [[ "$bronze_ok" != true ]]; then
    mark_failed "Bronze check: FAILED"
  else
    echo "Lakehouse test: run Silver batch"
    docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/batch/bronze_to_silver.py" | tee -a "${log_dir}/lakehouse_silver_${run_id}.log"

    echo "Lakehouse test: check Silver"
    silver_ok=false
    for i in {1..6}; do
      if docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/tests/lakehouse/lakehouse_check.py lakehouse.silver.events 1" | tee -a "${log_dir}/lakehouse_silver_${run_id}.log"; then
        silver_ok=true
        break
      fi
      sleep 10
    done
    if [[ "$silver_ok" != true ]]; then
      mark_failed "Silver check: FAILED"
    fi
  fi

  echo "Stopping Bronze streaming job"
  docker exec spark-master /bin/bash -lc "if [[ -f /opt/spark/tests/lakehouse/bronze_stream.pid ]]; then cat /opt/spark/tests/lakehouse/bronze_stream.pid | xargs -r kill -9 || true; fi"
  fi
fi

if [[ "$do_clean" == true ]]; then
  echo "Cleaning stack and data..."
  echo "Saving container logs..."
  docker compose logs --no-color > "${log_dir}/compose_${run_id}.log" || true
  docker compose down
  rm -rf ./data/minio
fi

if [[ "$failed" == true ]]; then
  exit 1
fi
