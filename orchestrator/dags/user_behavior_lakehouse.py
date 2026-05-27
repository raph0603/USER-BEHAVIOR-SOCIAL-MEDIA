from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime

PROJECT_DIR = "/workspace"
def docker_compose(command: str) -> str:
    return f"cd {PROJECT_DIR} && docker compose {command}"


with DAG(
    dag_id="user_behavior_lakehouse",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
    default_args={"owner": "data-platform", "retries": 0},
    tags=["lakehouse", "spark", "realtime"],
    params={"producer_events": 20},
) as dag:
    start_stack = BashOperator(
        task_id="start_core_stack",
        bash_command=docker_compose(
            "up -d minio kafka schema-registry kafdrop "
            "spark-master spark-worker-1 spark-worker-2"
        ),
    )

    wait_services = BashOperator(
        task_id="wait_services",
        bash_command=r"""
        set -euo pipefail
        wait_for() {
          local label="$1"
          shift
          for i in $(seq 1 24); do
            if "$@"; then
              echo "$label ready"
              return 0
            fi
            sleep 5
          done
          echo "$label not ready"
          return 1
        }
        wait_for "kafka" docker exec kafka /bin/bash -lc "/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092 >/dev/null 2>&1"
        wait_for "schema registry" docker exec schema-registry /bin/bash -lc "curl -fsS http://schema-registry:8081/subjects >/dev/null 2>&1"
        wait_for "minio" docker exec minio /bin/sh -c "curl -fsS http://minio:9000/minio/health/ready >/dev/null 2>&1"
        wait_for "spark master" docker exec spark-master /bin/bash -lc "curl -fsS http://spark-master:8080 >/dev/null 2>&1"
        """,
    )

    cleanup_spark = BashOperator(
        task_id="cleanup_previous_spark_jobs",
        bash_command=r"""
        docker exec spark-master /bin/bash -lc "pkill -9 -f '[s]park-submit' || true"
        docker exec spark-master /bin/bash -lc "pkill -9 -f '[k]afka_stream_test.py' || true; pkill -9 -f '[k]afka_to_iceberg_bronze.py' || true; pkill -9 -f '[b]ronze_to_silver.py' || true; pkill -9 -f '[l]akehouse_check.py' || true"
        docker exec spark-master /bin/bash -lc "rm -f /opt/spark/tests/streaming/kafka_stream_test.pid /opt/spark/tests/lakehouse/bronze_stream.pid"
        """,
    )

    create_probe_topic = BashOperator(
        task_id="create_probe_topic",
        bash_command=r"""
        PROBE_TOPIC="spark-test-topic-{{ ts_nodash }}"
        docker exec kafka /opt/kafka/bin/kafka-topics.sh \
          --create \
          --if-not-exists \
          --topic "$PROBE_TOPIC" \
          --partitions 1 \
          --replication-factor 1 \
          --bootstrap-server kafka:9092
        """,
    )

    start_probe_stream = BashOperator(
        task_id="start_spark_kafka_probe_stream",
        bash_command=r"""
        set -euo pipefail
        PROBE_TOPIC="spark-test-topic-{{ ts_nodash }}"
        docker exec spark-master /bin/bash -lc "mkdir -p /opt/spark/tests/streaming; KAFKA_TEST_TOPIC=$PROBE_TOPIC /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=1 --conf spark.executor.cores=1 /opt/spark/tests/streaming/kafka_stream_test.py > /opt/spark/tests/streaming/kafka_stream_test.log 2>&1 & echo \$! > /opt/spark/tests/streaming/kafka_stream_test.pid"
        """,
    )

    emit_probe_message = BashOperator(
        task_id="emit_probe_message",
        bash_command=r"""
        set -euo pipefail
        PROBE_TOPIC="spark-test-topic-{{ ts_nodash }}"
        MESSAGE="test-{{ ts_nodash }}"
        printf "%s\n" "$MESSAGE" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
          --broker-list kafka:9092 \
          --topic "$PROBE_TOPIC"
        """,
    )

    assert_probe_message = BashOperator(
        task_id="assert_probe_message_consumed",
        bash_command=r"""
        set -euo pipefail
        MESSAGE="test-{{ ts_nodash }}"
        for i in $(seq 1 24); do
          if docker exec spark-master /bin/bash -lc "grep -F '$MESSAGE' /opt/spark/tests/streaming/kafka_stream_test.log >/dev/null 2>&1"; then
            exit 0
          fi
          sleep 5
        done
        docker exec spark-master /bin/bash -lc "tail -n 120 /opt/spark/tests/streaming/kafka_stream_test.log || true"
        exit 1
        """,
    )

    ensure_minio_bucket = BashOperator(
        task_id="ensure_minio_bucket",
        bash_command=docker_compose(
            "run --rm minio-init"
        ),
    )

    start_bronze_stream = BashOperator(
        task_id="start_bronze_stream",
        bash_command=r"""
        docker exec spark-master /bin/bash -lc "mkdir -p /opt/spark/tests/lakehouse; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/streaming/kafka_to_iceberg_bronze.py > /opt/spark/tests/lakehouse/bronze_stream.log 2>&1 & echo \$! > /opt/spark/tests/lakehouse/bronze_stream.pid"
        """,
    )

    run_bounded_producer = BashOperator(
        task_id="run_bounded_producer",
        bash_command=docker_compose(
            "run --rm "
            "-e PRODUCER_MODE=synthetic "
            "-e PRODUCER_MAX_EVENTS={{ params.producer_events }} "
            "-e EVENT_INTERVAL_SEC=0 "
            "playwright-producer"
        ),
    )

    wait_bronze = BashOperator(
        task_id="wait_bronze_rows",
        bash_command=r"""
        set -euo pipefail
        for i in $(seq 1 24); do
          if docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/tests/lakehouse/lakehouse_check.py lakehouse.bronze.events 1"; then
            exit 0
          fi
          sleep 5
        done
        docker exec spark-master /bin/bash -lc "tail -n 160 /opt/spark/tests/lakehouse/bronze_stream.log || true"
        exit 1
        """,
    )

    run_silver_batch = BashOperator(
        task_id="run_silver_batch",
        bash_command=r"""
        docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/batch/bronze_to_silver.py"
        """,
    )

    wait_silver = BashOperator(
        task_id="wait_silver_rows",
        bash_command=r"""
        set -euo pipefail
        for i in $(seq 1 12); do
          if docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/tests/lakehouse/lakehouse_check.py lakehouse.silver.events 1"; then
            exit 0
          fi
          sleep 5
        done
        exit 1
        """,
    )

    stop_realtime_streams = BashOperator(
        task_id="stop_realtime_streams",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=r"""
        if docker exec spark-master true >/dev/null 2>&1; then
          docker exec spark-master /bin/bash -lc "if [[ -f /opt/spark/tests/streaming/kafka_stream_test.pid ]]; then cat /opt/spark/tests/streaming/kafka_stream_test.pid | xargs -r kill -9 || true; rm -f /opt/spark/tests/streaming/kafka_stream_test.pid; fi"
          docker exec spark-master /bin/bash -lc "if [[ -f /opt/spark/tests/lakehouse/bronze_stream.pid ]]; then cat /opt/spark/tests/lakehouse/bronze_stream.pid | xargs -r kill -9 || true; rm -f /opt/spark/tests/lakehouse/bronze_stream.pid; fi"
          docker exec spark-master /bin/bash -lc "pkill -9 -f '[k]afka_stream_test.py' || true; pkill -9 -f '[k]afka_to_iceberg_bronze.py' || true"
        else
          echo "spark-master is not running; no realtime streams to stop"
        fi
        """,
    )

    pipeline_done = EmptyOperator(task_id="pipeline_done")

    start_stack >> wait_services >> cleanup_spark
    cleanup_spark >> create_probe_topic >> start_probe_stream >> emit_probe_message >> assert_probe_message
    cleanup_spark >> ensure_minio_bucket >> start_bronze_stream >> run_bounded_producer >> wait_bronze
    wait_bronze >> run_silver_batch >> wait_silver
    [assert_probe_message, wait_silver] >> stop_realtime_streams
    [assert_probe_message, wait_silver, stop_realtime_streams] >> pipeline_done
