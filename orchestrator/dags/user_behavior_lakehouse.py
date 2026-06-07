from __future__ import annotations

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime

PROJECT_DIR = "/workspace"


def docker_compose(command: str) -> str:
    return (
        f"cd {PROJECT_DIR} && "
        'HOST_PROJECT_DIR="${DOCKER_HOST_PROJECT_DIR:-.}" && '
        "export HOST_PROJECT_DIR && "
        f"docker compose {command}"
    )


with DAG(
    dag_id="user_behavior_lakehouse",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
    default_args={"owner": "data-platform", "retries": 0},
    params={
        "x_post_count": Param(
            5,
            type="integer",
            minimum=1,
            maximum=100,
            title="Nombre de posts X",
            description=(
                "Nombre maximal de nouveaux posts X à publier dans Kafka."
            ),
        ),
    },
    tags=["lakehouse", "spark", "realtime"],
) as dag:
    start_stack = BashOperator(
        task_id="start_core_stack",
        bash_command=docker_compose(
            "up -d --scale spark-worker=${SPARK_WORKER_COUNT:-4} "
            "minio kafka schema-registry kafdrop spark-master spark-worker"
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
        docker exec spark-master /bin/bash -lc "pkill -9 -f '[k]afka_to_iceberg_bronze.py' || true; pkill -9 -f '[b]ronze_to_silver.py' || true; pkill -9 -f '[b]ronze_to_silver_from_kafka.py' || true; pkill -9 -f '[l]akehouse_check.py' || true"
        docker exec spark-master /bin/bash -lc "rm -f /opt/spark/tests/lakehouse/bronze_stream.pid"
        """,
    )

    create_source_topics = BashOperator(
        task_id="create_source_topics",
        bash_command=r"""
        YOUTUBE_TOPIC="${YOUTUBE_KAFKA_TOPIC:-youtube.raw.events}"
        X_TOPIC="${X_KAFKA_TOPIC:-x.raw.events}"
        REDDIT_TOPIC="${REDDIT_KAFKA_TOPIC:-reddit.raw.events}"
        BRONZE_TOPIC="${BRONZE_KAFKA_OUT_TOPIC:-lakehouse.bronze.for_silver}"
        for TOPIC in "$YOUTUBE_TOPIC" "$X_TOPIC" "$REDDIT_TOPIC"; do
          docker exec kafka /opt/kafka/bin/kafka-topics.sh \
            --create \
            --if-not-exists \
            --topic "$TOPIC" \
            --partitions 2 \
            --replication-factor 1 \
            --bootstrap-server kafka:9092
        done
        docker exec kafka /opt/kafka/bin/kafka-topics.sh \
          --create \
          --if-not-exists \
          --topic "$BRONZE_TOPIC" \
          --partitions 1 \
          --replication-factor 1 \
          --bootstrap-server kafka:9092
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

    start_silver_stream = BashOperator(
        task_id="start_silver_stream",
        bash_command=r"""
        docker exec spark-master /bin/bash -lc "mkdir -p /opt/spark/tests/lakehouse; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/batch/bronze_to_silver_from_kafka.py > /opt/spark/tests/lakehouse/silver_stream.log 2>&1 & echo \$! > /opt/spark/tests/lakehouse/silver_stream.pid"
        """,
    )
    run_youtube_collection = BashOperator(
        task_id="run_youtube_collection",
        bash_command=docker_compose("run --rm youtube-collector"),
    )

    run_x_collection = BashOperator(
        task_id="run_x_collection",
        bash_command=r"""
        set -euo pipefail
        if [[ "${X_COLLECTION_ENABLED:-false}" != "true" ]]; then
          echo "X collection disabled; set X_COLLECTION_ENABLED=true to enable it"
          exit 0
        fi
        """ + docker_compose(
            "run --rm "
            "-e PRODUCER_MAX_EVENTS={{ params.x_post_count }} "
            "x-collector"
        ),
    )

    run_reddit_collection = BashOperator(
        task_id="run_reddit_collection",
        bash_command=r"""
        set -euo pipefail
        if [[ "${REDDIT_COLLECTION_ENABLED:-false}" != "true" ]]; then
          echo "Reddit collection disabled; set REDDIT_COLLECTION_ENABLED=true to enable it"
          exit 0
        fi
        """ + docker_compose("run --rm reddit-collector"),
    )

    wait_bronze = BashOperator(
        task_id="wait_bronze_rows",
        bash_command=r"""
        set -euo pipefail
        for i in $(seq 1 24); do
          if ! docker exec spark-master /bin/bash -lc 'pid="$(cat /opt/spark/tests/lakehouse/bronze_stream.pid 2>/dev/null)" && test -r "/proc/$pid/cmdline" && tr "\0" " " < "/proc/$pid/cmdline" | grep -Fq kafka_to_iceberg_bronze.py'; then
            echo "Bronze stream stopped before validation"
            docker exec spark-master /bin/bash -lc "tail -n 160 /opt/spark/tests/lakehouse/bronze_stream.log || true"
            exit 1
          fi
          if docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/tests/lakehouse/lakehouse_check.py lakehouse.bronze.events 1"; then
            exit 0
          fi
          sleep 5
        done
        docker exec spark-master /bin/bash -lc "tail -n 160 /opt/spark/tests/lakehouse/bronze_stream.log || true"
        exit 1
        """,
    )

    wait_silver = BashOperator(
        task_id="wait_silver_rows",
        bash_command=r"""
        set -euo pipefail
        for i in $(seq 1 12); do
          if ! docker exec spark-master /bin/bash -lc 'pid="$(cat /opt/spark/tests/lakehouse/silver_stream.pid 2>/dev/null)" && test -r "/proc/$pid/cmdline" && tr "\0" " " < "/proc/$pid/cmdline" | grep -Fq bronze_to_silver_from_kafka.py'; then
            echo "Silver stream stopped before validation"
            docker exec spark-master /bin/bash -lc "tail -n 160 /opt/spark/tests/lakehouse/silver_stream.log || true"
            exit 1
          fi
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
          docker exec spark-master /bin/bash -lc "if [[ -f /opt/spark/tests/lakehouse/bronze_stream.pid ]]; then cat /opt/spark/tests/lakehouse/bronze_stream.pid | xargs -r kill -9 || true; rm -f /opt/spark/tests/lakehouse/bronze_stream.pid; fi"
          docker exec spark-master /bin/bash -lc "if [[ -f /opt/spark/tests/lakehouse/silver_stream.pid ]]; then cat /opt/spark/tests/lakehouse/silver_stream.pid | xargs -r kill -9 || true; rm -f /opt/spark/tests/lakehouse/silver_stream.pid; fi"
          docker exec spark-master /bin/bash -lc "pkill -9 -f '[k]afka_to_iceberg_bronze.py' || true; pkill -9 -f '[b]ronze_to_silver_from_kafka.py' || true"
        else
          echo "spark-master is not running; no realtime streams to stop"
        fi
        """,
    )

    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start_stack >> wait_services >> cleanup_spark
    cleanup_spark >> create_source_topics >> ensure_minio_bucket
    ensure_minio_bucket >> [start_bronze_stream, start_silver_stream]
    [start_bronze_stream, start_silver_stream] >> run_youtube_collection
    [start_bronze_stream, start_silver_stream] >> run_x_collection
    [start_bronze_stream, start_silver_stream] >> run_reddit_collection
    run_youtube_collection >> wait_bronze
    run_x_collection >> wait_bronze
    run_reddit_collection >> wait_bronze
    wait_bronze >> wait_silver
    wait_silver >> stop_realtime_streams
    [
        run_youtube_collection,
        run_x_collection,
        run_reddit_collection,
        wait_bronze,
        wait_silver,
        stop_realtime_streams,
    ] >> pipeline_done
