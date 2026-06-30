from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime
from pipeline_lock import (
    acquire_pipeline_lock_command,
    release_pipeline_lock_command,
)


PROJECT_DIR = "/workspace"


def docker_compose(command: str) -> str:
    return (
        f"cd {PROJECT_DIR} && "
        'HOST_PROJECT_DIR="${DOCKER_HOST_PROJECT_DIR:-.}" && '
        "export HOST_PROJECT_DIR && "
        f"docker compose {command}"
    )


def clean_import_command(platform: str) -> str:
    return f"""
    set -euo pipefail
    docker exec \\
      -e PLATFORM={platform} \\
      -e COLLECTOR_SOURCE_TOPIC="${{MANUAL_{platform.upper()}_KAFKA_TOPIC:-manual.{platform}.raw.events}}" \\
      -e CLEAN_KAFKA_TOPIC="${{{platform.upper()}_CLEAN_KAFKA_TOPIC:-{platform}.clean.events}}" \\
      -e DLQ_KAFKA_TOPIC="${{{platform.upper()}_DLQ_KAFKA_TOPIC:-{platform}.dlq.events}}" \\
      -e CLEAN_SOURCE_VALUE_FORMAT=json \\
      -e CLEAN_CHECKPOINT_VERSION=manual_import_v1 \\
      -e CLEAN_TRIGGER_MODE=available_now \\
      spark-master /bin/bash -lc "set -o pipefail; mkdir -p /tmp/user-behavior-lakehouse; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 512m --conf spark.cores.max=1 --conf spark.executor.cores=1 /opt/spark/jobs/pipeline/collector_stream_pipeline.py 2>&1 | tee /tmp/user-behavior-lakehouse/manual_clean_{platform}.log"
    """


def wait_clean_command(platform: str) -> str:
    return f"""
    set -euo pipefail
    CLEAN_TOPIC="${{{platform.upper()}_CLEAN_KAFKA_TOPIC:-{platform}.clean.events}}"
    DLQ_TOPIC="${{{platform.upper()}_DLQ_KAFKA_TOPIC:-{platform}.dlq.events}}"
    CLEAN_TOTAL=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "$CLEAN_TOPIC" 2>/dev/null | awk -F: '{{s+=$3}} END{{print s+0}}')
    DLQ_TOTAL=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "$DLQ_TOPIC" 2>/dev/null | awk -F: '{{s+=$3}} END{{print s+0}}')
    echo "{platform} manual import cleaning completed: clean=${{CLEAN_TOTAL:-0}}, dlq=${{DLQ_TOTAL:-0}}"
    """


def build_balancing_report_command() -> str:
    return r"""
    set -euo pipefail
    mkdir -p /workspace/data/balancing
    docker exec \
      -e BALANCE_SEED="${BALANCE_SEED:-42}" \
      -e BALANCE_TARGET_PER_GROUP="${BALANCE_TARGET_PER_GROUP:-0}" \
      -e BALANCE_DIMENSIONS="${BALANCE_DIMENSIONS:-source}" \
      -e BALANCE_REPORT_PATH=/opt/spark/balancing/report.json \
      spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 512m \
      --executor-memory 512m \
      --conf spark.cores.max=2 \
      --conf spark.executor.cores=1 \
      /opt/spark/jobs/maintenance/build_balanced_dataset.py
    """


with DAG(
    dag_id="manual_file_import_lakehouse",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    default_args={"owner": "data-platform", "retries": 0},
    tags=["manual-import", "clean", "lakehouse", "spark"],
) as dag:
    start_stack = BashOperator(
        task_id="initialize_manual_import_services",
        bash_command=docker_compose(
            "up -d --scale spark-worker=${SPARK_WORKER_COUNT:-4} "
            "minio kafka kafdrop spark-master spark-worker"
        ),
    )

    wait_services = BashOperator(
        task_id="verify_manual_import_services",
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
        wait_for "minio" docker exec minio /bin/sh -c "curl -fsS http://minio:9000/minio/health/ready >/dev/null 2>&1"
        wait_for "spark master" docker exec spark-master /bin/bash -lc "curl -fsS http://spark-master:8080 >/dev/null 2>&1"
        """,
    )

    acquire_pipeline_lock = BashOperator(
        task_id="acquire_pipeline_lock",
        execution_timeout=timedelta(hours=2, minutes=5),
        bash_command=acquire_pipeline_lock_command(),
    )

    cleanup_spark = BashOperator(
        task_id="terminate_stale_spark_jobs",
        bash_command=r"""
        docker exec spark-master /bin/bash -lc "pkill -9 -f '[s]park-submit' || true"
        docker exec spark-master /bin/bash -lc "pkill -9 -f '[c]ollector_stream_pipeline.py' || true; pkill -9 -f '[k]afka_to_iceberg_bronze.py' || true; pkill -9 -f '[b]ronze_to_silver_from_kafka.py' || true"
        docker exec spark-master /bin/bash -lc "rm -rf /tmp/user-behavior-lakehouse; mkdir -p /tmp/user-behavior-lakehouse"
        """,
    )

    create_topics = BashOperator(
        task_id="provision_manual_import_topics",
        bash_command=r"""
        set -euo pipefail
        YOUTUBE_TOPIC="${MANUAL_YOUTUBE_KAFKA_TOPIC:-manual.youtube.raw.events}"
        X_TOPIC="${MANUAL_X_KAFKA_TOPIC:-manual.x.raw.events}"
        REDDIT_TOPIC="${MANUAL_REDDIT_KAFKA_TOPIC:-manual.reddit.raw.events}"
        YOUTUBE_CLEAN_TOPIC="${YOUTUBE_CLEAN_KAFKA_TOPIC:-youtube.clean.events}"
        X_CLEAN_TOPIC="${X_CLEAN_KAFKA_TOPIC:-x.clean.events}"
        REDDIT_CLEAN_TOPIC="${REDDIT_CLEAN_KAFKA_TOPIC:-reddit.clean.events}"
        YOUTUBE_DLQ_TOPIC="${YOUTUBE_DLQ_KAFKA_TOPIC:-youtube.dlq.events}"
        X_DLQ_TOPIC="${X_DLQ_KAFKA_TOPIC:-x.dlq.events}"
        REDDIT_DLQ_TOPIC="${REDDIT_DLQ_KAFKA_TOPIC:-reddit.dlq.events}"
        BRONZE_TOPIC="${BRONZE_KAFKA_OUT_TOPIC:-lakehouse.bronze.for_silver}"
        for TOPIC in \
          "$YOUTUBE_TOPIC" "$X_TOPIC" "$REDDIT_TOPIC" \
          "$YOUTUBE_CLEAN_TOPIC" "$X_CLEAN_TOPIC" "$REDDIT_CLEAN_TOPIC" \
          "$YOUTUBE_DLQ_TOPIC" "$X_DLQ_TOPIC" "$REDDIT_DLQ_TOPIC" \
          "$BRONZE_TOPIC"; do
          docker exec kafka /opt/kafka/bin/kafka-topics.sh \
            --create \
            --if-not-exists \
            --topic "$TOPIC" \
            --partitions 2 \
            --replication-factor 1 \
            --bootstrap-server kafka:9092
        done
        """,
    )

    ensure_minio_bucket = BashOperator(
        task_id="initialize_lakehouse_storage",
        bash_command=docker_compose("run --rm minio-init"),
    )

    clean_youtube = BashOperator(
        task_id="clean_manual_youtube_events",
        bash_command=clean_import_command("youtube"),
    )
    clean_x = BashOperator(
        task_id="clean_manual_x_events",
        bash_command=clean_import_command("x"),
    )
    clean_reddit = BashOperator(
        task_id="clean_manual_reddit_events",
        bash_command=clean_import_command("reddit"),
    )

    wait_clean_youtube = BashOperator(
        task_id="report_manual_youtube_clean_offsets",
        bash_command=wait_clean_command("youtube"),
    )
    wait_clean_x = BashOperator(
        task_id="report_manual_x_clean_offsets",
        bash_command=wait_clean_command("x"),
    )
    wait_clean_reddit = BashOperator(
        task_id="report_manual_reddit_clean_offsets",
        bash_command=wait_clean_command("reddit"),
    )

    start_bronze_stream = BashOperator(
        task_id="merge_clean_events_to_bronze",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        bash_command=r"""
        set -euo pipefail
        docker exec \
          -e KAFKA_TOPIC="${YOUTUBE_CLEAN_KAFKA_TOPIC:-youtube.clean.events},${X_CLEAN_KAFKA_TOPIC:-x.clean.events},${REDDIT_CLEAN_KAFKA_TOPIC:-reddit.clean.events}" \
          -e KAFKA_VALUE_FORMAT=json \
          -e BRONZE_KAFKA_OUT_TOPIC="${BRONZE_KAFKA_OUT_TOPIC:-lakehouse.bronze.for_silver}" \
          -e BRONZE_CHECKPOINT_VERSION=manual_import_v1 \
          -e BRONZE_TRIGGER_MODE=available_now \
          spark-master /bin/bash -lc "set -o pipefail; mkdir -p /tmp/user-behavior-lakehouse; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 512m --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/streaming/kafka_to_iceberg_bronze.py 2>&1 | tee /tmp/user-behavior-lakehouse/manual_bronze_stream.log"
        """,
    )

    start_silver_stream = BashOperator(
        task_id="transmit_bronze_to_silver",
        bash_command=r"""
        set -euo pipefail
        docker exec \
          -e SILVER_KAFKA_TOPICS="${BRONZE_KAFKA_OUT_TOPIC:-lakehouse.bronze.for_silver}" \
          -e SILVER_TRIGGER_MODE=available_now \
          -e SILVER_CHECKPOINT_VERSION=manual_import_v1 \
          spark-master /bin/bash -lc "set -o pipefail; mkdir -p /tmp/user-behavior-lakehouse; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 512m --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/batch/bronze_to_silver_from_kafka.py 2>&1 | tee /tmp/user-behavior-lakehouse/manual_silver_stream.log"
        """,
    )

    update_balancing_report = BashOperator(
        task_id="update_balancing_report",
        execution_timeout=timedelta(hours=1),
        bash_command=build_balancing_report_command(),
    )

    stop_realtime_streams = BashOperator(
        task_id="terminate_pipeline_spark_jobs",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=r"""
        if docker exec spark-master true >/dev/null 2>&1; then
          docker exec spark-master /bin/bash -lc "pkill -9 -f '[c]ollector_stream_pipeline.py' || true; pkill -9 -f '[k]afka_to_iceberg_bronze.py' || true; pkill -9 -f '[b]ronze_to_silver_from_kafka.py' || true"
        else
          echo "spark-master is not running; no realtime streams to stop"
        fi
        """,
    )

    release_pipeline_lock = BashOperator(
        task_id="release_pipeline_lock",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=release_pipeline_lock_command(),
    )

    pipeline_done = EmptyOperator(
        task_id="manual_file_import_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start_stack >> wait_services >> acquire_pipeline_lock >> cleanup_spark
    cleanup_spark >> create_topics >> ensure_minio_bucket
    ensure_minio_bucket >> [clean_youtube, clean_x, clean_reddit]
    clean_youtube >> wait_clean_youtube
    clean_x >> wait_clean_x
    clean_reddit >> wait_clean_reddit
    [
        wait_clean_youtube,
        wait_clean_x,
        wait_clean_reddit,
    ] >> start_bronze_stream
    start_bronze_stream >> start_silver_stream
    start_silver_stream >> update_balancing_report >> stop_realtime_streams
    stop_realtime_streams >> release_pipeline_lock
    [
        clean_youtube,
        clean_x,
        clean_reddit,
        start_bronze_stream,
        start_silver_stream,
        update_balancing_report,
        stop_realtime_streams,
        acquire_pipeline_lock,
        release_pipeline_lock,
    ] >> pipeline_done
