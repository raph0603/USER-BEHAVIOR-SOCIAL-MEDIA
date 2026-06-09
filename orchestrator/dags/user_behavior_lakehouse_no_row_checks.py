from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime

PROJECT_DIR = "/workspace"


def schedule_interval() -> timedelta | None:
    raw_value = os.getenv(
        "LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES",
        "60",
    ).strip()
    try:
        minutes = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            "LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES must be an integer"
        ) from exc

    return timedelta(minutes=minutes) if minutes > 0 else None


def docker_compose(command: str) -> str:
    return (
        f"cd {PROJECT_DIR} && "
        'HOST_PROJECT_DIR="${DOCKER_HOST_PROJECT_DIR:-.}" && '
        "export HOST_PROJECT_DIR && "
        f"docker compose {command}"
    )


def clean_stream_command(
    platform: str,
    source_variable: str,
    source_default: str,
    clean_variable: str,
    clean_default: str,
    dlq_variable: str,
    dlq_default: str,
) -> str:
    return f"""
    set -euo pipefail
    docker exec \\
      -e PLATFORM={platform} \\
      -e COLLECTOR_SOURCE_TOPIC="${{{source_variable}:-{source_default}}}" \\
      -e CLEAN_KAFKA_TOPIC="${{{clean_variable}:-{clean_default}}}" \\
      -e DLQ_KAFKA_TOPIC="${{{dlq_variable}:-{dlq_default}}}" \\
      -e CLEAN_SOURCE_VALUE_FORMAT=avro \\
      -e CLEAN_CHECKPOINT_VERSION=pre_bronze_v3 \\
      -e CLEAN_TRIGGER_MODE=available_now \\
      spark-master /bin/bash -lc "set -o pipefail; mkdir -p /tmp/user-behavior-lakehouse; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 512m --conf spark.cores.max=1 --conf spark.executor.cores=1 /opt/spark/jobs/pipeline/collector_stream_pipeline.py 2>&1 | tee /tmp/user-behavior-lakehouse/clean_{platform}.log"
    """


def wait_clean_command(
    platform: str,
    clean_variable: str,
    clean_default: str,
    dlq_variable: str,
    dlq_default: str,
    enabled_variable: str | None = None,
) -> str:
    enabled_check = ""
    if enabled_variable:
        enabled_check = f"""
        if [[ "${{{enabled_variable}:-false}}" != "true" ]]; then
          echo "{platform} collection disabled; no cleaned event required"
          exit 0
        fi
        """

    return f"""
    set -euo pipefail
    {enabled_check}
    CLEAN_TOPIC="${{{clean_variable}:-{clean_default}}}"
    DLQ_TOPIC="${{{dlq_variable}:-{dlq_default}}}"
    CLEAN_TOTAL=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "$CLEAN_TOPIC" 2>/dev/null | awk -F: '{{s+=$3}} END{{print s+0}}')
    DLQ_TOTAL=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "$DLQ_TOPIC" 2>/dev/null | awk -F: '{{s+=$3}} END{{print s+0}}')
    echo "{platform} cleaning completed: clean=${{CLEAN_TOTAL:-0}}, dlq=${{DLQ_TOTAL:-0}}"
    """


def acquire_pipeline_lock_command() -> str:
    return r"""
    set -euo pipefail
    LOCK_DIR=/tmp/user-behavior-lakehouse.pipeline.lock
    OWNER="${AIRFLOW_CTX_DAG_ID}/${AIRFLOW_CTX_DAG_RUN_ID}"
    for attempt in $(seq 1 720); do
      if docker exec spark-master mkdir "$LOCK_DIR" 2>/dev/null; then
        docker exec -e LOCK_OWNER="$OWNER" spark-master /bin/bash -lc \
          'printf "%s\n" "$LOCK_OWNER" > /tmp/user-behavior-lakehouse.pipeline.lock/owner'
        echo "Acquired shared pipeline lock for $OWNER"
        exit 0
      fi
      if (( attempt % 6 == 1 )); then
        CURRENT_OWNER=$(docker exec spark-master /bin/bash -lc \
          'cat /tmp/user-behavior-lakehouse.pipeline.lock/owner 2>/dev/null || echo unknown')
        echo "Pipeline busy with $CURRENT_OWNER; waiting..."
      fi
      sleep 10
    done
    echo "Timed out waiting for the shared pipeline lock"
    exit 1
    """


def release_pipeline_lock_command() -> str:
    return r"""
    set -euo pipefail
    OWNER="${AIRFLOW_CTX_DAG_ID}/${AIRFLOW_CTX_DAG_RUN_ID}"
    if ! docker exec spark-master true >/dev/null 2>&1; then
      echo "spark-master is not running; pipeline lock is already unavailable"
      exit 0
    fi
    docker exec -e LOCK_OWNER="$OWNER" spark-master /bin/bash -lc '
      LOCK_DIR=/tmp/user-behavior-lakehouse.pipeline.lock
      CURRENT_OWNER=$(cat "$LOCK_DIR/owner" 2>/dev/null || true)
      if [[ "$CURRENT_OWNER" == "$LOCK_OWNER" ]]; then
        rm -rf "$LOCK_DIR"
        echo "Released shared pipeline lock for $LOCK_OWNER"
      else
        echo "Pipeline lock belongs to ${CURRENT_OWNER:-nobody}; nothing to release"
      fi
    '
    """


with DAG(
    dag_id="user_behavior_lakehouse_no_row_checks",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=schedule_interval(),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    default_args={"owner": "data-platform", "retries": 0},
    params={
        "youtube_event_count": Param(
            5,
            type="integer",
            minimum=1,
            maximum=50,
            title="Nombre d'evenements YouTube",
            description=(
                "Nombre maximal de nouvelles videos YouTube a publier dans Kafka."
            ),
        ),
        "x_event_count": Param(
            5,
            type="integer",
            minimum=1,
            maximum=100,
            title="Nombre d'evenements X",
            description=(
                "Nombre maximal de nouveaux posts X à publier dans Kafka."
            ),
        ),
        "reddit_event_count": Param(
            5,
            type="integer",
            minimum=1,
            maximum=100,
            title="Nombre d'evenements Reddit",
            description=(
                "Nombre maximal de nouveaux commentaires Reddit a publier "
                "dans Kafka."
            ),
        ),
        "x_headless": Param(
            True,
            type="boolean",
            title="X en mode headless",
            description=(
                "Active Edge sans fenetre. Desactivez cette option pour voir "
                "le navigateur et terminer une connexion Google."
            ),
        ),
    },
    tags=[
        "collection",
        "clean",
        "lakehouse",
        "spark",
        "realtime",
        "no-row-checks",
    ],
) as dag:
    start_stack = BashOperator(
        task_id="initialize_core_services",
        bash_command=docker_compose(
            "up -d --scale spark-worker=${SPARK_WORKER_COUNT:-4} "
            "minio kafka schema-registry kafdrop spark-master spark-worker"
        ),
    )

    wait_services = BashOperator(
        task_id="verify_core_services",
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

    acquire_pipeline_lock = BashOperator(
        task_id="acquire_pipeline_lock",
        execution_timeout=timedelta(hours=2, minutes=5),
        bash_command=acquire_pipeline_lock_command(),
    )

    cleanup_spark = BashOperator(
        task_id="terminate_stale_spark_jobs",
        bash_command=r"""
        docker exec spark-master /bin/bash -lc "pkill -9 -f '[s]park-submit' || true"
        docker exec spark-master /bin/bash -lc "pkill -9 -f '[c]ollector_stream_pipeline.py' || true; pkill -9 -f '[k]afka_to_iceberg_bronze.py' || true; pkill -9 -f '[b]ronze_to_silver.py' || true; pkill -9 -f '[b]ronze_to_silver_from_kafka.py' || true; pkill -9 -f '[l]akehouse_check.py' || true"
        docker exec spark-master /bin/bash -lc "rm -rf /tmp/user-behavior-lakehouse; mkdir -p /tmp/user-behavior-lakehouse"
        """,
    )

    create_source_topics = BashOperator(
        task_id="provision_kafka_pipeline_topics",
        bash_command=r"""
        YOUTUBE_TOPIC="${YOUTUBE_KAFKA_TOPIC:-youtube.raw.events}"
        X_TOPIC="${X_KAFKA_TOPIC:-x.raw.events}"
        REDDIT_TOPIC="${REDDIT_KAFKA_TOPIC:-reddit.raw.events}"
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
          "$YOUTUBE_DLQ_TOPIC" "$X_DLQ_TOPIC" "$REDDIT_DLQ_TOPIC"; do
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
        task_id="initialize_lakehouse_storage",
        bash_command=docker_compose(
            "run --rm minio-init"
        ),
    )

    start_clean_youtube = BashOperator(
        task_id="sha256_hash_pii_redact_validate_youtube",
        bash_command=clean_stream_command(
            "youtube",
            "YOUTUBE_KAFKA_TOPIC",
            "youtube.raw.events",
            "YOUTUBE_CLEAN_KAFKA_TOPIC",
            "youtube.clean.events",
            "YOUTUBE_DLQ_KAFKA_TOPIC",
            "youtube.dlq.events",
        ),
    )

    start_clean_x = BashOperator(
        task_id="sha256_hash_pii_redact_validate_x",
        bash_command=clean_stream_command(
            "x",
            "X_KAFKA_TOPIC",
            "x.raw.events",
            "X_CLEAN_KAFKA_TOPIC",
            "x.clean.events",
            "X_DLQ_KAFKA_TOPIC",
            "x.dlq.events",
        ),
    )

    start_clean_reddit = BashOperator(
        task_id="sha256_hash_pii_redact_validate_reddit",
        bash_command=clean_stream_command(
            "reddit",
            "REDDIT_KAFKA_TOPIC",
            "reddit.raw.events",
            "REDDIT_CLEAN_KAFKA_TOPIC",
            "reddit.clean.events",
            "REDDIT_DLQ_KAFKA_TOPIC",
            "reddit.dlq.events",
        ),
    )

    start_bronze_stream = BashOperator(
        task_id="merge_clean_events_to_bronze",
        bash_command=r"""
        set -euo pipefail
        docker exec \
          -e KAFKA_TOPIC="${YOUTUBE_CLEAN_KAFKA_TOPIC:-youtube.clean.events},${X_CLEAN_KAFKA_TOPIC:-x.clean.events},${REDDIT_CLEAN_KAFKA_TOPIC:-reddit.clean.events}" \
          -e KAFKA_VALUE_FORMAT=json \
          -e BRONZE_KAFKA_OUT_TOPIC="${BRONZE_KAFKA_OUT_TOPIC:-lakehouse.bronze.for_silver}" \
          -e BRONZE_CHECKPOINT_VERSION=post_clean_v1 \
          -e BRONZE_TRIGGER_MODE=available_now \
          spark-master /bin/bash -lc "set -o pipefail; mkdir -p /tmp/user-behavior-lakehouse; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 512m --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/streaming/kafka_to_iceberg_bronze.py 2>&1 | tee /tmp/user-behavior-lakehouse/bronze_stream.log"
        """,
    )

    start_silver_stream = BashOperator(
        task_id="transmit_bronze_to_silver",
        bash_command=r"""
        set -euo pipefail
        docker exec \
          -e SILVER_KAFKA_TOPICS="${BRONZE_KAFKA_OUT_TOPIC:-lakehouse.bronze.for_silver}" \
          -e SILVER_TRIGGER_MODE=available_now \
          -e SILVER_CHECKPOINT_VERSION=post_clean_v1 \
          spark-master /bin/bash -lc "set -o pipefail; mkdir -p /tmp/user-behavior-lakehouse; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 512m --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/batch/bronze_to_silver_from_kafka.py 2>&1 | tee /tmp/user-behavior-lakehouse/silver_stream.log"
        """,
    )
    run_youtube_collection = BashOperator(
        task_id="collect_youtube_api_events",
        bash_command=docker_compose(
            "run --rm "
            "-e PRODUCER_MAX_EVENTS={{ params.youtube_event_count }} "
            "-e YOUTUBE_SEARCH_MAX_RESULTS={{ params.youtube_event_count }} "
            "youtube-collector"
        ),
    )

    run_x_collection = BashOperator(
        task_id="collect_x_playwright_events",
        retries=2,
        retry_delay=timedelta(seconds=20),
        bash_command=r"""
        set -euo pipefail
        if [[ "${X_COLLECTION_ENABLED:-false}" != "true" ]]; then
          echo "X collection disabled; set X_COLLECTION_ENABLED=true to enable it"
          exit 0
        fi
        """ + docker_compose(
            "run --rm "
            "-e PRODUCER_MAX_EVENTS={{ params.x_event_count }} "
            "-e X_HEADLESS={{ params.x_headless | lower }} "
            "x-collector"
        ),
    )

    run_reddit_collection = BashOperator(
        task_id="collect_reddit_online_events",
        bash_command=r"""
        set -euo pipefail
        if [[ "${REDDIT_COLLECTION_ENABLED:-false}" != "true" ]]; then
          echo "Reddit collection disabled; set REDDIT_COLLECTION_ENABLED=true to enable it"
          exit 0
        fi
        """ + docker_compose(
            "run --rm "
            "-e PRODUCER_MAX_EVENTS={{ params.reddit_event_count }} "
            "reddit-collector"
        ),
    )

    wait_clean_youtube = BashOperator(
        task_id="report_youtube_clean_dlq_offsets",
        bash_command=wait_clean_command(
            "youtube",
            "YOUTUBE_CLEAN_KAFKA_TOPIC",
            "youtube.clean.events",
            "YOUTUBE_DLQ_KAFKA_TOPIC",
            "youtube.dlq.events",
        ),
    )

    wait_clean_x = BashOperator(
        task_id="report_x_clean_dlq_offsets",
        bash_command=wait_clean_command(
            "x",
            "X_CLEAN_KAFKA_TOPIC",
            "x.clean.events",
            "X_DLQ_KAFKA_TOPIC",
            "x.dlq.events",
            "X_COLLECTION_ENABLED",
        ),
    )

    wait_clean_reddit = BashOperator(
        task_id="report_reddit_clean_dlq_offsets",
        bash_command=wait_clean_command(
            "reddit",
            "REDDIT_CLEAN_KAFKA_TOPIC",
            "reddit.clean.events",
            "REDDIT_DLQ_KAFKA_TOPIC",
            "reddit.dlq.events",
            "REDDIT_COLLECTION_ENABLED",
        ),
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
        task_id="lakehouse_pipeline_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start_stack >> wait_services >> acquire_pipeline_lock >> cleanup_spark
    cleanup_spark >> create_source_topics >> ensure_minio_bucket
    ensure_minio_bucket >> [
        run_youtube_collection,
        run_x_collection,
        run_reddit_collection,
    ]
    run_youtube_collection >> start_clean_youtube
    run_x_collection >> start_clean_x
    run_reddit_collection >> start_clean_reddit
    start_clean_youtube >> wait_clean_youtube
    start_clean_x >> wait_clean_x
    start_clean_reddit >> wait_clean_reddit
    [
        wait_clean_youtube,
        wait_clean_x,
        wait_clean_reddit,
    ] >> start_bronze_stream
    start_bronze_stream >> start_silver_stream
    start_silver_stream >> stop_realtime_streams >> release_pipeline_lock
    [
        run_youtube_collection,
        run_x_collection,
        run_reddit_collection,
        wait_clean_youtube,
        wait_clean_x,
        wait_clean_reddit,
        start_bronze_stream,
        start_silver_stream,
        stop_realtime_streams,
        acquire_pipeline_lock,
        release_pipeline_lock,
    ] >> pipeline_done
