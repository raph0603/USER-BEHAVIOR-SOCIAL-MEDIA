from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime


def schedule_interval() -> timedelta | None:
    raw_value = os.getenv("ICEBERG_COMPACTION_SCHEDULE_MINUTES", "360").strip()
    try:
        minutes = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            "ICEBERG_COMPACTION_SCHEDULE_MINUTES must be an integer"
        ) from exc
    return timedelta(minutes=minutes) if minutes > 0 else None


def compaction_retries() -> int:
    raw_value = os.getenv("ICEBERG_COMPACTION_RETRIES", "3").strip()
    try:
        retries = int(raw_value)
    except ValueError as exc:
        raise ValueError("ICEBERG_COMPACTION_RETRIES must be an integer") from exc
    if retries < 0:
        raise ValueError("ICEBERG_COMPACTION_RETRIES must be zero or greater")
    return retries


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
    dag_id="iceberg_parquet_compaction",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=schedule_interval(),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 0},
    params={
        "target_file_size_mb": Param(
            128,
            type="integer",
            minimum=1,
            maximum=1024,
            title="Taille cible des fichiers (Mo)",
        ),
        "min_input_files": Param(
            2,
            type="integer",
            minimum=2,
            maximum=100,
            title="Nombre minimal de fichiers a compacter",
        ),
    },
    tags=["maintenance", "iceberg", "parquet", "compaction"],
) as dag:
    start_stack = BashOperator(
        task_id="start_storage_and_spark",
        bash_command=r"""
        set -euo pipefail
        cd /workspace
        HOST_PROJECT_DIR="${DOCKER_HOST_PROJECT_DIR:-.}"
        export HOST_PROJECT_DIR
        docker compose up -d --scale spark-worker=${SPARK_WORKER_COUNT:-4} \
          minio spark-master spark-worker
        """,
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
        wait_for "minio" docker exec minio /bin/sh -c \
          "curl -fsS http://minio:9000/minio/health/ready >/dev/null 2>&1"
        wait_for "spark master" docker exec spark-master /bin/bash -lc \
          "curl -fsS http://spark-master:8080 >/dev/null 2>&1"
        """,
    )

    acquire_pipeline_lock = BashOperator(
        task_id="acquire_pipeline_lock",
        execution_timeout=timedelta(hours=2, minutes=5),
        bash_command=acquire_pipeline_lock_command(),
    )

    compact_tables = BashOperator(
        task_id="compact_iceberg_tables",
        retries=compaction_retries(),
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=2),
        bash_command=r"""
        set -euo pipefail
        docker exec \
          -e COMPACTION_TABLES="${ICEBERG_COMPACTION_TABLES:-lakehouse.bronze.events,lakehouse.silver.events}" \
          -e COMPACTION_TARGET_FILE_SIZE_MB="{{ params.target_file_size_mb }}" \
          -e COMPACTION_MIN_INPUT_FILES="{{ params.min_input_files }}" \
          spark-master /bin/bash -lc \
          "set -o pipefail; mkdir -p /tmp/user-behavior-lakehouse; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 512m --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/maintenance/compact_iceberg.py 2>&1 | tee /tmp/user-behavior-lakehouse/compaction.log"
        """,
    )

    release_pipeline_lock = BashOperator(
        task_id="release_pipeline_lock",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=release_pipeline_lock_command(),
    )

    compaction_done = EmptyOperator(
        task_id="compaction_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    (
        start_stack
        >> wait_services
        >> acquire_pipeline_lock
        >> compact_tables
        >> release_pipeline_lock
    )
    [acquire_pipeline_lock, compact_tables, release_pipeline_lock] >> compaction_done
