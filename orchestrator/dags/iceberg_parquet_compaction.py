from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime
from pipeline_lock import (
    acquire_pipeline_lock_command,
    release_pipeline_lock_command,
)


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
