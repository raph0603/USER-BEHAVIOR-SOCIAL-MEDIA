from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from pendulum import datetime
from pipeline_lock import (
    acquire_pipeline_lock_command,
    release_pipeline_lock_command,
)


PROJECT_DIR = "/workspace"


def schedule_interval() -> timedelta | None:
    raw_value = os.getenv("BALANCE_DATASET_SCHEDULE_MINUTES", "1440").strip()
    try:
        minutes = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            "BALANCE_DATASET_SCHEDULE_MINUTES must be an integer"
        ) from exc
    return timedelta(minutes=minutes) if minutes > 0 else None


def docker_compose(command: str) -> str:
    return (
        f"cd {PROJECT_DIR} && "
        'HOST_PROJECT_DIR="${DOCKER_HOST_PROJECT_DIR:-.}" && '
        "export HOST_PROJECT_DIR && "
        f"docker compose {command}"
    )


with DAG(
    dag_id="build_balanced_comment_dataset",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=schedule_interval(),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 0},
    params={
        "seed": Param(42, type="integer", minimum=0, title="Sampling seed"),
        "target_per_group": Param(
            0,
            type="integer",
            minimum=0,
            title="Rows per source; 0 uses the smallest source",
        ),
        "dimensions": Param(
            "source",
            type="string",
            title="Comma-separated balancing dimensions",
        ),
    },
    tags=["dataset", "balancing", "lakehouse"],
) as dag:
    initialize_services = BashOperator(
        task_id="initialize_balancing_services",
        bash_command=docker_compose(
            "up -d --scale spark-worker=${SPARK_WORKER_COUNT:-4} "
            "minio spark-master spark-worker"
        ),
    )

    acquire_lock = BashOperator(
        task_id="acquire_pipeline_lock",
        execution_timeout=timedelta(hours=2, minutes=5),
        bash_command=acquire_pipeline_lock_command(),
    )

    build_dataset = BashOperator(
        task_id="build_balanced_dataset",
        execution_timeout=timedelta(hours=1),
        bash_command=r"""
        set -euo pipefail
        mkdir -p /workspace/data/balancing
        docker exec \
          -e BALANCE_SEED={{ params.seed }} \
          -e BALANCE_TARGET_PER_GROUP={{ params.target_per_group }} \
          -e BALANCE_DIMENSIONS="{{ params.dimensions }}" \
          -e BALANCE_REPORT_PATH=/opt/spark/balancing/report.json \
          spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --driver-memory 512m \
          --executor-memory 512m \
          --conf spark.cores.max=2 \
          --conf spark.executor.cores=1 \
          /opt/spark/jobs/maintenance/build_balanced_dataset.py
        """,
    )

    release_lock = BashOperator(
        task_id="release_pipeline_lock",
        trigger_rule="all_done",
        bash_command=release_pipeline_lock_command(),
    )

    initialize_services >> acquire_lock >> build_dataset >> release_lock
