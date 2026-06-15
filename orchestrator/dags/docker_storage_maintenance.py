from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime


def schedule_interval() -> timedelta | None:
    raw_value = os.getenv(
        "DOCKER_MAINTENANCE_SCHEDULE_MINUTES",
        "1440",
    ).strip()
    try:
        minutes = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            "DOCKER_MAINTENANCE_SCHEDULE_MINUTES must be an integer"
        ) from exc
    return timedelta(minutes=minutes) if minutes > 0 else None


with DAG(
    dag_id="docker_storage_maintenance",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=schedule_interval(),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 1},
    tags=["docker", "maintenance", "storage"],
) as dag:
    prune_unused_objects = BashOperator(
        task_id="prune_unused_docker_objects",
        execution_timeout=timedelta(minutes=30),
        bash_command=r"""
        set -euo pipefail
        CONTAINER_AGE="${DOCKER_MAINTENANCE_CONTAINER_AGE:-24h}"
        IMAGE_AGE="${DOCKER_MAINTENANCE_IMAGE_AGE:-24h}"
        CACHE_AGE="${DOCKER_MAINTENANCE_BUILD_CACHE_AGE:-168h}"
        CACHE_KEEP="${DOCKER_MAINTENANCE_BUILD_CACHE_KEEP:-1GB}"

        docker container prune --force --filter "until=$CONTAINER_AGE"
        docker image prune --force --filter "until=$IMAGE_AGE"
        docker builder prune --force \
          --filter "until=$CACHE_AGE" \
          --reserved-space "$CACHE_KEEP"

        echo "Named and anonymous volumes are intentionally preserved."
        docker system df
        """,
    )

    remove_expired_airflow_logs = BashOperator(
        task_id="remove_expired_airflow_logs",
        execution_timeout=timedelta(minutes=10),
        bash_command=r"""
        set -euo pipefail
        RETENTION_DAYS="${AIRFLOW_LOG_RETENTION_DAYS:-14}"
        if [[ ! "$RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
          echo "AIRFLOW_LOG_RETENTION_DAYS must be a non-negative integer"
          exit 1
        fi

        find /opt/airflow/logs -type f -mtime "+$RETENTION_DAYS" -delete
        find /opt/airflow/logs -depth -type d -empty -delete
        """,
    )

    prune_unused_objects >> remove_expired_airflow_logs
