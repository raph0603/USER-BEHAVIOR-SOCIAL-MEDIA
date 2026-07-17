from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from crawler_configuration import load_insight_config
from pendulum import datetime
from pipeline_lock import (
    acquire_pipeline_lock_command,
    release_pipeline_lock_command,
)


PROJECT_DIR = "/workspace"


def schedule_interval() -> timedelta | None:
    raw_value = os.getenv("INSIGHT_REFRESH_SCHEDULE_MINUTES", "30").strip()
    try:
        minutes = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            "INSIGHT_REFRESH_SCHEDULE_MINUTES must be an integer"
        ) from exc
    return timedelta(minutes=minutes) if minutes > 0 else None


def docker_compose(command: str) -> str:
    return (
        f"cd {PROJECT_DIR} && "
        'HOST_PROJECT_DIR="${DOCKER_HOST_PROJECT_DIR:-.}" && '
        "export HOST_PROJECT_DIR && "
        f"docker compose {command}"
    )


INSIGHT_CONFIG = load_insight_config()


with DAG(
    dag_id="refresh_recent_engagement_insights",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=schedule_interval(),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    default_args={"owner": "data-platform", "retries": 1},
    params={
        "lookback_days": Param(
            INSIGHT_CONFIG["lookback_days"],
            type="integer",
            minimum=1,
            maximum=365,
            title="Insight lookback in days",
        ),
        "max_events_per_source": Param(
            INSIGHT_CONFIG["max_events_per_source"],
            type="integer",
            minimum=1,
            maximum=5000,
            title="Maximum events refreshed per source",
        ),
        "x_headless": Param(
            INSIGHT_CONFIG["x_headless"],
            type="boolean",
            title="Run X browser headlessly",
        ),
    },
    tags=["engagement", "insights", "refresh", "lakehouse"],
) as dag:
    initialize_services = BashOperator(
        task_id="initialize_refresh_services",
        bash_command=docker_compose(
            "up -d --scale spark-worker=${SPARK_WORKER_COUNT:-4} "
            "minio spark-master spark-worker"
        ),
    )

    verify_services = BashOperator(
        task_id="verify_refresh_services",
        execution_timeout=timedelta(minutes=3),
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

    acquire_lock = BashOperator(
        task_id="acquire_pipeline_lock",
        execution_timeout=timedelta(hours=2, minutes=5),
        bash_command=acquire_pipeline_lock_command(),
    )

    reset_output = BashOperator(
        task_id="reset_refresh_output",
        bash_command=r"""
        set -euo pipefail
        mkdir -p /workspace/data/insight-refresh
        rm -f /workspace/data/insight-refresh/targets.jsonl
        rm -f /workspace/data/insight-refresh/youtube.jsonl
        rm -f /workspace/data/insight-refresh/x.jsonl
        rm -f /workspace/data/insight-refresh/reddit.jsonl
        """,
    )

    export_targets = BashOperator(
        task_id="export_recent_silver_targets",
        execution_timeout=timedelta(minutes=20),
        bash_command=r"""
        set -euo pipefail
        docker exec \
          -e INSIGHT_REFRESH_LOOKBACK_DAYS={{ params.lookback_days }} \
          -e INSIGHT_REFRESH_MAX_PER_SOURCE={{ params.max_events_per_source }} \
          spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --driver-memory 512m \
          --executor-memory 512m \
          --conf spark.cores.max=2 \
          --conf spark.executor.cores=1 \
          /opt/spark/jobs/maintenance/export_recent_insight_targets.py
        """,
    )

    refresh_youtube = BashOperator(
        task_id="refresh_youtube_insights",
        execution_timeout=timedelta(minutes=30),
        bash_command=docker_compose(
            "run --rm --no-deps "
            "-e INSIGHT_REFRESH_SOURCE=youtube "
            "youtube-collector python /app/insight_refresh.py"
        ),
    )

    refresh_x = BashOperator(
        task_id="refresh_x_insights",
        retries=0,
        execution_timeout=timedelta(hours=2),
        bash_command=r"""
        set -euo pipefail
        if [[ "${X_COLLECTION_ENABLED:-false}" != "true" ]]; then
          echo "X insight refresh disabled"
          : > /workspace/data/insight-refresh/x.jsonl
          exit 0
        fi
        """ + docker_compose(
            "run --rm --no-deps "
            "-e INSIGHT_REFRESH_SOURCE=x "
            "-e X_HEADLESS={{ params.x_headless | lower }} "
            "x-collector python /app/insight_refresh.py"
        ),
    )

    refresh_reddit = BashOperator(
        task_id="refresh_reddit_insights",
        execution_timeout=timedelta(hours=1),
        bash_command=r"""
        set -euo pipefail
        if [[ "${REDDIT_COLLECTION_ENABLED:-false}" != "true" ]]; then
          echo "Reddit insight refresh disabled"
          : > /workspace/data/insight-refresh/reddit.jsonl
          exit 0
        fi
        """ + docker_compose(
            "run --rm --no-deps "
            "-e INSIGHT_REFRESH_SOURCE=reddit "
            "reddit-collector python /app/insight_refresh.py"
        ),
    )

    validate_refresh_output = BashOperator(
        task_id="validate_refresh_output",
        execution_timeout=timedelta(minutes=5),
        bash_command=docker_compose(
            "run --rm --no-deps youtube-collector "
            "python /app/validate_insight_refresh.py"
        ),
    )

    append_snapshots = BashOperator(
        task_id="append_engagement_snapshots",
        execution_timeout=timedelta(minutes=30),
        bash_command=r"""
        set -euo pipefail
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --driver-memory 512m \
          --executor-memory 512m \
          --conf spark.cores.max=2 \
          --conf spark.executor.cores=1 \
          /opt/spark/jobs/batch/engagement_snapshots.py
        """,
    )

    compute_velocity = BashOperator(
        task_id="compute_youtube_velocity_and_virality",
        execution_timeout=timedelta(minutes=30),
        bash_command=r"""
        set -euo pipefail
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --driver-memory 512m \
          --executor-memory 512m \
          --conf spark.cores.max=2 \
          --conf spark.executor.cores=1 \
          /opt/spark/jobs/batch/youtube_engagement_velocity.py
        """,
    )

    apply_updates = BashOperator(
        task_id="merge_latest_engagement_values",
        execution_timeout=timedelta(minutes=30),
        bash_command=r"""
        set -euo pipefail
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --driver-memory 512m \
          --executor-memory 512m \
          --conf spark.cores.max=2 \
          --conf spark.executor.cores=1 \
          /opt/spark/jobs/maintenance/apply_insight_updates.py
        """,
    )

    release_lock = BashOperator(
        task_id="release_pipeline_lock",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=release_pipeline_lock_command(),
    )

    initialize_services >> verify_services >> acquire_lock
    acquire_lock >> reset_output >> export_targets
    export_targets >> [refresh_youtube, refresh_x, refresh_reddit]
    [refresh_youtube, refresh_x, refresh_reddit] >> validate_refresh_output
    validate_refresh_output >> [append_snapshots, apply_updates]
    append_snapshots >> compute_velocity
    [compute_velocity, apply_updates] >> release_lock
