"""Build an exact lakehouse dataset version, then train Stage 1 from it."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/workspace"


def docker_compose(command: str) -> str:
    return (
        f"cd {PROJECT_DIR} && "
        'HOST_PROJECT_DIR="${DOCKER_HOST_PROJECT_DIR:-.}" && '
        "export HOST_PROJECT_DIR && "
        f"docker compose {command}"
    )


with DAG(
    dag_id="ai_train_pipeline",
    description="Build and train from one reproducible lakehouse dataset version",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "dataset_version": Param(
            "auto",
            type="string",
            pattern=r"^(auto|dataset-v3-[a-f0-9]{20})$",
            title="Exact dataset version, or auto to build from pinned snapshots",
        ),
        "label_horizon_hours": Param(
            24,
            type="integer",
            enum=[1, 6, 24, 72, 168],
            title="Engagement label horizon",
        ),
        "label_tolerance_hours": Param(
            24,
            type="integer",
            minimum=0,
            title="Maximum wait after the target horizon",
        ),
        "virality_policy": Param(
            "training_reference_quantile",
            type="string",
            enum=["training_reference_quantile", "platform_reference_quantile"],
            title="Frozen virality engagement-threshold policy",
        ),
        "virality_contract": Param(
            "",
            type="string",
            title="Pinned historical contract path (required for platform reference)",
        ),
        "min_reference_examples_per_platform": Param(
            1,
            type="integer",
            minimum=1,
            title="Explicit operational floor; set deliberately for each run",
        ),
        "post_features_snapshot_id": Param(
            0,
            type="integer",
            minimum=0,
            title="Pinned post_features snapshot ID (0 resolves latest once)",
        ),
        "engagement_snapshots_snapshot_id": Param(
            0,
            type="integer",
            minimum=0,
            title="Pinned engagement_snapshots snapshot ID (0 resolves latest once)",
        ),
    },
    tags=["ai", "stage1", "lakehouse"],
) as dag:
    initialize_services = BashOperator(
        task_id="initialize_training_services",
        bash_command=docker_compose(
            "up -d --scale spark-worker=${SPARK_WORKER_COUNT:-4} minio spark-master spark-worker"
        ),
    )

    build_lakehouse_dataset = BashOperator(
        task_id="build_lakehouse_training_dataset",
        execution_timeout=timedelta(hours=1),
        bash_command=r"""
        set -euo pipefail
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --driver-memory 512m \
          --executor-memory 512m \
          --conf spark.cores.max=2 \
          --conf spark.executor.cores=1 \
          /opt/spark/jobs/maintenance/build_training_dataset.py \
          --dataset-version "{{ params.dataset_version }}" \
          --label-horizon-hours {{ params.label_horizon_hours }} \
          --label-tolerance-hours {{ params.label_tolerance_hours }} \
          --virality-policy "{{ params.virality_policy }}" \
          --min-reference-examples-per-platform {{ params.min_reference_examples_per_platform }} \
          {% if params.virality_contract %}
          --virality-contract "{{ params.virality_contract }}" \
          {% endif %}
          --post-features-snapshot-id {{ params.post_features_snapshot_id }} \
          --engagement-snapshots-snapshot-id {{ params.engagement_snapshots_snapshot_id }} \
          --export-root /opt/spark/balancing/ml \
          --manifest-output "/opt/spark/balancing/ml/runs/{{ ts_nodash }}.json"
        """,
    )

    train_stage1 = BashOperator(
        task_id="train_stage1",
        execution_timeout=timedelta(hours=4),
        bash_command=docker_compose(
            "run --rm ai-trainer python ml/run_pipeline.py "
            "--lakehouse-manifest "
            '"/workspace/data/lakehouse-ml/ml/runs/{{ ts_nodash }}.json" '
            "--report"
        ),
    )

    initialize_services >> build_lakehouse_dataset >> train_stage1
