"""Stage-1 AI training pipeline (manual trigger).

Runs ml/run_pipeline.py inside the `ai-trainer` container (role -> dataset ->
train -> evaluate -> report). The container bind-mounts the host project, so it
trains on the current data/samples/filtered_events.csv and writes ml/models +
ml/data/report.md back to the host.

Prereqs:
  - `docker compose build ai-trainer` has been run once (image exists).
  - To train on fresh lakehouse data, refresh the export on the HOST first, e.g.
    `python ml/run_pipeline.py --export`, before triggering this DAG.
    (docker cp from inside Airflow cannot write to the host filesystem.)
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ai_train_pipeline",
    description="Train the Stage-1 viral model via the ai-trainer container",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ai", "stage1"],
) as dag:
    train_stage1 = BashOperator(
        task_id="train_stage1",
        bash_command=(
            "set -e\n"
            "cd /workspace\n"
            'HOST_PROJECT_DIR="${DOCKER_HOST_PROJECT_DIR:-.}"\n'
            "export HOST_PROJECT_DIR\n"
            "docker compose run --rm ai-trainer python ml/run_pipeline.py --report\n"
        ),
    )
