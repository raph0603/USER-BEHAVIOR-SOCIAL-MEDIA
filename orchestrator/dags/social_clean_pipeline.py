from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime

PROJECT_DIR = "/workspace"

# The Spark image already bundles the Kafka jars (see spark/master/Dockerfile),
# so no --packages / network is needed. Each stream is capped at 2 cores so the
# three platforms run in parallel (3 x 2 = 6 <= 16 cores).
SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.cores.max=2 --conf spark.executor.cores=1"
)

# platform -> (sample CSV, raw topic)
PLATFORMS = {
    "youtube": ("data/samples/youtube.csv", "raw.youtube"),
    "reddit": ("data/samples/reddit_data.csv", "raw.reddit"),
    "x": ("data/samples/x_dataset.csv", "raw.x"),
}
INGEST_LIMIT = 1000  # bounded rows per platform per run


def compose(cmd: str) -> str:
    return (
        f"cd {PROJECT_DIR} && "
        'HOST_PROJECT_DIR="${DOCKER_HOST_PROJECT_DIR:-.}" && '
        "export HOST_PROJECT_DIR && "
        f"docker compose {cmd}"
    )


START_CLEAN_TMPL = r'''
docker exec -e PLATFORM=__P__ spark-master /bin/bash -lc "mkdir -p /opt/spark/tests/pipeline; __SUBMIT__ /opt/spark/jobs/pipeline/stream_pipeline.py > /opt/spark/tests/pipeline/__P__.log 2>&1 & echo \$! > /opt/spark/tests/pipeline/__P__.pid"
sleep 45
docker exec spark-master /bin/bash -lc "kill -0 \$(cat /opt/spark/tests/pipeline/__P__.pid)" || { echo "stream __P__ died on startup"; docker exec spark-master /bin/bash -lc "tail -n 60 /opt/spark/tests/pipeline/__P__.log"; exit 1; }
'''

INGEST_TMPL = r'''
set -euo pipefail
cd /workspace
python scripts/replay_csv_to_kafka.py __CSV__ /tmp/__P__.jsonl
head -n __LIMIT__ /tmp/__P__.jsonl | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic __TOPIC__
'''


with DAG(
    dag_id="social_clean_pipeline",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    default_args={"owner": "data-platform", "retries": 0},
    tags=["pipeline", "spark", "kafka", "clean"],
) as dag:

    start_stack = BashOperator(
        task_id="start_core_stack",
        bash_command=compose(
            "up -d --scale spark-worker=${SPARK_WORKER_COUNT:-4} "
            "kafka spark-master spark-worker"
        ),
    )

    wait_services = BashOperator(
        task_id="wait_services",
        bash_command=r"""
        set -euo pipefail
        wait_for() { local label="$1"; shift; for i in $(seq 1 24); do if "$@"; then echo "$label ready"; return 0; fi; sleep 5; done; echo "$label not ready"; return 1; }
        wait_for "kafka" docker exec kafka /bin/bash -lc "/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092 >/dev/null 2>&1"
        wait_for "spark master" docker exec spark-master /bin/bash -lc "curl -fsS http://spark-master:8080 >/dev/null 2>&1"
        """,
    )

    cleanup = BashOperator(
        task_id="cleanup_previous_jobs",
        bash_command=r"""
        docker exec spark-master /bin/bash -lc "pkill -9 -f '[s]tream_pipeline.py' || true; pkill -9 -f '[d]lq_report.py' || true"
        docker exec spark-master /bin/bash -lc "rm -rf /tmp/spark-checkpoints/pipeline"
        exit 0
        """,
    )


    create_topics = BashOperator(
        task_id="create_topics",
        bash_command=r"""
        set -euo pipefail
        for t in raw.youtube raw.reddit raw.x clean.posts dlq.posts; do
          docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
        done
        """,
    )

    wait_clean = BashOperator(
        task_id="wait_clean_rows",
        bash_command=r"""
        set -euo pipefail
        for i in $(seq 1 24); do
          TOTAL=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic clean.posts 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
          echo "clean.posts total offsets: ${TOTAL:-0}"
          if [ "${TOTAL:-0}" -ge 1 ]; then exit 0; fi
          sleep 5
        done
        echo "no rows landed in clean.posts"
        exit 1
        """,
    )

    stop_streams = BashOperator(
        task_id="stop_clean_streams",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=r"""
        docker exec spark-master /bin/bash -lc "pkill -9 -f '[s]tream_pipeline.py' || true"
        exit 0
        """,
    )

    done = EmptyOperator(task_id="pipeline_done")

    start_stack >> wait_services >> cleanup >> create_topics

    for platform, (csv, topic) in PLATFORMS.items():
        start_clean = BashOperator(
            task_id=f"start_clean_{platform}",
            bash_command=START_CLEAN_TMPL
            .replace("__P__", platform)
            .replace("__SUBMIT__", SPARK_SUBMIT),
        )
        ingest = BashOperator(
            task_id=f"ingest_{platform}",
            bash_command=INGEST_TMPL
            .replace("__P__", platform)
            .replace("__CSV__", csv)
            .replace("__TOPIC__", topic)
            .replace("__LIMIT__", str(INGEST_LIMIT)),
        )
        create_topics >> start_clean >> ingest >> wait_clean

    wait_clean >> stop_streams >> done
