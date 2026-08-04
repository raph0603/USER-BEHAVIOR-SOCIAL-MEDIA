#!/usr/bin/env bash
set -Eeuo pipefail

MAX_EVENTS=1
OUTPUT="artifacts/x-lineage"
TIMEOUT_SECONDS=900

usage() {
  cat <<'EOF'
Usage: ./scripts/export_x_lineage.sh [--max-events 1] [--output PATH] [--timeout SECONDS]

Collect exactly one new real X post and export its RAW -> Bronze -> Silver -> Gold lineage.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-events)
      MAX_EVENTS="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$MAX_EVENTS" != "1" ]]; then
  echo "This demonstration intentionally supports exactly --max-events 1." >&2
  exit 2
fi
if ! [[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "--timeout must be a positive integer." >&2
  exit 2
fi
for command_name in docker python3 timeout tee; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command is unavailable: $command_name" >&2
    exit 2
  fi
done

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$PROJECT_ROOT"
mkdir -p "$OUTPUT"
OUTPUT_ROOT="$(cd "$OUTPUT" && pwd -P)"
export HOST_PROJECT_DIR="$PROJECT_ROOT"
export X_LINEAGE_HOST_OUTPUT="$OUTPUT_ROOT"

RUN_ID="x_lineage_$(date -u +%Y%m%dT%H%M%SZ)_$(python3 -c 'import secrets; print(secrets.token_hex(4))')"
CHECKPOINT_VERSION="$RUN_ID"
STAGING_DIR="$OUTPUT_ROOT/.runs/$RUN_ID"
STAGING_LOGS="$STAGING_DIR/logs"
mkdir -p "$STAGING_LOGS"
chmod 700 "$STAGING_DIR" || true

AIRFLOW_WRITER_DAGS=(
  user_behavior_lakehouse
  user_behavior_lakehouse_no_row_checks
  refresh_recent_engagement_insights
  iceberg_parquet_compaction
  build_balanced_comment_dataset
)
AIRFLOW_DAGS_TO_RESTORE=()

restore_airflow_schedules() {
  if [[ ${#AIRFLOW_DAGS_TO_RESTORE[@]} -eq 0 ]]; then
    return
  fi
  for dag_id in "${AIRFLOW_DAGS_TO_RESTORE[@]}"; do
    docker compose exec -T airflow-scheduler airflow dags unpause "$dag_id" \
      >/dev/null 2>&1 || true
  done
  echo "Restored ${#AIRFLOW_DAGS_TO_RESTORE[@]} Airflow schedule(s)"
}
trap restore_airflow_schedules EXIT

pause_airflow_writers() {
  if ! docker compose ps --status running --services |
    grep -qx "airflow-scheduler"; then
    return
  fi

  local dags_json
  dags_json="$(
    docker compose exec -T airflow-scheduler airflow dags list --output json
  )"
  for dag_id in "${AIRFLOW_WRITER_DAGS[@]}"; do
    local pause_state
    pause_state="$(
      python3 -c '
import json
import sys

dag_id = sys.argv[1]
rows = json.load(sys.stdin)
match = next((row for row in rows if row.get("dag_id") == dag_id), None)
if match is None:
    print("absent")
else:
    raw_state = match.get("is_paused")
    is_paused = raw_state is True or str(raw_state).strip().lower() == "true"
    print("paused" if is_paused else "active")
' "$dag_id" <<<"$dags_json"
    )"
    if [[ "$pause_state" == "active" ]]; then
      docker compose exec -T airflow-scheduler airflow dags pause "$dag_id" \
        >/dev/null
      AIRFLOW_DAGS_TO_RESTORE+=("$dag_id")
    fi
  done

  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  while true; do
    local active_runs=0
    for dag_id in "${AIRFLOW_WRITER_DAGS[@]}"; do
      for state in queued running; do
        local count
        count="$(
          docker compose exec -T airflow-scheduler \
            airflow dags list-runs --dag-id "$dag_id" --state "$state" \
            --output json |
            python3 -c 'import json,sys; print(len(json.load(sys.stdin)))'
        )"
        active_runs=$((active_runs + count))
      done
    done
    if [[ "$active_runs" -eq 0 ]]; then
      echo "Airflow lakehouse writers are idle"
      return
    fi
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for $active_runs Airflow writer run(s)" >&2
      return 1
    fi
    echo "Waiting for $active_runs Airflow writer run(s) to finish..."
    sleep 10
  done
}

echo "X lineage run: $RUN_ID"
echo "Output root: $OUTPUT_ROOT"

pause_airflow_writers
docker compose build x-collector spark-master spark-worker
docker compose up -d --scale spark-worker=1 \
  minio kafka schema-registry spark-master spark-worker

wait_for() {
  local label="$1"
  shift
  for _ in $(seq 1 36); do
    if "$@" >/dev/null 2>&1; then
      echo "$label ready"
      return 0
    fi
    sleep 5
  done
  echo "$label did not become ready" >&2
  return 1
}

wait_for "MinIO" docker exec minio /bin/sh -c \
  "curl -fsS http://minio:9000/minio/health/ready"
docker compose run --rm minio-init >/dev/null
wait_for "Kafka" docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server kafka:9092
wait_for "Schema Registry" docker exec schema-registry curl -fsS \
  http://schema-registry:8081/subjects
wait_for "Spark master" docker exec spark-master curl -fsS http://spark-master:8080

for topic in x.raw.events x.clean.events x.dlq.events \
  lakehouse.bronze.for_silver lakehouse.bronze.ingress.dlq; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists --topic "$topic" --partitions 2 \
    --replication-factor 1 --bootstrap-server kafka:9092 >/dev/null
done

topic_offsets() {
  local topic="$1"
  docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 --topic "$topic" |
    awk -F: -v topic="$topic" '
      BEGIN { printf "{\"%s\":{", topic; separator="" }
      { printf "%s\"%s\":%s", separator, $2, $3; separator="," }
      END { print "}}" }
    '
}

RAW_STARTING_OFFSETS="$(topic_offsets x.raw.events)"
CLEAN_STARTING_OFFSETS="$(topic_offsets x.clean.events)"
SILVER_STARTING_OFFSETS="$(topic_offsets lakehouse.bronze.for_silver)"

set +e
timeout --kill-after=30s "${TIMEOUT_SECONDS}s" \
  docker compose run --rm \
  -e PRODUCER_MAX_EVENTS=1 \
  -e PIPELINE_RUN_ID="$RUN_ID" \
  -e X_RAW_CAPTURE_ENABLED=true \
  -e X_RAW_CAPTURE_DIR="/app/captures/x/.runs/$RUN_ID" \
  -e X_RAW_CAPTURE_LIMIT=1 \
  x-collector 2>&1 | tee "$STAGING_LOGS/collector.log"
COLLECTOR_STATUS=${PIPESTATUS[0]}
set -e
if [[ "$COLLECTOR_STATUS" -ne 0 ]]; then
  echo "X collection failed; see $STAGING_LOGS/collector.log" >&2
  exit "$COLLECTOR_STATUS"
fi

RAW_CAPTURE="$STAGING_DIR/raw.json"
if [[ ! -s "$RAW_CAPTURE" ]]; then
  echo "RAW capture was not produced; no synthetic fallback is allowed." >&2
  exit 1
fi
PLATFORM_EVENT_ID="$(
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["event"]["platform_event_id"])' \
    "$RAW_CAPTURE"
)"
if ! [[ "$PLATFORM_EVENT_ID" =~ ^[0-9]+$ ]]; then
  echo "Captured X platform_event_id is invalid: $PLATFORM_EVENT_ID" >&2
  exit 1
fi

EVENT_DIR="$OUTPUT_ROOT/$PLATFORM_EVENT_ID"
if [[ -e "$EVENT_DIR" ]]; then
  echo "Refusing to overwrite existing lineage bundle: $EVENT_DIR" >&2
  exit 1
fi
mkdir -p "$EVENT_DIR/logs"
mv "$RAW_CAPTURE" "$EVENT_DIR/raw.json"
mv "$STAGING_LOGS/collector.log" "$EVENT_DIR/logs/collector.log"

run_spark_stage() {
  local log_path="$1"
  shift
  set +e
  timeout --kill-after=30s "${TIMEOUT_SECONDS}s" "$@" 2>&1 | tee -a "$log_path"
  local status=${PIPESTATUS[0]}
  set -e
  if [[ "$status" -ne 0 ]]; then
    echo "Stage failed with status $status; see $log_path" >&2
    return "$status"
  fi
}

run_spark_stage "$EVENT_DIR/logs/privacy-cleaning.log" \
  docker exec \
  -e PLATFORM=x \
  -e COLLECTOR_SOURCE_TOPIC=x.raw.events \
  -e CLEAN_KAFKA_TOPIC=x.clean.events \
  -e DLQ_KAFKA_TOPIC=x.dlq.events \
  -e CLEAN_SOURCE_VALUE_FORMAT=avro \
  -e CLEAN_STARTING_OFFSETS="$RAW_STARTING_OFFSETS" \
  -e CLEAN_CHECKPOINT_VERSION="$CHECKPOINT_VERSION" \
  -e CLEAN_TRIGGER_MODE=available_now \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m --executor-memory 512m \
  --conf spark.cores.max=1 --conf spark.executor.cores=1 \
  /opt/spark/jobs/pipeline/collector_stream_pipeline.py

run_spark_stage "$EVENT_DIR/logs/bronze.log" \
  docker exec \
  -e KAFKA_TOPIC=x.clean.events \
  -e KAFKA_VALUE_FORMAT=json \
  -e KAFKA_STARTING_OFFSETS="$CLEAN_STARTING_OFFSETS" \
  -e BRONZE_KAFKA_OUT_TOPIC=lakehouse.bronze.for_silver \
  -e BRONZE_INGRESS_DLQ_TOPIC=lakehouse.bronze.ingress.dlq \
  -e PIPELINE_RUN_ID="$RUN_ID" \
  -e BRONZE_CHECKPOINT_VERSION="$CHECKPOINT_VERSION" \
  -e BRONZE_TRIGGER_MODE=available_now \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m --executor-memory 512m \
  --conf spark.cores.max=1 --conf spark.executor.cores=1 \
  /opt/spark/jobs/streaming/kafka_to_iceberg_bronze.py

run_spark_stage "$EVENT_DIR/logs/silver.log" \
  docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m --executor-memory 512m \
  --conf spark.cores.max=1 --conf spark.executor.cores=1 \
  /opt/spark/jobs/maintenance/replay_x_lineage_event.py \
  --platform-event-id "$PLATFORM_EVENT_ID" \
  --pipeline-run-id "$RUN_ID"

run_spark_stage "$EVENT_DIR/logs/silver.log" \
  docker exec \
  -e SILVER_KAFKA_TOPICS=lakehouse.bronze.for_silver \
  -e SILVER_STARTING_OFFSETS="$SILVER_STARTING_OFFSETS" \
  -e PIPELINE_RUN_ID="$RUN_ID" \
  -e SILVER_CHECKPOINT_VERSION="$CHECKPOINT_VERSION" \
  -e SILVER_TRIGGER_MODE=available_now \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m --executor-memory 512m \
  --conf spark.cores.max=1 --conf spark.executor.cores=1 \
  /opt/spark/jobs/batch/bronze_to_silver_from_kafka.py

run_spark_stage "$EVENT_DIR/logs/silver.log" \
  docker exec -e PROCESSING_MODE=availableNow \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m --executor-memory 512m \
  --conf spark.cores.max=1 --conf spark.executor.cores=1 \
  /opt/spark/jobs/batch/silver_post_features.py

run_spark_stage "$EVENT_DIR/logs/gold.log" \
  docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m --executor-memory 512m \
  --conf spark.cores.max=1 --conf spark.executor.cores=1 \
  /opt/spark/jobs/batch/content_analytics.py

run_spark_stage "$EVENT_DIR/logs/export.log" \
  docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m --executor-memory 512m \
  --conf spark.cores.max=1 --conf spark.executor.cores=1 \
  /opt/spark/jobs/maintenance/export_x_lineage.py \
  --raw "/opt/spark/x-lineage/$PLATFORM_EVENT_ID/raw.json" \
  --output-root /opt/spark/x-lineage \
  --pipeline-run-id "$RUN_ID" \
  --checkpoint-version "$CHECKPOINT_VERSION"

python3 scripts/finalize_x_lineage.py "$EVENT_DIR"
rm -rf "$STAGING_DIR"
