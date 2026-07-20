#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

project_name="${E2E_COMPOSE_PROJECT_NAME:-user-behavior-pipeline-e2e}"
artifacts="$repo_root/tests/e2e/.artifacts"
mkdir -p "$artifacts"

compose=(docker compose -p "$project_name" -f tests/e2e/docker-compose.yml)
raw_topic="e2e.clean.events"
handoff_topic="lakehouse.bronze.for_silver"
dlq_topic="lakehouse.bronze.ingress.dlq"

cleanup() {
  status=$?
  trap - EXIT
  "${compose[@]}" ps --all >"$artifacts/compose-ps.txt" 2>&1 || true
  "${compose[@]}" logs --no-color >"$artifacts/compose.log" 2>&1 || true
  "${compose[@]}" down --volumes --remove-orphans || true
  exit "$status"
}
trap cleanup EXIT

create_topic() {
  "${compose[@]}" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic "$1" \
    --partitions 1 \
    --replication-factor 1
}

topic_offset() {
  "${compose[@]}" exec -T kafka \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 \
    --topic "$1" \
    | awk -F: '{ total += $3 } END { print total + 0 }'
}

require_topic_offset() {
  actual="$(topic_offset "$1")"
  if [[ "$actual" != "$2" ]]; then
    echo "Topic $1 offset is $actual; expected $2" >&2
    return 1
  fi
}

spark_submit() {
  "${compose[@]}" exec -T spark \
    /opt/spark/bin/spark-submit \
    --master 'local[2]' \
    "$@"
}

run_bronze() {
  fail_after_commit="$1"
  "${compose[@]}" exec -T spark env \
    KAFKA_TOPIC="$raw_topic" \
    KAFKA_STARTING_OFFSETS=earliest \
    BRONZE_KAFKA_OUT_TOPIC="$handoff_topic" \
    BRONZE_INGRESS_DLQ_TOPIC="$dlq_topic" \
    BRONZE_TRIGGER_MODE=available_now \
    BRONZE_CHECKPOINT_VERSION=pipeline_e2e_v1 \
    PIPELINE_RUN_ID=pipeline-e2e-bronze \
    PIPELINE_TEST_MODE=true \
    PIPELINE_TEST_FAIL_AFTER_BRONZE_COMMIT="$fail_after_commit" \
    /opt/spark/bin/spark-submit \
    --master 'local[2]' \
    /opt/spark/jobs/streaming/kafka_to_iceberg_bronze.py
}

run_silver() {
  "${compose[@]}" exec -T spark env \
    SILVER_KAFKA_TOPICS="$handoff_topic" \
    SILVER_STARTING_OFFSETS=earliest \
    SILVER_TRIGGER_MODE=available_now \
    SILVER_CHECKPOINT_VERSION=pipeline_e2e_v1 \
    PIPELINE_RUN_ID=pipeline-e2e-silver \
    /opt/spark/bin/spark-submit \
    --master 'local[2]' \
    /opt/spark/jobs/batch/bronze_to_silver_from_kafka.py
}

produce_fixture() {
  "${compose[@]}" exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$raw_topic" \
    <"$artifacts/events.jsonl"
}

echo "Building deterministic E2E fixture"
python3 tests/e2e/pipeline_fixture.py \
  --events-output "$artifacts/events.jsonl" \
  --schema-path schemas/playwright_event.avsc \
  --schema-payload-output "$artifacts/schema-registry.json"

echo "Starting Kafka, Schema Registry, MinIO, Spark and dashboard loader"
"${compose[@]}" up --build --detach minio minio-init kafka schema-registry spark dashboard

for topic in "$raw_topic" "$handoff_topic" "$dlq_topic"; do
  create_topic "$topic"
done

"${compose[@]}" exec -T schema-registry curl \
  --fail \
  --silent \
  --show-error \
  --request POST \
  --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data-binary @- \
  http://localhost:8081/subjects/e2e-clean-event-value/versions \
  <"$artifacts/schema-registry.json" \
  | tee "$artifacts/schema-registration.json"

produce_fixture

echo "Injecting the post-commit/pre-projection Bronze failure"
set +e
run_bronze true 2>&1 | tee "$artifacts/bronze-injected-failure.log"
bronze_failure_status=${PIPESTATUS[0]}
set -e
if [[ "$bronze_failure_status" == "0" ]]; then
  echo "Bronze fault injection unexpectedly succeeded" >&2
  exit 1
fi
grep -q "injected failure after Bronze event-log commit" \
  "$artifacts/bronze-injected-failure.log"
spark_submit /opt/spark/e2e/validate_pipeline.py --phase after-failure \
  | tee "$artifacts/validate-after-failure.log"
require_topic_offset "$handoff_topic" 0

echo "Restarting Bronze at the same checkpoint"
run_bronze false 2>&1 | tee "$artifacts/bronze-restart.log"
spark_submit /opt/spark/e2e/validate_pipeline.py --phase after-bronze \
  | tee "$artifacts/validate-after-bronze.log"
require_topic_offset "$handoff_topic" 14

echo "Applying the durable handoff to Silver twice"
run_silver 2>&1 | tee "$artifacts/silver-first-apply.log"
run_silver 2>&1 | tee "$artifacts/silver-restart.log"
spark_submit /opt/spark/e2e/validate_pipeline.py --phase after-silver \
  | tee "$artifacts/validate-after-silver.log"
spark_submit /opt/spark/jobs/maintenance/reconcile_bronze_silver.py --mode check \
  | tee "$artifacts/reconciliation-before-analytics.log"

echo "Materializing analytics and the official versioned ML dataset"
spark_submit /opt/spark/jobs/batch/content_analytics.py \
  | tee "$artifacts/content-analytics.log"
"${compose[@]}" exec -T spark env \
  PROCESSING_MODE=availableNow \
  /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  /opt/spark/jobs/batch/silver_post_features.py \
  2>&1 | tee "$artifacts/post-features.log"

for pass_number in 1 2; do
  spark_submit /opt/spark/jobs/maintenance/build_training_dataset.py \
    --dataset-version auto \
    --label-horizon-hours 1 \
    --label-tolerance-hours 1 \
    --export-root /opt/spark/e2e-artifacts/ml \
    --manifest-output /opt/spark/e2e-artifacts/ml/current.json \
    | tee "$artifacts/training-dataset-pass-$pass_number.log"
done

spark_submit /opt/spark/e2e/validate_pipeline.py --phase analytics \
  | tee "$artifacts/validate-analytics.log"
"${compose[@]}" exec -T dashboard python -m e2e.validate_dashboard_loader \
  | tee "$artifacts/validate-dashboard-loader.log"

echo "Replaying the complete Kafka fixture"
produce_fixture
run_bronze false 2>&1 | tee "$artifacts/bronze-replay.log"
require_topic_offset "$handoff_topic" 28
run_silver 2>&1 | tee "$artifacts/silver-replay.log"
spark_submit /opt/spark/jobs/maintenance/reconcile_bronze_silver.py --mode check \
  | tee "$artifacts/reconciliation-after-replay.log"
spark_submit /opt/spark/jobs/batch/content_analytics.py \
  | tee "$artifacts/content-analytics-replay.log"
spark_submit /opt/spark/e2e/validate_pipeline.py --phase replay \
  | tee "$artifacts/validate-replay.log"
"${compose[@]}" exec -T dashboard python -m e2e.validate_dashboard_loader \
  | tee "$artifacts/validate-dashboard-loader-replay.log"

echo "Pipeline reliability E2E completed successfully"
