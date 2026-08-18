# Reproducible pipeline evaluation

This directory provides the reporting and reliability contract for evaluating
the event pipeline independently from machine learning. It does not alter
labels, training splits, features, models, calibration, or ML metrics.

## Existing production mechanisms reused

The benchmark contract follows the implemented pipeline rather than defining a
second data architecture:

- deterministic `event_id`, payload fingerprints, provenance, and coverage
  come from `common/event_envelope.py`;
- source-specific cleaning and DLQs come from
  `spark/jobs/pipeline/collector_stream_pipeline.py`;
- the insert-only Bronze journal is `lakehouse.bronze.event_log`;
- the current Bronze projection is `lakehouse.bronze.events`;
- Bronze handoff uses `lakehouse.bronze.for_silver`;
- idempotent Silver state and application proofs use
  `lakehouse.silver.events` and `lakehouse.silver.applied_events`;
- reconciliation uses the same event-log/proof set contract as
  `spark/jobs/maintenance/reconcile_bronze_silver.py`;
- environment integration fields are compatible with
  `common/reproducibility.py` and may receive the shared Git, container, and
  environment identities after integration.

The repository contains both a legacy `stream_pipeline.py` path and the newer
collector/Bronze/Silver reliability path. System evaluation must use the newer
path because it has deterministic IDs, protected ingress DLQ evidence, an
immutable journal, and application proofs.

## Validation benchmark

Run the fast, explicit contract validation:

```bash
python -m benchmarks.pipeline run \
  --backend contract_validation \
  --sizes 100,500,1000 \
  --repetitions 3 \
  --warmup-runs 1 \
  --platform youtube \
  --workload generated_load
```

This backend runs deterministic generated events through an in-process model
of the implemented validation, insert-only journal, Silver proof, replay, and
reconciliation contracts. It also injects controlled DLQ cases for missing
identifiers, URLs, timestamps, collector errors, and empty content.

Its output is deliberately marked:

```text
run_class = validation
backend = contract_validation
system_performance_measured = false
```

Therefore its timing and throughput values test metric calculation and report
generation only. They are not evidence of Kafka, Spark, Iceberg, MinIO, or
distributed-system performance. The CLI rejects `--run-class official` for
this backend.

## Full system benchmark

The `system` backend runs the production Clean, Bronze, Silver, and optional
Gold jobs against the repository's Kafka, Spark, MinIO, and Iceberg services.
Schema Registry and collectors are not required for generated JSON input. The
normal Compose services may be stopped simply because they are not in an
active profile or were not started; `--start-stack` starts only `minio`,
`kafka`, `spark-master`, and `spark-worker`.

The explicit safety switch prevents accidental execution:

```bash
export RUN_PIPELINE_BENCHMARKS=1
python -m benchmarks.pipeline run \
  --backend system \
  --sizes 100,1000 \
  --repetitions 1 \
  --warmup-runs 0 \
  --platform youtube \
  --workload generated_load \
  --start-stack
```

PowerShell uses `$env:RUN_PIPELINE_BENCHMARKS='1'`. Every run enforces these
guards and never points at the normal source topics or `lakehouse` bucket:

```text
topic prefix:       benchmark.<benchmark_id>.*
warehouse bucket:   benchmark-<benchmark_id>
checkpoint prefix:  benchmark/<benchmark_id>/checkpoints
output path:        benchmarks/runs/<benchmark_id>
```

An `official` run additionally requires a clean Git worktree and a non-empty
`BENCHMARK_ENVIRONMENT_FINGERPRINT`. It captures the Git revision, container
image identities/digests, Spark/Kafka configuration, topology, workload
identity, Iceberg snapshots, and physical storage metadata. Missing
measurements, stage failures, timeouts, or reliability anomalies fail the
campaign. Validation-class system runs remain valid measured evidence but are
not labeled official.

## Workload identity

Generated workloads use a fixed base time, stable source identifiers, stable
payload ordering, and canonical SHA-256 fingerprints. A size is the total
number of nominal input messages. The system backend runs five invalid cases
in a separate isolated DLQ experiment so they do not contaminate performance
timings. Generated load is never described as collected social content.

Workload adapters retain separate `workload.type` values:

- `replay`: deterministic JSONL reread identified by its SHA-256 fingerprint;
- `generated_load`: generated or replicated input used for load testing.

Replay input is selected with `--workload replay --input <events.jsonl>`.

## Measurements and units

Durations use `time.perf_counter()`, a monotonic clock. Audit timestamps use
UTC wall time but are not subtracted to obtain elapsed durations. Throughput is
`processed events / elapsed seconds`; a zero duration yields an absent value,
not infinity. Measured repetitions are retained individually, while summaries
provide median, minimum, maximum, mean, and population standard deviation.
Warmups are stored but excluded from summaries.

JSON is the source of truth. CSV and Markdown are derived from the JSONL
measurements. SVG figures are generated from summary values and contain no
hard-coded benchmark result. End-to-end duration covers production through
the requested materialization stages, excluding audit probes and replay.
Stage durations include Spark startup. Physical bytes are Iceberg data-file
plus manifest lengths. Kafka starting/ending offsets are recorded; consumer
lag and peak lag remain null because this bounded available-now execution has
no reliable sampled consumer-lag source.

## Reliability experiments

Idempotence processes batch `B`, records actual Iceberg logical state `S1`,
replays the exact payloads into the same Kafka topics and Spark checkpoints,
runs the production jobs again, and compares `S2`. A system run fails unless
Bronze journal rows, Silver rows, and application proofs do not increase.
Reconciliation runs both the read-only benchmark probe and
`reconcile_bronze_silver.py --mode check`. DLQ checks consume the real DLQ
topic and compare exact rejection reasons; false acceptance or unexpected
rejection fails the run.

## Artifacts

Each campaign writes:

```text
benchmarks/runs/<benchmark_id>/
  manifest.json
  measurements.jsonl
  summary.json
  benchmark_summary.csv
  paper_table.md
  reliability_table.md
  input_vs_duration.svg
  input_vs_throughput.svg
  input_vs_storage.svg
  artifacts.json
  system-runs/<system_run_id>/logs/
  reliability-runs/<dlq_run_id>/logs/
```

Runtime artifacts are ignored by Git. A failed campaign should remain on disk
with its status, failing stage, checks, and partial counts; it must not be
silently discarded.

## Interpretation limits

Do not claim linear, industrial, or distributed scalability from validation
runs or from a single-host Docker Compose run. Describe only measured volumes,
hardware, topology, versions, and configurations. Missing physical-storage,
lag, or snapshot measurements remain absent rather than estimated. Gold may be
timed only as data materialization; it must not change or evaluate the ML
protocol.
