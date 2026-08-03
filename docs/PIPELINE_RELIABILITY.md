# Pipeline reliability and operations

This runbook describes the reliability architecture that is implemented in the
repository. Iceberg tables stored in MinIO are the system of record. Kafka is
the transport and replay boundary; SQLite holds durable worker scheduling,
quota, circuit-breaker, and outbox state for state-dependent workers. The
engagement metrics refresh publishes to Kafka before touching SQLite.

Elasticsearch is not part of the ingestion, serving, monitoring, or recovery
path. No Elasticsearch integration is required for this architecture.

## Durable event flow

```text
privacy-clean clean topics
        |
        +-- invalid JSON/contract rows
        |      -> lakehouse.bronze.ingress_dlq
        |      -> lakehouse.bronze.ingress.dlq
        |
        `-- valid rows
               -> lakehouse.bronze.event_log       (insert only by event_id)
               -> lakehouse.bronze.events          (compatible current view)
               -> reread committed journal rows
               -> lakehouse.bronze.for_silver      (durable handoff)
               -> lakehouse.silver.events          (idempotent current merge)
               -> lakehouse.silver.applied_events  (insert-only application proof)
               -> Bronze/Silver reconciliation
               -> quality, analytics, ML, and dashboard tables
```

The Bronze micro-batch order is strict:

1. parse and separate invalid input;
2. persist protected invalid-row evidence;
3. insert valid rows into `lakehouse.bronze.event_log`;
4. update the compatible `lakehouse.bronze.events` projection;
5. reread the rows that actually exist in the journal;
6. publish those committed rows to `lakehouse.bronze.for_silver`.

`event_id` is deterministic for the canonical payload and observation. The
journal merge is insert-only, so a Kafka duplicate or checkpoint replay cannot
rewrite history. The current projection may merge a newer event for the same
source object without changing the journal.

Silver first applies the event through its idempotent current-state merge and
then inserts its `event_id` into `lakehouse.silver.applied_events`. A crash
between those operations is safe: reconciliation or Kafka replay applies the
same merge again and then records the missing proof.

Malformed input is never silently dropped. `lakehouse.bronze.ingress_dlq`
stores Kafka topic, partition, offset and timestamp, a category, a fingerprint,
and a protected payload envelope containing only length and SHA-256 metadata.
The rejected content is neither persisted nor written to logs.

Spark reads Kafka with `failOnDataLoss=true` by default. Setting
`KAFKA_FAIL_ON_DATA_LOSS=false` is rejected unless
`ALLOW_KAFKA_DATA_LOSS=true` is also set. Treat that pair as a recovery-only,
audited acceptance of data loss.

## YouTube worker and snapshot guarantees

Discovery, metadata, metrics, transcripts, comments, and channels remain
separate bounded workers with independent Kafka topics, consumer groups,
retry state, cooldowns, circuit breakers, and health metrics.

The metrics worker consumes due Silver targets and calls `videos.list` with no
more than 50 video IDs per request. It does not open watch pages. Every result
is published as `youtube.engagement.snapshot` and carries a deterministic
`observation_id`, producer/run information, collection method, API endpoint,
payload fingerprint, provenance, coverage, and explicit metric availability.

`lakehouse.silver.engagement_snapshots` is insert-only by `observation_id`.
The refresh DAG enforces this order:

```text
select due targets
  -> refresh source metrics
  -> validate output
  -> append historical snapshots
  -> merge current state
  -> compute velocity and analytics
```

Rates, deltas, labels, and audience features are calculated only when their
required inputs are available. A metric value of zero with `*_available=true`
is observed zero. A null value or `*_available=false` is unknown and must be
displayed as `N/A`, not zero.

### Transactional worker outbox

Each state-dependent worker writes its outcome state and Kafka publish intent
into the shared SQLite transaction. `youtube_worker_outbox` uses a
deterministic hash of topic and canonical event JSON. At startup, these workers
redrain undelivered rows.
An outbox row receives `delivered_at` only after the Kafka producer confirms
delivery; failures retain the row and schedule bounded exponential backoff.

This ordering prevents a worker from recording success while losing the
corresponding Kafka result. Back up the SQLite database at
`/app/state/youtube-pipeline.sqlite` before migration or rollback.

The metrics refresh is the deliberate exception: its targets are recoverable
from Silver, so it publishes idempotently and synchronously to
`youtube.engagement.snapshots` first. SQLite metrics, quota, and health rows are
then updated with a short timeout. If that update is locked, the task reports
`state_persisted=false` but keeps the Kafka-acknowledged result successful.

## Transcript lifecycle and compatibility

`transcript_lifecycle_status` is the canonical status. It is stored per video
and requested language, so one video's language choice cannot leak into the
next video.

| Lifecycle status | Class | Legacy `transcript_status` |
|---|---|---|
| `pending` | retryable | `pending` |
| `available` | terminal | `success` |
| `unavailable` | terminal, explicit functional response only | `not_available` |
| `disabled` | terminal, explicit functional response only | `disabled` |
| `rate_limited` | retryable with cooldown | `rate_limited` |
| `blocked` | retryable with cooldown/circuit breaker | `rate_limited` |
| `retryable_error` | retryable with cooldown | `failed` |
| `permanent_error` | terminal | `failed` |

A retryable state becomes `permanent_error` when the configured attempt limit
is exhausted. Terminal states have no `next_attempt_at`. Technical blocking is
not converted to `unavailable`; only an explicit source response may produce
`unavailable` or `disabled`.

The stored transcript record includes requested, obtained, and available
languages; manual/automatic generation; translation; provider; attempts;
last/next attempt; normalized error; recovery time; and deterministic
`content_version`. `transcript_status` remains additive compatibility data and
must not be used for new lifecycle decisions.

## Additive canonical contract

All new Avro fields are nullable with a `null` default. The Python envelope,
Avro schema, Spark contract, Bronze, Silver, analytics, dashboard, and ML
fixtures share these field families:

- identity: `event_id`, `observation_id`, `platform_event_id`;
- observation: `observed_at`, `collected_at`, producer and run identifiers;
- integrity: `payload_fingerprint`;
- provenance: `collection_method`, `api_endpoint`, `provenance_json`;
- coverage: `coverage_json` and explicit `*_available` booleans;
- transcript lifecycle and per-language attempt fields.

Existing fields and topics are retained. This is an additive migration, not a
breaking contract change.

## Monitoring and quota policy

The monitoring source is the workers' persistent SQLite state. The
`youtube_api_usage.py` materializer inserts immutable rows into:

| Table | Evidence |
|---|---|
| `lakehouse.monitoring.external_api_usage` | Provider/operation, requests, quota units and unit cost, daily budget, remaining and reserved units, cache hits/misses, retries, errors, latency, circuit and priority |
| `lakehouse.monitoring.pipeline_health` | Worker/outbox queue depth and age, success/error counts, circuit state, and Bronze/Silver boundary lag |
| `lakehouse.monitoring.data_quality_results` | One deterministic result per quality rule/run, including severity, outcome, observed value, threshold, and details |

`lakehouse.monitoring.youtube_api_usage` is retained as a compatibility
projection. Quota decisions use units, not only request counts. Recent
`videos.list` snapshots have a dedicated reserve. Under pressure, descriptive
metadata, channel, and comment work is deferred before recent snapshots.

Relevant settings are validated at startup and have non-sensitive examples in
`.env.example`, including:

- `YOUTUBE_DAILY_QUOTA_UNITS` and
  `YOUTUBE_RECENT_SNAPSHOT_RESERVE_UNITS`;
- `YOUTUBE_QUOTA_PRESSURE_RATIO` and
  `YOUTUBE_QUOTA_CRITICAL_RATIO`;
- per-worker batch, concurrency, attempt, cooldown, and budget values;
- `PIPELINE_QUEUE_WARNING_AGE_SECONDS`,
  `PIPELINE_BRONZE_LAG_WARNING_SECONDS`, and
  `PIPELINE_SILVER_LAG_WARNING_SECONDS`.

## Reproducible ML datasets

`lakehouse.gold.training_examples` is built from pinned Iceberg snapshots of
the official post-feature and engagement-snapshot tables. The dataset version
is a deterministic hash of source snapshot IDs, filters, label policy, and
schema version. Repeating a build with the same inputs produces the same
version.

`lakehouse.gold.dataset_manifests` records the dataset/schema version, period,
source tables and Iceberg snapshot IDs, filters, example count, missingness,
distributions, fingerprint, and creation time. The Airflow training DAG runs
the builder before training and accepts a precise `dataset_version`. CSV is an
export artifact only; it is not the official training input.

Build from current pinned source snapshots:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/build_training_dataset.py \
  --dataset-version auto \
  --label-horizon-hours 24 \
  --label-tolerance-hours 24 \
  --export-root /opt/spark/balancing/ml
```

Export an existing exact version by replacing `auto` with its manifest
version. The command fails if the manifest exists without matching examples.

## Configurable lakehouse quality

Rules live in `spark/jobs/config/lakehouse_quality_rules.json`. Supported
severities are `info`, `warning`, and `error`; supported checks cover schemas,
unique business keys, partitions, empty tables, completeness, inter-stage
volume ratios, freshness, and orphan files.

Orphan detection compares objects visible through MinIO/S3 with data files
referenced by Iceberg metadata. It reports and samples differences but never
deletes objects.

Run the standard profile:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/lakehouse_quality.py \
  --profile standard \
  --fail-on error
```

Use `--threshold-overrides` with inline JSON or a JSON file path for an
operational override. The `no_row_checks` profile permits an empty run but
persists an explicit warning; it does not disable the remaining checks. The
factory preserves both DAG IDs: `user_behavior_lakehouse` and
`user_behavior_lakehouse_no_row_checks`.

## Additive migration

### Pre-migration evidence

1. Pause both lakehouse DAGs, the engagement refresh DAG, and all YouTube
   workers. Wait for active Spark writes to finish.
2. Record the latest Iceberg snapshot ID and row count for every table that may
   be changed, especially Bronze/Silver current state, transcripts, and
   engagement snapshots.
3. Back up the `collector-state` volume, including
   `youtube-pipeline.sqlite`.
4. Record counts of missing/duplicate `event_id` and `observation_id` values.
5. Keep the Kafka topics and checkpoints; do not reset them as part of the
   migration.

### Dry run

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/migrate_pipeline_reliability.py --dry-run

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/migrate_engagement_snapshots.py --dry-run
```

The first report includes source row counts, table existence, transcript rows,
and available Bronze/Silver snapshot IDs. The second reports missing, invalid,
and duplicate observation identities.

### Apply

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/migrate_pipeline_reliability.py --apply

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/migrate_engagement_snapshots.py --apply
```

Both commands are idempotent. The reliability migration creates/evolves the
Bronze journal, ingress DLQ, Silver application proof, transcript lifecycle,
and quality-result tables. It inserts one deterministic synthetic journal row
for each historical row still visible in `lakehouse.bronze.events`, then marks
only matching existing Silver rows as applied.

History that was overwritten before the journal existed cannot be
reconstructed. The synthetic rows are evidence of the migration-time current
state, not recovered observations.

The snapshot migration adds provenance, coverage, availability, and
`observation_id` columns. With no duplicate effective IDs it updates in place.
If duplicates exist, it writes and validates a deduplicated staging table,
renames the old table to a timestamped backup, and switches the staging table
into place. Keep that backup until post-migration validation is complete.

Worker startup applies additive SQLite schema changes automatically. Opening a
worker after its database backup creates the outbox, quota-unit, cache,
circuit, and health columns/tables without deleting existing result JSON,
errors, attempts, or schedules.

### Post-migration validation

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/reconcile_bronze_silver.py --mode check

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/lakehouse_quality.py --profile standard --fail-on error
```

Confirm that journal and applied IDs are unique, missing and orphan-applied
counts are zero, snapshot IDs are unique, and dashboard cards distinguish null
from zero. Resume streams before analytics so new committed events are visible
to the reconciliation and materialization tasks.

## Reconciliation and replay

Check mode reports journal/applied counts, missing events, duplicates,
orphan-applied rows, oldest missing age, and missing counts by source. It exits
non-zero while the boundary is not clean.

Repair mode reads missing rows directly from the immutable journal and calls
the same Silver merge used by the stream:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/reconcile_bronze_silver.py \
  --mode repair \
  --repair-limit 100000
```

The two online DAGs schedule repair after Bronze and Silver stream execution
and before quality and analytical materializations. Replaying Kafka is also
safe: Bronze and Silver identities suppress duplicate state while preserving
distinct observations.

Never repair by copying `lakehouse.bronze.events`; that table is only the
current projection. `lakehouse.bronze.event_log` is the replay source.

## Rollback

1. Pause the two lakehouse DAGs, the engagement DAG, and all workers.
2. Redeploy the previous application revision without deleting Kafka topics,
   the new Bronze journal, or the ingress DLQ.
3. Restore any table mutated in place to the pre-migration Iceberg snapshot.
   If the engagement migration performed a staging switch, rename the current
   table aside and restore the timestamped backup table recorded by the
   migration output.
4. Restore the backed-up SQLite database/volume before starting the previous
   workers.
5. Validate current Bronze/Silver counts and dashboard reads before resuming
   schedules.

The additive journal and topics should normally be retained during rollback.
After the defect is corrected, a roll-forward can rerun the idempotent
migrations and replay `lakehouse.bronze.event_log` through reconciliation.

Do not delete Iceberg files or reported orphan files as a rollback shortcut.
Use recorded table snapshots and the explicit engagement backup instead.
