# Troubleshooting metadata and captions

## Start with component status

Do not diagnose every missing value as the same failure. Inspect the event's
`collection_status`, `metadata_status`, `comments_status`,
`transcript_lifecycle_status`, retained `transcript_status`, and
`storage_status` together with the component error code.

| Symptom | Likely interpretation |
|---|---|
| `transcript_lifecycle_status=unavailable` | The provider explicitly confirms that no usable caption track exists |
| `transcript_lifecycle_status=disabled` | Caption collection is intentionally disabled |
| `transcript_lifecycle_status=rate_limited` | Retry after the persisted cooldown |
| `transcript_lifecycle_status=blocked` | Access is temporarily blocked and remains retryable |
| `transcript_lifecycle_status=retryable_error` | Inspect the normalized error and next-attempt time |
| `transcript_lifecycle_status=permanent_error` | The error is permanent or the attempt ceiling is exhausted |
| `comments_status=partial` | Earlier pages succeeded but the configured bound or a later failure stopped collection |
| `storage_status=pending` | The raw event has not completed its Bronze commit |
| Bronze row exists but Silver lags | Inspect the post-commit Kafka handoff and Silver consumer |

## Collector logs

```bash
docker compose logs --tail=200 youtube-collector
docker compose logs --tail=200 x-collector
docker compose logs --tail=200 reddit-collector
```

For one-off collection, run the corresponding `docker compose run --rm` command
in the foreground so its exit code and final summary remain visible.

## Caption retry behavior

The backfill job stores every outcome. `available`, `unavailable`, `disabled`,
and `permanent_error` are terminal. `pending`, `rate_limited`, `blocked`, and
`retryable_error` remain retryable while their persisted cooldown and attempt
ceiling allow another request. `unavailable` and `disabled` are emitted only
for explicit functional outcomes; network, provider, parsing, and access
failures remain operational errors.

The job materializes caption data already present on Silver events before
making an external request. This avoids fetching the same track twice.

Transcript language selection is evaluated independently for every video and
requested language. The stored result records requested, obtained, and
available languages, so one video's selection cannot leak into the next one.

| Setting | Default | Purpose |
|---|---:|---|
| `YOUTUBE_TRANSCRIPT_BACKFILL_LIMIT` | `500` | Maximum candidates per run |
| `YOUTUBE_TRANSCRIPT_BACKFILL_SLEEP_SECONDS` | `0.25` | Delay after every external attempt |
| `YOUTUBE_TRANSCRIPT_BACKFILL_MAX_ATTEMPTS` | `5` | Retry ceiling per content ID |
| `YOUTUBE_TRANSCRIPT_BACKFILL_RETRY_COOLDOWN_SECONDS` | `3600` | Minimum delay before retrying |
| `YOUTUBE_TRANSCRIPT_BACKFILL_STOP_ON_RATE_LIMIT` | `true` | Stop requesting after a rate limit |
| `YOUTUBE_TRANSCRIPT_BACKFILL_FAIL_ON_RETRYABLE` | `false` | Persist retriable outcomes without blocking downstream jobs |

Run a manual backfill with:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/batch/youtube_transcripts.py
```

Both online DAGs run this materialization/backfill step before content
analytics. The no-row-checks DAG is scheduled by default; set
`LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES=0` to keep it manual.

## Avro decode failures

If a cleaner reports an unknown schema ID:

1. verify that Schema Registry is healthy;
2. verify that the topic's configured subject matches the producer subject;
3. confirm that every retained writer schema is still registered;
4. inspect the source DLQ for topic, partition, offset, and decode reason.

Do not replace writer-schema decoding with the latest local schema. Old Kafka
records may have a different Avro field order.

If Spark reports that the number of sources in a cleaning checkpoint differs
from the current query, inspect the logical plan for duplicated streaming
branches. Writer-schema selection must remain a conditional projection over a
single Kafka source. Do not delete or version-bump the checkpoint before this
topology check, because a new checkpoint can replay retained Kafka records.
If the checkpoint offsets belong to a deleted or recreated Kafka topic and are
beyond the current partition offsets, increment the checkpoint version after
fixing the topology. Start the replacement checkpoint from retained Kafka
data; downstream MERGE stages deduplicate the replay by canonical identifier.

## Bronze-to-Silver lag

```bash
docker compose ps
docker compose logs --tail=200 spark-master spark-worker
```

The Bronze job first inserts valid events into immutable
`lakehouse.bronze.event_log`, then updates the compatible
`lakehouse.bronze.events` projection. It rereads the committed journal rows
before publishing them to `lakehouse.bronze.for_silver`. Silver applies its
idempotent current-state MERGE before inserting the corresponding `event_id`
into `lakehouse.silver.applied_events`. A failure at either boundary therefore
causes a safe replay instead of silent loss.

Check the following in order:

1. Bronze Spark query is running without repeated checkpoint errors.
2. The affected `event_id` exists in `lakehouse.bronze.event_log`.
3. Kafka accepts the post-commit handoff batch.
4. The Silver consumer group is running and the current-state MERGE completes.
5. The `event_id` exists in `lakehouse.silver.applied_events`.

Quantify the gap without mutating data:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/reconcile_bronze_silver.py --mode check
```

After reviewing the missing, duplicate, and oldest-unapplied counts, replay
only the journal entries that lack an application proof:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/reconcile_bronze_silver.py --mode repair
```

See [Pipeline reliability operations](PIPELINE_RELIABILITY.md) before repair or
rollback.

## A sparse replay erased metadata

Nullable enrichment columns should be updated with `COALESCE`. If a previously
populated field becomes null, identify the MERGE statement for that table and
verify that nullable source values do not overwrite existing target values.

## Dashboard says captions are missing

The dashboard should display the stored status, not infer one generic state
from empty text. Refresh content analytics after the transcript table changes.
An unavailable track, a pending attempt, a rate limit, and a failed request
need different user-facing messages.
