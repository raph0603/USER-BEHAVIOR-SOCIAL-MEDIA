# Troubleshooting metadata and captions

## Start with component status

Do not diagnose every missing value as the same failure. Inspect the event's
`collection_status`, `metadata_status`, `comments_status`,
`transcript_status`, and `storage_status` together with the component error
code.

| Symptom | Likely interpretation |
|---|---|
| `transcript_status=not_available` | The video has no usable caption track or access is intentionally unavailable |
| `transcript_status=rate_limited` | Retry later from an allowed network path |
| `transcript_status=failed` | Inspect the bounded error code and retry policy |
| `comments_status=partial` | Earlier pages succeeded but the configured bound or a later failure stopped collection |
| `storage_status=pending` | The raw event has not completed the Bronze MERGE |
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

The backfill job stores every outcome, including unavailable and failed
attempts. Successful, unavailable, and disabled outcomes are terminal.
Partial, rate-limited, and failed outcomes can be retried while their attempt
count remains below the configured maximum.

The job materializes caption data already present on Silver events before
making an external request. This avoids fetching the same track twice.

| Setting | Default | Purpose |
|---|---:|---|
| `YOUTUBE_TRANSCRIPT_LANGUAGES` | `en,vi` | Preferred language order |
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
analytics. The no-row-checks DAG is manual by default through
`LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES=0`.

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

The Bronze job publishes to `lakehouse.bronze.for_silver` only after the
Iceberg MERGE succeeds. A handoff failure causes the micro-batch to retry. It is
therefore normal to see a repeated idempotent Bronze MERGE before Silver
catches up.

Check the following in order:

1. Bronze Spark query is running without repeated checkpoint errors.
2. The Bronze MERGE completes for the affected batch.
3. Kafka accepts the post-commit batch.
4. The Silver consumer group is running.
5. The Silver MERGE key contains a stable `platform_event_id`.

## A sparse replay erased metadata

Nullable enrichment columns should be updated with `COALESCE`. If a previously
populated field becomes null, identify the MERGE statement for that table and
verify that nullable source values do not overwrite existing target values.

## Dashboard says captions are missing

The dashboard should display the stored status, not infer one generic state
from empty text. Refresh content analytics after the transcript table changes.
An unavailable track, a pending attempt, a rate limit, and a failed request
need different user-facing messages.
