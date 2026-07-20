# Lakehouse architecture

The operational migration, replay, and rollback procedures are documented in
[Pipeline reliability and operations](PIPELINE_RELIABILITY.md).

## End-to-end flow

```text
YouTube Data API / public caption service
X browser collection
Reddit public pages and JSON endpoints
        |
        v
source collectors
        |
        | Avro with Schema Registry framing
        v
*.raw.events
        |
        v
source-specific Spark privacy and validation streams
        |
        +---------------------------> *.dlq.events
        |
        | JSON, stage=clean
        v
*.clean.events
        |
        v
validate and split malformed input
        |
        +---------------------------> lakehouse.bronze.ingress_dlq
        |
        v
lakehouse.bronze.event_log (immutable, insert only)
        |
        v
lakehouse.bronze.events (compatible current projection)
        |
        | reread and publish only committed journal rows
        v
lakehouse.bronze.for_silver
        |
        v
lakehouse.silver.events (idempotent current MERGE)
        |
        v
lakehouse.silver.applied_events (insert-only application proof)
        |
        v
Bronze/Silver reconciliation and configurable quality
        |
        +--> content analytics and transcript materialization
        +--> engagement snapshots and versioned ML datasets
        +--> dashboard
```

## Adaptive YouTube pipeline

Scheduled YouTube collection is split into independent, bounded workers:

```text
search.list (discovery only)
  -> youtube.discovery.events
  -> yt-dlp metadata worker (download disabled)
  -> youtube.metadata.events
       |-> youtube.transcript.requests -> youtube.transcript.results
       |-> youtube.comment.requests    -> youtube.comment.results
       `-> youtube.channel.requests    -> youtube.channel.results
  -> privacy stream -> Bronze -> Silver current state
  -> lakehouse.silver.youtube_metadata_versions (changed hashes only)

due Silver videos -> videos.list statistics, up to 50 IDs per request
  -> youtube.engagement.snapshots
  -> lakehouse.silver.engagement_snapshots (append only)
  -> lakehouse.gold.youtube_engagement_velocity (current signals)
```

`search.list` uses one page of up to 50 results per structured query in
continuous mode. A SQLite watermark stores the last successful search and the
latest publication time per query; `publishedAfter` includes a configurable
overlap. Multi-page traversal is available only in explicit backfill mode.

The metadata worker stores the raw `yt-dlp` response in the mounted raw JSON
area and publishes both raw and normalized JSON in the Avro envelope. It uses
the persistent metadata state to schedule the default 6 h, 24 h, 3 d, 7 d and
optional 30 d checks. Canonical normalization makes unordered tags,
categories, thumbnails and caption maps stable before hashing. An unchanged
hash updates the current refresh state but creates no history row.

Transcript, comment-text and channel-statistic work has its own Kafka consumer
group and persistent state. Transcript success is terminal. Comment pagination
stops at a known comment ID and remains bounded. Channel statistics are cached
by `channel_id` and fetched with `channels.list`, never by reopening every
video page. A failure in these secondary workers does not prevent the separate
engagement DAG from refreshing metrics.

All independent workers use the same transactional SQLite outbox pattern.
Outcome state and the Kafka publish intent commit together; the intent is
marked delivered only after producer acknowledgement and is redrained on the
next worker start.

The `refresh_recent_engagement_insights` DAG runs every 30 minutes by default,
but selects only due rows according to `next_metrics_refresh_at`. After output
validation it appends retry-safe observations and independently merges the
latest values into the current event table. The append key is a stable hash of
`source`, `platform_event_id` and `observed_at`. Velocity and virality are
materialized only after the append succeeds.

API calls are recorded first in persistent collector state and then appended
to `lakehouse.monitoring.external_api_usage`; the legacy
`lakehouse.monitoring.youtube_api_usage` projection remains compatible.
`lakehouse.monitoring.pipeline_health` carries queue age/depth, circuit state,
and Bronze/Silver lag. Unit-based budgets and a recent-snapshot reserve
prioritize video statistics and stop secondary discovery, descriptive
metadata, channel, or comment work when quota pressure rises. The design never
rotates keys, accounts or network addresses to evade source limits.

## Collector boundary

Collectors translate source responses into one canonical event contract.
Source-specific details remain available in JSON envelopes, while cross-source
consumers use stable identity, relationship, timestamp, status, and engagement
fields.

Each enrichment has its own status. A video can therefore have successful
metadata, unavailable captions, partial comments, and pending storage without
collapsing those facts into one error flag. Incomplete retriable work remains
eligible for a later run.

## Schema-aware privacy streams

Raw Kafka values use Confluent framing: a magic byte followed by the writer
schema ID and Avro data. The privacy stream reads the schema ID, retrieves
registered subject versions, decodes each record with its writer schema, and
unions the results with missing columns allowed. This matters when retained
Kafka records were written before a nullable field was added.
An unregistered writer schema is routed to the source DLQ with its Kafka
coordinates instead of being decoded with the wrong schema or dropped.

The source-specific streams then:

1. normalize text and timestamps;
2. redact PII from free text;
3. replace source identities with a salted SHA-256 value;
4. validate required routing fields;
5. publish valid canonical JSON to the clean topic;
6. publish invalid records and reasons to the source DLQ.

Bronze is consequently the first durable lakehouse table, but it already
contains privacy-cleaned events. It is not an untouched copy of the source
response. The sanitized source response is retained only in the explicit JSON
payload field.

## Bronze-to-Silver ordering

`kafka_to_iceberg_bronze.py` uses one `foreachBatch` callback:

1. validate the micro-batch and persist protected invalid-row evidence in
   `lakehouse.bronze.ingress_dlq`;
2. insert valid, deduplicated events into the immutable
   `lakehouse.bronze.event_log` by deterministic `event_id`;
3. update the compatible `lakehouse.bronze.events` current projection;
4. reread the batch's committed journal rows;
5. publish only those rows to `lakehouse.bronze.for_silver`.

There is no independent handoff stream racing the Iceberg write. If Kafka
publication fails after the journal commit, Spark retries the batch. The
journal insert and Bronze/Silver current merges are idempotent. Silver records
each successfully applied `event_id` in
`lakehouse.silver.applied_events` after its current-state merge. A crash before
that proof is inserted causes a safe replay through the same merge.

`reconcile_bronze_silver.py` compares the journal with the application proof.
Check mode reports missing, duplicate, orphan-applied, and oldest-missing
evidence. Repair mode replays missing journal rows directly through the shared
Silver merge. Both online DAGs run reconciliation before quality and analytical
materializations.

## Silver and derived data

`lakehouse.silver.events` is the canonical monitoring and downstream event
table. Batch jobs derive:

- content and interaction entities;
- append-only engagement snapshots;
- caption text, selection details, and collection outcomes;
- balanced analysis datasets;
- post and context features;
- content and user aggregates in Gold.

The dashboard reads both the event table and derived analytical tables.
Official model training uses a precise version from
`lakehouse.gold.training_examples`, paired with
`lakehouse.gold.dataset_manifests`. CSV is only an export artifact. Both ML and
dashboard consumers preserve missing engagement as unknown rather than
silently turning it into observed zero.

## Operational invariants

- `platform_event_id` is the preferred source identity for idempotent merges.
- `event_id` is the immutable journal and Silver application identity.
- `observation_id` is the insert-only engagement-snapshot identity.
- `published_at` is source time; `collected_at` is observation time.
- `event_ts` is derived from `published_at`, with the legacy timestamp as a
  fallback.
- Nullable schema additions always have an Avro default of `null`.
- A root post or video is content; only actual comments and replies are
  interactions.
- Missing, unavailable, and failed enrichments are distinct states.
- Scheduled row checks require `collected_at` at or after the current DAG run
  start, so historical rows cannot make an empty run appear healthy.
- Payload JSON must not contain credentials, authentication state, or
  unnecessary personal data.
- Iceberg on MinIO is the source of truth. Elasticsearch is not in the critical
  path and requires no integration.
