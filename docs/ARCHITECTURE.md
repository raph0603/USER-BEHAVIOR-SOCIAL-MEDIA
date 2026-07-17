# Lakehouse architecture

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
Iceberg Bronze MERGE
        |
        | publish only after the MERGE succeeds
        v
lakehouse.bronze.for_silver
        |
        v
Iceberg Silver MERGE
        |
        +--> content analytics and transcript materialization
        +--> engagement snapshots and balanced datasets
        +--> dashboard and model-data exports
```

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

1. deduplicate the micro-batch;
2. MERGE it into `lakehouse.bronze.events`;
3. after the MERGE returns successfully, publish that batch to
   `lakehouse.bronze.for_silver`.

There is no independent handoff stream racing the Iceberg write. If Kafka
publication fails after the Bronze commit, Spark retries the batch. The Bronze
MERGE is idempotent and the Silver MERGE tolerates a repeated handoff, giving
at-least-once delivery without exposing uncommitted Bronze rows.

## Silver and derived data

`lakehouse.silver.events` is the canonical monitoring and downstream event
table. Batch jobs derive:

- content and interaction entities;
- append-only engagement snapshots;
- caption text, selection details, and collection outcomes;
- balanced analysis datasets;
- post and context features;
- content and user aggregates in Gold.

The dashboard reads both the event table and derived analytical tables. Model
training must use a versioned export from Silver or a documented annotation
dataset; it must preserve missing engagement as unknown rather than silently
turning it into observed zero.

## Operational invariants

- `platform_event_id` is the preferred source identity for idempotent merges.
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
