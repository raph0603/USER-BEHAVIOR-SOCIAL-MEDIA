# Canonical event schema

The canonical contract is defined by `schemas/playwright_event.avsc` and the
Spark field registry in `spark/jobs/event_contract.py`. New optional fields
must be added to both definitions and propagated without changing their
meaning.

## Identity and relationships

| Field | Meaning |
|---|---|
| `event_id` | Deterministic identity of one immutable collected event |
| `observation_id` | Deterministic identity of one temporal metric observation, when applicable |
| `source` | `youtube`, `x`, or `reddit` |
| `platform_event_id` | Native source ID for the collected row |
| `content_id` | Stable source-scoped ID for this content or interaction |
| `parent_content_id` | Immediate parent, null for a root |
| `root_content_id` | Root post or video ID |
| `conversation_id` | Native thread or conversation ID |
| `content_type` | Root or interaction kind, such as video, post, comment, or reply |
| `relation_type` | Relationship to the parent or root |
| `depth` | Zero-based depth in the thread |
| `position_in_thread` | Source order when available |

`user_id` is the source identity before the privacy stream and a salted hash
after cleaning. Unknown identities use an event-scoped surrogate; they must not
all collapse to one shared anonymous identity.

## Text and time

| Field | Meaning |
|---|---|
| `title` | Content title, cleaned independently from body text |
| `raw_text` | Collected body or description before the privacy text transform |
| `clean_text` | Redacted and normalized body text |
| `text_for_model` | Model-oriented normalized text |
| `published_at` | Source publication time |
| `collected_at` | Time the collector observed the row |
| `updated_at` | Last event update time |
| `last_attempt_at` | Last enrichment attempt |
| `event_ts` | Iceberg timestamp derived from publication time, then legacy timestamp |
| `event_date` | Silver partition date derived from `event_ts` |

Source publication time and collection time answer different questions and
must never overwrite each other.

## Observation, provenance, and coverage

The contract carries enough information to distinguish the producer, the
collection run, and which values were actually observed:

| Field | Meaning |
|---|---|
| `observed_at` | Time at which a point-in-time metric was observed |
| `producer_name` | Stable name of the collector or worker |
| `producer_run_id` | Correlation ID for the producing run |
| `payload_fingerprint` | Deterministic digest of the protected source payload |
| `collection_method` | Collection mechanism, such as `youtube_data_api` or `playwright` |
| `api_endpoint` | External endpoint used when relevant |
| `provenance_json` | Extensible source and processing provenance |
| `coverage_json` | Extensible description of requested and observed fields |

These fields are additive, nullable Avro fields with `null` defaults. A null
value means that an older producer did not supply the fact; it must not be
interpreted as a negative observation.

## Independent outcome fields

The legacy component status vocabulary is:

| Status | Meaning | Terminal for that component |
|---|---|---|
| `pending` | Not attempted or awaiting a later stage | No |
| `success` | Requested data was collected | Yes |
| `partial` | Some requested data was collected | No |
| `not_available` | The source confirms the data does not exist or cannot be exposed | Yes |
| `disabled` | Collection is intentionally disabled by configuration | Yes |
| `rate_limited` | A retryable source or network restriction | No |
| `failed` | A retryable technical failure | No |

The event carries `collection_status` plus component fields:
`metadata_status`, `transcript_status`, `comments_status`, and
`storage_status`. Error details use stable machine-readable codes and bounded
messages. `attempt_count` and attempt timestamps make retry behavior auditable.

Transcript collection additionally carries `transcript_lifecycle_status`.
Unlike the retained `transcript_status`, this field distinguishes functional
absence from operational failures:

| Lifecycle status | Meaning | Terminal |
|---|---|---|
| `pending` | Awaiting an attempt | No |
| `available` | A requested transcript was collected | Yes |
| `unavailable` | The provider explicitly confirmed no usable track | Yes |
| `disabled` | Collection is intentionally disabled | Yes |
| `rate_limited` | Provider quota or throttling requires cooldown | No |
| `blocked` | Access is temporarily blocked | No |
| `retryable_error` | A technical error can be retried | No |
| `permanent_error` | The retry policy is exhausted or the error is permanent | Yes |

`transcript_status` remains populated through the legacy mapping for existing
consumers. Technical failures and blocks never become `unavailable`.

## Metadata envelopes

| Field | Contents |
|---|---|
| `canonical_metadata` | Cross-platform metadata with stable names and meanings |
| `source_specific_metadata` | Useful source-only fields that have no safe common mapping |
| `raw_source_payload` | Sanitized source response needed for reprocessing or diagnosis |

All three are JSON strings in Avro and Iceberg. Serialization is deterministic.
Secrets, browser state, authentication headers, and unnecessary personal
details are excluded.

## Engagement

Counters are nullable. Null means unknown or not exposed; zero means an
observed count of zero. Each snapshot also carries explicit availability flags
such as `view_count_available`, `like_count_available`, and
`comment_count_available`; derived rates, deltas, and labels are null unless
all required inputs are available.

Common fields include `like_count`, `view_count`, `comment_count`,
`reply_count`, `retweet_count`, `bookmark_count`, `score`,
`follower_count`, `subscriber_count`, and `subreddit_member_count`. Do not map
unrelated source metrics into a common counter merely to avoid nulls.

YouTube video rows also carry `thumbnail_url` when available. Historical rows
can be backfilled without YouTube Data API quota by deriving the public
`https://img.youtube.com/vi/<video_id>/default.jpg` URL from the existing video
id. Dashboards use it as a visual identifier only; the source URL remains the
durable video reference.

## YouTube event families

All YouTube Kafka families use the same backward-compatible Avro envelope, but
each event contains only one responsibility. Routing fields include
`event_type`, `event_version`, `video_id`, `channel_id`, `correlation_id`,
`collected_at` and `attempt_count`.

| Family | Payload responsibility |
|---|---|
| `youtube.discovery.events` | Search identity, query identity and publication time |
| `youtube.metadata.events` | Normalized and raw descriptive metadata from `yt-dlp` |
| `youtube.metadata.changes` | Hash transition and exact `changed_fields` |
| `youtube.engagement.snapshots` | One temporal view/like/comment observation |
| `youtube.transcript.*` | Transcript request or result and transcript lifecycle |
| `youtube.comment.*` | Text-comment request or incremental comment results |
| `youtube.channel.*` | Cached channel-level statistics such as subscribers |

Descriptive metadata uses `metadata_hash`, `previous_metadata_hash`,
`changed_fields`, `metadata_source`, `metadata_schema_version`,
`yt_dlp_version`, refresh timestamps/counts, and bounded error details.
Engagement scheduling uses `last_metrics_refresh_at`,
`next_metrics_refresh_at`, `metrics_refresh_count` and
`metrics_refresh_status`. These field groups remain nullable so retained Avro
records written by an older schema still decode.

## YouTube historical and monitoring tables

| Table | Grain and mutation policy |
|---|---|
| `lakehouse.bronze.event_log` | One immutable row per deterministic `event_id`; insert only |
| `lakehouse.bronze.events` | Latest compatible Bronze projection; idempotent MERGE |
| `lakehouse.bronze.ingress_dlq` | One protected invalid ingress record with Kafka coordinates and reason |
| `lakehouse.silver.events` | Latest canonical event state; idempotent MERGE |
| `lakehouse.silver.applied_events` | Durable proof that an `event_id` was applied to Silver; insert only |
| `lakehouse.silver.youtube_metadata_versions` | One changed canonical metadata hash per observation; append only |
| `lakehouse.silver.engagement_snapshots` | One row per deterministic `observation_id`; insert-only MERGE |
| `lakehouse.gold.youtube_engagement_velocity` | Latest velocity, acceleration and virality signal per video; MERGE |
| `lakehouse.gold.training_examples` | Versioned official ML examples built from lakehouse tables |
| `lakehouse.gold.dataset_manifests` | Deterministic dataset lineage, Iceberg snapshots, filters, coverage and fingerprint |
| `lakehouse.monitoring.external_api_usage` | Quota units, budget, reserve, cache, retry and circuit observations; append only |
| `lakehouse.monitoring.pipeline_health` | Queue depth/age and Bronze/Silver lag observations; append only |
| `lakehouse.monitoring.data_quality_results` | Rule, severity, profile, evidence and outcome; append only |

`lakehouse.monitoring.youtube_api_usage` is retained as a legacy compatibility
table. New monitoring consumers use `external_api_usage`.

Metadata history keeps `valid_from`, the previous hash, exact changed fields,
descriptive values and JSON for chapters, thumbnails and caption maps.
Engagement history keeps nullable deltas, hourly rates, engagement rates and
view acceleration. A missing or decreasing counter produces a null derived
metric instead of a fabricated negative rate.

## Caption fields

Caption collection preserves both content and provenance:

- `transcript_text` and `transcript_segments_json`;
- requested, obtained, and available language codes;
- whether the selected track is manual, generated, or translated;
- provider, selection source, and selection strategy;
- segment count, available languages, and covered duration;
- attempt count, last and next attempt times, and recovery time;
- collection time, lifecycle and legacy statuses, normalized error code, and
  bounded error message;
- deterministic `content_version`.

The Silver transcript table also stores unsuccessful outcomes so dashboards
can distinguish unavailable captions from a technical failure and retry jobs
can apply explicit policies.

## Compatibility rules

1. Additive Avro fields are nullable and default to `null`.
2. Retained Kafka records are decoded with their writer schema ID.
3. Spark unions schema versions by name with missing columns allowed.
4. Iceberg tables add missing columns before a MERGE.
5. MERGE updates use `COALESCE` for nullable enrichments so a sparse replay
   does not erase a previously collected value.
6. Timestamp-derived columns are recomputed together when publication time
   changes.

See [Pipeline reliability operations](PIPELINE_RELIABILITY.md) for migration,
reconciliation, replay, and rollback procedures.
