# Canonical event schema

The canonical contract is defined by `schemas/playwright_event.avsc` and the
Spark field registry in `spark/jobs/event_contract.py`. New optional fields
must be added to both definitions and propagated without changing their
meaning.

## Identity and relationships

| Field | Meaning |
|---|---|
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

## Independent outcome fields

The allowed status values are:

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
observed count of zero.

Common fields include `like_count`, `view_count`, `comment_count`,
`reply_count`, `retweet_count`, `bookmark_count`, `score`,
`follower_count`, `subscriber_count`, and `subreddit_member_count`. Do not map
unrelated source metrics into a common counter merely to avoid nulls.

## Caption fields

Caption collection preserves both content and provenance:

- `transcript_text` and `transcript_segments_json`;
- language name and code;
- whether the selected track is generated or translated;
- selection source and strategy;
- segment count, available languages, and covered duration;
- collection time, status, error code, and error message.

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
