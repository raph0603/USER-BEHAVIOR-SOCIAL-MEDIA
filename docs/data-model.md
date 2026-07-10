# Lakehouse data model

## Layer semantics

```text
raw Kafka topics
  -> privacy cleaning, validation, and source DLQs
  -> clean Kafka topics
  -> lakehouse.bronze.events
  -> post-commit Kafka handoff
  -> lakehouse.silver.events
  -> analytical Silver and Gold tables
```

Bronze is the first durable Iceberg layer. Because privacy cleaning occurs
before Bronze, it contains canonical, redacted events plus a sanitized source
payload. It does not contain untouched source responses or unhashed source
identities.

Silver keeps the canonical event contract, native timestamps and partitions,
and merge-safe enrichment values. Gold contains aggregates, predictions, and
training examples rather than another copy of source events.

## Event text

| Field | Use |
|---|---|
| `title` | Cleaned title for root content |
| `raw_text` | Collected body before privacy text cleaning |
| `clean_text` | Redacted, normalized body |
| `text_for_model` | Text normalized for feature extraction |

Title and body are cleaned separately. A YouTube transcript must not overwrite
the video title or description.

## Tables

| Table | Purpose | Primary writer | Main readers |
|---|---|---|---|
| `lakehouse.bronze.events` | Durable privacy-cleaned canonical events | `kafka_to_iceberg_bronze.py` | Silver stream, maintenance |
| `lakehouse.silver.events` | Canonical monitoring and downstream events | `bronze_to_silver_from_kafka.py` | Dashboard, analytics, exports |
| `lakehouse.silver.contents` | Root posts and videos | `content_analytics.py` | Dashboard, content aggregates |
| `lakehouse.silver.interactions` | Actual comments and replies | `content_analytics.py` | Dashboard, content aggregates |
| `lakehouse.silver.engagement_snapshots` | Append-only engagement observations | Analytics and refresh jobs | Delayed labels, dashboard |
| `lakehouse.silver.transcripts` | Caption content, provenance, status, and attempts | Collector materialization and `youtube_transcripts.py` | Dashboard, content analytics |
| `lakehouse.silver.post_features` | Model-oriented text and structural features | `silver_post_features.py` | Classifier training and inference |
| `lakehouse.silver.context_features` | Time-stamped retrieval context | Context feature job | Classifier |
| `lakehouse.silver.balanced_events` | Reproducible analysis sample | `build_balanced_dataset.py` | Dashboard and analysis |
| `lakehouse.gold.content_stats` | Content engagement and interaction aggregates | `content_analytics.py` | Dashboard and reporting |
| `lakehouse.gold.user_evolution` | Daily anonymized user activity | `content_analytics.py` | Dashboard and reporting |
| `lakehouse.gold.model_predictions` | Versioned classifier outputs | Inference service | Evaluation and reporting |
| `lakehouse.gold.training_examples` | Versioned labeled examples | Dataset build job | Model training |

## Content graph

A root post or video has `depth=0`, no parent, and its own
`root_content_id`. A top-level comment points to that root. A nested reply
points to its immediate parent while preserving the same root. Content
statistics group by the root ID; they do not treat every root X post or
YouTube video as an interaction.

## Engagement and labels

Engagement changes after publication. Append-only snapshots preserve
`observed_at` and age so labels can be derived at a defined horizon such as
T+1h or T+24h.

Unknown engagement remains null. Training preparation must carry a coverage
indicator or exclude rows that lack the required label inputs. Filling every
missing counter with zero creates false negative labels.

## Dashboard scope

The Streamlit dashboard is both an operational and analytical interface. It
reads `silver.events` for pipeline monitoring and also reads content,
interaction, transcript, snapshot, balanced, and Gold aggregate tables when
they exist. Missing derived tables are handled as unavailable views rather
than as evidence that the base event stream is empty.

For field-level definitions and compatibility rules, see
[DATA_SCHEMA.md](DATA_SCHEMA.md). For sequencing and delivery guarantees, see
[ARCHITECTURE.md](ARCHITECTURE.md).
