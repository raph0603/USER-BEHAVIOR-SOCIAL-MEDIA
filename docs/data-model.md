# Data Model — Lakehouse Architecture

This document describes the layered data model used in the
`USER-BEHAVIOR-SOCIAL-MEDIA` lakehouse and explains how each table
relates to the BERT-based classification pipeline.

---

## Table of contents

1. [Overview](#overview)
2. [Layer definitions](#layer-definitions)
3. [Text field naming strategy](#text-field-naming-strategy)
4. [Monitoring tables vs model-ready tables](#monitoring-tables-vs-model-ready-tables)
5. [Engagement snapshots and delayed labels](#engagement-snapshots-and-delayed-labels)
6. [Context features for retrieval-enhanced classification](#context-features-for-retrieval-enhanced-classification)
7. [Gold layer — predictions and training examples](#gold-layer--predictions-and-training-examples)
8. [Future classification service integration](#future-classification-service-integration)
9. [Periodic retraining data flow](#periodic-retraining-data-flow)
10. [Dashboard monitoring scope](#dashboard-monitoring-scope)
11. [Table reference](#table-reference)

---

## Overview

The pipeline ingests social media events from YouTube, X (Twitter), and
Reddit, then processes them through a Bronze → Silver → Gold lakehouse.
Each layer has a specific purpose:

```text
Kafka raw topics
  → source-specific cleaning and validation (Spark streaming)
  → clean Kafka topics
  → lakehouse.bronze.events            (raw ingestion)
  → lakehouse.silver.events            (monitoring-friendly cleaned events)
  → lakehouse.silver.post_features     (model-input feature layer)
  → lakehouse.silver.engagement_snapshots  (delayed engagement observations)
  → lakehouse.silver.context_features  (retrieval-enhanced context signals)
  → lakehouse.gold.model_predictions   (classifier outputs)
  → lakehouse.gold.training_examples   (labeled examples for retraining)
```

---

## Layer definitions

### Bronze — `lakehouse.bronze.events`

- Contains raw events exactly as received from Kafka.
- Minimal transformation: Avro deserialization, timestamp parsing.
- Retains all original fields including potential PII placeholders.
- Used for reprocessing and debugging.

### Silver — cleaned and model-ready

The Silver layer is split into purpose-specific tables:

| Table | Purpose |
|-------|---------|
| `silver.events` | Monitoring-friendly cleaned event stream |
| `silver.post_features` | Model-input feature layer |
| `silver.engagement_snapshots` | Append-only engagement observations |
| `silver.context_features` | Retrieval-enhanced context signals |

### Gold — outputs

| Table | Purpose |
|-------|---------|
| `gold.model_predictions` | Classifier outputs from the inference service |
| `gold.training_examples` | Labeled examples for periodic retraining |

---

## Text field naming strategy

Each event passes through three text states:

| Field | Description | Where set |
|-------|-------------|-----------|
| `raw_text` | Original text exactly as collected — no cleaning | Collector / Kafka producer |
| `clean_text` | HTML stripped, URLs removed, PII scrubbed, whitespace normalized | `cleaning.clean_text()` in Spark |
| `text_for_model` | Lowercased `clean_text`, ready for tokenization | `cleaning.prepare_text_for_model()` in Spark |

The legacy `title` field is preserved for backward compatibility in
`silver.events` but downstream model jobs use `text_for_model`.

---

## Monitoring tables vs model-ready tables

### `silver.events` — monitoring table

- Updated by the MERGE-based streaming job (`bronze_to_silver.py`).
- Contains engagement metrics (`like_count`, `view_count`, …) refreshed by `apply_insight_updates.py`.
- Used by the Streamlit monitoring dashboard.
- **Not** used directly for model training or inference.

### `silver.post_features` — model-input table

- Built by `silver_post_features.py` from `silver.events`.
- Contains derived text features: character length, word count, hashtag count, mention count, URL count, emoji count, question marker.
- Does **not** contain monitoring-only columns (`error`, Kafka offsets, etc.).
- Used as the primary input layer for BERT-based classification.

---

## Engagement snapshots and delayed labels

Social media engagement evolves over time.  A post with 10 likes at
T+1h may reach 10 000 likes at T+24h.  To build reliable classification
labels, the pipeline records engagement observations at multiple time
horizons.

### `silver.engagement_snapshots`

- **Append-only** — rows are never updated after insertion.
- Each refresh run produces a new snapshot row for each observed post.
- Key fields: `observed_at`, `age_minutes`, and all engagement counters.
- `age_minutes` = `(observed_at - created_at) / 60`.

### Building labels

Labels for training examples are built by querying snapshots at a
specific horizon:

```sql
SELECT platform_event_id, source, like_count, view_count
FROM lakehouse.silver.engagement_snapshots
WHERE age_minutes BETWEEN 55 AND 65   -- T+1h window
```

Supported horizons: `T+1h`, `T+6h`, `T+24h`, `T+72h`, `T+7d`.

Backward compatibility: `silver.events.metadata_refreshed_at` is still
updated by `apply_insight_updates.py` so the monitoring dashboard
continues to show the last refresh time.

---

## Context features for retrieval-enhanced classification

The classification service will optionally enrich predictions with fresh
context signals retrieved from a **remote vector-similarity retrieval
service**.  This is retrieval-enhanced classification — not a generative
or LLM-response flow.

### `silver.context_features`

Schema defined in `spark/jobs/pipeline/context_features.py`.

| Field | Description |
|-------|-------------|
| `source` | Origin platform |
| `platform_event_id` | Stable post identifier |
| `retrieved_at` | When context was retrieved |
| `top_similarity` | Cosine similarity to the most similar recent post |
| `avg_similarity_top10` | Average similarity over top-10 retrieved posts |
| `recent_posts_1h` | Post count on the same topic in the last hour |
| `trend_growth_1h` | Relative posting-rate growth over the last hour |
| `trend_growth_24h` | Relative posting-rate growth over the last 24 hours |
| `topic_freshness_hours` | Hours since the topic was first seen in the corpus |
| `matched_topics` | Topic labels from the retrieval service |

Context features are written **before inference** and joined to
`post_features` at prediction time using `platform_event_id`.

The `ContextFeatureRow` Python dataclass in `context_features.py` is the
stable contract between the retrieval service and the lakehouse.

---

## Gold layer — predictions and training examples

### `gold.model_predictions`

Stores classifier outputs written by the inference service.

| Field | Description |
|-------|-------------|
| `predicted_class` | e.g. `viral`, `not_viral` |
| `confidence` | Softmax probability [0, 1] |
| `virality_score` | Continuous engagement score |
| `context_used` | Whether retrieval context was available |
| `model_version` | Semantic version of the deployed model |

### `gold.training_examples`

Stores labeled examples for periodic retraining.

| Field | Description |
|-------|-------------|
| `text_for_model` | Cleaned, lowercased text |
| `feature_version` | Version of `silver.post_features` job |
| `label_horizon` | Time horizon used: `T+1h`, `T+6h`, `T+24h`, … |
| `label_value` | Ground-truth label |
| `dataset_version` | Build version for reproducibility |
| `context_feature_snapshot` | JSON snapshot of context features if used |

---

## Future classification service integration

When the BERT-based classification service is deployed:

1. It reads `silver.post_features` for text and derived features.
2. It optionally queries the remote retrieval service for context signals.
3. It writes context signals to `silver.context_features`.
4. It writes prediction outputs to `gold.model_predictions`.

The service consumes the `ModelPredictionRow` and `ContextFeatureRow`
dataclasses defined in `spark/jobs/pipeline/gold_schemas.py` and
`spark/jobs/pipeline/context_features.py` respectively.

---

## Periodic retraining data flow

```text
silver.engagement_snapshots  (engagement at T+1h, T+6h, T+24h …)
  + silver.post_features      (text and derived features)
  + silver.context_features   (optional retrieval context at label time)
  → label derivation job
  → gold.training_examples
  → BERT fine-tuning job
  → new model_version deployed
  → gold.model_predictions updated
```

The dataset-build job queries `engagement_snapshots` at the target
horizon, joins to `post_features` on `platform_event_id`, and writes
one `TrainingExampleRow` per labeled post.

---

## Dashboard monitoring scope

The Streamlit dashboard (`dashboard/`) is a **pipeline health monitoring
tool**, not a final product interface.  It reads from:

- `silver.events` — event counts, source distribution, text field
  availability, error rates.
- `silver.events.metadata_refreshed_at` — last engagement refresh time
  per source.

The dashboard does **not** read from `post_features`, `gold.*`, or
`engagement_snapshots`.  Those tables are for the classification pipeline
and retraining workflows.

---

## Table reference

| Table | Layer | Purpose | Written by | Read by |
|-------|-------|---------|-----------|---------|
| `bronze.events` | Bronze | Raw ingestion | `kafka_to_iceberg_bronze.py` | `bronze_to_silver.py` |
| `silver.events` | Silver | Pipeline monitoring | `bronze_to_silver.py`, `apply_insight_updates.py` | Dashboard, `silver_post_features.py` |
| `silver.post_features` | Silver | Model input | `silver_post_features.py` | Classifier service |
| `silver.engagement_snapshots` | Silver | Delayed engagement | `engagement_snapshots.py` | Dataset-build job |
| `silver.context_features` | Silver | Retrieval context | Retrieval service | Classifier service |
| `gold.model_predictions` | Gold | Classifier outputs | Classifier service | Monitoring, evaluation |
| `gold.training_examples` | Gold | Retraining dataset | Dataset-build job | BERT fine-tuning job |
