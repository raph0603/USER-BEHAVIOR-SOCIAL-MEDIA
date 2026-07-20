"""
Gold layer schemas and contracts for the classification pipeline.

Defines:
- ``lakehouse.gold.model_predictions``  — classifier outputs
- ``lakehouse.gold.training_examples``  — labeled examples for retraining
- ``lakehouse.gold.dataset_manifests``  — immutable dataset lineage manifests

Design intent
-------------
These tables sit at the Gold layer of the lakehouse.  They are intentionally
separated from the Silver monitoring tables (``silver.events``) and the
model-input tables (``silver.post_features``) so that model serving,
retraining, and pipeline monitoring remain independent.

The classifier is BERT-based.  Context enrichment comes from a remote
retrieval service (not a generative flow).  These schemas are stable
enough for initial service integration while remaining extensible.

Schema versioning
-----------------
All tables expose a ``schema_version`` field.  Increment
``PREDICTIONS_SCHEMA_VERSION`` / ``TRAINING_EXAMPLES_SCHEMA_VERSION``
when adding or removing columns to keep downstream consumers in sync.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional


# ---------------------------------------------------------------------------
# Schema versions
# ---------------------------------------------------------------------------

PREDICTIONS_SCHEMA_VERSION = "v1"
TRAINING_EXAMPLES_SCHEMA_VERSION = "v2"
DATASET_MANIFESTS_SCHEMA_VERSION = "v1"


# ---------------------------------------------------------------------------
# model_predictions DDL
# ---------------------------------------------------------------------------

MODEL_PREDICTIONS_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.gold.model_predictions (
  source                STRING    COMMENT 'Origin platform: youtube, x, reddit, playwright',
  platform_event_id     STRING    COMMENT 'Platform-native stable identifier',
  prediction_ts         TIMESTAMP COMMENT 'UTC timestamp when the prediction was produced',
  model_version         STRING    COMMENT 'Semantic version of the deployed classifier (e.g. 1.2.0)',
  model_type            STRING    COMMENT 'Model architecture identifier (e.g. bert-base-uncased)',
  predicted_class       STRING    COMMENT 'Predicted label (e.g. viral, not_viral)',
  confidence            DOUBLE    COMMENT 'Softmax confidence for the predicted class [0, 1]',
  virality_score        DOUBLE    COMMENT 'Continuous virality / engagement score if applicable',
  context_used          BOOLEAN   COMMENT 'True if retrieval context features were used for this prediction',
  schema_version        STRING    COMMENT 'Schema version for consumer compatibility',
  prediction_date       DATE      COMMENT 'Partition column derived from prediction_ts'
)
USING iceberg
PARTITIONED BY (prediction_date)
"""

MODEL_PREDICTIONS_COLUMNS = [
    "source",
    "platform_event_id",
    "prediction_ts",
    "model_version",
    "model_type",
    "predicted_class",
    "confidence",
    "virality_score",
    "context_used",
    "schema_version",
    "prediction_date",
]


# ---------------------------------------------------------------------------
# training_examples DDL
# ---------------------------------------------------------------------------

TRAINING_EXAMPLES_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.gold.training_examples (
  example_id                 STRING    COMMENT 'Deterministic identity within a dataset version',
  source                     STRING    COMMENT 'Origin platform',
  platform_event_id          STRING    COMMENT 'Platform-native stable identifier',
  observation_id             STRING    COMMENT 'Immutable engagement observation used for the label',
  event_ts                   TIMESTAMP COMMENT 'Original content timestamp',
  label_observed_at          TIMESTAMP COMMENT 'Observation timestamp used to derive the label',
  author_hash                STRING    COMMENT 'Privacy-safe author identity for grouped splitting',
  text_for_model             STRING    COMMENT 'Lowercased cleaned text as used during feature extraction',
  feature_version            STRING    COMMENT 'Version of the post_features job that produced the features',
  label_horizon              STRING    COMMENT 'Time horizon used to derive label: T+1h, T+6h, T+24h, etc.',
  label_value                STRING    COMMENT 'Ground-truth label derived from engagement at label_horizon',
  engagement_score           DOUBLE    COMMENT 'Coverage-aware score used to derive label_value',
  engagement_observed_metrics INT      COMMENT 'Number of actually observed engagement counters',
  engagement_coverage        DOUBLE    COMMENT 'Observed source-specific counters divided by expected counters',
  audience_count             BIGINT    COMMENT 'Known author/channel/community audience, including a real zero',
  audience_available         BOOLEAN   COMMENT 'True only when audience_count was actually observed',
  dataset_version            STRING    COMMENT 'Dataset build version for reproducibility',
  context_feature_snapshot   STRING    COMMENT 'JSON snapshot of context features used at labeling time (nullable)',
  schema_version             STRING    COMMENT 'Schema version for consumer compatibility',
  example_date               DATE      COMMENT 'Partition column derived from the original event date'
)
USING iceberg
PARTITIONED BY (example_date)
"""

TRAINING_EXAMPLES_COLUMNS = [
    "example_id",
    "source",
    "platform_event_id",
    "observation_id",
    "event_ts",
    "label_observed_at",
    "author_hash",
    "text_for_model",
    "feature_version",
    "label_horizon",
    "label_value",
    "engagement_score",
    "engagement_observed_metrics",
    "engagement_coverage",
    "audience_count",
    "audience_available",
    "dataset_version",
    "context_feature_snapshot",
    "schema_version",
    "example_date",
]


DATASET_MANIFESTS_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.gold.dataset_manifests (
  dataset_version          STRING    COMMENT 'Deterministic version for identical inputs and filters',
  schema_version           STRING    COMMENT 'Training-example schema version',
  period_start             TIMESTAMP COMMENT 'Earliest original event timestamp in the dataset',
  period_end               TIMESTAMP COMMENT 'Latest original event timestamp in the dataset',
  source_tables_json       STRING    COMMENT 'Canonical JSON list of official input tables',
  iceberg_snapshots_json   STRING    COMMENT 'Canonical JSON map of table to pinned Iceberg snapshot ID',
  filters_json             STRING    COMMENT 'Canonical JSON of deterministic build filters',
  example_count            BIGINT    COMMENT 'Number of labeled examples',
  missing_rates_json       STRING    COMMENT 'Canonical JSON of field-level missing rates',
  distributions_json       STRING    COMMENT 'Canonical JSON of label and source distributions',
  dataset_fingerprint      STRING    COMMENT 'SHA-256 of input snapshots, schema, and filters',
  created_at               TIMESTAMP COMMENT 'UTC manifest creation timestamp'
)
USING iceberg
PARTITIONED BY (days(created_at))
"""

DATASET_MANIFESTS_COLUMNS = [
    "dataset_version",
    "schema_version",
    "period_start",
    "period_end",
    "source_tables_json",
    "iceberg_snapshots_json",
    "filters_json",
    "example_count",
    "missing_rates_json",
    "distributions_json",
    "dataset_fingerprint",
    "created_at",
]


# ---------------------------------------------------------------------------
# Python dataclass contracts
# ---------------------------------------------------------------------------


@dataclass
class ModelPredictionRow:
    """
    Single classifier prediction output.

    Written to ``lakehouse.gold.model_predictions`` by the inference service.
    """

    source: str
    platform_event_id: str
    prediction_ts: datetime
    model_version: str
    model_type: str
    predicted_class: str
    confidence: float
    virality_score: Optional[float] = None
    context_used: bool = False
    schema_version: str = PREDICTIONS_SCHEMA_VERSION
    prediction_date: Optional[str] = None  # ISO date string

    def to_dict(self) -> dict:
        d = asdict(self)
        d["prediction_ts"] = self.prediction_ts.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "ModelPredictionRow":
        prediction_ts = data.get("prediction_ts")
        if isinstance(prediction_ts, str):
            prediction_ts = datetime.fromisoformat(prediction_ts)
        if not isinstance(prediction_ts, datetime):
            raise ValueError("prediction_ts must be an ISO timestamp or datetime")
        return cls(
            source=data["source"],
            platform_event_id=data["platform_event_id"],
            prediction_ts=prediction_ts,
            model_version=data["model_version"],
            model_type=data["model_type"],
            predicted_class=data["predicted_class"],
            confidence=float(data["confidence"]),
            virality_score=data.get("virality_score"),
            context_used=bool(data.get("context_used", False)),
            schema_version=data.get("schema_version", PREDICTIONS_SCHEMA_VERSION),
            prediction_date=data.get("prediction_date"),
        )


@dataclass
class TrainingExampleRow:
    """
    Single labeled training example.

    Written to ``lakehouse.gold.training_examples`` by the dataset-build job.
    Labels are derived from ``silver.engagement_snapshots`` at a specified
    time horizon after the post was created.
    """

    source: str
    platform_event_id: str
    text_for_model: str
    feature_version: str
    label_horizon: str  # e.g. "T+1h", "T+6h", "T+24h"
    label_value: str  # e.g. "viral", "not_viral", "pending"
    dataset_version: str
    example_id: Optional[str] = None
    observation_id: Optional[str] = None
    event_ts: Optional[datetime] = None
    label_observed_at: Optional[datetime] = None
    author_hash: Optional[str] = None
    engagement_score: Optional[float] = None
    engagement_observed_metrics: int = 0
    engagement_coverage: float = 0.0
    audience_count: Optional[int] = None
    audience_available: bool = False
    context_feature_snapshot: Optional[str] = None  # JSON string
    schema_version: str = TRAINING_EXAMPLES_SCHEMA_VERSION
    example_date: Optional[str] = None  # ISO date string

    def to_dict(self) -> dict:
        data = asdict(self)
        for name in ("event_ts", "label_observed_at"):
            value = data.get(name)
            if isinstance(value, datetime):
                data[name] = value.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "TrainingExampleRow":
        event_ts = data.get("event_ts")
        if isinstance(event_ts, str):
            event_ts = datetime.fromisoformat(event_ts)
        label_observed_at = data.get("label_observed_at")
        if isinstance(label_observed_at, str):
            label_observed_at = datetime.fromisoformat(label_observed_at)
        return cls(
            source=data["source"],
            platform_event_id=data["platform_event_id"],
            text_for_model=data["text_for_model"],
            feature_version=data["feature_version"],
            label_horizon=data["label_horizon"],
            label_value=data["label_value"],
            dataset_version=data["dataset_version"],
            example_id=data.get("example_id"),
            observation_id=data.get("observation_id"),
            event_ts=event_ts,
            label_observed_at=label_observed_at,
            author_hash=data.get("author_hash"),
            engagement_score=data.get("engagement_score"),
            engagement_observed_metrics=int(data.get("engagement_observed_metrics", 0)),
            engagement_coverage=float(data.get("engagement_coverage", 0.0)),
            audience_count=data.get("audience_count"),
            audience_available=bool(data.get("audience_available", False)),
            context_feature_snapshot=data.get("context_feature_snapshot"),
            schema_version=data.get("schema_version", TRAINING_EXAMPLES_SCHEMA_VERSION),
            example_date=data.get("example_date"),
        )


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

VALID_LABEL_HORIZONS = {"T+1h", "T+6h", "T+24h", "T+72h", "T+7d"}


def validate_prediction_row(row: ModelPredictionRow) -> list[str]:
    """Return validation errors for a ModelPredictionRow; empty list means valid."""
    errors: list[str] = []
    if not row.source:
        errors.append("source must not be empty")
    if not row.platform_event_id:
        errors.append("platform_event_id must not be empty")
    if not isinstance(row.prediction_ts, datetime):
        errors.append("prediction_ts must be a datetime instance")
    if not row.model_version:
        errors.append("model_version must not be empty")
    if not row.predicted_class:
        errors.append("predicted_class must not be empty")
    if not (0.0 <= row.confidence <= 1.0):
        errors.append(f"confidence must be in [0, 1], got {row.confidence}")
    return errors


def validate_training_example(row: TrainingExampleRow) -> list[str]:
    """Return validation errors for a TrainingExampleRow; empty list means valid."""
    errors: list[str] = []
    if not row.source:
        errors.append("source must not be empty")
    if not row.platform_event_id:
        errors.append("platform_event_id must not be empty")
    if not row.text_for_model:
        errors.append("text_for_model must not be empty")
    if not row.label_value:
        errors.append("label_value must not be empty")
    if not row.label_horizon:
        errors.append("label_horizon must not be empty")
    elif row.label_horizon not in VALID_LABEL_HORIZONS:
        errors.append(
            f"label_horizon must be one of {sorted(VALID_LABEL_HORIZONS)}, "
            f"got {row.label_horizon!r}"
        )
    return errors


# ---------------------------------------------------------------------------
# Spark DDL helpers (callable from Airflow / batch jobs)
# ---------------------------------------------------------------------------


def create_gold_tables(spark) -> None:
    """
    Create the Gold namespace and its tables if they do not yet exist.

    Parameters
    ----------
    spark : SparkSession
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(MODEL_PREDICTIONS_DDL)
    spark.sql(TRAINING_EXAMPLES_DDL)
    spark.sql(DATASET_MANIFESTS_DDL)
    current_columns = set(spark.table("lakehouse.gold.training_examples").columns)
    additive_columns = {
        "example_id": "STRING",
        "observation_id": "STRING",
        "event_ts": "TIMESTAMP",
        "label_observed_at": "TIMESTAMP",
        "author_hash": "STRING",
        "engagement_score": "DOUBLE",
        "engagement_observed_metrics": "INT",
        "engagement_coverage": "DOUBLE",
        "audience_count": "BIGINT",
        "audience_available": "BOOLEAN",
    }
    for name, data_type in additive_columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE lakehouse.gold.training_examples ADD COLUMN {name} {data_type}")
