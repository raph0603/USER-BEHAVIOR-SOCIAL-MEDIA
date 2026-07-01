"""
Gold layer schemas and contracts for the classification pipeline.

Defines:
- ``lakehouse.gold.model_predictions``  — classifier outputs
- ``lakehouse.gold.training_examples``  — labeled examples for retraining

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
Both tables expose a ``schema_version`` field.  Increment
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
TRAINING_EXAMPLES_SCHEMA_VERSION = "v1"


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
  source                     STRING    COMMENT 'Origin platform',
  platform_event_id          STRING    COMMENT 'Platform-native stable identifier',
  text_for_model             STRING    COMMENT 'Lowercased cleaned text as used during feature extraction',
  feature_version            STRING    COMMENT 'Version of the post_features job that produced the features',
  label_horizon              STRING    COMMENT 'Time horizon used to derive label: T+1h, T+6h, T+24h, etc.',
  label_value                STRING    COMMENT 'Ground-truth label derived from engagement at label_horizon',
  dataset_version            STRING    COMMENT 'Dataset build version for reproducibility',
  context_feature_snapshot   STRING    COMMENT 'JSON snapshot of context features used at labeling time (nullable)',
  schema_version             STRING    COMMENT 'Schema version for consumer compatibility',
  example_date               DATE      COMMENT 'Partition column derived from the original event date'
)
USING iceberg
PARTITIONED BY (example_date)
"""

TRAINING_EXAMPLES_COLUMNS = [
    "source",
    "platform_event_id",
    "text_for_model",
    "feature_version",
    "label_horizon",
    "label_value",
    "dataset_version",
    "context_feature_snapshot",
    "schema_version",
    "example_date",
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
    context_feature_snapshot: Optional[str] = None  # JSON string
    schema_version: str = TRAINING_EXAMPLES_SCHEMA_VERSION
    example_date: Optional[str] = None  # ISO date string

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "TrainingExampleRow":
        return cls(
            source=data["source"],
            platform_event_id=data["platform_event_id"],
            text_for_model=data["text_for_model"],
            feature_version=data["feature_version"],
            label_horizon=data["label_horizon"],
            label_value=data["label_value"],
            dataset_version=data["dataset_version"],
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
    Create the Gold namespace and both Gold tables if they do not yet exist.

    Parameters
    ----------
    spark : SparkSession
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(MODEL_PREDICTIONS_DDL)
    spark.sql(TRAINING_EXAMPLES_DDL)
