"""
Context feature contract for retrieval-enhanced classification.

This module defines the stable schema for ``lakehouse.silver.context_features``.

Design intent
-------------
The classification service enriches predictions with fresh context signals
retrieved from a **remote retrieval service** (not a generative model).
The retrieval service computes similarity and trend signals by querying a
vector index of recent posts.

This contract covers:
- The Iceberg table DDL
- The Python dataclass / typed dict for the context feature row
- A schema validation helper

The contract is intentionally decoupled from the retrieval implementation so
that the lakehouse, the classifier, and the retrieval service can evolve
independently.

Usage in a future retrieval call
---------------------------------
    from spark.jobs.pipeline.context_features import ContextFeatureRow

    row = ContextFeatureRow(
        source="x",
        platform_event_id="1234567890",
        retrieved_at=datetime.utcnow(),
        top_similarity=0.87,
        avg_similarity_top10=0.71,
        recent_posts_1h=42,
        trend_growth_1h=0.15,
        trend_growth_24h=0.03,
        topic_freshness_hours=2.5,
        matched_topics=["electric vehicles", "battery"],
    )
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional


# ---------------------------------------------------------------------------
# Iceberg DDL
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.context_features (
  source                  STRING    COMMENT 'Origin platform of the post being enriched',
  platform_event_id       STRING    COMMENT 'Platform-native stable identifier of the post',
  retrieved_at            TIMESTAMP COMMENT 'Timestamp when context was retrieved (UTC)',
  top_similarity          DOUBLE    COMMENT 'Cosine similarity score to the most similar recent post',
  avg_similarity_top10    DOUBLE    COMMENT 'Average cosine similarity across the top-10 retrieved posts',
  recent_posts_1h         BIGINT    COMMENT 'Number of posts on the same topic in the past 1 hour',
  trend_growth_1h         DOUBLE    COMMENT 'Relative growth in posting rate over the past 1 hour',
  trend_growth_24h        DOUBLE    COMMENT 'Relative growth in posting rate over the past 24 hours',
  topic_freshness_hours   DOUBLE    COMMENT 'Hours since the topic was first observed in the corpus',
  matched_topics          ARRAY<STRING> COMMENT 'Topic labels matched by the retrieval service',
  retrieval_date          DATE      COMMENT 'Partition column derived from retrieved_at'
)
USING iceberg
PARTITIONED BY (retrieval_date)
"""

# Canonical ordered list of columns — used by writers and validators.
CONTEXT_FEATURE_COLUMNS = [
    "source",
    "platform_event_id",
    "retrieved_at",
    "top_similarity",
    "avg_similarity_top10",
    "recent_posts_1h",
    "trend_growth_1h",
    "trend_growth_24h",
    "topic_freshness_hours",
    "matched_topics",
    "retrieval_date",
]

# Schema version — bump when new columns are added.
SCHEMA_VERSION = "v1"


# ---------------------------------------------------------------------------
# Python dataclass contract
# ---------------------------------------------------------------------------


@dataclass
class ContextFeatureRow:
    """
    Single context-feature observation for one post.

    Produced by the remote retrieval service and written to
    ``lakehouse.silver.context_features`` before inference.
    """

    source: str
    platform_event_id: str
    retrieved_at: datetime
    top_similarity: Optional[float] = None
    avg_similarity_top10: Optional[float] = None
    recent_posts_1h: Optional[int] = None
    trend_growth_1h: Optional[float] = None
    trend_growth_24h: Optional[float] = None
    topic_freshness_hours: Optional[float] = None
    matched_topics: List[str] = field(default_factory=list)

    # derived — set by the writer, not the retrieval service
    retrieval_date: Optional[str] = None  # ISO date string, e.g. "2026-06-01"

    def to_dict(self) -> dict:
        d = asdict(self)
        # Serialize datetime to ISO string for JSON / Spark compatibility
        d["retrieved_at"] = self.retrieved_at.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "ContextFeatureRow":
        retrieved_at = data.get("retrieved_at")
        if isinstance(retrieved_at, str):
            retrieved_at = datetime.fromisoformat(retrieved_at)
        return cls(
            source=data["source"],
            platform_event_id=data["platform_event_id"],
            retrieved_at=retrieved_at,
            top_similarity=data.get("top_similarity"),
            avg_similarity_top10=data.get("avg_similarity_top10"),
            recent_posts_1h=data.get("recent_posts_1h"),
            trend_growth_1h=data.get("trend_growth_1h"),
            trend_growth_24h=data.get("trend_growth_24h"),
            topic_freshness_hours=data.get("topic_freshness_hours"),
            matched_topics=data.get("matched_topics") or [],
            retrieval_date=data.get("retrieval_date"),
        )


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validate_context_feature_row(row: ContextFeatureRow) -> list[str]:
    """
    Return a list of validation errors for a ContextFeatureRow.
    Empty list means the row is valid.
    """
    errors: list[str] = []

    if not row.source:
        errors.append("source must not be empty")
    if not row.platform_event_id:
        errors.append("platform_event_id must not be empty")
    if not isinstance(row.retrieved_at, datetime):
        errors.append("retrieved_at must be a datetime instance")

    # Optional numeric fields — if provided, must be finite floats / ints
    for attr, label in [
        ("top_similarity", "top_similarity"),
        ("avg_similarity_top10", "avg_similarity_top10"),
        ("trend_growth_1h", "trend_growth_1h"),
        ("trend_growth_24h", "trend_growth_24h"),
        ("topic_freshness_hours", "topic_freshness_hours"),
    ]:
        value = getattr(row, attr)
        if value is not None:
            try:
                float(value)
            except (TypeError, ValueError):
                errors.append(f"{label} must be a numeric value, got {value!r}")

    if row.recent_posts_1h is not None and row.recent_posts_1h < 0:
        errors.append("recent_posts_1h must be non-negative")

    if not isinstance(row.matched_topics, list):
        errors.append("matched_topics must be a list")

    return errors
