"""Canonical event identity, provenance, and coverage helpers.

The helpers in this module deliberately avoid collector-specific dependencies so
that every Python producer can emit the same additive envelope before Avro
serialization.  Raw payloads are never copied into provenance metadata.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Mapping


UTC = timezone.utc

ENGAGEMENT_METRICS = (
    "like_count",
    "view_count",
    "comment_count",
    "reply_count",
    "retweet_count",
    "bookmark_count",
    "score",
    "follower_count",
    "subscriber_count",
    "subreddit_member_count",
)

COMPONENT_AVAILABILITY = (
    "metadata_available",
    "transcript_available",
    "comments_available",
)

METRIC_AVAILABILITY = tuple(f"{metric}_available" for metric in ENGAGEMENT_METRICS)
COVERAGE_FIELDS = METRIC_AVAILABILITY + COMPONENT_AVAILABILITY

ENVELOPE_FIELDS = (
    "event_id",
    "observation_id",
    "observed_at",
    "producer_name",
    "producer_run_id",
    "payload_fingerprint",
    "collection_method",
    "api_endpoint",
    "provenance_json",
    "coverage_json",
) + COVERAGE_FIELDS

_FINGERPRINT_EXCLUDED_FIELDS = frozenset(
    {
        "event_id",
        "observation_id",
        "payload_fingerprint",
        "provenance_json",
        "coverage_json",
    }
)


def canonical_json(value: Any) -> str:
    """Return a stable JSON representation suitable for hashes and storage."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _sha256(parts: list[str]) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def _is_sha256(value: object) -> bool:
    text = str(value or "").strip()
    return len(text) == 64 and all(character in "0123456789abcdefABCDEF" for character in text)


def event_payload_fingerprint(event: Mapping[str, Any]) -> str:
    """Fingerprint canonical payload fields without recursively hashing the envelope."""

    supplied = str(event.get("payload_fingerprint") or "").strip()
    if _is_sha256(supplied):
        return supplied.lower()
    payload = {
        key: value for key, value in event.items() if key not in _FINGERPRINT_EXCLUDED_FIELDS
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _component_available(event: Mapping[str, Any], component: str) -> bool:
    explicit = event.get(f"{component}_available")
    if explicit is not None:
        return bool(explicit)
    status_name = (
        "transcript_lifecycle_status" if component == "transcript" else f"{component}_status"
    )
    status = str(event.get(status_name) or "").strip().lower()
    return status in {"available", "success"}


def event_coverage(event: Mapping[str, Any]) -> dict[str, bool]:
    """Describe observed values explicitly, preserving known zero versus unknown."""

    coverage: dict[str, bool] = {}
    for metric in ENGAGEMENT_METRICS:
        availability_name = f"{metric}_available"
        explicit = event.get(availability_name)
        coverage[metric] = bool(explicit) if explicit is not None else event.get(metric) is not None
    for component in ("metadata", "transcript", "comments"):
        coverage[component] = _component_available(event, component)
    return coverage


def deterministic_observation_id(
    event: Mapping[str, Any],
    *,
    payload_fingerprint: str | None = None,
) -> str:
    """Build a stable identity for one source observation."""

    supplied = event.get("observation_id")
    if _is_sha256(supplied):
        return str(supplied).lower()
    del payload_fingerprint
    return _sha256(
        [
            str(event.get("source") or ""),
            str(event.get("platform_event_id") or event.get("video_id") or event.get("url") or ""),
            str(
                event.get("observed_at")
                or event.get("collected_at")
                or event.get("timestamp")
                or ""
            ),
        ]
    )


def deterministic_event_id(
    event: Mapping[str, Any],
    *,
    observation_id: str | None = None,
) -> str:
    """Return a deterministic immutable-journal identity for an event."""

    supplied = event.get("event_id")
    if _is_sha256(supplied):
        return str(supplied).lower()
    stable_observation_id = observation_id or deterministic_observation_id(event)
    return _sha256(
        [
            "event-v1",
            stable_observation_id,
            str(event.get("event_type") or ""),
            event_payload_fingerprint(event),
        ]
    )


def enrich_event_envelope(
    event: Mapping[str, Any],
    *,
    producer_name: str,
    producer_run_id: str | None = None,
    collection_method: str | None = None,
    api_endpoint: str | None = None,
) -> dict[str, Any]:
    """Add deterministic identity, provenance, and explicit coverage fields."""

    prepared = dict(event)
    observed_at = str(
        prepared.get("observed_at")
        or prepared.get("metadata_refreshed_at")
        or prepared.get("collected_at")
        or prepared.get("timestamp")
        or datetime.now(UTC).isoformat()
    )
    prepared["observed_at"] = observed_at
    prepared["producer_name"] = str(prepared.get("producer_name") or producer_name)
    prepared["producer_run_id"] = str(
        prepared.get("producer_run_id")
        or producer_run_id
        or os.getenv("PIPELINE_RUN_ID")
        or "standalone"
    )
    prepared["collection_method"] = prepared.get("collection_method") or collection_method
    prepared["api_endpoint"] = prepared.get("api_endpoint") or api_endpoint

    coverage = event_coverage(prepared)
    prepared.update({f"{name}_available": available for name, available in coverage.items()})
    prepared["coverage_json"] = canonical_json(coverage)

    fingerprint = event_payload_fingerprint(prepared)
    prepared["payload_fingerprint"] = fingerprint
    observation_id = deterministic_observation_id(
        prepared,
        payload_fingerprint=fingerprint,
    )
    prepared["observation_id"] = observation_id
    prepared["event_id"] = deterministic_event_id(
        prepared,
        observation_id=observation_id,
    )
    prepared["provenance_json"] = canonical_json(
        {
            "api_endpoint": prepared.get("api_endpoint"),
            "collection_method": prepared.get("collection_method"),
            "collector_version": prepared.get("collector_version"),
            "event_type": prepared.get("event_type"),
            "event_version": prepared.get("event_version"),
            "observed_at": observed_at,
            "producer_name": prepared["producer_name"],
            "producer_run_id": prepared["producer_run_id"],
            "source": prepared.get("source"),
            "source_payload_version": prepared.get("source_payload_version"),
        }
    )
    return prepared
