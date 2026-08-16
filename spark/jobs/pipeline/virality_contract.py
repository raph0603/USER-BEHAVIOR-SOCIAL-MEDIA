"""Versioned, dataset-independent contracts for virality ground-truth labels."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "1"
PLATFORM_REFERENCE_POLICY = "platform_reference_quantile"
TRAINING_REFERENCE_POLICY = "training_reference_quantile"
SUPPORTED_POLICIES = {PLATFORM_REFERENCE_POLICY, TRAINING_REFERENCE_POLICY}
QUANTILE_METHOD = "linear"
ENGAGEMENT_SCORE_VERSION = "coverage_aware_log_sum_sqrt_observed_v1"
OBSERVATION_SELECTION_POLICY = "earliest_observation_at_or_after_horizon_within_tolerance_v1"
BOUNDARY_POLICY = "greater_than_or_equal"


def _finite_values(values: Iterable[float]) -> list[float]:
    clean: list[float] = []
    for value in values:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric):
            clean.append(numeric)
    return sorted(clean)


def canonical_json(value: Any) -> str:
    """Serialize logical contract inputs with deterministic JSON rules."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def linear_quantile(values: Iterable[float], quantile: float) -> float:
    """Return NumPy-compatible linear interpolation without implicit defaults."""

    if not 0.0 < quantile < 1.0:
        raise ValueError("quantile must be strictly between zero and one")
    clean = _finite_values(values)
    if not clean:
        raise ValueError("reference engagement scores must contain a finite value")
    position = (len(clean) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return clean[lower]
    fraction = position - lower
    return clean[lower] + (clean[upper] - clean[lower]) * fraction


def _logical_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Select only semantic fields; generation metadata and diagnostics are excluded."""

    return {
        "schema_version": payload["schema_version"],
        "policy": payload["policy"],
        "quantile": payload["quantile"],
        "quantile_method": payload["quantile_method"],
        "boundary_policy": payload["boundary_policy"],
        "engagement": payload["engagement"],
        "reference": payload["reference"],
        "reference_validation": payload["reference_validation"],
        "eligibility_filters": payload["eligibility_filters"],
        "thresholds": payload["thresholds"],
    }


def contract_fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(_logical_payload(payload)).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ViralityContract:
    """Validated immutable labeling contract."""

    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.payload.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"virality contract schema_version must be {SCHEMA_VERSION!r}")
        policy = str(self.payload.get("policy") or "")
        if policy not in SUPPORTED_POLICIES:
            raise ValueError(f"Unsupported virality policy: {policy!r}")
        if self.payload.get("quantile_method") != QUANTILE_METHOD:
            raise ValueError(f"quantile_method must be {QUANTILE_METHOD!r}")
        if self.payload.get("boundary_policy") != BOUNDARY_POLICY:
            raise ValueError(f"boundary_policy must be {BOUNDARY_POLICY!r}")
        engagement = self.payload.get("engagement")
        if not isinstance(engagement, Mapping):
            raise ValueError("virality contract must contain an engagement contract")
        required_engagement = {
            "engagement_score_version",
            "horizon_hours",
            "tolerance_hours",
            "observation_selection_policy",
        }
        if required_engagement - set(engagement):
            raise ValueError("virality contract engagement provenance is incomplete")
        thresholds = self.payload.get("thresholds")
        if not isinstance(thresholds, Mapping) or not thresholds:
            raise ValueError("virality contract must contain platform thresholds")
        for platform, threshold in thresholds.items():
            if not str(platform).strip() or not isinstance(threshold, Mapping):
                raise ValueError("invalid platform threshold entry")
            value = float(threshold.get("value"))
            count = int(threshold.get("reference_count", 0))
            if not math.isfinite(value) or count <= 0:
                raise ValueError(f"invalid threshold provenance for platform {platform}")
        expected = contract_fingerprint(self.payload)
        supplied = str(self.payload.get("virality_contract_fingerprint") or "")
        if supplied and supplied != expected:
            raise ValueError(
                "virality contract fingerprint does not match its logical labeling inputs"
            )

    @property
    def fingerprint(self) -> str:
        return contract_fingerprint(self.payload)

    @property
    def policy(self) -> str:
        return str(self.payload["policy"])

    @property
    def thresholds(self) -> Mapping[str, Mapping[str, Any]]:
        return self.payload["thresholds"]

    def to_dict(self) -> dict[str, Any]:
        result = json.loads(json.dumps(self.payload))
        result["virality_contract_fingerprint"] = self.fingerprint
        return result

    def label(self, platform: str, engagement_score: float) -> int:
        threshold = self.thresholds.get(platform)
        if threshold is None:
            raise ValueError(f"No frozen virality engagement threshold for platform {platform!r}")
        score = float(engagement_score)
        if not math.isfinite(score):
            raise ValueError("engagement_score must be finite")
        return int(score >= float(threshold["value"]))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ViralityContract":
        return cls(dict(payload))

    @classmethod
    def load(cls, path: Path) -> "ViralityContract":
        return cls.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )


def build_contract(
    reference_scores: Mapping[str, Iterable[float]],
    *,
    policy: str,
    quantile: float,
    reference: Mapping[str, Any],
    horizon_hours: int,
    tolerance_hours: int,
    eligibility_filters: Mapping[str, Any],
    min_reference_examples_per_platform: int,
    generated_at: datetime | None = None,
) -> ViralityContract:
    """Estimate per-platform thresholds from the explicitly supplied reference only."""

    if policy not in SUPPORTED_POLICIES:
        raise ValueError(f"Unsupported virality policy: {policy!r}")
    if min_reference_examples_per_platform <= 0:
        raise ValueError("min_reference_examples_per_platform must be configured above zero")
    if horizon_hours <= 0 or tolerance_hours < 0:
        raise ValueError("invalid engagement horizon or tolerance")
    thresholds: dict[str, dict[str, Any]] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    for platform in sorted(reference_scores):
        values = _finite_values(reference_scores[platform])
        if len(values) < min_reference_examples_per_platform:
            raise ValueError(
                f"Platform {platform!r} has {len(values)} reference examples; "
                f"minimum is {min_reference_examples_per_platform}"
            )
        value = linear_quantile(values, quantile)
        thresholds[platform] = {
            "platform": platform,
            "quantile": float(quantile),
            "quantile_method": QUANTILE_METHOD,
            "value": value,
            "reference_count": len(values),
            "reference": dict(reference),
            "engagement_contract": {
                "engagement_score_version": ENGAGEMENT_SCORE_VERSION,
                "horizon_hours": int(horizon_hours),
                "tolerance_hours": int(tolerance_hours),
                "observation_selection_policy": OBSERVATION_SELECTION_POLICY,
            },
        }
        diagnostics[platform] = {
            "reference_count": len(values),
            "min": values[0],
            "median": linear_quantile(values, 0.5),
            "q75": linear_quantile(values, 0.75),
            "max": values[-1],
        }
    if not thresholds:
        raise ValueError("No platform has a valid reference population")
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "policy": policy,
        "quantile": float(quantile),
        "quantile_method": QUANTILE_METHOD,
        "boundary_policy": BOUNDARY_POLICY,
        "engagement": {
            "engagement_score_version": ENGAGEMENT_SCORE_VERSION,
            "horizon_hours": int(horizon_hours),
            "tolerance_hours": int(tolerance_hours),
            "observation_selection_policy": OBSERVATION_SELECTION_POLICY,
        },
        "reference": dict(reference),
        "reference_validation": {
            "min_reference_examples_per_platform": int(min_reference_examples_per_platform),
            "insufficient_reference_behavior": "fail",
        },
        "eligibility_filters": dict(eligibility_filters),
        "thresholds": thresholds,
        "diagnostics": diagnostics,
        "generated_at": (generated_at or datetime.now(timezone.utc)).isoformat(),
    }
    return ViralityContract(payload)
