"""Pure construction and validation helpers for evaluation artifacts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from common.reproducibility import (
    compact_lineage,
    fingerprint,
    manifest_sha256,
    validate_environment_manifest,
    validate_lineage_match,
)


def validate_evaluation_inputs(
    *,
    bundle: Mapping[str, Any],
    lineage: Mapping[str, Any],
    dataset_manifest: Mapping[str, Any] | None,
    environment_manifest: Mapping[str, Any] | None,
    model_sha256: str,
) -> None:
    bundle_lineage = bundle.get("lineage")
    if not isinstance(bundle_lineage, Mapping):
        raise ValueError("Model bundle has no experiment lineage")
    validate_lineage_match(lineage, bundle_lineage, context="Model bundle")
    if lineage.get("model_sha256") != model_sha256:
        raise ValueError("Serialized model SHA-256 does not match experiment lineage")
    if dataset_manifest is not None:
        expected_manifest_sha = str(dataset_manifest.get("manifest_sha256") or "")
        if expected_manifest_sha != manifest_sha256(dataset_manifest):
            raise ValueError("Dataset manifest SHA-256 does not match its canonical contents")
        for field in ("dataset_version", "dataset_fingerprint", "manifest_sha256"):
            if lineage.get(field) != dataset_manifest.get(field):
                raise ValueError(f"Dataset/model mismatch for {field}")
    if environment_manifest is not None:
        validate_environment_manifest(environment_manifest)
        if lineage.get("environment_fingerprint") != environment_manifest.get(
            "environment_fingerprint"
        ):
            raise ValueError("Environment/model fingerprint mismatch")


def build_evaluation_artifact(
    *,
    lineage: Mapping[str, Any],
    model_sha256: str,
    overall_metrics: Mapping[str, Any],
    source_metrics: Mapping[str, Any],
    predictions: list[dict[str, Any]],
) -> dict[str, Any]:
    prediction_identity = [
        {
            "content_id": row["content_id"],
            "label": int(row["label"]),
            "probability": float(row["probability"]),
            "source": row["source"],
        }
        for row in sorted(predictions, key=lambda item: str(item["content_id"]))
    ]
    artifact: dict[str, Any] = {
        "schema_version": "evaluation-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": lineage["experiment_id"],
        "lineage": compact_lineage(lineage),
        "model_sha256": model_sha256,
        "split_fingerprint": lineage["split_fingerprint"],
        "metrics": {"overall": dict(overall_metrics), "by_source": dict(source_metrics)},
        "predictions": prediction_identity,
        "predictions_fingerprint": fingerprint(prediction_identity),
    }
    artifact["evaluation_fingerprint"] = fingerprint(
        {key: value for key, value in artifact.items() if key != "generated_at"}
    )
    return artifact
