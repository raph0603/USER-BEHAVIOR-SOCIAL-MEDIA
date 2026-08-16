"""Validate and serialize the lineage of an official lakehouse dataset."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


DATASET_VERSION_PATTERN = re.compile(r"^dataset-(?P<schema>v\d+)-(?P<prefix>[a-f0-9]{20})$")
TRAINING_EXAMPLES_TABLE = "lakehouse.gold.training_examples"
AUDIENCE_FEATURE_POLICY = "excluded_no_prepublication_history"


def _json_object(manifest: dict[str, Any], field: str) -> dict[str, Any]:
    value = manifest.get(field)
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Manifest {field} must contain valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Manifest {field} must be a JSON object")
    return value


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def load_dataset_lineage(
    path: Path,
    *,
    expected_dataset_version: str | None = None,
    require_dataset: bool = True,
) -> tuple[Path | None, dict[str, Any]]:
    """Load one manifest and verify its dataset, fingerprint, and snapshots."""

    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Lakehouse dataset manifest is missing: {path}")
    manifest_bytes = path.read_bytes()
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("Lakehouse dataset manifest must be a JSON object")
    if manifest.get("official_input") is not True:
        raise ValueError("Manifest is not marked as an official lakehouse input")

    version = str(manifest.get("dataset_version") or "").strip()
    schema_version = str(manifest.get("schema_version") or "").strip()
    fingerprint = str(manifest.get("dataset_fingerprint") or "").strip()
    version_match = DATASET_VERSION_PATTERN.fullmatch(version)
    if version_match is None:
        raise ValueError("Manifest dataset_version has an invalid format")
    if version_match.group("schema") != schema_version:
        raise ValueError("Manifest dataset_version does not match schema_version")
    if not re.fullmatch(r"[a-f0-9]{64}", fingerprint):
        raise ValueError("Manifest dataset_fingerprint must be a SHA-256 hex digest")
    if version_match.group("prefix") != fingerprint[:20]:
        raise ValueError("Manifest dataset_version does not match dataset_fingerprint")
    if expected_dataset_version and version != expected_dataset_version:
        raise ValueError(f"Expected lakehouse dataset {expected_dataset_version}, received {version}")

    snapshots = _json_object(manifest, "iceberg_snapshots_json")
    filters = _json_object(manifest, "filters_json")
    if filters.get("audience_feature_policy") != AUDIENCE_FEATURE_POLICY:
        raise ValueError(
            "Official manifests must exclude audience features until timestamped "
            "pre-publication reputation history is available"
        )
    source_tables = _json_object(manifest, "source_tables_json")
    normalized_snapshots: dict[str, int] = {}
    for table, snapshot_id in snapshots.items():
        table_name = str(table).strip()
        if not table_name:
            raise ValueError("Manifest contains an empty Iceberg table name")
        try:
            normalized_snapshot_id = int(snapshot_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Snapshot ID for {table_name} must be an integer") from exc
        if normalized_snapshot_id <= 0:
            raise ValueError(f"Snapshot ID for {table_name} must be greater than zero")
        normalized_snapshots[table_name] = normalized_snapshot_id
    if not normalized_snapshots:
        raise ValueError("Manifest must pin at least one Iceberg snapshot")
    declared_tables = source_tables.get("tables")
    if not isinstance(declared_tables, list) or sorted(map(str, declared_tables)) != sorted(
        normalized_snapshots
    ):
        raise ValueError("Manifest source tables do not match its Iceberg snapshot map")

    identity = {
        "schema_version": schema_version,
        "source_snapshots": dict(sorted(normalized_snapshots.items())),
        "filters": dict(sorted(filters.items())),
    }
    computed_fingerprint = hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()
    if computed_fingerprint != fingerprint:
        raise ValueError("Manifest dataset_fingerprint does not match its pinned inputs")

    relative_path = str(manifest.get("dataset_relative_path") or "").strip()
    if not relative_path:
        raise ValueError("Manifest must contain dataset_relative_path")
    relative_dataset_path = Path(relative_path)
    if relative_dataset_path.is_absolute():
        raise ValueError("Manifest dataset_relative_path must be relative")
    export_root = (path.parent.parent if path.parent.name == "runs" else path.parent).resolve()
    dataset_path = (path.parent / relative_dataset_path).resolve()
    if not dataset_path.is_relative_to(export_root):
        raise ValueError("Manifest dataset_relative_path escapes the export root")
    if dataset_path.name != version:
        raise ValueError("Manifest dataset path does not match dataset_version")
    if require_dataset and not dataset_path.exists():
        raise FileNotFoundError(f"Versioned lakehouse dataset is missing for {version}: {dataset_path}")
    if str(manifest.get("format") or "").lower() != "parquet":
        raise ValueError("Official lakehouse training input must use Parquet")

    training_table = str(manifest.get("training_table") or "").strip()
    if training_table != TRAINING_EXAMPLES_TABLE:
        raise ValueError(
            f"Manifest training_table must be the official Gold table {TRAINING_EXAMPLES_TABLE}"
        )
    try:
        training_snapshot_id = int(manifest.get("training_snapshot_id"))
    except (TypeError, ValueError) as exc:
        raise ValueError("Manifest training_snapshot_id must be an integer") from exc
    if training_snapshot_id <= 0:
        raise ValueError("Manifest training_snapshot_id must be greater than zero")

    lineage = {
        "dataset_version": version,
        "schema_version": schema_version,
        "dataset_fingerprint": fingerprint,
        "source_tables": sorted(normalized_snapshots),
        "iceberg_snapshot_ids": dict(sorted(normalized_snapshots.items())),
        "training_table": training_table,
        "training_snapshot_id": training_snapshot_id,
        "filters": dict(sorted(filters.items())),
        "example_count": int(manifest.get("example_count") or 0),
        "period_start": str(manifest.get("period_start") or ""),
        "period_end": str(manifest.get("period_end") or ""),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    }
    return dataset_path, lineage


def model_lineage_path(model_path: Path) -> Path:
    """Return the stable sidecar path paired with a serialized model."""

    return model_path.with_suffix(".lineage.json")
