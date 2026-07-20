"""Deterministic identities for reproducible lakehouse training datasets."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping


def canonical_json(value: Mapping[str, Any]) -> str:
    """Serialize manifest identity inputs with stable ordering and separators."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


@dataclass(frozen=True)
class DatasetIdentity:
    """Immutable source snapshot and filtering contract for one dataset."""

    schema_version: str
    source_snapshots: Mapping[str, int]
    filters: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.schema_version.strip():
            raise ValueError("schema_version must not be empty")
        if not self.source_snapshots:
            raise ValueError("source_snapshots must not be empty")
        for table, snapshot_id in self.source_snapshots.items():
            if not str(table).strip():
                raise ValueError("source table names must not be empty")
            if int(snapshot_id) <= 0:
                raise ValueError(f"snapshot ID for {table} must be greater than zero")

    def inputs(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "source_snapshots": {
                table: int(snapshot_id)
                for table, snapshot_id in sorted(self.source_snapshots.items())
            },
            "filters": dict(sorted(self.filters.items())),
        }

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(canonical_json(self.inputs()).encode("utf-8")).hexdigest()

    @property
    def dataset_version(self) -> str:
        return f"dataset-{self.schema_version}-{self.fingerprint[:20]}"

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "source_snapshots": dict(sorted(self.source_snapshots.items())),
            "filters": dict(sorted(self.filters.items())),
            "dataset_fingerprint": self.fingerprint,
            "dataset_version": self.dataset_version,
        }


def missing_rate(missing_count: int, total_count: int) -> float | None:
    """Return a bounded missing-value rate while preserving an empty population."""

    missing = max(0, int(missing_count))
    total = max(0, int(total_count))
    if total == 0:
        return None
    return min(1.0, missing / float(total))
