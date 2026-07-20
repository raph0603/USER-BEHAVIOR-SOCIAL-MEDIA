"""Pure reconciliation reporting primitives used by Spark and unit tests."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class ReconciliationReport:
    mode: str
    event_log_events: int
    applied_events: int
    missing_events: int
    duplicate_event_log_ids: int
    duplicate_applied_ids: int
    orphan_applied_events: int
    oldest_missing_age_seconds: float | None
    missing_by_source: dict[str, int]
    repaired_events: int = 0

    @property
    def is_clean(self) -> bool:
        return not any(
            (
                self.missing_events,
                self.duplicate_event_log_ids,
                self.duplicate_applied_ids,
                self.orphan_applied_events,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "is_clean": self.is_clean}


def reconciliation_epoch_id(run_id: str) -> int:
    """Derive a stable positive Spark epoch ID from an orchestration run ID."""

    digest = hashlib.sha256(str(run_id).encode("utf-8")).hexdigest()
    return int(digest[:15], 16)
