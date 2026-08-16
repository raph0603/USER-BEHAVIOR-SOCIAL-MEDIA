"""Fast contract backend used to test benchmark logic without system services.

Its timings are explicitly not Kafka/Spark/Iceberg performance measurements.
The backend exists to exercise deterministic workloads, quality gates,
idempotence, reconciliation, DLQ scenarios, isolation, and artifact generation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from .core import reconciliation_counts, safe_throughput


@dataclass
class ContractState:
    bronze_journal: dict[str, dict[str, Any]] = field(default_factory=dict)
    silver_state: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    silver_proofs: list[str] = field(default_factory=list)
    dlq: list[dict[str, str]] = field(default_factory=list)

    def logical_rows(self) -> int:
        return len(self.silver_state)


def invalid_reason(event: Mapping[str, Any]) -> str | None:
    if not event.get("user_id"):
        return "missing_user_id"
    if not event.get("url"):
        return "missing_url"
    if not event.get("timestamp"):
        return "missing_timestamp"
    if event.get("error"):
        return "collector_error"
    text = str(event.get("raw_text") or event.get("title") or "").strip()
    if not text:
        return "empty_after_clean"
    return None


def run_contract_pipeline(
    state: ContractState,
    events: Sequence[Mapping[str, Any]],
    *,
    expected_valid_events: int,
    expected_invalid_events: int,
    replay: bool = False,
) -> dict[str, Any]:
    started = time.perf_counter()
    timings: dict[str, float] = {}

    stage = time.perf_counter()
    produced = [dict(event) for event in events]
    timings["producer_seconds"] = time.perf_counter() - stage

    stage = time.perf_counter()
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = []
    false_accepts = 0
    unexpected_rejections = 0
    reason_mismatches = 0
    for event in produced:
        reason = invalid_reason(event)
        if reason is None:
            valid.append(event)
            false_accepts += int(bool(event.get("benchmark_invalid_case")))
        else:
            rejected.append({"event_id": str(event.get("event_id") or ""), "reason": reason})
            expected_reason = event.get("benchmark_invalid_case")
            unexpected_rejections += int(expected_reason is None)
            reason_mismatches += int(expected_reason is not None and expected_reason != reason)
    state.dlq.extend(rejected)
    timings["clean_seconds"] = time.perf_counter() - stage

    logical_before = state.logical_rows()
    stage = time.perf_counter()
    for event in valid:
        state.bronze_journal.setdefault(str(event["event_id"]), dict(event))
    timings["bronze_seconds"] = time.perf_counter() - stage

    stage = time.perf_counter()
    newly_applied = 0
    for event in valid:
        event_id = str(event["event_id"])
        if event_id in state.silver_proofs:
            continue
        business_id = str(event.get("platform_event_id") or event_id)
        state.silver_state[(str(event.get("source") or ""), business_id)] = dict(event)
        state.silver_proofs.append(event_id)
        newly_applied += 1
    timings["silver_seconds"] = time.perf_counter() - stage
    timings["gold_seconds"] = 0.0
    timings["end_to_end_seconds"] = time.perf_counter() - started

    logical_after = state.logical_rows()
    duplicates_created = max(0, logical_after - logical_before - newly_applied)
    reconciliation = reconciliation_counts(state.bronze_journal, state.silver_proofs)
    checks = {
        "input_count_matches": len(events) == expected_valid_events + expected_invalid_events,
        "valid_count_matches": len(valid) == expected_valid_events,
        "dlq_count_matches": len(rejected) == expected_invalid_events,
        "dlq_has_no_false_accepts": false_accepts == 0,
        "dlq_has_no_unexpected_rejections": unexpected_rejections == 0,
        "dlq_reasons_match": reason_mismatches == 0,
        "reconciliation_clean": bool(reconciliation["passed"]),
        "idempotence_preserved": (not replay) or logical_after == logical_before,
    }
    status = "passed" if all(checks.values()) else "failed"
    throughput = safe_throughput(len(events), timings["end_to_end_seconds"])
    return {
        "status": status,
        "failure": None if status == "passed" else {"stage": "quality_gate", "checks": checks},
        "timings": timings,
        "throughput": {
            "producer_events_per_second": safe_throughput(len(events), timings["producer_seconds"]),
            "end_to_end_events_per_second": throughput,
        },
        "counts": {
            "messages_produced": len(events),
            "messages_consumed": len(events),
            "valid_events": len(valid),
            "rejected_events": len(rejected),
            "dlq_events": len(rejected),
            "bronze_events_committed": len(state.bronze_journal),
            "bronze_logical_rows": len(state.bronze_journal),
            "silver_rows_applied": newly_applied,
            "silver_rows": state.logical_rows(),
            "gold_rows": None,
        },
        "storage": {
            "bronze_physical_bytes": None,
            "silver_physical_bytes": None,
            "gold_physical_bytes": None,
            "iceberg_metadata_bytes": None,
            "data_files": None,
            "manifest_files": None,
            "snapshots": None,
        },
        "snapshots": {
            "bronze_before": None,
            "bronze_after": None,
            "silver_before": None,
            "silver_after": None,
            "gold_before": None,
            "gold_after": None,
        },
        "reliability": {
            "logical_rows_before_replay": logical_before if replay else None,
            "logical_rows_after_replay": logical_after if replay else None,
            "duplicate_logical_rows_created": duplicates_created,
            **reconciliation,
            "checks": checks,
            "dlq_experiment": {
                "injected_invalid_events": expected_invalid_events,
                "detected_invalid_events": len(rejected) - unexpected_rejections,
                "false_accepts": false_accepts,
                "unexpected_rejections": unexpected_rejections,
                "reason_mismatches": reason_mismatches,
                "passed": not any((false_accepts, unexpected_rejections, reason_mismatches))
                and len(rejected) == expected_invalid_events,
            },
        },
    }


def injected_reconciliation_cases() -> dict[str, dict[str, int | bool]]:
    nominal = reconciliation_counts(["a", "b"], ["a", "b"])
    missing = reconciliation_counts(["a", "b"], ["a"])
    duplicate = reconciliation_counts(["a", "b"], ["a", "a", "b"])
    orphan = reconciliation_counts(["a"], ["a", "orphan"])
    if not nominal["passed"] or missing["passed"] or duplicate["passed"] or orphan["passed"]:
        raise AssertionError("controlled reconciliation anomaly was not detected")
    return {"nominal": nominal, "missing": missing, "duplicate": duplicate, "orphan": orphan}
