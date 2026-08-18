"""Deterministic benchmark identities, metrics, and reliability checks.

This module is intentionally free of Spark and Kafka imports so the benchmark
contract can be tested in ordinary CI. System measurements are produced only
by a system backend and are never inferred from these helpers.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "pipeline-benchmark-v1"
WORKLOAD_TYPES = frozenset({"real", "replay", "generated_load"})
RUN_CLASSES = frozenset({"test", "validation", "official"})
STAGES = ("kafka", "clean", "bronze", "silver", "gold")
SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def workload_fingerprint(events: Sequence[Mapping[str, Any]]) -> str:
    """Hash ordered canonical inputs; payload order is part of replay identity."""

    return sha256([dict(event) for event in events])


def benchmark_manifest_fingerprint(manifest: Mapping[str, Any]) -> str:
    """Fingerprint deterministic campaign inputs, excluding run timestamps and IDs."""

    environment = manifest.get("environment", {})
    if not isinstance(environment, Mapping):
        raise ValueError("manifest environment must be an object")
    return sha256(
        {
            "schema_version": manifest.get("schema_version"),
            "configuration": manifest.get("configuration"),
            "workloads": manifest.get("workloads"),
            "pipeline": manifest.get("pipeline"),
            "environment_fingerprint": environment.get("environment_fingerprint"),
        }
    )


def safe_throughput(processed_events: int, elapsed_seconds: float) -> float | None:
    if processed_events < 0:
        raise ValueError("processed_events must be non-negative")
    if not math.isfinite(elapsed_seconds) or elapsed_seconds < 0:
        raise ValueError("elapsed_seconds must be finite and non-negative")
    if elapsed_seconds == 0:
        return None
    return processed_events / elapsed_seconds


@dataclass(frozen=True)
class BenchmarkConfig:
    sizes: tuple[int, ...] = (10_000, 25_000, 50_000, 100_000)
    repetitions: int = 3
    warmup_runs: int = 1
    platform: str = "youtube"
    workload_type: str = "generated_load"
    run_class: str = "validation"
    isolation_mode: str = "isolated"
    stages: tuple[str, ...] = ("kafka", "clean", "bronze", "silver")
    inject_dlq_cases: bool = True

    def validate(self) -> None:
        if not self.sizes or any(size <= 0 for size in self.sizes):
            raise ValueError("benchmark sizes must contain positive integers")
        if len(set(self.sizes)) != len(self.sizes):
            raise ValueError("benchmark sizes must be unique")
        if self.repetitions <= 0 or self.warmup_runs < 0:
            raise ValueError("repetitions must be positive and warmup_runs non-negative")
        if self.platform not in {"youtube", "x", "reddit"}:
            raise ValueError("platform must be youtube, x, or reddit")
        if self.workload_type not in WORKLOAD_TYPES:
            raise ValueError(f"unsupported workload type: {self.workload_type}")
        if self.run_class not in RUN_CLASSES:
            raise ValueError(f"unsupported run class: {self.run_class}")
        if self.isolation_mode not in {"isolated", "incremental"}:
            raise ValueError("isolation_mode must be isolated or incremental")
        unknown = set(self.stages).difference(STAGES)
        if unknown:
            raise ValueError(f"unsupported pipeline stages: {sorted(unknown)}")
        positions = [STAGES.index(stage) for stage in self.stages]
        if positions != sorted(positions):
            raise ValueError("pipeline stages must follow pipeline order")

    def identity(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)


@dataclass(frozen=True)
class BenchmarkIsolation:
    benchmark_id: str
    topic_prefix: str
    warehouse_bucket: str
    checkpoint_prefix: str
    output_path: str

    @classmethod
    def build(cls, benchmark_id: str, output_root: Path) -> "BenchmarkIsolation":
        normalized = re.sub(r"[^a-z0-9-]+", "-", benchmark_id.lower()).strip("-")
        if not normalized:
            raise ValueError("benchmark_id cannot normalize to an empty value")
        short = (
            normalized
            if len(normalized) <= 48
            else f"{normalized[:39]}-{hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:8]}"
        )
        result = cls(
            benchmark_id=normalized,
            topic_prefix=f"benchmark.{short}",
            warehouse_bucket=f"benchmark-{short}",
            checkpoint_prefix=f"benchmark/{short}/checkpoints",
            output_path=str((output_root / normalized).resolve()),
        )
        result.validate()
        return result

    def validate(self) -> None:
        for label, value in (
            ("benchmark_id", self.benchmark_id),
            ("topic_prefix", self.topic_prefix),
            ("warehouse_bucket", self.warehouse_bucket),
        ):
            if not SAFE_NAME.fullmatch(value):
                raise ValueError(f"unsafe {label}: {value!r}")
        if not self.topic_prefix.startswith("benchmark."):
            raise ValueError("benchmark topics must use the benchmark. prefix")
        if not self.warehouse_bucket.startswith("benchmark-"):
            raise ValueError("benchmark storage must use a benchmark- bucket")
        if not self.checkpoint_prefix.startswith("benchmark/"):
            raise ValueError("benchmark checkpoints must use a benchmark/ prefix")

    def topic(self, role: str) -> str:
        if not SAFE_NAME.fullmatch(role):
            raise ValueError(f"unsafe topic role: {role!r}")
        return f"{self.topic_prefix}.{role}"


def reconciliation_counts(
    bronze_event_ids: Iterable[str], silver_proof_ids: Iterable[str]
) -> dict[str, int | bool]:
    bronze = list(bronze_event_ids)
    silver = list(silver_proof_ids)
    bronze_set = set(bronze)
    silver_set = set(silver)
    report: dict[str, int | bool] = {
        "bronze_committed": len(bronze),
        "silver_application_proofs": len(silver),
        "missing_application_proofs": len(bronze_set - silver_set),
        "duplicate_bronze_event_ids": len(bronze) - len(bronze_set),
        "duplicate_application_proofs": len(silver) - len(silver_set),
        "orphan_application_proofs": len(silver_set - bronze_set),
    }
    report["passed"] = not any(
        int(report[key])
        for key in (
            "missing_application_proofs",
            "duplicate_bronze_event_ids",
            "duplicate_application_proofs",
            "orphan_application_proofs",
        )
    )
    return report


def summarize_measurements(measurements: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[int, list[Mapping[str, Any]]] = {}
    for measurement in measurements:
        if measurement.get("status") != "passed" or measurement.get("warmup"):
            continue
        grouped.setdefault(int(measurement["workload"]["input_events"]), []).append(measurement)

    summary: list[dict[str, Any]] = []
    for size in sorted(grouped):
        rows = grouped[size]
        durations = [float(row["timings"]["end_to_end_seconds"]) for row in rows]
        throughputs = [
            float(row["throughput"]["end_to_end_events_per_second"])
            for row in rows
            if row["throughput"].get("end_to_end_events_per_second") is not None
        ]
        item: dict[str, Any] = {
            "input_events": size,
            "measured_runs": len(rows),
            "duration_seconds": _distribution(durations),
            "throughput_events_per_second": _distribution(throughputs),
            "bronze_logical_rows": int(rows[-1]["counts"]["bronze_logical_rows"]),
            "silver_rows": int(rows[-1]["counts"]["silver_rows"]),
            "dlq_events": int(rows[-1]["counts"]["dlq_events"]),
            "duplicate_logical_rows_created": max(
                int(row["reliability"]["duplicate_logical_rows_created"]) for row in rows
            ),
        }
        storage_values = [
            float(row["storage"]["total_physical_bytes"])
            for row in rows
            if row.get("storage", {}).get("total_physical_bytes") is not None
        ]
        item["storage_bytes"] = _distribution(storage_values)
        summary.append(item)
    for index, item in enumerate(summary):
        item["duration_ratio_vs_previous"] = (
            None
            if index == 0
            else item["duration_seconds"]["median"]
            / summary[index - 1]["duration_seconds"]["median"]
        )
    return summary


def _distribution(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"median": None, "min": None, "max": None, "mean": None, "stddev": None}
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "mean": statistics.fmean(values),
        "stddev": statistics.pstdev(values),
    }
