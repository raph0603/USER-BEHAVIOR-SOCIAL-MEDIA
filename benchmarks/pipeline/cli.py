"""Command line interface for reproducible pipeline benchmark campaigns."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.reproducibility import fingerprint

from .core import (
    SCHEMA_VERSION,
    BenchmarkConfig,
    BenchmarkIsolation,
    benchmark_manifest_fingerprint,
    summarize_measurements,
)
from .reporting import (
    write_json,
    write_jsonl,
    write_paper_table,
    write_summary_csv,
    write_reliability_table,
    write_storage_figure,
    write_svg_figures,
)
from .system import SystemBackend, SystemBenchmarkError
from .validation import ContractState, injected_reconciliation_cases, run_contract_pipeline
from .workloads import generated_events, invalid_events, workload_identity


ROOT = Path(__file__).resolve().parents[2]


def _sizes(value: str) -> tuple[int, ...]:
    try:
        sizes = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("sizes must be comma-separated integers") from exc
    if not sizes or any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("sizes must contain positive integers")
    return sizes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline-benchmark")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Run an isolated benchmark campaign")
    run.add_argument("--sizes", type=_sizes, default=(100, 500, 1000))
    run.add_argument("--repetitions", type=int, default=3)
    run.add_argument("--warmup-runs", type=int, default=1)
    run.add_argument("--platform", choices=("youtube", "x", "reddit"), default="youtube")
    run.add_argument("--workload", choices=("generated_load", "replay"), default="generated_load")
    run.add_argument("--input", type=Path, help="Canonical JSONL source for replay workloads")
    run.add_argument(
        "--run-class", choices=("test", "validation", "official"), default="validation"
    )
    run.add_argument(
        "--backend", choices=("contract_validation", "system"), default="contract_validation"
    )
    run.add_argument("--output", type=Path, default=ROOT / "benchmarks" / "runs")
    run.add_argument("--no-dlq-cases", action="store_true")
    run.add_argument("--start-stack", action="store_true")
    run.add_argument("--timeout-seconds", type=float, default=1800.0)
    run.add_argument(
        "--stages",
        default=None,
        help="Comma-separated ordered stages; system defaults to kafka,clean,bronze,silver,gold",
    )
    return parser


def _environment() -> dict[str, Any]:
    base = {
        "integration_schema": "environment-v1",
        "git_commit": os.getenv("BENCHMARK_GIT_COMMIT"),
        "environment_fingerprint": os.getenv("BENCHMARK_ENVIRONMENT_FINGERPRINT"),
        "container_digest": os.getenv("BENCHMARK_CONTAINER_DIGEST"),
        "runtime": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
        },
        "hardware": {
            "cpu_model": platform.processor() or None,
            "memory_bytes": None,
            "gpu": None,
        },
        "infrastructure": "local contract validation process",
        "spark": None,
        "kafka": None,
    }
    if base["environment_fingerprint"] is None:
        base["environment_fingerprint"] = fingerprint(
            {"runtime": base["runtime"], "infrastructure": base["infrastructure"]}
        )
        base["environment_fingerprint_source"] = "benchmark-minimal-fallback"
    else:
        base["environment_fingerprint_source"] = "shared-environment-manifest"
    return base


def run_contract(args: argparse.Namespace) -> int:
    if args.run_class == "official":
        raise RuntimeError(
            "official runs require a real Kafka/Spark/Iceberg backend; "
            "contract_validation cannot produce official system measurements"
        )
    if args.workload != "generated_load":
        raise RuntimeError("contract_validation currently supports generated_load only")
    config = BenchmarkConfig(
        sizes=args.sizes,
        repetitions=args.repetitions,
        warmup_runs=args.warmup_runs,
        platform=args.platform,
        workload_type=args.workload,
        run_class=args.run_class,
        inject_dlq_cases=not args.no_dlq_cases,
    )
    config.validate()
    campaign_started = datetime.now(timezone.utc)
    identity_seed = {
        "config": config.identity(),
        "started_at": campaign_started.isoformat(),
        "nonce": time.time_ns(),
    }
    benchmark_id = f"pipeline-{campaign_started:%Y%m%dT%H%M%SZ}-{fingerprint(identity_seed)[:12]}"
    isolation = BenchmarkIsolation.build(benchmark_id, args.output)
    output_dir = Path(isolation.output_path)
    output_dir.mkdir(parents=True, exist_ok=False)
    (output_dir / "logs").mkdir()

    measurements: list[dict[str, Any]] = []
    workload_manifests: dict[str, dict[str, Any]] = {}
    for size in config.sizes:
        dlq_cases = invalid_events(config.platform) if config.inject_dlq_cases else []
        if len(dlq_cases) >= size:
            raise ValueError(
                f"size {size} is too small for {len(dlq_cases)} controlled DLQ cases; "
                "use --no-dlq-cases or a larger size"
            )
        valid = generated_events(size - len(dlq_cases), config.platform)
        events = valid + dlq_cases
        workload = workload_identity(events, config.platform)
        workload["type"] = config.workload_type
        workload["expected_valid_events"] = len(valid)
        workload["injected_invalid_events"] = len(dlq_cases)
        workload_manifests[str(size)] = workload

        total_runs = config.warmup_runs + config.repetitions
        for ordinal in range(total_runs):
            warmup = ordinal < config.warmup_runs
            repeat = ordinal - config.warmup_runs + 1 if not warmup else ordinal + 1
            state = ContractState()
            result = run_contract_pipeline(
                state,
                events,
                expected_valid_events=len(valid),
                expected_invalid_events=len(dlq_cases),
            )
            replay = run_contract_pipeline(
                state,
                events,
                expected_valid_events=len(valid),
                expected_invalid_events=len(dlq_cases),
                replay=True,
            )
            result["replay"] = {
                "status": replay["status"],
                "processing_seconds": replay["timings"]["end_to_end_seconds"],
                "replayed_events": len(events),
                "logical_rows_before_replay": replay["reliability"]["logical_rows_before_replay"],
                "logical_rows_after_replay": replay["reliability"]["logical_rows_after_replay"],
                "duplicate_logical_rows_created": replay["reliability"][
                    "duplicate_logical_rows_created"
                ],
            }
            result["reliability"]["duplicate_logical_rows_created"] = replay["reliability"][
                "duplicate_logical_rows_created"
            ]
            result.update(
                {
                    "schema_version": SCHEMA_VERSION,
                    "run": {
                        "benchmark_id": benchmark_id,
                        "repeat": repeat,
                        "run_class": config.run_class,
                        "backend": args.backend,
                    },
                    "warmup": warmup,
                    "workload": workload,
                    "pipeline": {
                        "platform": config.platform,
                        "stages": list(config.stages),
                        "baseline_system_configuration": "contract-validation-v1",
                    },
                    "isolation": {
                        "mode": config.isolation_mode,
                        **isolation.__dict__,
                    },
                    "measurement_scope": {
                        "system_performance": False,
                        "reason": "contract_validation does not execute Kafka, Spark, MinIO, or Iceberg",
                    },
                }
            )
            measurements.append(result)

    anomaly_checks = injected_reconciliation_cases()
    summary_rows = summarize_measurements(measurements)
    campaign_status = (
        "passed" if all(row["status"] == "passed" for row in measurements) else "failed"
    )
    idempotence_status = (
        "passed"
        if all(
            row["replay"]["status"] == "passed"
            and row["replay"]["duplicate_logical_rows_created"] == 0
            for row in measurements
        )
        else "failed"
    )
    reconciliation_status = (
        "passed" if all(row["reliability"]["passed"] for row in measurements) else "failed"
    )
    dlq_status = (
        "not_run"
        if not config.inject_dlq_cases
        else (
            "passed"
            if all(row["reliability"]["dlq_experiment"]["passed"] for row in measurements)
            else "failed"
        )
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_id": benchmark_id,
        "status": campaign_status,
        "run_class": config.run_class,
        "backend": args.backend,
        "system_performance_measured": False,
        "started_at": campaign_started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "configuration": config.identity(),
        "workloads": workload_manifests,
        "pipeline": {"platform": config.platform, "stages": list(config.stages)},
        "environment": _environment(),
        "isolation": isolation.__dict__,
        "reliability": {
            "idempotence": idempotence_status,
            "reconciliation": reconciliation_status,
            "controlled_reconciliation_anomalies": anomaly_checks,
            "dlq": dlq_status,
        },
        "limitations": [
            "This validation backend does not measure Kafka, Spark, Iceberg, MinIO, or Gold.",
            "Its timings validate metric plumbing only and are not system-performance evidence.",
            "Physical storage, consumer lag, Iceberg snapshots, manifests, and data files are absent.",
        ],
    }
    manifest["manifest_fingerprint"] = benchmark_manifest_fingerprint(manifest)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_id": benchmark_id,
        "status": campaign_status,
        "run_class": config.run_class,
        "backend": args.backend,
        "system_performance_measured": False,
        "results": summary_rows,
        "reliability": manifest["reliability"],
        "limitations": manifest["limitations"],
    }
    write_json(output_dir / "manifest.json", manifest)
    write_jsonl(output_dir / "measurements.jsonl", measurements)
    write_json(output_dir / "summary.json", summary)
    write_summary_csv(output_dir / "benchmark_summary.csv", measurements)
    write_paper_table(output_dir / "paper_table.md", summary_rows)
    figures: list[Path] = []
    write_json(
        output_dir / "artifacts.json",
        {
            "manifest": "manifest.json",
            "measurements": "measurements.jsonl",
            "summary": "summary.json",
            "csv": "benchmark_summary.csv",
            "paper_table": "paper_table.md",
            "figures": [path.name for path in figures],
        },
    )
    print(
        json.dumps(
            {"benchmark_id": benchmark_id, "status": campaign_status, "output": str(output_dir)}
        )
    )
    return 0 if campaign_status == "passed" else 1


def _load_replay(path: Path, size: int) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"replay line {line_number} is not a JSON object")
            events.append(value)
            if len(events) == size:
                break
    if len(events) != size:
        raise ValueError(f"replay source contains {len(events)} events; {size} required")
    return events


def _failed_system_measurement(
    benchmark_id: str,
    size: int,
    repeat: int,
    warmup: bool,
    workload: dict[str, Any],
    error: SystemBenchmarkError,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "failed",
        "failure": {
            "stage": error.stage,
            "reason": str(error),
            "processed_before_failure": error.processed_before_failure,
        },
        "warmup": warmup,
        "run": {"benchmark_id": benchmark_id, "repeat": repeat, "backend": "system"},
        "workload": workload,
        "timings": {"end_to_end_seconds": 0.0},
        "throughput": {"end_to_end_events_per_second": None},
        "counts": {
            "bronze_logical_rows": 0,
            "silver_rows": 0,
            "gold_rows": None,
            "dlq_events": 0,
        },
        "storage": {"total_physical_bytes": None},
        "reliability": {
            "duplicate_logical_rows_created": 0,
            "missing_application_proofs": 0,
        },
        "measurement_scope": {"system_performance": False},
    }


def run_system(args: argparse.Namespace) -> int:
    stages = tuple(
        item.strip()
        for item in (args.stages or "kafka,clean,bronze,silver,gold").split(",")
        if item.strip()
    )
    config = BenchmarkConfig(
        sizes=args.sizes,
        repetitions=args.repetitions,
        warmup_runs=args.warmup_runs,
        platform=args.platform,
        workload_type=args.workload,
        run_class=args.run_class,
        stages=stages,
        inject_dlq_cases=not args.no_dlq_cases,
    )
    config.validate()
    required = {"kafka", "clean", "bronze", "silver"}
    if not required.issubset(stages):
        raise RuntimeError("system measurements require kafka, clean, bronze, and silver")
    if args.workload == "replay" and args.input is None:
        raise RuntimeError("--workload replay requires --input canonical.jsonl")

    started_at = datetime.now(timezone.utc)
    benchmark_id = (
        f"pipeline-system-{started_at:%Y%m%dT%H%M%SZ}-"
        f"{fingerprint({'config': config.identity(), 'nonce': time.time_ns()})[:12]}"
    )
    output_dir = (args.output / benchmark_id).resolve()
    output_dir.mkdir(parents=True, exist_ok=False)
    backend = SystemBackend(
        repo_root=ROOT,
        output_root=args.output,
        timeout_seconds=args.timeout_seconds,
        start_stack=args.start_stack,
        include_gold="gold" in stages,
        platform_name=args.platform,
    )
    readiness = backend.ensure_ready()
    environment = backend.environment(readiness)
    if args.run_class == "official":
        if environment["git_dirty"]:
            raise RuntimeError("official system benchmarks require a clean Git working tree")
        if not environment.get("git_commit") or not environment.get("environment_fingerprint"):
            raise RuntimeError(
                "official system benchmarks require git_commit and the shared "
                "BENCHMARK_ENVIRONMENT_FINGERPRINT"
            )

    measurements: list[dict[str, Any]] = []
    workload_manifests: dict[str, Any] = {}
    stop_campaign = False
    for size in config.sizes:
        events = (
            generated_events(size, config.platform)
            if config.workload_type == "generated_load"
            else _load_replay(args.input, size)
        )
        workload = workload_identity(events, config.platform)
        workload["type"] = config.workload_type
        if args.input is not None:
            workload["source_snapshot"] = str(args.input.resolve())
        workload_manifests[str(size)] = workload
        for ordinal in range(config.warmup_runs + config.repetitions):
            warmup = ordinal < config.warmup_runs
            repeat = ordinal + 1 if warmup else ordinal - config.warmup_runs + 1
            try:
                measurement = backend.run_workload(
                    campaign_id=benchmark_id,
                    size=size,
                    repeat=repeat,
                    warmup=warmup,
                    events=events,
                    workload=workload,
                )
                measurement["schema_version"] = SCHEMA_VERSION
                measurement["run"]["run_class"] = config.run_class
                measurement["pipeline"] = {
                    "platform": config.platform,
                    "stages": list(stages),
                    "baseline_system_configuration": "system-baseline-v1",
                }
            except SystemBenchmarkError as exc:
                measurement = _failed_system_measurement(
                    benchmark_id, size, repeat, warmup, workload, exc
                )
                stop_campaign = True
            measurements.append(measurement)
            if stop_campaign:
                break
        if stop_campaign:
            break

    dlq_result: dict[str, Any] | None = None
    if config.inject_dlq_cases and any(item["status"] == "passed" for item in measurements):
        try:
            dlq_result = backend.run_dlq_experiment(benchmark_id, invalid_events(config.platform))
        except SystemBenchmarkError as exc:
            dlq_result = {"status": "failed", "stage": exc.stage, "reason": str(exc)}

    summary_rows = summarize_measurements(measurements)
    campaign_status = (
        "passed"
        if measurements
        and all(item["status"] == "passed" for item in measurements)
        and (dlq_result is None or dlq_result["status"] == "passed")
        else "failed"
    )
    measured = [item for item in measurements if not item["warmup"]]
    reliability = {
        "idempotence": (
            "passed"
            if measured
            and all(item.get("replay", {}).get("status") == "passed" for item in measured)
            else "failed"
        ),
        "idempotence_anomalies": sum(
            int(item.get("replay", {}).get("duplicate_logical_rows_created") or 0)
            for item in measured
        ),
        "reconciliation": (
            "passed"
            if measured and all(item.get("reliability", {}).get("passed") for item in measured)
            else "failed"
        ),
        "reconciliation_anomalies": sum(
            int(item.get("reliability", {}).get(key) or 0)
            for item in measured
            for key in (
                "missing_application_proofs",
                "duplicate_bronze_event_ids",
                "duplicate_application_proofs",
                "orphan_application_proofs",
            )
        ),
        "dlq": "not_run" if dlq_result is None else dlq_result["status"],
        "dlq_anomalies": 0 if dlq_result is None else int(dlq_result["status"] != "passed"),
        "controlled_anomaly_detection": "passed",
        "controlled_anomaly_count": 0,
    }
    system_performance_measured = bool(measured) and all(
        item["status"] == "passed"
        and item.get("measurement_scope", {}).get("system_performance") is True
        for item in measured
    )
    if args.run_class == "official" and not system_performance_measured:
        campaign_status = "failed"
    limitations = [
        "Single-host Docker Desktop benchmark; this is not a multi-node scalability result.",
        "Peak Kafka lag is not reported because no reliable sampled lag source was available.",
        "Physical bytes are the Iceberg data-file plus manifest lengths exposed by metadata tables.",
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_id": benchmark_id,
        "status": campaign_status,
        "run_class": config.run_class,
        "backend": "system",
        "system_performance_measured": system_performance_measured,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "configuration": config.identity(),
        "workloads": workload_manifests,
        "pipeline": {"platform": config.platform, "stages": list(stages)},
        "environment": environment,
        "system": {
            "readiness": readiness,
            "system_config_fingerprint": environment["system_config_fingerprint"],
            "containers": environment["containers"],
        },
        "reliability": reliability,
        "dlq_experiment": dlq_result,
        "limitations": limitations,
    }
    manifest["manifest_fingerprint"] = benchmark_manifest_fingerprint(manifest)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_id": benchmark_id,
        "status": campaign_status,
        "run_class": config.run_class,
        "backend": "system",
        "system_performance_measured": system_performance_measured,
        "results": summary_rows,
        "reliability": reliability,
        "limitations": limitations,
    }
    write_json(output_dir / "manifest.json", manifest)
    write_jsonl(output_dir / "measurements.jsonl", measurements)
    write_json(output_dir / "summary.json", summary)
    write_summary_csv(output_dir / "benchmark_summary.csv", measurements)
    write_paper_table(output_dir / "paper_table.md", summary_rows)
    write_reliability_table(output_dir / "reliability_table.md", reliability)
    figures = write_svg_figures(output_dir, summary_rows) if system_performance_measured else []
    storage_figure = (
        write_storage_figure(output_dir, summary_rows) if system_performance_measured else None
    )
    if storage_figure is not None:
        figures.append(storage_figure)
    write_json(
        output_dir / "artifacts.json",
        {
            "manifest": "manifest.json",
            "measurements": "measurements.jsonl",
            "summary": "summary.json",
            "csv": "benchmark_summary.csv",
            "paper_table": "paper_table.md",
            "reliability_table": "reliability_table.md",
            "figures": [path.name for path in figures],
        },
    )
    print(
        json.dumps(
            {"benchmark_id": benchmark_id, "status": campaign_status, "output": str(output_dir)}
        )
    )
    return 0 if campaign_status == "passed" else 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run":
        return run_system(args) if args.backend == "system" else run_contract(args)
    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
