"""Real Kafka -> Spark -> Iceberg benchmark backend.

This module only orchestrates production jobs. Counts, snapshots, and storage
metrics come from Kafka offsets and a read-only Spark/Iceberg probe; no in-memory
pipeline model contributes to system-performance measurements.
"""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .core import BenchmarkIsolation, canonical_json, safe_throughput, sha256


REQUIRED_SERVICES = ("minio", "kafka", "spark-master", "spark-worker")
PROBE_PREFIX = "BENCHMARK_PROBE_JSON="


class SystemBenchmarkError(RuntimeError):
    def __init__(self, stage: str, message: str, *, processed_before_failure: int | None = None):
        super().__init__(message)
        self.stage = stage
        self.processed_before_failure = processed_before_failure


def parse_probe_output(stdout: str) -> dict[str, Any]:
    for line in stdout.splitlines():
        if line.startswith(PROBE_PREFIX):
            value = json.loads(line[len(PROBE_PREFIX) :])
            if not isinstance(value, dict):
                raise ValueError("probe payload must be a JSON object")
            return value
    raise ValueError("probe JSON marker not found")


def offset_delta(before: Mapping[int, int], after: Mapping[int, int]) -> int:
    if set(before) != set(after):
        raise ValueError("Kafka partition set changed during benchmark")
    deltas = [after[partition] - offset for partition, offset in before.items()]
    if any(value < 0 for value in deltas):
        raise ValueError("Kafka ending offset precedes starting offset")
    return sum(deltas)


def system_completion_checks(
    *,
    input_events: int,
    kafka_produced: int,
    clean_valid: int,
    dlq_events: int,
    bronze_handoff: int,
    bronze_rows: int | None,
    silver_rows: int | None,
    silver_proofs: int | None,
    reconciliation_clean: bool,
    reconciliation_command_clean: bool,
) -> dict[str, bool]:
    return {
        "input_count_matches": kafka_produced == input_events,
        "clean_count_matches": clean_valid + dlq_events == input_events,
        "nominal_dlq_empty": dlq_events == 0,
        "bronze_handoff_matches": bronze_handoff == clean_valid,
        "bronze_rows_match": bronze_rows == clean_valid,
        "silver_rows_match": silver_rows == clean_valid,
        "silver_proofs_match": silver_proofs == clean_valid,
        "reconciliation_clean": reconciliation_clean,
        "real_reconciliation_command_clean": reconciliation_command_clean,
    }


@dataclass(frozen=True)
class CommandResult:
    elapsed_seconds: float
    stdout: str
    stderr: str


class SystemBackend:
    def __init__(
        self,
        *,
        repo_root: Path,
        output_root: Path,
        timeout_seconds: float,
        start_stack: bool,
        include_gold: bool,
        platform_name: str,
    ) -> None:
        self.repo_root = repo_root.resolve()
        self.output_root = output_root.resolve()
        self.timeout_seconds = timeout_seconds
        self.start_stack = start_stack
        self.include_gold = include_gold
        self.platform_name = platform_name
        self.spark_submit = (
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "--driver-memory",
            "512m",
            "--executor-memory",
            "1024m",
            "--conf",
            "spark.cores.max=2",
            "--conf",
            "spark.executor.cores=1",
        )

    def _run(
        self,
        command: Sequence[str],
        *,
        stage: str,
        log_path: Path | None = None,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
        check: bool = True,
    ) -> CommandResult:
        started = time.perf_counter()
        try:
            completed = subprocess.run(
                list(command),
                cwd=self.repo_root,
                input=input_text,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=timeout_seconds or self.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            if log_path is not None:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(
                    (exc.stdout or "") + "\n" + (exc.stderr or ""), encoding="utf-8"
                )
            raise SystemBenchmarkError(
                stage, f"stage timed out after {exc.timeout} seconds"
            ) from exc
        elapsed = time.perf_counter() - started
        if log_path is not None:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(
                f"command={json.dumps(list(command))}\n"
                f"elapsed_seconds={elapsed}\n"
                f"exit_code={completed.returncode}\n"
                f"--- stdout ---\n{completed.stdout}\n"
                f"--- stderr ---\n{completed.stderr}\n",
                encoding="utf-8",
            )
        if check and completed.returncode != 0:
            tail = (completed.stderr or completed.stdout)[-2000:]
            raise SystemBenchmarkError(
                stage,
                f"command failed with exit code {completed.returncode}: {tail}",
            )
        return CommandResult(elapsed, completed.stdout, completed.stderr)

    def _compose(self, *arguments: str) -> list[str]:
        return ["docker", "compose", *arguments]

    def ensure_ready(self) -> dict[str, Any]:
        if os.getenv("RUN_PIPELINE_BENCHMARKS") != "1":
            raise SystemBenchmarkError(
                "safety",
                "system backend requires RUN_PIPELINE_BENCHMARKS=1",
            )
        if self.start_stack:
            self._run(
                self._compose("up", "-d", *REQUIRED_SERVICES),
                stage="service_startup",
                timeout_seconds=max(self.timeout_seconds, 300),
            )

        deadline = time.monotonic() + min(self.timeout_seconds, 180)
        last_error = "services did not become ready"
        while time.monotonic() < deadline:
            try:
                running = self._run(
                    self._compose("ps", "--services", "--status", "running"),
                    stage="service_readiness",
                ).stdout.splitlines()
                missing = sorted(set(REQUIRED_SERVICES).difference(running))
                if missing:
                    last_error = f"missing running services: {', '.join(missing)}"
                    time.sleep(1)
                    continue
                self._run(
                    self._compose(
                        "exec",
                        "-T",
                        "kafka",
                        "/opt/kafka/bin/kafka-topics.sh",
                        "--bootstrap-server",
                        "kafka:9092",
                        "--list",
                    ),
                    stage="kafka_readiness",
                    timeout_seconds=30,
                )
                with urllib.request.urlopen("http://127.0.0.1:8080/json", timeout=5) as response:
                    master = json.load(response)
                if int(master.get("aliveworkers", 0)) < 1:
                    last_error = "Spark master has no registered live worker"
                    time.sleep(1)
                    continue
                return {
                    "required_services": list(REQUIRED_SERVICES),
                    "running_services": sorted(running),
                    "spark_alive_workers": int(master["aliveworkers"]),
                    "spark_cores": int(master.get("cores", 0)),
                }
            except (OSError, ValueError, SystemBenchmarkError) as exc:
                last_error = str(exc)
                time.sleep(1)
        raise SystemBenchmarkError("service_readiness", last_error)

    def _create_bucket(self, bucket: str, log_path: Path) -> None:
        if not bucket.startswith("benchmark-"):
            raise SystemBenchmarkError("isolation", f"unsafe benchmark bucket: {bucket}")
        access = os.getenv("MINIO_ROOT_USER", "minioadmin")
        secret = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        script = (
            f"/usr/bin/mc alias set bench http://minio:9000 {access} {secret} >/dev/null && "
            f"/usr/bin/mc mb --ignore-existing bench/{bucket}"
        )
        self._run(
            self._compose(
                "run",
                "--rm",
                "--no-deps",
                "--entrypoint",
                "/bin/sh",
                "minio-init",
                "-c",
                script,
            ),
            stage="storage_isolation",
            log_path=log_path,
        )

    def _create_topics(self, isolation: BenchmarkIsolation, log_path: Path) -> dict[str, str]:
        topics = {
            role: isolation.topic(role) for role in ("raw", "clean", "dlq", "bronze", "bronze-dlq")
        }
        for topic in topics.values():
            if not topic.startswith("benchmark."):
                raise SystemBenchmarkError("isolation", f"unsafe benchmark topic: {topic}")
            self._run(
                self._compose(
                    "exec",
                    "-T",
                    "kafka",
                    "/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server",
                    "kafka:9092",
                    "--create",
                    "--if-not-exists",
                    "--topic",
                    topic,
                    "--partitions",
                    "1",
                    "--replication-factor",
                    "1",
                ),
                stage="topic_isolation",
                log_path=log_path.with_name(f"{topic.rsplit('.', 1)[-1]}.log"),
            )
        return topics

    def topic_offsets(self, topic: str) -> dict[int, int]:
        result = self._run(
            self._compose(
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-get-offsets.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--topic",
                topic,
            ),
            stage="kafka_offsets",
            timeout_seconds=30,
        )
        offsets: dict[int, int] = {}
        for line in result.stdout.splitlines():
            match = re.search(r":(\d+):(\d+)\s*$", line)
            if match:
                offsets[int(match.group(1))] = int(match.group(2))
        if not offsets:
            raise SystemBenchmarkError("kafka_offsets", f"no offsets returned for {topic}")
        return offsets

    def _produce(self, topic: str, events: Sequence[Mapping[str, Any]], log_path: Path) -> float:
        payload = "".join(canonical_json(event) + "\n" for event in events)
        result = self._run(
            self._compose(
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--topic",
                topic,
                "--producer-property",
                "acks=all",
                "--producer-property",
                "enable.idempotence=true",
            ),
            stage="kafka_producer",
            log_path=log_path,
            input_text=payload,
        )
        return result.elapsed_seconds

    def _spark_env_command(self, env: Mapping[str, str], script: str) -> list[str]:
        command = self._compose("exec", "-T")
        for key, value in env.items():
            command.extend(("-e", f"{key}={value}"))
        command.extend(("spark-master", *self.spark_submit, script))
        return command

    def _run_stages(
        self,
        *,
        isolation: BenchmarkIsolation,
        topics: Mapping[str, str],
        log_dir: Path,
        replay: bool,
    ) -> dict[str, float]:
        suffix = "replay" if replay else "initial"
        common = {
            "MINIO_BUCKET": isolation.warehouse_bucket,
            "PIPELINE_RUN_ID": f"{isolation.benchmark_id}-{suffix}",
            "KAFKA_FAIL_ON_DATA_LOSS": "true",
            "SPARK_SQL_SHUFFLE_PARTITIONS": "4",
            "SPARK_DEFAULT_PARALLELISM": "2",
        }
        clean = self._run(
            self._spark_env_command(
                {
                    **common,
                    "PLATFORM": self.platform_name,
                    "COLLECTOR_SOURCE_TOPIC": topics["raw"],
                    "CLEAN_KAFKA_TOPIC": topics["clean"],
                    "DLQ_KAFKA_TOPIC": topics["dlq"],
                    "CLEAN_SOURCE_VALUE_FORMAT": "json",
                    "CLEAN_STARTING_OFFSETS": "earliest",
                    "CLEAN_TRIGGER_MODE": "available_now",
                    "CLEAN_CHECKPOINT_VERSION": "system-v1",
                },
                "/opt/spark/jobs/pipeline/collector_stream_pipeline.py",
            ),
            stage="clean",
            log_path=log_dir / f"clean-{suffix}.log",
        )
        bronze = self._run(
            self._spark_env_command(
                {
                    **common,
                    "KAFKA_TOPIC": topics["clean"],
                    "KAFKA_VALUE_FORMAT": "json",
                    "KAFKA_STARTING_OFFSETS": "earliest",
                    "BRONZE_KAFKA_OUT_TOPIC": topics["bronze"],
                    "BRONZE_INGRESS_DLQ_TOPIC": topics["bronze-dlq"],
                    "BRONZE_TRIGGER_MODE": "available_now",
                    "BRONZE_CHECKPOINT_VERSION": "system-v1",
                },
                "/opt/spark/jobs/streaming/kafka_to_iceberg_bronze.py",
            ),
            stage="bronze",
            log_path=log_dir / f"bronze-{suffix}.log",
        )
        silver = self._run(
            self._spark_env_command(
                {
                    **common,
                    "SILVER_KAFKA_TOPICS": topics["bronze"],
                    "SILVER_STARTING_OFFSETS": "earliest",
                    "SILVER_TRIGGER_MODE": "available_now",
                    "SILVER_CHECKPOINT_VERSION": "system-v1",
                },
                "/opt/spark/jobs/batch/bronze_to_silver_from_kafka.py",
            ),
            stage="silver",
            log_path=log_dir / f"silver-{suffix}.log",
        )
        timings = {
            "clean_seconds": clean.elapsed_seconds,
            "bronze_seconds": bronze.elapsed_seconds,
            "silver_seconds": silver.elapsed_seconds,
            "gold_seconds": 0.0,
        }
        if self.include_gold:
            gold = self._run(
                self._spark_env_command(
                    common,
                    "/opt/spark/jobs/batch/content_analytics.py",
                ),
                stage="gold",
                log_path=log_dir / f"gold-{suffix}.log",
            )
            timings["gold_seconds"] = gold.elapsed_seconds
        return timings

    def probe(self, isolation: BenchmarkIsolation, log_path: Path) -> dict[str, Any]:
        result = self._run(
            self._spark_env_command(
                {"MINIO_BUCKET": isolation.warehouse_bucket},
                "/opt/spark/jobs/benchmark/pipeline_probe.py",
            ),
            stage="iceberg_probe",
            log_path=log_path,
        )
        try:
            return parse_probe_output(result.stdout)
        except (ValueError, json.JSONDecodeError) as exc:
            raise SystemBenchmarkError("iceberg_probe", str(exc)) from exc

    def _reconcile(self, isolation: BenchmarkIsolation, log_path: Path) -> dict[str, Any]:
        result = self._run(
            self._spark_env_command(
                {"MINIO_BUCKET": isolation.warehouse_bucket},
                "/opt/spark/jobs/maintenance/reconcile_bronze_silver.py",
            )
            + ["--mode", "check"],
            stage="reconciliation",
            log_path=log_path,
        )
        for line in reversed(result.stdout.splitlines()):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "event_log_events" in payload:
                return payload
        raise SystemBenchmarkError("reconciliation", "reconciliation JSON not found")

    @staticmethod
    def _table_rows(probe: Mapping[str, Any], name: str) -> int | None:
        table = probe["tables"].get(name, {})
        return None if not table.get("exists") else int(table["rows"])

    @staticmethod
    def _storage(probe: Mapping[str, Any]) -> dict[str, Any]:
        groups = {
            "bronze": ("bronze_event_log", "bronze_events", "bronze_ingress_dlq"),
            "silver": (
                "silver_events",
                "silver_applied_events",
                "silver_contents",
                "silver_interactions",
                "silver_engagement_snapshots",
            ),
            "gold": ("gold_content_stats", "gold_user_evolution"),
        }
        result: dict[str, Any] = {}
        for group, names in groups.items():
            existing = [
                probe["tables"][name]["metadata"]
                for name in names
                if probe["tables"].get(name, {}).get("exists")
                and probe["tables"][name].get("metadata")
            ]
            result[f"{group}_physical_bytes"] = sum(
                int(item.get("data_bytes") or 0) + int(item.get("manifest_bytes") or 0)
                for item in existing
            )
            result[f"{group}_data_files"] = sum(
                int(item.get("data_files") or 0) for item in existing
            )
            result[f"{group}_manifest_files"] = sum(
                int(item.get("manifest_files") or 0) for item in existing
            )
        result["total_physical_bytes"] = sum(
            int(result[f"{group}_physical_bytes"]) for group in groups
        )
        return result

    def run_workload(
        self,
        *,
        campaign_id: str,
        size: int,
        repeat: int,
        warmup: bool,
        events: Sequence[Mapping[str, Any]],
        workload: Mapping[str, Any],
    ) -> dict[str, Any]:
        run_name = f"{campaign_id}-n{size}-r{repeat}-{'warmup' if warmup else 'measured'}"
        isolation = BenchmarkIsolation.build(
            run_name, self.output_root / campaign_id / "system-runs"
        )
        run_dir = Path(isolation.output_path)
        log_dir = run_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=False)
        self._create_bucket(isolation.warehouse_bucket, log_dir / "bucket.log")
        topics = self._create_topics(isolation, log_dir / "topics.log")
        starting_offsets = {role: self.topic_offsets(topic) for role, topic in topics.items()}

        end_to_end_started = time.perf_counter()
        producer_seconds = self._produce(topics["raw"], events, log_dir / "producer.log")
        stage_timings = self._run_stages(
            isolation=isolation,
            topics=topics,
            log_dir=log_dir,
            replay=False,
        )
        end_to_end_seconds = time.perf_counter() - end_to_end_started
        ending_offsets = {role: self.topic_offsets(topic) for role, topic in topics.items()}
        probe = self.probe(isolation, log_dir / "probe-initial.log")
        reconciliation = self._reconcile(isolation, log_dir / "reconciliation.log")

        input_events = len(events)
        kafka_produced = offset_delta(starting_offsets["raw"], ending_offsets["raw"])
        clean_valid = offset_delta(starting_offsets["clean"], ending_offsets["clean"])
        dlq_events = offset_delta(starting_offsets["dlq"], ending_offsets["dlq"])
        bronze_handoff = offset_delta(starting_offsets["bronze"], ending_offsets["bronze"])
        bronze_rows = self._table_rows(probe, "bronze_event_log")
        silver_rows = self._table_rows(probe, "silver_events")
        silver_proofs = self._table_rows(probe, "silver_applied_events")
        gold_rows = None
        if self.include_gold:
            gold_rows = sum(
                self._table_rows(probe, name) or 0
                for name in ("gold_content_stats", "gold_user_evolution")
            )
        checks = system_completion_checks(
            input_events=input_events,
            kafka_produced=kafka_produced,
            clean_valid=clean_valid,
            dlq_events=dlq_events,
            bronze_handoff=bronze_handoff,
            bronze_rows=bronze_rows,
            silver_rows=silver_rows,
            silver_proofs=silver_proofs,
            reconciliation_clean=bool(probe.get("reconciliation", {}).get("passed")),
            reconciliation_command_clean=bool(reconciliation.get("is_clean")),
        )

        before_replay = {
            "bronze_logical_rows": bronze_rows,
            "silver_logical_rows": silver_rows,
            "silver_application_proofs": silver_proofs,
        }
        replay_started = time.perf_counter()
        self._produce(topics["raw"], events, log_dir / "producer-replay.log")
        replay_timings = self._run_stages(
            isolation=isolation,
            topics=topics,
            log_dir=log_dir,
            replay=True,
        )
        replay_seconds = time.perf_counter() - replay_started
        replay_probe = self.probe(isolation, log_dir / "probe-replay.log")
        after_replay = {
            "bronze_logical_rows": self._table_rows(replay_probe, "bronze_event_log"),
            "silver_logical_rows": self._table_rows(replay_probe, "silver_events"),
            "silver_application_proofs": self._table_rows(replay_probe, "silver_applied_events"),
        }
        additional = {
            key: int(after_replay[key] or 0) - int(before_replay[key] or 0) for key in before_replay
        }
        idempotence_passed = not any(additional.values())
        checks["idempotence_preserved"] = idempotence_passed
        status = "passed" if all(checks.values()) else "failed"

        snapshots = {}
        for name, table in replay_probe["tables"].items():
            initial_metadata = probe["tables"].get(name, {}).get("metadata") or {}
            replay_metadata = table.get("metadata") or {}
            snapshots[name] = {
                "before": initial_metadata.get("snapshot_id"),
                "after": replay_metadata.get("snapshot_id"),
            }
        storage = self._storage(replay_probe)
        result = {
            "status": status,
            "failure": None if status == "passed" else {"stage": "quality_gate", "checks": checks},
            "warmup": warmup,
            "run": {
                "benchmark_id": campaign_id,
                "system_run_id": isolation.benchmark_id,
                "repeat": repeat,
                "backend": "system",
            },
            "workload": dict(workload),
            "timings": {
                "producer_seconds": producer_seconds,
                **stage_timings,
                "end_to_end_seconds": end_to_end_seconds,
            },
            "throughput": {
                "producer_events_per_second": safe_throughput(input_events, producer_seconds),
                "bronze_events_per_second": safe_throughput(
                    int(bronze_rows or 0), stage_timings["bronze_seconds"]
                ),
                "silver_events_per_second": safe_throughput(
                    int(silver_proofs or 0), stage_timings["silver_seconds"]
                ),
                "end_to_end_events_per_second": safe_throughput(input_events, end_to_end_seconds),
            },
            "counts": {
                "messages_produced": kafka_produced,
                "messages_consumed": clean_valid + dlq_events,
                "valid_events": clean_valid,
                "rejected_events": dlq_events,
                "dlq_events": dlq_events,
                "bronze_events_committed": bronze_rows,
                "bronze_logical_rows": bronze_rows,
                "bronze_handoff_events": bronze_handoff,
                "silver_rows_applied": silver_proofs,
                "silver_rows": silver_rows,
                "silver_application_proofs": silver_proofs,
                "gold_rows": gold_rows,
            },
            "kafka": {
                "topics": dict(topics),
                "partitions": 1,
                "replication_factor": 1,
                "starting_offsets": starting_offsets,
                "ending_offsets": ending_offsets,
                "consumer_lag": None,
                "peak_lag": None,
            },
            "spark": probe["spark"],
            "storage": storage,
            "snapshots": snapshots,
            "reliability": {
                "duplicate_logical_rows_created": sum(
                    max(0, value) for value in additional.values()
                ),
                **(probe.get("reconciliation") or {}),
                "checks": checks,
            },
            "replay": {
                "status": "passed" if idempotence_passed else "failed",
                "processing_seconds": replay_seconds,
                "stage_timings": replay_timings,
                "replayed_events": input_events,
                "logical_rows_before_replay": before_replay,
                "logical_rows_after_replay": after_replay,
                "additional_logical_rows": additional,
                "duplicate_logical_rows_created": sum(
                    max(0, value) for value in additional.values()
                ),
            },
            "isolation": {**isolation.__dict__, "topics": topics},
            "measurement_scope": {
                "system_performance": True,
                "components_executed": [
                    "kafka",
                    "spark_clean",
                    "iceberg_bronze",
                    "iceberg_silver",
                    *(["iceberg_gold"] if self.include_gold else []),
                ],
            },
        }
        (run_dir / "measurement.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return result

    def run_dlq_experiment(
        self, campaign_id: str, events: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any]:
        isolation = BenchmarkIsolation.build(
            f"{campaign_id}-dlq", self.output_root / campaign_id / "reliability-runs"
        )
        run_dir = Path(isolation.output_path)
        logs = run_dir / "logs"
        logs.mkdir(parents=True, exist_ok=False)
        self._create_bucket(isolation.warehouse_bucket, logs / "bucket.log")
        topics = self._create_topics(isolation, logs / "topics.log")
        before = {role: self.topic_offsets(topic) for role, topic in topics.items()}
        producer_seconds = self._produce(topics["raw"], events, logs / "producer.log")
        clean = self._run(
            self._spark_env_command(
                {
                    "MINIO_BUCKET": isolation.warehouse_bucket,
                    "PLATFORM": self.platform_name,
                    "COLLECTOR_SOURCE_TOPIC": topics["raw"],
                    "CLEAN_KAFKA_TOPIC": topics["clean"],
                    "DLQ_KAFKA_TOPIC": topics["dlq"],
                    "CLEAN_SOURCE_VALUE_FORMAT": "json",
                    "CLEAN_STARTING_OFFSETS": "earliest",
                    "CLEAN_TRIGGER_MODE": "available_now",
                    "CLEAN_CHECKPOINT_VERSION": "system-dlq-v1",
                    "KAFKA_FAIL_ON_DATA_LOSS": "true",
                },
                "/opt/spark/jobs/pipeline/collector_stream_pipeline.py",
            ),
            stage="dlq_clean",
            log_path=logs / "clean.log",
        )
        after = {role: self.topic_offsets(topic) for role, topic in topics.items()}
        actual_dlq = offset_delta(before["dlq"], after["dlq"])
        clean_accepted = offset_delta(before["clean"], after["clean"])
        consumed = self._run(
            self._compose(
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-console-consumer.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--topic",
                topics["dlq"],
                "--from-beginning",
                "--max-messages",
                str(len(events)),
                "--timeout-ms",
                "30000",
            ),
            stage="dlq_consume",
            log_path=logs / "consume.log",
        )
        reasons = []
        for line in consumed.stdout.splitlines():
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("reason"):
                reasons.append(payload["reason"])
        expected_reasons = sorted(str(event["benchmark_invalid_case"]) for event in events)
        passed = (
            actual_dlq == len(events)
            and clean_accepted == 0
            and sorted(reasons) == expected_reasons
        )
        result = {
            "status": "passed" if passed else "failed",
            "producer_seconds": producer_seconds,
            "clean_seconds": clean.elapsed_seconds,
            "invalid_injected": len(events),
            "expected_dlq": len(events),
            "actual_dlq": actual_dlq,
            "false_accepts": clean_accepted,
            "unexpected_rejections": max(0, actual_dlq - len(events)),
            "expected_reasons": expected_reasons,
            "actual_reasons": sorted(reasons),
            "isolation": isolation.__dict__,
        }
        (run_dir / "dlq-result.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return result

    def environment(self, readiness: Mapping[str, Any]) -> dict[str, Any]:
        git_commit = self._run(
            ["git", "rev-parse", "HEAD"], stage="environment", timeout_seconds=30
        ).stdout.strip()
        git_dirty = bool(
            self._run(
                ["git", "status", "--porcelain=v1"],
                stage="environment",
                timeout_seconds=30,
            ).stdout.strip()
        )
        containers: dict[str, Any] = {}
        for service in REQUIRED_SERVICES:
            container_id = self._run(
                self._compose("ps", "-q", service), stage="environment"
            ).stdout.strip()
            inspected = json.loads(
                self._run(["docker", "inspect", container_id], stage="environment").stdout
            )[0]
            image = json.loads(
                self._run(
                    ["docker", "image", "inspect", inspected["Image"]],
                    stage="environment",
                ).stdout
            )[0]
            containers[service] = {
                "container_image_id": inspected["Image"],
                "repo_digests": image.get("RepoDigests") or [],
                "memory_limit_bytes": int(inspected["HostConfig"].get("Memory") or 0),
                "nano_cpus": int(inspected["HostConfig"].get("NanoCpus") or 0),
            }
        docker_info = json.loads(
            self._run(["docker", "info", "--format", "{{json .}}"], stage="environment").stdout
        )
        system_config = {
            "spark": {
                "master": "spark://spark-master:7077",
                "driver_memory": "512m",
                "executor_memory": "1024m",
                "max_cores": 2,
                "executor_cores": 1,
                "shuffle_partitions": 4,
                "trigger_mode": "available_now",
            },
            "kafka": {
                "partitions": 1,
                "replication_factor": 1,
                "producer_acks": "all",
                "producer_idempotence": True,
            },
            "topology": "single-host Docker Desktop",
        }
        return {
            "git_commit": git_commit,
            "git_dirty": git_dirty,
            "environment_fingerprint": os.getenv("BENCHMARK_ENVIRONMENT_FINGERPRINT"),
            "system_config_fingerprint": sha256(system_config),
            "system_config": system_config,
            "runtime": {
                "python": platform.python_version(),
                "os": platform.platform(),
                "machine": platform.machine(),
            },
            "hardware": {
                "cpu_model": platform.processor() or None,
                "logical_cpu_count": int(docker_info.get("NCPU") or 0),
                "docker_memory_bytes": int(docker_info.get("MemTotal") or 0),
                "docker_operating_system": docker_info.get("OperatingSystem"),
            },
            "containers": containers,
            "readiness": dict(readiness),
        }
