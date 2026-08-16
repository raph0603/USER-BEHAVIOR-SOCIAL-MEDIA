import json
import io
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from benchmarks.pipeline.core import (  # noqa: E402
    BenchmarkConfig,
    BenchmarkIsolation,
    benchmark_manifest_fingerprint,
    reconciliation_counts,
    safe_throughput,
    summarize_measurements,
    workload_fingerprint,
)
from benchmarks.pipeline.reporting import write_summary_csv  # noqa: E402
from benchmarks.pipeline.cli import build_parser, run_contract  # noqa: E402
from benchmarks.pipeline.system import (  # noqa: E402
    CommandResult,
    SystemBackend,
    SystemBenchmarkError,
    offset_delta,
    parse_probe_output,
    system_completion_checks,
)
from benchmarks.pipeline.validation import (  # noqa: E402
    ContractState,
    injected_reconciliation_cases,
    run_contract_pipeline,
)
from benchmarks.pipeline.workloads import generated_events, invalid_events  # noqa: E402


class WorkloadDeterminismTests(unittest.TestCase):
    def test_same_generated_workload_has_same_fingerprint_and_event_ids(self):
        first = generated_events(10, "youtube")
        second = generated_events(10, "youtube")

        self.assertEqual(workload_fingerprint(first), workload_fingerprint(second))
        self.assertEqual(
            [event["event_id"] for event in first],
            [event["event_id"] for event in second],
        )

    def test_distinct_logical_events_have_distinct_event_ids(self):
        events = generated_events(10, "reddit")

        self.assertEqual(len({event["event_id"] for event in events}), 10)

    def test_controlled_invalid_cases_have_distinct_source_identities(self):
        cases = invalid_events("youtube")

        self.assertEqual(len({event["platform_event_id"] for event in cases}), len(cases))

    def test_mapping_key_order_does_not_change_workload_fingerprint(self):
        self.assertEqual(
            workload_fingerprint([{"a": 1, "b": 2}]),
            workload_fingerprint([{"b": 2, "a": 1}]),
        )


class MetricTests(unittest.TestCase):
    def test_throughput_uses_events_over_seconds(self):
        self.assertEqual(safe_throughput(100, 4.0), 25.0)
        self.assertIsNone(safe_throughput(100, 0.0))

    def test_invalid_metric_values_are_rejected(self):
        with self.assertRaises(ValueError):
            safe_throughput(-1, 1.0)
        with self.assertRaises(ValueError):
            safe_throughput(1, -1.0)


class ReliabilityTests(unittest.TestCase):
    def test_exact_replay_creates_no_logical_rows(self):
        events = generated_events(20, "x")
        state = ContractState()
        initial = run_contract_pipeline(
            state, events, expected_valid_events=20, expected_invalid_events=0
        )
        replay = run_contract_pipeline(
            state,
            events,
            expected_valid_events=20,
            expected_invalid_events=0,
            replay=True,
        )

        self.assertEqual(initial["status"], "passed")
        self.assertEqual(replay["status"], "passed")
        self.assertEqual(replay["reliability"]["duplicate_logical_rows_created"], 0)
        self.assertEqual(
            replay["reliability"]["logical_rows_before_replay"],
            replay["reliability"]["logical_rows_after_replay"],
        )

    def test_nominal_and_injected_reconciliation_cases(self):
        checks = injected_reconciliation_cases()

        self.assertTrue(checks["nominal"]["passed"])
        self.assertEqual(checks["missing"]["missing_application_proofs"], 1)
        self.assertEqual(checks["duplicate"]["duplicate_application_proofs"], 1)
        self.assertEqual(checks["orphan"]["orphan_application_proofs"], 1)

    def test_reconciliation_detects_all_anomaly_types_together(self):
        report = reconciliation_counts(["a", "a", "b"], ["a", "a", "orphan"])

        self.assertFalse(report["passed"])
        self.assertEqual(report["duplicate_bronze_event_ids"], 1)
        self.assertEqual(report["duplicate_application_proofs"], 1)
        self.assertEqual(report["missing_application_proofs"], 1)
        self.assertEqual(report["orphan_application_proofs"], 1)

    def test_controlled_invalid_events_reach_expected_dlq_reasons(self):
        events = invalid_events("youtube")
        state = ContractState()
        result = run_contract_pipeline(
            state,
            events,
            expected_valid_events=0,
            expected_invalid_events=len(events),
        )

        self.assertEqual(result["status"], "passed")
        self.assertTrue(result["reliability"]["dlq_experiment"]["passed"])
        self.assertEqual(result["reliability"]["dlq_experiment"]["false_accepts"], 0)
        self.assertEqual(
            {item["reason"] for item in state.dlq},
            {
                "missing_user_id",
                "missing_url",
                "missing_timestamp",
                "collector_error",
                "empty_after_clean",
            },
        )


class IsolationTests(unittest.TestCase):
    def test_benchmark_ids_do_not_share_topics_checkpoints_or_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            first = BenchmarkIsolation.build("pipeline-one", Path(directory))
            second = BenchmarkIsolation.build("pipeline-two", Path(directory))

        self.assertNotEqual(first.topic_prefix, second.topic_prefix)
        self.assertNotEqual(first.checkpoint_prefix, second.checkpoint_prefix)
        self.assertNotEqual(first.output_path, second.output_path)
        self.assertTrue(first.warehouse_bucket.startswith("benchmark-"))

    def test_production_like_namespace_is_rejected(self):
        unsafe = BenchmarkIsolation(
            benchmark_id="run",
            topic_prefix="youtube",
            warehouse_bucket="lakehouse",
            checkpoint_prefix="checkpoints/run",
            output_path="out",
        )
        with self.assertRaises(ValueError):
            unsafe.validate()

    def test_config_rejects_out_of_order_stages(self):
        with self.assertRaises(ValueError):
            BenchmarkConfig(stages=("silver", "bronze")).validate()


class SystemBackendTests(unittest.TestCase):
    def _backend(self, directory, **overrides):
        values = {
            "repo_root": ROOT,
            "output_root": Path(directory),
            "timeout_seconds": 30,
            "start_stack": False,
            "include_gold": True,
            "platform_name": "youtube",
        }
        values.update(overrides)
        return SystemBackend(**values)

    def test_backend_selection_is_explicit(self):
        parser = build_parser()

        contract = parser.parse_args(["run", "--backend", "contract_validation"])
        system = parser.parse_args(["run", "--backend", "system"])

        self.assertEqual(contract.backend, "contract_validation")
        self.assertEqual(system.backend, "system")

    def test_system_backend_requires_explicit_safety_switch(self):
        with tempfile.TemporaryDirectory() as directory:
            backend = self._backend(directory)
            with mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(SystemBenchmarkError, "RUN_PIPELINE_BENCHMARKS"):
                    backend.ensure_ready()

    def test_service_readiness_requires_kafka_and_live_spark_worker(self):
        running = "minio\nkafka\nspark-master\nspark-worker\n"
        master = io.BytesIO(json.dumps({"aliveworkers": 1, "cores": 2}).encode("utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            backend = self._backend(directory)
            with mock.patch.dict(os.environ, {"RUN_PIPELINE_BENCHMARKS": "1"}, clear=True):
                with mock.patch.object(
                    backend,
                    "_run",
                    side_effect=[CommandResult(0.1, running, ""), CommandResult(0.1, "", "")],
                ):
                    with mock.patch("urllib.request.urlopen", return_value=master):
                        readiness = backend.ensure_ready()

        self.assertEqual(readiness["spark_alive_workers"], 1)
        self.assertEqual(set(readiness["required_services"]), set(readiness["running_services"]))

    def test_timeout_is_propagated_with_failing_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            backend = self._backend(directory)
            timeout = subprocess.TimeoutExpired(["slow"], 1, output="partial", stderr="late")
            with mock.patch("subprocess.run", side_effect=timeout):
                with self.assertRaises(SystemBenchmarkError) as raised:
                    backend._run(["slow"], stage="silver", timeout_seconds=1)

        self.assertEqual(raised.exception.stage, "silver")

    def test_kafka_offset_accounting_rejects_partition_changes_and_regressions(self):
        self.assertEqual(offset_delta({0: 10, 1: 5}, {0: 20, 1: 8}), 13)
        with self.assertRaises(ValueError):
            offset_delta({0: 10}, {0: 9})
        with self.assertRaises(ValueError):
            offset_delta({0: 10}, {0: 20, 1: 1})

    def test_probe_marker_and_iceberg_snapshot_metadata_are_parsed(self):
        payload = {
            "tables": {
                "bronze_event_log": {
                    "exists": True,
                    "rows": 100,
                    "metadata": {"snapshot_id": 42, "data_files": 2, "data_bytes": 1024},
                }
            }
        }
        parsed = parse_probe_output(
            "Spark noise\nBENCHMARK_PROBE_JSON=" + json.dumps(payload) + "\n"
        )

        self.assertEqual(parsed["tables"]["bronze_event_log"]["metadata"]["snapshot_id"], 42)

    def test_system_completion_conditions_fail_on_unexplained_count(self):
        nominal = system_completion_checks(
            input_events=100,
            kafka_produced=100,
            clean_valid=100,
            dlq_events=0,
            bronze_handoff=100,
            bronze_rows=100,
            silver_rows=100,
            silver_proofs=100,
            reconciliation_clean=True,
            reconciliation_command_clean=True,
        )
        missing = system_completion_checks(
            input_events=100,
            kafka_produced=100,
            clean_valid=100,
            dlq_events=0,
            bronze_handoff=100,
            bronze_rows=99,
            silver_rows=99,
            silver_proofs=99,
            reconciliation_clean=False,
            reconciliation_command_clean=False,
        )

        self.assertTrue(all(nominal.values()))
        self.assertFalse(all(missing.values()))

    def test_contract_backend_cannot_emit_system_performance_or_figures(self):
        parser = build_parser()
        with tempfile.TemporaryDirectory() as directory:
            args = parser.parse_args(
                [
                    "run",
                    "--backend",
                    "contract_validation",
                    "--sizes",
                    "10",
                    "--repetitions",
                    "1",
                    "--warmup-runs",
                    "0",
                    "--no-dlq-cases",
                    "--output",
                    directory,
                ]
            )
            self.assertEqual(run_contract(args), 0)
            campaign = next(Path(directory).iterdir())
            manifest = json.loads((campaign / "manifest.json").read_text(encoding="utf-8"))
            artifacts = json.loads((campaign / "artifacts.json").read_text(encoding="utf-8"))

        self.assertFalse(manifest["system_performance_measured"])
        self.assertEqual(artifacts["figures"], [])

    def test_contract_backend_rejects_official_classification(self):
        args = build_parser().parse_args(
            ["run", "--backend", "contract_validation", "--run-class", "official"]
        )
        with self.assertRaisesRegex(RuntimeError, "official runs require"):
            run_contract(args)


class ReportingTests(unittest.TestCase):
    def _measurement(self, duration, warmup=False):
        return {
            "status": "passed",
            "warmup": warmup,
            "run": {"benchmark_id": "pipeline-test", "repeat": 1},
            "workload": {"type": "generated_load", "input_events": 100},
            "timings": {"end_to_end_seconds": duration},
            "throughput": {"end_to_end_events_per_second": 100 / duration},
            "counts": {
                "bronze_logical_rows": 100,
                "silver_rows": 100,
                "gold_rows": None,
                "dlq_events": 0,
            },
            "reliability": {
                "duplicate_logical_rows_created": 0,
                "missing_application_proofs": 0,
            },
        }

    def test_summary_uses_measured_median_and_excludes_warmup(self):
        summary = summarize_measurements(
            [self._measurement(100, warmup=True), self._measurement(1), self._measurement(3)]
        )

        self.assertEqual(summary[0]["duration_seconds"]["median"], 2)

    def test_csv_is_derived_from_measurements(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.csv"
            write_summary_csv(path, [self._measurement(2)])
            text = path.read_text(encoding="utf-8")

        self.assertIn("duration_seconds", text)
        self.assertIn("pipeline-test", text)

    def test_manifest_schema_is_valid_json(self):
        schema = json.loads(
            (
                ROOT / "benchmarks" / "pipeline" / "schemas" / "benchmark_manifest.schema.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(schema["properties"]["schema_version"]["const"], "pipeline-benchmark-v1")

    def test_manifest_fingerprint_ignores_run_specific_audit_fields(self):
        base = {
            "schema_version": "pipeline-benchmark-v1",
            "configuration": {"sizes": [100]},
            "workloads": {"100": {"input_fingerprint": "a" * 64}},
            "pipeline": {"stages": ["kafka"]},
            "environment": {"environment_fingerprint": "b" * 64},
            "benchmark_id": "pipeline-one",
            "started_at": "first",
        }
        changed_audit = {**base, "benchmark_id": "pipeline-two", "started_at": "second"}

        self.assertEqual(
            benchmark_manifest_fingerprint(base),
            benchmark_manifest_fingerprint(changed_audit),
        )


if __name__ == "__main__":
    unittest.main()
