import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "orchestrator" / "dags" / "pipeline_lock.py"
SPEC = importlib.util.spec_from_file_location("pipeline_lock", MODULE_PATH)
PIPELINE_LOCK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PIPELINE_LOCK)


class PipelineLockTests(unittest.TestCase):
    def test_parse_lock_owner(self):
        self.assertEqual(
            PIPELINE_LOCK.parse_lock_owner("example_dag/manual__123"),
            ("example_dag", "manual__123"),
        )
        self.assertIsNone(PIPELINE_LOCK.parse_lock_owner("invalid"))
        self.assertIsNone(PIPELINE_LOCK.parse_lock_owner("/missing-dag"))
        self.assertIsNone(PIPELINE_LOCK.parse_lock_owner("missing-run/"))

    def test_active_owner_is_never_reclaimable(self):
        for state in ("queued", "running"):
            with self.subTest(state=state):
                status = PIPELINE_LOCK.classify_lock_owner(
                    "example_dag/manual__123",
                    lambda _dag_id, _run_id: state,
                )
                self.assertEqual(status, "active")

    def test_terminal_missing_and_invalid_owners_are_detected(self):
        self.assertEqual(
            PIPELINE_LOCK.classify_lock_owner(
                "example_dag/manual__123",
                lambda _dag_id, _run_id: "success",
            ),
            "terminal:success",
        )
        self.assertEqual(
            PIPELINE_LOCK.classify_lock_owner(
                "example_dag/manual__123",
                lambda _dag_id, _run_id: None,
            ),
            "missing",
        )
        self.assertEqual(
            PIPELINE_LOCK.classify_lock_owner(
                "invalid",
                lambda _dag_id, _run_id: "running",
            ),
            "invalid",
        )

    def test_lock_commands_use_atomic_guard_and_owner_state(self):
        acquire_command = PIPELINE_LOCK.acquire_pipeline_lock_command()
        release_command = PIPELINE_LOCK.release_pipeline_lock_command()

        self.assertIn("flock -x 9", acquire_command)
        self.assertIn("pipeline_lock.py", acquire_command)
        self.assertIn("terminal:", acquire_command)
        self.assertIn("PIPELINE_LOCK_STALE_GRACE_SECONDS", acquire_command)
        self.assertIn("flock -x 9", release_command)

    def test_dags_import_shared_lock_implementation(self):
        for dag_name in (
            "user_behavior_lakehouse.py",
            "user_behavior_lakehouse_no_row_checks.py",
            "iceberg_parquet_compaction.py",
        ):
            with self.subTest(dag_name=dag_name):
                source = (
                    ROOT / "orchestrator" / "dags" / dag_name
                ).read_text(encoding="utf-8")
                self.assertIn("from pipeline_lock import", source)
                self.assertNotIn(
                    "def acquire_pipeline_lock_command()",
                    source,
                )


if __name__ == "__main__":
    unittest.main()
