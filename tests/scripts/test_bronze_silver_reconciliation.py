import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "spark" / "jobs"))

from pipeline.reconciliation import (  # noqa: E402
    ReconciliationReport,
    reconciliation_epoch_id,
)


class ReconciliationReportTests(unittest.TestCase):
    def _report(self, **overrides):
        values = {
            "mode": "check",
            "event_log_events": 2,
            "applied_events": 2,
            "missing_events": 0,
            "duplicate_event_log_ids": 0,
            "duplicate_applied_ids": 0,
            "orphan_applied_events": 0,
            "oldest_missing_age_seconds": None,
            "missing_by_source": {},
        }
        values.update(overrides)
        return ReconciliationReport(**values)

    def test_clean_report_has_no_discrepancy(self):
        report = self._report()

        self.assertTrue(report.is_clean)
        self.assertTrue(report.to_dict()["is_clean"])

    def test_each_discrepancy_makes_the_report_fail(self):
        fields = (
            "missing_events",
            "duplicate_event_log_ids",
            "duplicate_applied_ids",
            "orphan_applied_events",
        )

        for field in fields:
            with self.subTest(field=field):
                self.assertFalse(self._report(**{field: 1}).is_clean)

    def test_reconciliation_epoch_is_stable_and_run_specific(self):
        self.assertEqual(reconciliation_epoch_id("run-a"), reconciliation_epoch_id("run-a"))
        self.assertNotEqual(reconciliation_epoch_id("run-a"), reconciliation_epoch_id("run-b"))
        self.assertGreaterEqual(reconciliation_epoch_id("run-a"), 0)


class ReconciliationCommandTests(unittest.TestCase):
    def test_repair_reuses_the_streaming_silver_merge(self):
        source = (
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "reconcile_bronze_silver.py"
        ).read_text(encoding="utf-8")

        self.assertIn('choices=("check", "repair")', source)
        self.assertIn('join(applied_ids, ["event_id"], "left_anti")', source)
        self.assertIn("apply_events_to_silver(", source)
        self.assertIn("oldest_missing_age_seconds", source)
        self.assertIn("duplicate_event_log_ids", source)

    def test_migration_is_additive_and_has_explicit_modes(self):
        source = (
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "migrate_pipeline_reliability.py"
        ).read_text(encoding="utf-8")

        self.assertIn('mode.add_argument("--dry-run"', source)
        self.assertIn('mode.add_argument("--apply"', source)
        self.assertIn("historical_v1", source)
        self.assertIn("historical-backfill-v1", source)
        self.assertIn("pre-journal history cannot be reconstructed", source)
        self.assertNotIn("DROP TABLE", source)
        self.assertNotIn("DELETE FROM", source)

    def test_both_lakehouse_dags_reconcile_before_analytics(self):
        dags = (
            ROOT / "orchestrator" / "dags" / "user_behavior_lakehouse.py",
            ROOT
            / "orchestrator"
            / "dags"
            / "user_behavior_lakehouse_no_row_checks.py",
        )

        for dag in dags:
            with self.subTest(dag=dag.name):
                source = dag.read_text(encoding="utf-8")
                self.assertIn('task_id="reconcile_bronze_silver"', source)
                self.assertIn("reconcile_bronze_silver.py", source)
                self.assertIn("--mode repair", source)
                self.assertIn("event_log_v1", source)
                self.assertIn("applied_events_v1", source)
                reconciliation_dependency = source.index(
                    "reconcile_bronze_silver >> [append_youtube_metadata_versions"
                )
                analytics_dependency = source.index(
                    "backfill_youtube_thumbnails >> update_content_analytics"
                )
                self.assertLess(reconciliation_dependency, analytics_dependency)
                self.assertIn("BRONZE_INGRESS_DLQ_TOPIC", source)


if __name__ == "__main__":
    unittest.main()
