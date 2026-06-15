import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class InsightRefreshTests(unittest.TestCase):
    def test_python_sources_are_syntactically_valid(self):
        paths = [
            ROOT / "playwright" / "insight_refresh.py",
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "export_recent_insight_targets.py",
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "apply_insight_updates.py",
            ROOT
            / "orchestrator"
            / "dags"
            / "refresh_recent_engagement_insights.py",
        ]
        for path in paths:
            with self.subTest(path=path):
                ast.parse(path.read_text(encoding="utf-8"))

    def test_dag_has_bounded_automatic_refresh(self):
        source = (
            ROOT
            / "orchestrator"
            / "dags"
            / "refresh_recent_engagement_insights.py"
        ).read_text(encoding="utf-8")

        self.assertIn('dag_id="refresh_recent_engagement_insights"', source)
        self.assertIn("INSIGHT_REFRESH_SCHEDULE_MINUTES", source)
        self.assertIn('"lookback_days"', source)
        self.assertIn('"max_events_per_source"', source)
        self.assertIn("execution_timeout", source)
        self.assertIn("verify_refresh_services", source)
        self.assertIn("trigger_rule=TriggerRule.ALL_DONE", source)
        self.assertIn("acquire_pipeline_lock_command", source)
        self.assertIn("release_pipeline_lock_command", source)

    def test_export_enforces_date_and_source_limits(self):
        source = (
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "export_recent_insight_targets.py"
        ).read_text(encoding="utf-8")

        self.assertIn("INTERVAL {lookback_days} DAYS", source)
        self.assertIn("Window.partitionBy", source)
        self.assertIn("max_per_source", source)

    def test_apply_job_updates_bronze_and_silver_idempotently(self):
        source = (
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "apply_insight_updates.py"
        ).read_text(encoding="utf-8")

        self.assertIn("MERGE INTO", source)
        self.assertIn("COALESCE", source)
        self.assertIn("lakehouse.bronze.events", source)
        self.assertIn("lakehouse.silver.events", source)
        self.assertNotIn("WHEN NOT MATCHED", source)

    def test_x_refresh_keeps_shared_cdp_browser_open(self):
        source = (
            ROOT / "playwright" / "insight_refresh.py"
        ).read_text(encoding="utf-8")

        refresh_x = source[
            source.index("def _refresh_x") : source.index(
                "def _reddit_comment_id"
            )
        ]
        self.assertNotIn("browser.close()", refresh_x)


if __name__ == "__main__":
    unittest.main()
