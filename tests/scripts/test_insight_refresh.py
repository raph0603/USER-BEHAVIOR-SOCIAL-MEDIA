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

    def test_x_refresh_skips_unavailable_cdp_without_failing_dag(self):
        source = (
            ROOT / "playwright" / "insight_refresh.py"
        ).read_text(encoding="utf-8")

        refresh_x = source[
            source.index("def _refresh_x") : source.index(
                "def _reddit_comment_id"
            )
        ]
        self.assertIn("Skipping X insight refresh", refresh_x)
        self.assertIn('SKIPPED_REFRESH_SOURCES.add("x")', refresh_x)
        self.assertIn("requests.RequestException", refresh_x)
        self.assertIn("return []", refresh_x)

        self.assertIn(
            "source not in SKIPPED_REFRESH_SOURCES",
            source,
        )

    def test_reddit_refresh_uses_json_endpoint_without_browser_timeouts(self):
        source = (
            ROOT / "playwright" / "insight_refresh.py"
        ).read_text(encoding="utf-8")

        refresh_reddit = source[
            source.index("def _refresh_reddit") : source.index("def main")
        ]
        self.assertIn("_reddit_comment_json_url", refresh_reddit)
        self.assertIn("requests.get", refresh_reddit)
        self.assertIn('SKIPPED_REFRESH_SOURCES.add("reddit")', refresh_reddit)
        self.assertNotIn("sync_playwright", refresh_reddit)
        self.assertNotIn("wait_for(state=\"attached\"", refresh_reddit)


if __name__ == "__main__":
    unittest.main()
