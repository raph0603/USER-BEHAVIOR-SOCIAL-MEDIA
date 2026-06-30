import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class DashboardUserProgressionTests(unittest.TestCase):
    def test_dashboard_exposes_user_progression_metrics(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")

        expected = [
            "Identifier tracking",
            "Reply rate",
            "Average engagement",
            "Cumulative events",
            "Cumulative likes",
            "Cumulative views",
            "Cumulative replies",
            "Rank identifiers by",
        ]
        for value in expected:
            with self.subTest(value=value):
                self.assertIn(value, source)

    def test_dashboard_groups_youtube_metrics_by_video(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")

        self.assertIn("def deduplicate_youtube_videos", source)
        self.assertIn("def build_analytics_rows", source)
        self.assertIn("analytics_df = build_analytics_rows(df_filtered)", source)
        self.assertIn("deduplicate_youtube_videos(df_filtered)", source)
        self.assertIn("grouped by video", source)

    def test_dashboard_links_balancing_report(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")

        expected = [
            "DEFAULT_BALANCING_REPORT_PATH",
            "DASHBOARD_BALANCING_REPORT_PATH",
            "def get_balancing_report",
            "Dataset balanced by source",
            "distribution_before",
            "distribution_after",
            "build_balanced_comment_dataset",
        ]
        for value in expected:
            with self.subTest(value=value):
                self.assertIn(value, source)

    def test_dashboard_uses_tabs_for_navigation(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")

        expected = [
            "st.tabs",
            "Overview",
            "Engagement",
            "YouTube authors",
            "Identifier tracking",
            "Quality",
            "Events",
            "render_overview_summary()",
            "render_recent_events()",
            "Last collector runs",
            "get_recent_collector_runs",
        ]
        for value in expected:
            with self.subTest(value=value):
                self.assertIn(value, source)


if __name__ == "__main__":
    unittest.main()
