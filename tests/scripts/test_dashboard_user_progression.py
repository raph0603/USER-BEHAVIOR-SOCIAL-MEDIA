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

    def test_dashboard_explains_missing_engagement_metadata(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")

        expected = [
            "def build_engagement_by_source",
            "Metadata coverage",
            "Latest metadata",
            "format_metric_cell",
            "known value",
            "instead of a sum",
        ]
        for value in expected:
            with self.subTest(value=value):
                self.assertIn(value, source)

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

    def test_dashboard_surfaces_model_pipeline_tables(self):
        app_source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")
        loader_source = (ROOT / "dashboard" / "loaders.py").read_text(
            encoding="utf-8"
        )

        expected_app_values = [
            "MODEL_PIPELINE_TABLES",
            "Model pipeline",
            "silver.post_features",
            "silver.engagement_snapshots",
            "silver.context_features",
            "gold.model_predictions",
            "gold.training_examples",
            "render_model_pipeline()",
            "text_for_model",
            "feature_version",
            "predicted_class",
            "label_horizon",
        ]
        for value in expected_app_values:
            with self.subTest(value=value):
                self.assertIn(value, app_source)

        expected_loader_values = [
            "DASHBOARD_ICEBERG_WAREHOUSE_PATH",
            "load_optional_iceberg_table",
            "iceberg_table_path",
        ]
        for value in expected_loader_values:
            with self.subTest(value=value):
                self.assertIn(value, loader_source)


if __name__ == "__main__":
    unittest.main()
