import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class DashboardUserProgressionTests(unittest.TestCase):
    def test_dashboard_exposes_user_progression_metrics(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")

        expected = [
            "Suivi par identifiant",
            "Taux de réponses",
            "Engagement moyen",
            "Événements cumulés",
            "Likes cumulés",
            "Vues cumulées",
            "Réponses cumulées",
            "Classer les identifiants par",
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
        self.assertIn("regroupees par video", source)

    def test_dashboard_links_balancing_report(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")

        expected = [
            "DEFAULT_BALANCING_REPORT_PATH",
            "DASHBOARD_BALANCING_REPORT_PATH",
            "def get_balancing_report",
            "Dataset équilibré par source",
            "distribution_before",
            "distribution_after",
            "build_balanced_comment_dataset",
        ]
        for value in expected:
            with self.subTest(value=value):
                self.assertIn(value, source)


if __name__ == "__main__":
    unittest.main()
