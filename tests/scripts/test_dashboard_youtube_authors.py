import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class DashboardYouTubeAuthorTests(unittest.TestCase):
    def test_dashboard_loads_and_displays_youtube_author_metadata(self):
        loader_source = (ROOT / "dashboard" / "loaders.py").read_text(
            encoding="utf-8"
        )
        app_source = (ROOT / "dashboard" / "app.py").read_text(
            encoding="utf-8"
        )

        for field in (
            "platform_event_id",
            "metadata_refreshed_at",
            "owner_channel_id",
            "collaborator_channel_ids",
        ):
            with self.subTest(field=field):
                self.assertIn(field, loader_source)
                self.assertIn(field, app_source)

        self.assertIn("optional_columns=optional_columns", loader_source)
        self.assertIn("missing_columns", loader_source)
        self.assertIn("YouTube authors and collaborations", app_source)
        self.assertIn("format_collaborators", app_source)
        self.assertIn("Owner channel ID", app_source)
        self.assertIn("Collaborator channel IDs", app_source)

    def test_dashboard_loader_reads_optional_engagement_metadata(self):
        loader_source = (ROOT / "dashboard" / "loaders.py").read_text(
            encoding="utf-8"
        )

        for field in (
            "comment_count",
            "reply_count",
            "retweet_count",
            "bookmark_count",
            "score",
        ):
            with self.subTest(field=field):
                self.assertIn(field, loader_source)

        self.assertIn("OPTIONAL_ENGAGEMENT_COLUMNS", loader_source)
        self.assertIn("*OPTIONAL_ENGAGEMENT_COLUMNS", loader_source)


if __name__ == "__main__":
    unittest.main()
