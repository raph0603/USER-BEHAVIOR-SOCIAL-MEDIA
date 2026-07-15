import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "spark" / "jobs" / "batch" / "youtube_thumbnail_backfill.py"
SPEC = importlib.util.spec_from_file_location("youtube_thumbnail_backfill", MODULE_PATH)
THUMBNAILS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(THUMBNAILS)


class YouTubeThumbnailBackfillTests(unittest.TestCase):
    def test_low_resolution_thumbnail_url_is_public_and_quota_free(self):
        self.assertEqual(
            THUMBNAILS.low_resolution_thumbnail_url("abc_123"),
            "https://img.youtube.com/vi/abc_123/default.jpg",
        )

    def test_backfill_does_not_use_youtube_api_key_or_data_api(self):
        source = MODULE_PATH.read_text(encoding="utf-8")

        self.assertNotIn("YOUTUBE_API_KEY", source)
        self.assertNotIn("youtube/v3/videos", source)
        self.assertNotIn("googleapiclient", source)
        self.assertIn("img.youtube.com/vi/", source)


if __name__ == "__main__":
    unittest.main()
