import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
PLAYWRIGHT_DIR = ROOT / "playwright"
sys.path.insert(0, str(PLAYWRIGHT_DIR))

googleapiclient = types.ModuleType("googleapiclient")
googleapiclient_discovery = types.ModuleType("googleapiclient.discovery")
googleapiclient_discovery.build = lambda *_args, **_kwargs: None
googleapiclient.discovery = googleapiclient_discovery
sys.modules.setdefault("googleapiclient", googleapiclient)
sys.modules.setdefault("googleapiclient.discovery", googleapiclient_discovery)

playwright_module = types.ModuleType("playwright")
playwright_sync_api = types.ModuleType("playwright.sync_api")
playwright_sync_api.Error = RuntimeError
playwright_sync_api.TimeoutError = TimeoutError
playwright_sync_api.sync_playwright = lambda: None
playwright_module.sync_api = playwright_sync_api
sys.modules.setdefault("playwright", playwright_module)
sys.modules.setdefault("playwright.sync_api", playwright_sync_api)

import insight_refresh


class FakeRequest:
    def __init__(self, response):
        self.response = response

    def execute(self):
        return self.response


class FakeVideos:
    def __init__(self, calls, missing=None):
        self.calls = calls
        self.missing = set(missing or ())

    def list(self, **kwargs):
        self.calls.append(kwargs)
        ids = kwargs["id"].split(",")
        return FakeRequest(
            {
                "items": [
                    {
                        "id": video_id,
                        "statistics": {
                            "viewCount": "1000",
                            "likeCount": "25",
                            "commentCount": "7",
                        },
                    }
                    for video_id in ids
                    if video_id not in self.missing
                ]
            }
        )


class FakeYouTube:
    def __init__(self, calls, missing=None):
        self._videos = FakeVideos(calls, missing)

    def videos(self):
        return self._videos


class YouTubeEngagementRefreshTests(unittest.TestCase):
    def _target(self, index):
        video_id = f"video-{index}"
        return {
            "user_id": f"user-{index}",
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "event_ts": "2026-07-16T00:00:00+00:00",
            "source": "youtube",
            "platform_event_id": video_id,
            "metadata_refreshed_at": "2026-07-16T01:00:00+00:00",
            "last_metrics_refresh_at": "2026-07-16T01:00:00+00:00",
            "metrics_refresh_count": 2,
            "view_count": 500,
        }

    def test_refresh_batches_fifty_ids_and_reads_comment_count(self):
        calls = []
        targets = [self._target(index) for index in range(51)]
        with patch.dict(os.environ, {"YOUTUBE_API_KEY": "test-key"}), patch.object(
            insight_refresh,
            "build",
            return_value=FakeYouTube(calls),
        ):
            updates = insight_refresh._refresh_youtube(targets)

        self.assertEqual([len(call["id"].split(",")) for call in calls], [50, 1])
        self.assertTrue(all(call["part"] == "statistics" for call in calls))
        self.assertEqual(len(updates), 51)
        self.assertEqual(updates[0]["comment_count"], 7)
        self.assertEqual(updates[0]["metrics_refresh_count"], 3)
        self.assertEqual(updates[0]["metrics_refresh_status"], "success")

    def test_missing_video_has_explicit_status(self):
        calls = []
        target = self._target(1)
        with patch.dict(os.environ, {"YOUTUBE_API_KEY": "test-key"}), patch.object(
            insight_refresh,
            "build",
            return_value=FakeYouTube(calls, {"video-1"}),
        ):
            updates = insight_refresh._refresh_youtube([target])

        self.assertEqual(updates[0]["metrics_refresh_status"], "not_available")
        self.assertIsNone(updates[0]["view_count"])

    def test_refresh_source_has_no_watch_page_or_collaborator_fetch(self):
        source = (PLAYWRIGHT_DIR / "insight_refresh.py").read_text(encoding="utf-8")

        self.assertNotIn("fetch_youtube_collaborators", source)
        self.assertNotIn("youtube_authors", source)
        self.assertNotIn("YOUTUBE_WATCH_PAGE_TIMEOUT_SECONDS", source)
        self.assertIn('"comment_count": parse_count(statistics.get("commentCount"))', source)


if __name__ == "__main__":
    unittest.main()
