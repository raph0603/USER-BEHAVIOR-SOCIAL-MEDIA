import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "dashboard" / "manual_import.py"
SPEC = importlib.util.spec_from_file_location("manual_import", MODULE_PATH)
MANUAL_IMPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MANUAL_IMPORT)


class ManualImportTests(unittest.TestCase):
    def test_loads_youtube_csv_as_raw_events(self):
        payload = (
            "video_id,comment_id,author_hash,text,comment_published_at,"
            "comment_like_count,video_view_count\n"
            "abc123,c1,u1,Great EV review,2026-06-01T10:00:00Z,7,1000\n"
        ).encode("utf-8")

        events = MANUAL_IMPORT.load_import_events("youtube.csv", payload)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["source"], "youtube")
        self.assertEqual(events[0]["platform_event_id"], "c1")
        self.assertEqual(events[0]["url"], "https://www.youtube.com/watch?v=abc123")
        self.assertEqual(events[0]["title"], "Great EV review")
        self.assertEqual(events[0]["conversation_id"], "abc123")
        self.assertEqual(events[0]["like_count"], 7)
        self.assertEqual(events[0]["view_count"], 1000)

    def test_loads_x_jsonl_as_raw_events(self):
        payload = (
            json.dumps(
                {
                    "status_id": "42",
                    "tweet_url": "https://x.com/user/status/42",
                    "screen_name": "driver",
                    "tweet_text": "Charging was fast",
                    "tweet_time_iso": "2026-06-01T10:00:00Z",
                    "like_count": "1.2K",
                    "view_count": "10K",
                }
            )
            + "\n"
        ).encode("utf-8")

        events = MANUAL_IMPORT.load_import_events("x.jsonl", payload)

        self.assertEqual(events[0]["source"], "x")
        self.assertEqual(events[0]["platform_event_id"], "42")
        self.assertEqual(events[0]["conversation_id"], "42")
        self.assertEqual(events[0]["x_account"], "driver")
        self.assertEqual(events[0]["like_count"], 1200)
        self.assertEqual(events[0]["view_count"], 10000)

    def test_loads_reddit_json_with_forced_source(self):
        payload = json.dumps(
            [
                {
                    "comment_id": "r1",
                    "post_url": "https://reddit.com/r/ev/comments/post",
                    "author": "user",
                    "comment_text": "Battery range matters",
                    "created_utc": 1780308000,
                }
            ]
        ).encode("utf-8")

        events = MANUAL_IMPORT.load_import_events(
            "comments.json",
            payload,
            source="reddit",
        )

        self.assertEqual(events[0]["source"], "reddit")
        self.assertEqual(events[0]["platform_event_id"], "r1")
        self.assertEqual(events[0]["conversation_id"], "post")
        self.assertEqual(events[0]["subreddit"], "ev")
        self.assertEqual(events[0]["like_count"], None)
        self.assertTrue(events[0]["timestamp"].startswith("2026-06-01"))

    def test_manual_import_dag_consumes_json_manual_topics(self):
        source = (
            ROOT / "orchestrator" / "dags" / "manual_file_import_lakehouse.py"
        ).read_text(encoding="utf-8")

        self.assertIn('dag_id="manual_file_import_lakehouse"', source)
        self.assertIn("manual.youtube.raw.events", source)
        self.assertIn("manual.x.raw.events", source)
        self.assertIn("manual.reddit.raw.events", source)
        self.assertIn("CLEAN_SOURCE_VALUE_FORMAT=json", source)


if __name__ == "__main__":
    unittest.main()
