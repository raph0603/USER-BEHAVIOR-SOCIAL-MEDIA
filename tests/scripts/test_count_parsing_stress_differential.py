import sys
from unittest.mock import MagicMock

# Mock missing dependencies before importing playwright scripts
sys.modules['googleapiclient'] = MagicMock()
sys.modules['googleapiclient.discovery'] = MagicMock()
sys.modules['playwright'] = MagicMock()
sys.modules['playwright.sync_api'] = MagicMock()

import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]

# Load engagement module
sys.path.insert(0, str(ROOT / "playwright"))
ENG_PATH = ROOT / "playwright" / "engagement.py"
ENG_SPEC = importlib.util.spec_from_file_location("engagement", ENG_PATH)
ENGAGEMENT = importlib.util.module_from_spec(ENG_SPEC)
ENG_SPEC.loader.exec_module(ENGAGEMENT)

# Load youtube_authors module
YT_PATH = ROOT / "playwright" / "youtube_authors.py"
YT_SPEC = importlib.util.spec_from_file_location("youtube_authors", YT_PATH)
YOUTUBE_AUTHORS = importlib.util.module_from_spec(YT_SPEC)
YT_SPEC.loader.exec_module(YOUTUBE_AUTHORS)

# Load reddit_json_crawler module
REDDIT_PATH = ROOT / "playwright" / "reddit_json_crawler.py"
REDDIT_SPEC = importlib.util.spec_from_file_location("reddit_json_crawler", REDDIT_PATH)
REDDIT_CRAWLER = importlib.util.module_from_spec(REDDIT_SPEC)
REDDIT_SPEC.loader.exec_module(REDDIT_CRAWLER)

# Load insight_refresh module
REFRESH_PATH = ROOT / "playwright" / "insight_refresh.py"
REFRESH_SPEC = importlib.util.spec_from_file_location("insight_refresh", REFRESH_PATH)
INSIGHT_REFRESH = importlib.util.module_from_spec(REFRESH_SPEC)
REFRESH_SPEC.loader.exec_module(INSIGHT_REFRESH)

# Load manual_import module
DASHBOARD_PATH = ROOT / "dashboard" / "manual_import.py"
DASHBOARD_SPEC = importlib.util.spec_from_file_location("manual_import", DASHBOARD_PATH)
MANUAL_IMPORT = importlib.util.module_from_spec(DASHBOARD_SPEC)
DASHBOARD_SPEC.loader.exec_module(MANUAL_IMPORT)

# Load replay_csv_to_kafka module
REPLAY_PATH = ROOT / "scripts" / "replay_csv_to_kafka.py"
REPLAY_SPEC = importlib.util.spec_from_file_location("replay_csv_to_kafka", REPLAY_PATH)
REPLAY = importlib.util.module_from_spec(REPLAY_SPEC)
REPLAY_SPEC.loader.exec_module(REPLAY)


class DifferentialStressTests(unittest.TestCase):
    # -------------------------------------------------------------
    # 1. Differential test for the three parse_count implementations
    # -------------------------------------------------------------
    def test_parse_count_differential(self):
        # A test suite of inputs to verify differential behavior
        test_cases = [
            # Standard formats
            ("123", 123),
            ("1.2K", 1200),
            ("3.5M", 3500000),
            ("2B", 2000000000),
            ("1,234", 1234),
            ("0", 0),
            # Extreme values
            ("100B", 100000000000),
            ("-500", -500),
            ("-1.2K", -1200),
            # Large digits (overflow check)
            ("1" + "0" * 309, None), # Should return None or handle gracefully
            # Whitespace and empty
            (None, None),
            ("", None),
            ("   ", None),
            ("\n\t  ", None),
            ("nan", None),
            ("None", None),
            ("null", None),
            ("<na>", None),
            # Non-standard multiplier/text
            ("100T", 100), # Suffix T is not handled, treated as 100
            ("1.2M subscribers", 1200000),
            ("123 followers and 456 following", 123),
            ("  @#$%-12.4K  ", -12400),
            # Types other than strings
            (True, 1),
            (False, 0),
            (1234, 1234),
            (12.34, 12),
            ([], None),
            ([123], 123),
            ({}, None),
            ({"key": 456}, 456),
        ]

        for val, expected in test_cases:
            # Run engagement.parse_count
            try:
                res_eng = ENGAGEMENT.parse_count(val)
            except Exception as e:
                res_eng = f"CRASH: {type(e).__name__}: {e}"

            # Run manual_import._parse_count
            try:
                res_manual = MANUAL_IMPORT._parse_count(val)
            except Exception as e:
                res_manual = f"CRASH: {type(e).__name__}: {e}"

            # Run replay_csv_to_kafka._parse_count
            try:
                res_replay = REPLAY._parse_count(val)
            except Exception as e:
                res_replay = f"CRASH: {type(e).__name__}: {e}"

            # Print to stdout for visibility if any output differs or if crashes occurred
            if "CRASH" in str(res_eng) or "CRASH" in str(res_manual) or "CRASH" in str(res_replay):
                print(f"INPUT: {repr(val)} -> ENG: {res_eng} | MANUAL: {res_manual} | REPLAY: {res_replay}")
            
            # Assertions to enforce no crashes
            self.assertFalse(isinstance(res_eng, str) and "CRASH" in res_eng, f"engagement crashed on {repr(val)}: {res_eng}")
            self.assertFalse(isinstance(res_manual, str) and "CRASH" in res_manual, f"manual_import crashed on {repr(val)}: {res_manual}")
            self.assertFalse(isinstance(res_replay, str) and "CRASH" in res_replay, f"replay_csv crashed on {repr(val)}: {res_replay}")

    # -------------------------------------------------------------
    # 2. YouTube Subscriber Count Extraction Stress Tests
    # -------------------------------------------------------------
    def test_youtube_extraction_crashes(self):
        # We test extract_youtube_subscriber_count on malformed HTML and types
        test_inputs = [
            None,
            123,
            True,
            "",
            "   ",
            "var ytInitialData = ",
            "var ytInitialData = {",
            "var ytInitialData = 123;",
            "var ytInitialData = [];",
            "var ytInitialData = {'a': 1};", # Invalid JSON (single quotes)
            "var ytInitialData = {\"videoOwnerRenderer\": null};",
            "var ytInitialData = {\"videoOwnerRenderer\": {\"subscriberCountText\": null}};",
            "var ytInitialData = {\"videoOwnerRenderer\": {\"subscriberCountText\": 1234}};",
            "var ytInitialData = {\"videoOwnerRenderer\": {\"subscriberCountText\": {\"runs\": null}}};",
            "var ytInitialData = {\"videoOwnerRenderer\": {\"subscriberCountText\": {\"runs\": [\"not-a-dict\"]}}};",
            "var ytInitialData = {\"videoOwnerRenderer\": {\"subscriberCountText\": {\"runs\": [{\"text\": 123}]}}};",
        ]

        for html in test_inputs:
            try:
                res = YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html)
                # Should return None instead of crashing
                if html is not None and not isinstance(html, str):
                    self.assertIsNone(res)
            except Exception as e:
                self.fail(f"YouTube extractor crashed on HTML {repr(html)}: {type(e).__name__}: {e}")

    # -------------------------------------------------------------
    # 3. Reddit Subreddit Member Count Extraction Stress Tests
    # -------------------------------------------------------------
    def test_reddit_crawler_crashes(self):
        # We test fetch_post_comments where payload has malformed formats
        malformed_payloads = [
            None,
            [],
            [{}],
            [{}, {}],
            # children list is None/empty
            [{"data": {"children": None}}, {}],
            [{"data": {"children": []}}, {}],
            # children element is None/empty
            [{"data": {"children": [None]}}, {}],
            [{"data": {"children": [{}]}}, {}],
            # data is None
            [{"data": {"children": [{"data": None}]}}, {}],
            # data is not a dict
            [{"data": {"children": [{"data": 1234}]}}, {}],
            [{"data": {"children": [{"data": "string"}]}}, {}],
            [{"data": {"children": [{"data": []}]}}, {}],
            # Normal dict but missing subscribers field
            [{"data": {"children": [{"data": {}}]}}, {}],
        ]

        for payload in malformed_payloads:
            with patch("requests.get") as mock_get:
                mock_get.return_value.status_code = 200
                mock_get.return_value.json.return_value = payload
                try:
                    # Execute fetch_post_comments
                    rows = REDDIT_CRAWLER.fetch_post_comments("https://reddit.com/r/test/comments/123")
                    # If it successfully parses, we expect rows to be empty since no comments exist
                    self.assertEqual(rows, [])
                except Exception as e:
                    self.fail(f"Reddit crawler crashed on payload {repr(payload)}: {type(e).__name__}: {e}")

    def test_reddit_insight_refresh_crashes(self):
        # We test insight_refresh _refresh_reddit where payload is malformed
        malformed_payloads = [
            [{"data": {"children": [{"data": None}]}}, {"data": {"children": [{"kind": "t1", "data": {"author": "u1", "body": "hi", "id": "c1"}}]}}],
            [{"data": {"children": [{"data": 123}]}}, {"data": {"children": [{"kind": "t1", "data": {"author": "u1", "body": "hi", "id": "c1"}}]}}],
            [{"data": {"children": [{"data": []}]}}, {"data": {"children": [{"kind": "t1", "data": {"author": "u1", "body": "hi", "id": "c1"}}]}}],
        ]
        targets = [{"url": "https://reddit.com/r/test/comments/123/title/c1", "source": "reddit", "platform_event_id": "c1"}]

        for payload in malformed_payloads:
            with patch("requests.get") as mock_get:
                mock_get.return_value.status_code = 200
                mock_get.return_value.json.return_value = payload
                try:
                    updates = INSIGHT_REFRESH._refresh_reddit(targets)
                    # We expect it to either fail to find the comment or handle data=None gracefully
                except Exception as e:
                    self.fail(f"Reddit insight refresh crashed on payload {repr(payload)}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    unittest.main()
