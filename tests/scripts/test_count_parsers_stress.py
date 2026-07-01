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


class StressTestCountParsers(unittest.TestCase):
    # -------------------------------------------------------------
    # 1. parse_count Stress Tests (X, YouTube, manual_import)
    # -------------------------------------------------------------
    def test_parse_count_standard(self):
        self.assertEqual(ENGAGEMENT.parse_count("1.2K"), 1200)
        self.assertEqual(ENGAGEMENT.parse_count("3.5M"), 3500000)
        self.assertEqual(ENGAGEMENT.parse_count("2B"), 2000000000)
        self.assertEqual(ENGAGEMENT.parse_count("150"), 150)
        self.assertEqual(ENGAGEMENT.parse_count("1,234"), 1234)

    def test_parse_count_extreme_values(self):
        # Zero
        self.assertEqual(ENGAGEMENT.parse_count("0"), 0)
        # Negative values (valid according to regex -? but semantically weird)
        self.assertEqual(ENGAGEMENT.parse_count("-500"), -500)
        self.assertEqual(ENGAGEMENT.parse_count("-1.2K"), -1200)
        # Large multiplier (Billion)
        self.assertEqual(ENGAGEMENT.parse_count("100B"), 100000000000)
        # Suffix not handled (T - trillion defaults to multiplier 1)
        self.assertEqual(ENGAGEMENT.parse_count("100T"), 100)

    def test_parse_count_overflow(self):
        # Desired: Handle overflow gracefully by returning None instead of raising OverflowError
        large_number_str = "1" + "0" * 309
        self.assertIsNone(ENGAGEMENT.parse_count(large_number_str))

    def test_parse_count_empty_and_whitespace(self):
        self.assertIsNone(ENGAGEMENT.parse_count(None))
        self.assertIsNone(ENGAGEMENT.parse_count(""))
        self.assertIsNone(ENGAGEMENT.parse_count("   "))
        self.assertIsNone(ENGAGEMENT.parse_count("\n\t  "))

    def test_parse_count_types(self):
        # Booleans
        self.assertEqual(ENGAGEMENT.parse_count(True), 1)
        self.assertEqual(ENGAGEMENT.parse_count(False), 0)
        # Integers and Floats
        self.assertEqual(ENGAGEMENT.parse_count(1234), 1234)
        self.assertEqual(ENGAGEMENT.parse_count(12.34), 12)

    def test_parse_count_malformed_types(self):
        # Lists and Dictionaries - check for unexpected number matching from str() representation
        self.assertEqual(ENGAGEMENT.parse_count([123]), 123)
        self.assertEqual(ENGAGEMENT.parse_count({"key": 456}), 456)
        # Empty list/dict should return None
        self.assertIsNone(ENGAGEMENT.parse_count([]))
        self.assertIsNone(ENGAGEMENT.parse_count({}))

    def test_parse_count_alphanumeric_and_special_chars(self):
        self.assertEqual(ENGAGEMENT.parse_count("123 followers and 456 following"), 123)
        self.assertEqual(ENGAGEMENT.parse_count("1.2e3"), 1)
        self.assertEqual(ENGAGEMENT.parse_count("  @#$%-12.4K  "), -12400)
        self.assertEqual(ENGAGEMENT.parse_count("10万"), 10)

    # -------------------------------------------------------------
    # 2. extract_youtube_subscriber_count Stress Tests
    # -------------------------------------------------------------
    def test_youtube_sub_count_standard_simple_text(self):
        html = 'var ytInitialData = {"videoOwnerRenderer": {"subscriberCountText": {"simpleText": "1.23M subscribers"}}};'
        self.assertEqual(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html), 1230000)

    def test_youtube_sub_count_standard_runs(self):
        html = 'var ytInitialData = {"videoOwnerRenderer": {"subscriberCountText": {"runs": [{"text": "1.23M"}, {"text": " subscribers"}]}}};'
        self.assertEqual(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html), 1230000)

    def test_youtube_sub_count_missing_marker(self):
        html = "<html>No ytInitialData here</html>"
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html))

    def test_youtube_sub_count_malformed_html_types(self):
        # Desired: Handle None / numeric HTML inputs gracefully without throwing AttributeError
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(None))
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(12345))

    def test_youtube_sub_count_empty_and_whitespace_html(self):
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(""))
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count("    "))

    def test_youtube_sub_count_malformed_json(self):
        html = "var ytInitialData = {invalid json};"
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html))

    def test_youtube_sub_count_missing_owner_renderer(self):
        html = 'var ytInitialData = {"someOtherRenderer": {}};'
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html))

    def test_youtube_sub_count_missing_subscriber_count_text(self):
        html = 'var ytInitialData = {"videoOwnerRenderer": {}};'
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html))

    def test_youtube_sub_count_text_obj_is_integer(self):
        # Desired: Handle subscriberCountText being an integer gracefully
        html = 'var ytInitialData = {"videoOwnerRenderer": {"subscriberCountText": 1234}};'
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html))

    def test_youtube_sub_count_runs_contains_non_dict(self):
        # Desired: Handle runs containing a non-dict gracefully
        html = 'var ytInitialData = {"videoOwnerRenderer": {"subscriberCountText": {"runs": [{"text": "1.23M"}, "subscribers"]}}};'
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html))

    def test_youtube_sub_count_runs_contains_dict_with_integer_text(self):
        # Desired: Handle runs text being integer gracefully
        html = 'var ytInitialData = {"videoOwnerRenderer": {"subscriberCountText": {"runs": [{"text": 123}, {"text": " subscribers"}]}}};'
        self.assertIsNone(YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html))

    # -------------------------------------------------------------
    # 3. Reddit Subreddit Subscribers Parsing Stress Tests
    # -------------------------------------------------------------
    def test_reddit_crawler_subscribers_standard(self):
        payload_with_comment = [
            {"data": {"children": [{"data": {"subreddit_subscribers": 54321}}]}},
            {"data": {"children": [{"kind": "t1", "data": {"author": "u1", "body": "hi", "id": "c1"}}]}}
        ]
        with patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = payload_with_comment
            rows = REDDIT_CRAWLER.fetch_post_comments("https://reddit.com/r/test/comments/123/title/c1")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["subreddit_member_count"], 54321)

    def test_reddit_crawler_subscribers_data_is_none_crash(self):
        # Desired: Handle None in children[0]["data"] gracefully by returning rows with subreddit_member_count = None
        payload = [
            {"data": {"children": [{"data": None}]}},
            {"data": {"children": [{"kind": "t1", "data": {"author": "u1", "body": "hi", "id": "c1"}}]}}
        ]
        with patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = payload
            rows = REDDIT_CRAWLER.fetch_post_comments("https://reddit.com/r/test/comments/123/title/c1")
            self.assertEqual(len(rows), 1)
            self.assertIsNone(rows[0]["subreddit_member_count"])

    def test_reddit_crawler_subscribers_data_is_integer_crash(self):
        # Desired: Handle integer in children[0]["data"] gracefully
        payload = [
            {"data": {"children": [{"data": 1234}]}},
            {"data": {"children": [{"kind": "t1", "data": {"author": "u1", "body": "hi", "id": "c1"}}]}}
        ]
        with patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = payload
            rows = REDDIT_CRAWLER.fetch_post_comments("https://reddit.com/r/test/comments/123/title/c1")
            self.assertEqual(len(rows), 1)
            self.assertIsNone(rows[0]["subreddit_member_count"])

    def test_reddit_insight_refresh_subscribers_data_is_none_crash(self):
        # Desired: Handle None gracefully in insight_refresh
        payload = [
            {"data": {"children": [{"data": None}]}},
            {"data": {"children": [{"kind": "t1", "data": {"author": "u1", "body": "hi", "id": "c1"}}]}}
        ]
        targets = [{"url": "https://reddit.com/r/test/comments/123/title/c1", "source": "reddit", "platform_event_id": "c1"}]
        with patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = payload
            updates = INSIGHT_REFRESH._refresh_reddit(targets)
            self.assertEqual(len(updates), 1)
            self.assertIsNone(updates[0]["subreddit_member_count"])

    # -------------------------------------------------------------
    # 4. extract_x_followers Stress Tests
    # -------------------------------------------------------------
    def test_extract_x_followers_graceful_failures(self):
        class FailingLocator:
            def first(self):
                raise RuntimeError("Element not found")
            def count(self):
                return 0
        class FailingArticle:
            @property
            def page(self):
                raise RuntimeError("Page not found")
            def locator(self, selector):
                return FailingLocator()

        self.assertIsNone(ENGAGEMENT.extract_x_followers(FailingArticle()))


if __name__ == "__main__":
    unittest.main()
