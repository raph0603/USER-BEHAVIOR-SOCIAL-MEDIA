import ast
import html as html_lib
import importlib.util
import json
import re
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "playwright" / "engagement.py"
SPEC = importlib.util.spec_from_file_location("engagement", MODULE_PATH)
ENGAGEMENT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ENGAGEMENT)


class FakeNode:
    def __init__(self, text="", aria_label=None):
        self.text = text
        self.aria_label = aria_label

    def inner_text(self, timeout):
        return self.text

    def get_attribute(self, name, timeout):
        if name == "aria-label":
            return self.aria_label
        return None


class FakeLocator:
    def __init__(self, node=None):
        self.node = node

    def count(self):
        return int(self.node is not None)

    @property
    def first(self):
        return self.node


class FakeArticle:
    def __init__(self, nodes):
        self.nodes = nodes

    def locator(self, selector):
        return FakeLocator(self.nodes.get(selector))


class EngagementMetadataTests(unittest.TestCase):
    def test_parse_count_supports_platform_abbreviations(self):
        cases = {
            "0": 0,
            "1,234": 1234,
            "1.2K likes": 1200,
            "3M views": 3_000_000,
            "-5 points": -5,
            "12.4K": 12400,
            "1.2M": 1200000,
            "3B": 3000000000,
        }
        for raw_value, expected in cases.items():
            with self.subTest(raw_value=raw_value):
                self.assertEqual(ENGAGEMENT.parse_count(raw_value), expected)

    def test_x_views_fall_back_to_analytics_link(self):
        article = FakeArticle(
            {
                'a[href*="/analytics"]': FakeNode(
                    text="1.2K",
                    aria_label="1.2K views. View post analytics",
                )
            }
        )

        self.assertEqual(
            ENGAGEMENT.extract_x_metric(article, "analytics"),
            1200,
        )

    def test_x_analytics_link_without_count_means_zero_views(self):
        article = FakeArticle(
            {
                'a[href*="/analytics"]': FakeNode(
                    aria_label="View post analytics",
                )
            }
        )

        self.assertEqual(
            ENGAGEMENT.extract_x_metric(article, "analytics"),
            0,
        )

    def test_x_metric_still_supports_data_testid(self):
        article = FakeArticle(
            {
                '[data-testid="like"]': FakeNode(
                    aria_label="23 Likes. Like",
                )
            }
        )

        self.assertEqual(ENGAGEMENT.extract_x_metric(article, "like"), 23)

    def test_avro_contract_contains_engagement_metadata(self):
        schema_path = ROOT / "schemas" / "playwright_event.avsc"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        field_names = {field["name"] for field in schema["fields"]}

        expected = {
            "platform_event_id",
            "content_id",
            "root_content_id",
            "content_type",
            "metadata_collected_at",
            "like_count",
            "view_count",
            "bookmark_count",
            "comment_count",
            "reply_count",
            "retweet_count",
            "score",
            "follower_count",
            "subscriber_count",
            "subreddit_member_count",
        }
        self.assertTrue(expected.issubset(field_names))
        self.assertFalse(
            {
                "crosspost_count",
                "event_id",
                "repost_count",
                "share_count",
            }
            & field_names
        )

    def test_engagement_metrics_are_propagated_to_silver(self):
        expected = {
            "platform_event_id",
            "like_count",
            "view_count",
            "bookmark_count",
            "comment_count",
            "reply_count",
            "retweet_count",
            "score",
            "follower_count",
            "subscriber_count",
            "subreddit_member_count",
        }
        paths = [
            ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py",
            ROOT
            / "spark"
            / "jobs"
            / "streaming"
            / "kafka_to_iceberg_bronze.py",
            ROOT / "spark" / "jobs" / "batch" / "bronze_to_silver.py",
            ROOT
            / "spark"
            / "jobs"
            / "batch"
            / "bronze_to_silver_from_kafka.py",
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "apply_insight_updates.py",
            ROOT / "playwright" / "insight_refresh.py",
        ]
        shared_contract = (
            ROOT / "spark" / "jobs" / "event_contract.py"
        ).read_text(encoding="utf-8")
        shared_stages = {
            "collector_stream_pipeline.py",
            "kafka_to_iceberg_bronze.py",
            "bronze_to_silver.py",
            "bronze_to_silver_from_kafka.py",
        }

        for path in paths:
            source = path.read_text(encoding="utf-8")
            if path.name in shared_stages:
                self.assertIn("event_contract", source)
                source += shared_contract
            with self.subTest(path=path):
                for field_name in expected:
                    self.assertIn(field_name, source)

    def test_entity_relationship_fields_are_propagated_to_core_pipeline(self):
        expected = {
            "subreddit",
            "subreddit_title",
            "subreddit_description",
            "subreddit_created_at",
            "subreddit_visibility",
            "subreddit_weekly_visitors",
            "subreddit_weekly_contributions",
            "x_account",
            "youtube_channel_name",
            "thumbnail_url",
            "language",
            "parent_interaction_id",
            "conversation_id",
            "transcript_text",
            "transcript_segments_json",
            "duration_seconds",
            "has_auto_captions",
        }
        paths = [
            ROOT / "schemas" / "playwright_event.avsc",
            ROOT / "playwright" / "producer.py",
            ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py",
            ROOT
            / "spark"
            / "jobs"
            / "streaming"
            / "kafka_to_iceberg_bronze.py",
            ROOT / "spark" / "jobs" / "batch" / "bronze_to_silver.py",
            ROOT
            / "spark"
            / "jobs"
            / "batch"
            / "bronze_to_silver_from_kafka.py",
            ROOT / "spark" / "jobs" / "batch" / "content_analytics.py",
        ]
        shared_contract = (
            ROOT / "spark" / "jobs" / "event_contract.py"
        ).read_text(encoding="utf-8")
        shared_stages = {
            "collector_stream_pipeline.py",
            "kafka_to_iceberg_bronze.py",
            "bronze_to_silver.py",
            "bronze_to_silver_from_kafka.py",
        }

        for path in paths:
            source = path.read_text(encoding="utf-8")
            if path.name in shared_stages:
                self.assertIn("event_contract", source)
                source += shared_contract
            with self.subTest(path=path):
                for field_name in expected:
                    self.assertIn(field_name, source)

    def test_reddit_score_is_propagated_separately(self):
        source = (ROOT / "playwright" / "insight_refresh.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('"score"', source)
        self.assertNotIn('"like_count": score', source)

    def test_cleaning_tolerates_malformed_avro_records(self):
        source = (
            ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py"
        ).read_text(encoding="utf-8")

        self.assertIn('{"mode": "PERMISSIVE"}', source)
        self.assertNotIn("FAILFAST", source)

    def test_reddit_collector_discovers_recent_comments(self):
        producer = (ROOT / "playwright" / "producer.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("LOGGER = logging.getLogger(__name__)", producer)
        self.assertIn('_env_int("PRODUCER_MAX_EVENTS", 50)', producer)
        self.assertIn('_env_int("REDDIT_COMMENT_SCAN_LIMIT", 1000)', producer)
        self.assertIn('f"https://old.reddit.com/r/{subreddit}/comments/', producer)
        self.assertIn("REDDIT_COMMENT_SCAN_LIMIT", producer)
        self.assertIn('page.locator("span.next-button a")', producer)
        self.assertIn("while listing_url and scanned_comments < scan_limit:", producer)
        self.assertIn("Reddit listing did not load", producer)
        self.assertIn("_collect_reddit_feed_events", producer)
        self.assertIn("trying RSS fallback", producer)
        self.assertNotIn("raise SystemExit(99)", producer)
        self.assertIn("Treating this as a load/parsing failure", producer)
        self.assertNotIn("?sort=top&t=month", producer)
        self.assertNotIn("Reddit online collection found no public comments", producer)

    def test_x_collector_is_bounded_and_optionally_strict(self):
        producer = (ROOT / "playwright" / "producer.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("X_SEARCH_NAVIGATION_TIMEOUT_MS", producer)
        self.assertIn("X_FAIL_ON_ERROR", producer)
        self.assertIn("X online collection explicitly ignored by configuration", producer)
        self.assertIn('_env_bool("X_FAIL_ON_ERROR", True)', producer)
        self.assertIn("_click_x_google_login_button", producer)
        self.assertIn("X_LOGIN_DEBUG_DIR", producer)
        self.assertIn("CollectorSoftBlock", producer)
        self.assertIn("Collector soft-blocked", producer)
        self.assertIn("_is_auth_or_quota_block", producer)

    def test_producer_emits_contract_engagement_metrics(self):
        producer = (ROOT / "playwright" / "producer.py").read_text(
            encoding="utf-8"
        )

        for field_name in (
            "comment_count",
            "reply_count",
            "retweet_count",
            "bookmark_count",
            "score",
        ):
            self.assertIn(f'"{field_name}"', producer)

    def test_youtube_search_supports_multiple_pages(self):
        producer = (ROOT / "playwright" / "producer.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('page_token = response.get("nextPageToken")', producer)
        self.assertIn("while len(video_ids) < max_results:", producer)
        self.assertIn("YOUTUBE_COMMENT_MAX_PAGES", producer)
        self.assertIn("YOUTUBE_TRANSCRIPT_MAX_FAILURES", producer)

    def test_reddit_keyword_filter_uses_word_boundaries(self):
        producer = (ROOT / "playwright" / "producer.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("def _matches_keywords(", producer)
        self.assertIn(r'rf"\b{re.escape(normalized_keyword)}\b"', producer)

    def test_extract_x_followers_success(self):
        class FakeElement:
            def hover(self):
                pass
        class FakeLocator:
            def __init__(self, elem):
                self.elem = elem
            @property
            def first(self):
                return self.elem
        class FakeLinks:
            def __init__(self, texts):
                self.texts = texts
            def count(self):
                return len(self.texts)
            def nth(self, idx):
                class Item:
                    def __init__(self, txt):
                        self.txt = txt
                    def inner_text(self, timeout=None):
                        return self.txt
                return Item(self.texts[idx])
        class FakeHoverCard:
            def __init__(self, links):
                self._links = links
            def wait_for(self, state=None, timeout=None):
                pass
            def locator(self, selector):
                return self._links
        class FakePage:
            def __init__(self, hover_card):
                self._hover_card = hover_card
            def locator(self, selector):
                return self._hover_card
        class FakeArticle:
            def __init__(self, page, first_elem):
                self.page = page
                self._first_elem = first_elem
            def locator(self, selector):
                return FakeLocator(self._first_elem)

        links = FakeLinks(["12.4K Followers"])
        hover_card = FakeHoverCard(links)
        page = FakePage(hover_card)
        article = FakeArticle(page, FakeElement())

        followers = ENGAGEMENT.extract_x_followers(article)
        self.assertEqual(followers, 12400)

    def test_extract_reddit_json_member_count(self):
        import importlib.util
        from unittest.mock import patch
        r_module_path = ROOT / "playwright" / "reddit_json_crawler.py"
        r_spec = importlib.util.spec_from_file_location("reddit_json_crawler", r_module_path)
        reddit_json = importlib.util.module_from_spec(r_spec)
        r_spec.loader.exec_module(reddit_json)

        class FakeResponse:
            status_code = 200

            def raise_for_status(self):
                pass
            def json(self):
                return [
                    {
                        "data": {
                            "children": [
                                {
                                    "data": {
                                        "subreddit_subscribers": 54321
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "data": {
                            "children": [
                                {
                                    "kind": "t1",
                                    "data": {
                                        "author": "user1",
                                        "body": "hello",
                                        "id": "c1",
                                        "parent_id": "p1",
                                        "created_utc": 1600000000,
                                        "score": 10,
                                        "permalink": "/r/test/comments/123/c1/"
                                    }
                                }
                            ]
                        }
                    
                    }
                ]
        
        with patch("requests.get", return_value=FakeResponse()):
            rows = reddit_json.fetch_post_comments("https://reddit.com/r/test/comments/123")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["subreddit_member_count"], 54321)

    def test_reddit_subreddit_about_json_populates_community_metadata(self):
        source = (ROOT / "playwright" / "producer.py").read_text(encoding="utf-8")
        module = ast.parse(source)
        function_node = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "_extract_reddit_subreddit_about_json"
        )

        class FakeLogger:
            def warning(self, *args, **kwargs):
                pass

        class FakeResponse:
            def raise_for_status(self):
                pass

            def json(self):
                return {
                    "data": {
                        "title": "Electric Vehicles",
                        "public_description": "EV discussion and news",
                        "created_utc": 1262304000,
                        "subreddit_type": "public",
                        "subscribers": 20456209,
                    }
                }

        class FakeRequests:
            RequestException = Exception

            @staticmethod
            def get(*args, **kwargs):
                return FakeResponse()

        namespace = {
            "datetime": datetime,
            "timezone": timezone,
            "LOGGER": FakeLogger(),
            "parse_count": ENGAGEMENT.parse_count,
            "requests": FakeRequests,
            "_clean_text": lambda value: " ".join(str(value or "").split()),
            "_env_int": lambda name, default: default,
            "_env_str": lambda name, default: default,
        }
        exec(
            compile(
                ast.Module(body=[function_node], type_ignores=[]),
                str(ROOT / "playwright" / "producer.py"),
                "exec",
            ),
            namespace,
        )

        info = namespace["_extract_reddit_subreddit_about_json"]("electricvehicles")

        self.assertEqual(info["subreddit_title"], "Electric Vehicles")
        self.assertEqual(info["subreddit_description"], "EV discussion and news")
        self.assertEqual(info["subreddit_created_at"], "2010-01-01")
        self.assertEqual(info["subreddit_visibility"], "public")
        self.assertEqual(info["subreddit_member_count"], 20456209)

    def test_reddit_subreddit_old_html_populates_title_and_description(self):
        source = (ROOT / "playwright" / "producer.py").read_text(encoding="utf-8")
        module = ast.parse(source)
        functions = [
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef)
            and node.name
            in {
                "_normalize_reddit_sidebar_label",
                "_parse_reddit_sidebar_count",
                "_extract_reddit_subreddit_old_html",
            }
        ]

        class FakeLogger:
            def warning(self, *args, **kwargs):
                pass

        class FakeResponse:
            text = """
            <html>
              <head>
                <title>Electric Vehicle News and Discussion</title>
                <meta name="description" content="The future of sustainable transportation is here!" />
              </head>
              <body></body>
            </html>
            """

            def raise_for_status(self):
                pass

        class FakeRequests:
            RequestException = Exception

            @staticmethod
            def get(*args, **kwargs):
                return FakeResponse()

        namespace = {
            "LOGGER": FakeLogger(),
            "html_lib": html_lib,
            "parse_count": ENGAGEMENT.parse_count,
            "re": re,
            "requests": FakeRequests,
            "STATIC_REDDIT_COMMUNITY_FALLBACKS": {},
            "unicodedata": __import__("unicodedata"),
            "_clean_text": lambda value: " ".join(str(value or "").split()),
            "_env_int": lambda name, default: default,
            "_env_str": lambda name, default: default,
        }
        exec(
            compile(
                ast.Module(body=functions, type_ignores=[]),
                str(ROOT / "playwright" / "producer.py"),
                "exec",
            ),
            namespace,
        )

        info = namespace["_extract_reddit_subreddit_old_html"]("electricvehicles")

        self.assertEqual(info["subreddit_title"], "Electric Vehicle News and Discussion")
        self.assertEqual(
            info["subreddit_description"],
            "The future of sustainable transportation is here!",
        )
        self.assertEqual(info["subreddit_visibility"], "public")


if __name__ == "__main__":
    unittest.main()
