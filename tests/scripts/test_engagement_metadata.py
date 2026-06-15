import importlib.util
import json
import unittest
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

    def test_avro_contract_contains_available_engagement_metadata(self):
        schema_path = ROOT / "schemas" / "playwright_event.avsc"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        field_names = {field["name"] for field in schema["fields"]}

        expected = {
            "like_count",
            "comment_count",
            "reply_count",
            "view_count",
            "retweet_count",
            "bookmark_count",
            "score",
        }
        self.assertTrue(expected.issubset(field_names))
        self.assertFalse(
            {
                "content_type",
                "crosspost_count",
                "event_id",
                "metadata_collected_at",
                "repost_count",
                "share_count",
            }
            & field_names
        )

    def test_engagement_metrics_are_propagated_to_silver(self):
        expected = {
            "like_count",
            "comment_count",
            "reply_count",
            "view_count",
            "retweet_count",
            "bookmark_count",
            "score",
        }
        paths = [
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
        ]

        for path in paths:
            source = path.read_text(encoding="utf-8")
            with self.subTest(path=path):
                for field_name in expected:
                    self.assertIn(field_name, source)

    def test_reddit_collector_discovers_recent_comments(self):
        producer = (ROOT / "playwright" / "producer.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('f"https://old.reddit.com/r/{subreddit}/comments/"', producer)
        self.assertIn("REDDIT_COMMENT_SCAN_LIMIT", producer)
        self.assertNotIn("?sort=top&t=month", producer)

    def test_youtube_search_supports_multiple_pages(self):
        producer = (ROOT / "playwright" / "producer.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('page_token = response.get("nextPageToken")', producer)
        self.assertIn("while len(video_ids) < max_results:", producer)

    def test_reddit_keyword_filter_uses_word_boundaries(self):
        producer = (ROOT / "playwright" / "producer.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("def _matches_keywords(", producer)
        self.assertIn(r'rf"\b{re.escape(normalized_keyword)}\b"', producer)


if __name__ == "__main__":
    unittest.main()
