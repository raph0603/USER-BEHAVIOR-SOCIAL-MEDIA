import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "playwright" / "engagement.py"
SPEC = importlib.util.spec_from_file_location("engagement", MODULE_PATH)
ENGAGEMENT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ENGAGEMENT)


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


if __name__ == "__main__":
    unittest.main()
