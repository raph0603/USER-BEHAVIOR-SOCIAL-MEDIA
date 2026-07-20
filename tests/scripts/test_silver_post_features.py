"""
Tests for spark/jobs/batch/silver_post_features.py

Validates:
- compute_post_features derives correct text-length features
- hashtag / mention / url / emoji counts are extracted from raw_text
- has_question flag is set correctly
- feature_version column is present
- post_features schema does not overlap with monitoring-only columns
"""

import sys
import os
import unittest
from pathlib import Path

import pytest


pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
if os.name == "nt":
    os.environ["PYSPARK_PYTHON"] = str(ROOT / "tests" / "scripts" / "run_python.bat")
else:
    os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Add the batch jobs folder to the path so we can import the module
BATCH_PATH = ROOT / "spark" / "jobs" / "batch"
sys.path.insert(0, str(BATCH_PATH))

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

import silver_post_features as spf


class PostFeatureComputationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("PostFeatureTests")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _make_df(self, raw_text, text_for_model):
        return self.spark.createDataFrame(
            [(raw_text, text_for_model)],
            schema="raw_text STRING, text_for_model STRING",
        )

    def test_text_len_chars(self):
        df = self._make_df("hello world", "hello world")
        row = spf.compute_post_features(df).collect()[0]
        self.assertEqual(row["text_len_chars"], 11)

    def test_text_len_words(self):
        df = self._make_df("one two three", "one two three")
        row = spf.compute_post_features(df).collect()[0]
        self.assertEqual(row["text_len_words"], 3)

    def test_has_question_true(self):
        df = self._make_df("Is this correct?", "is this correct?")
        row = spf.compute_post_features(df).collect()[0]
        self.assertEqual(row["has_question"], 1)

    def test_has_question_false(self):
        df = self._make_df("No question here.", "no question here.")
        row = spf.compute_post_features(df).collect()[0]
        self.assertEqual(row["has_question"], 0)

    def test_hashtag_count(self):
        df = self._make_df("Check #Python and #Spark!", "check #python and #spark!")
        row = spf.compute_post_features(df).collect()[0]
        self.assertEqual(row["hashtag_count"], 2)

    def test_hashtag_count_none(self):
        df = self._make_df("No hashtags here", "no hashtags here")
        row = spf.compute_post_features(df).collect()[0]
        self.assertEqual(row["hashtag_count"], 0)

    def test_mention_count(self):
        df = self._make_df("Hi @alice and @bob", "hi @alice and @bob")
        row = spf.compute_post_features(df).collect()[0]
        self.assertEqual(row["mention_count"], 2)

    def test_url_count(self):
        df = self._make_df("Visit https://example.com and http://foo.bar", "visit and")
        row = spf.compute_post_features(df).collect()[0]
        self.assertEqual(row["url_count"], 2)

    def test_emoji_count(self):
        df = self._make_df("Hello 😊 world 🚀", "hello world")
        row = spf.compute_post_features(df).collect()[0]
        self.assertGreaterEqual(row["emoji_count"], 2)

    def test_emoji_count_no_emojis(self):
        df = self._make_df("Plain text only", "plain text only")
        row = spf.compute_post_features(df).collect()[0]
        self.assertEqual(row["emoji_count"], 0)

    def test_null_text_for_model_produces_null_features(self):
        """Null text_for_model should produce null char/word lengths — not crash."""
        df = self._make_df("raw text", None)
        row = spf.compute_post_features(df).collect()[0]
        # char_len of NULL text is NULL in Spark
        self.assertIsNone(row["text_len_chars"])

    def test_feature_version_constant(self):
        """Feature version must be a non-empty string constant."""
        self.assertIsInstance(spf._FEATURE_VERSION, str)
        self.assertTrue(len(spf._FEATURE_VERSION) > 0)


class PostFeaturesSchemaContractTests(unittest.TestCase):
    """Validate the DDL and column contract for post_features."""

    def test_create_table_sql_contains_required_columns(self):
        expected = {
            "source",
            "platform_event_id",
            "user_id",
            "author_hash",
            "url",
            "event_ts",
            "event_date",
            "text_for_model",
            "clean_text",
            "text_len_chars",
            "text_len_words",
            "has_question",
            "hashtag_count",
            "mention_count",
            "url_count",
            "emoji_count",
            "feature_version",
        }
        for col_name in expected:
            self.assertIn(col_name, spf._CREATE_TABLE_SQL, f"Missing column: {col_name}")

    def test_upsert_columns_match_create_table(self):
        """Every upsert column must appear in the DDL."""
        for col_name in spf._UPSERT_COLUMNS:
            self.assertIn(
                col_name,
                spf._CREATE_TABLE_SQL,
                f"Column in _UPSERT_COLUMNS not in DDL: {col_name}",
            )

    def test_monitoring_columns_not_in_post_features(self):
        """
        Columns that belong to the monitoring table (silver.events) and carry
        no model-input value must NOT appear in post_features.
        """
        monitoring_only = {"error", "kafka_topic", "kafka_partition", "kafka_offset"}
        for col_name in monitoring_only:
            self.assertNotIn(
                col_name,
                spf._UPSERT_COLUMNS,
                f"Monitoring-only column leaked into post_features: {col_name}",
            )


if __name__ == "__main__":
    unittest.main()
