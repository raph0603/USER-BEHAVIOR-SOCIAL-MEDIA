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
import inspect
import unittest
from pathlib import Path

import pytest


pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
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
        return self.spark.range(1).select(
            lit(raw_text).cast("string").alias("raw_text"),
            lit(text_for_model).cast("string").alias("text_for_model"),
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

    def test_privacy_token_counts_use_cleaned_model_text(self):
        df = self.spark.range(1).select(
            lit("<USER> <EMAIL> <URL> #Data 🚀").alias("raw_text"),
            lit("<USER> <EMAIL> <URL> #data 🚀").alias("text_for_model"),
            lit("<USER> <EMAIL> <URL> #Data 🚀").alias("clean_text"),
        )

        row = spf.compute_post_features(df).collect()[0]

        self.assertEqual(row["mention_token_count"], 1)
        self.assertEqual(row["email_token_count"], 1)
        self.assertEqual(row["phone_token_count"], 0)
        self.assertEqual(row["ip_token_count"], 0)
        self.assertEqual(row["url_token_count"], 1)
        self.assertEqual(row["hashtag_count"], 1)
        self.assertGreaterEqual(row["emoji_count"], 1)

    def test_empty_model_text_has_null_ratios_and_lexical_diversity(self):
        df = self._make_df("", "")
        row = spf.compute_post_features(df).collect()[0]

        self.assertIsNone(row["uppercase_character_ratio"])
        self.assertIsNone(row["digit_character_ratio"])
        self.assertIsNone(row["lexical_diversity"])

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

    def test_author_hash_falls_back_to_privacy_safe_user_id(self):
        df = self.spark.range(1).select(lit("hashed-user").alias("user_id"))

        row = spf.with_author_hash(df).collect()[0]

        self.assertEqual(row["author_hash"], "hashed-user")

    def test_existing_author_hash_is_preserved(self):
        df = self.spark.range(1).select(
            lit("hashed-user").alias("user_id"),
            lit("source-author").alias("author_hash"),
        )

        row = spf.with_author_hash(df).collect()[0]

        self.assertEqual(row["author_hash"], "source-author")

    def test_prepare_post_features_filters_unknown_text_and_sets_version(self):
        first = self.spark.range(1).select(
            lit("youtube").alias("source"),
            lit("video-1").alias("platform_event_id"),
            lit("hashed-user").alias("user_id"),
            lit("https://example.test/video-1").alias("url"),
            lit("2026-07-20T00:00:00Z").alias("event_ts"),
            lit("Hello #Spark").alias("raw_text"),
            lit("hello #spark").alias("text_for_model"),
            lit("Hello #Spark").alias("clean_text"),
        )
        second = self.spark.range(1).select(
            lit("youtube").alias("source"),
            lit("video-2").alias("platform_event_id"),
            lit("hashed-user").alias("user_id"),
            lit("https://example.test/video-2").alias("url"),
            lit("2026-07-20T00:01:00Z").alias("event_ts"),
            lit("Unavailable").alias("raw_text"),
            lit(None).cast("string").alias("text_for_model"),
            lit("Unavailable").alias("clean_text"),
        )
        df = first.unionByName(second).withColumn(
            "event_ts", spf.to_timestamp("event_ts")
        )

        rows = spf.prepare_post_features(df).collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["author_hash"], "hashed-user")
        self.assertEqual(rows[0]["feature_version"], spf._FEATURE_VERSION)
        self.assertEqual(rows[0]["hashtag_count"], 1)


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
            "character_count",
            "word_count",
            "sentence_count",
            "line_count",
            "mention_token_count",
            "email_token_count",
            "phone_token_count",
            "ip_token_count",
            "url_token_count",
            "question_mark_count",
            "exclamation_mark_count",
            "uppercase_character_ratio",
            "digit_character_ratio",
            "lexical_diversity",
            "like_count",
            "view_count",
            "reply_count",
            "retweet_count",
            "bookmark_count",
            "follower_count",
            "like_count_available",
            "view_count_available",
            "reply_count_available",
            "retweet_count_available",
            "bookmark_count_available",
            "follower_count_available",
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

    def test_available_now_reads_current_state_as_batch(self):
        source = inspect.getsource(spf.main)

        self.assertIn("refresh_post_features", source)
        self.assertNotIn('readStream.format("iceberg")', source)
        self.assertNotIn(".toTable(features_table)", source)

    def test_merge_identity_includes_source_and_null_safe_fallback(self):
        source = inspect.getsource(spf.merge_post_features)

        self.assertIn("t.source <=> s.source", source)
        self.assertIn("t.platform_event_id = s.platform_event_id", source)
        self.assertIn("t.user_id <=> s.user_id", source)
        self.assertIn("t.url <=> s.url", source)

    def test_emoji_pattern_uses_java_supplementary_codepoint_syntax(self):
        self.assertIn(r"\x{1F300}", spf._EMOJI_PATTERN)
        self.assertNotIn(r"\U0001F300", spf._EMOJI_PATTERN)


if __name__ == "__main__":
    unittest.main()
