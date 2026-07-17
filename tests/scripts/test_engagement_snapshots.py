"""
Tests for spark/jobs/batch/engagement_snapshots.py

Validates:
- build_snapshots_from_updates produces correct schema
- age_minutes is derived correctly
- snapshot_date matches observed_at date
- append-only table DDL does not contain UPDATE or MERGE
- SNAPSHOT_COLUMNS does not overwrite existing engagement rows
"""

import sys
import os
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
bat_wrapper = str(ROOT / "tests" / "scripts" / "run_python.bat")
os.environ["PYSPARK_PYTHON"] = bat_wrapper
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BATCH_PATH = ROOT / "spark" / "jobs" / "batch"
sys.path.insert(0, str(BATCH_PATH))

from pyspark.sql import SparkSession

import engagement_snapshots as es


class EngagementSnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("EngagementSnapshotTests")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _make_update(self, **overrides):
        base = {
            "user_id": "user123",
            "url": "https://example.com/post/1",
            "event_ts": "2026-06-01T10:00:00",
            "source": "youtube",
            "platform_event_id": "abc123",
            "metadata_refreshed_at": "2026-06-01T11:00:00",
            "owner_channel_id": None,
            "collaborator_channel_ids": None,
            "like_count": 42,
            "view_count": 1000,
            "comment_count": 5,
            "reply_count": 2,
            "retweet_count": 0,
            "bookmark_count": 3,
            "score": None,
            "follower_count": None,
            "subscriber_count": None,
            "subreddit_member_count": None,
        }
        base.update(overrides)
        return base

    def test_snapshot_has_correct_age_minutes(self):
        """1h gap → 60 minutes."""
        update = self._make_update(
            event_ts="2026-06-01T10:00:00",
            metadata_refreshed_at="2026-06-01T11:00:00",
        )
        df = es.build_snapshots_from_updates(self.spark, [update])
        self.assertIsNotNone(df)
        row = df.collect()[0]
        self.assertEqual(row["age_minutes"], 60)

    def test_snapshot_date_matches_observed_at(self):
        update = self._make_update(
            metadata_refreshed_at="2026-06-15T14:30:00",
        )
        df = es.build_snapshots_from_updates(self.spark, [update])
        row = df.collect()[0]
        self.assertEqual(str(row["snapshot_date"]), "2026-06-15")

    def test_snapshot_columns_are_present(self):
        update = self._make_update()
        df = es.build_snapshots_from_updates(self.spark, [update])
        row = df.collect()[0]
        for col_name in es._SNAPSHOT_COLUMNS:
            self.assertIn(col_name, row.asDict(), f"Missing column: {col_name}")

    def test_engagement_values_preserved(self):
        update = self._make_update(like_count=99, view_count=5000, score=12)
        df = es.build_snapshots_from_updates(self.spark, [update])
        row = df.collect()[0]
        self.assertEqual(row["like_count"], 99)
        self.assertEqual(row["view_count"], 5000)
        self.assertEqual(row["score"], 12)

    def test_none_returns_none_for_empty_updates(self):
        result = es.build_snapshots_from_updates(self.spark, [])
        self.assertIsNone(result)

    def test_multiple_updates_produce_multiple_rows(self):
        updates = [
            self._make_update(platform_event_id="p1", metadata_refreshed_at="2026-06-01T11:00:00"),
            self._make_update(platform_event_id="p1", metadata_refreshed_at="2026-06-01T17:00:00"),
            self._make_update(platform_event_id="p2", metadata_refreshed_at="2026-06-01T12:00:00"),
        ]
        df = es.build_snapshots_from_updates(self.spark, updates)
        self.assertEqual(df.count(), 3)


class EngagementSnapshotSchemaContractTests(unittest.TestCase):
    """Validate DDL and design contracts."""

    def test_create_table_sql_is_append_only(self):
        """The DDL must not contain UPDATE or MERGE logic — table is append-only."""
        sql_upper = es._CREATE_TABLE_SQL.upper()
        self.assertNotIn("MERGE", sql_upper)
        self.assertNotIn("UPDATE SET", sql_upper)

    def test_create_table_sql_contains_required_columns(self):
        required = {
            "source",
            "platform_event_id",
            "user_id",
            "url",
            "created_at",
            "observed_at",
            "age_minutes",
            "like_count",
            "view_count",
            "comment_count",
            "reply_count",
            "retweet_count",
            "bookmark_count",
            "score",
            "observation_id",
            "views_delta",
            "likes_delta",
            "comments_delta",
            "views_per_hour",
            "likes_per_hour",
            "comments_per_hour",
            "like_rate",
            "comment_rate",
            "engagement_rate",
            "views_acceleration",
            "snapshot_date",
        }
        for col_name in required:
            self.assertIn(col_name, es._CREATE_TABLE_SQL, f"Missing column: {col_name}")

    def test_metadata_refreshed_at_not_in_snapshot_columns(self):
        """
        metadata_refreshed_at maps to observed_at in the snapshot table.
        The raw column name should not appear in _SNAPSHOT_COLUMNS.
        """
        self.assertNotIn("metadata_refreshed_at", es._SNAPSHOT_COLUMNS)

    def test_observed_at_in_snapshot_columns(self):
        self.assertIn("observed_at", es._SNAPSHOT_COLUMNS)

    def test_silver_events_compatibility(self):
        """
        The engagement refresh job (apply_insight_updates.py) must still update
        silver.events.metadata_refreshed_at.  Verify that file still references
        the column so monitoring dashboard compatibility is preserved.
        """
        insight_path = ROOT / "spark" / "jobs" / "maintenance" / "apply_insight_updates.py"
        source = insight_path.read_text(encoding="utf-8")
        self.assertIn("metadata_refreshed_at", source)
        self.assertIn("lakehouse.silver.events", source)

    def test_snapshot_write_filters_existing_observation_ids(self):
        source = (ROOT / "spark" / "jobs" / "batch" / "engagement_snapshots.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('dropDuplicates(["observation_id"])', source)
        self.assertIn('join(existing_ids, ["observation_id"], "left_anti")', source)
        self.assertNotIn("DELETE FROM lakehouse.silver.engagement_snapshots", source)


if __name__ == "__main__":
    unittest.main()
