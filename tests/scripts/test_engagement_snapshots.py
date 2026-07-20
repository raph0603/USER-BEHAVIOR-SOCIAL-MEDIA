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

import pytest


pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

BATCH_PATH = ROOT / "spark" / "jobs" / "batch"
sys.path.insert(0, str(BATCH_PATH))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import engagement_snapshots as es
import youtube_engagement_velocity as velocity


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

    def test_known_zero_is_preserved_and_marked_available(self):
        row = es.build_snapshots_from_updates(
            self.spark,
            [self._make_update(view_count=0, like_count=0, comment_count=0)],
        ).collect()[0]

        self.assertEqual(row["view_count"], 0)
        self.assertTrue(row["view_count_available"])
        self.assertTrue(row["like_count_available"])
        self.assertIsNone(row["engagement_rate"])

    def test_missing_metric_is_not_coalesced_to_zero(self):
        row = es.build_snapshots_from_updates(
            self.spark,
            [self._make_update(like_count=None, like_count_available=False)],
        ).collect()[0]

        self.assertIsNone(row["like_count"])
        self.assertFalse(row["like_count_available"])
        self.assertIsNone(row["engagement_rate"])

    def test_counter_decrease_does_not_create_negative_delta(self):
        first = es.build_snapshots_from_updates(self.spark, [self._make_update()])
        second = es.build_snapshots_from_updates(
            self.spark,
            [
                self._make_update(
                    metadata_refreshed_at="2026-06-01T12:00:00",
                    view_count=900,
                )
            ],
            previous_snapshots=first,
        ).collect()[0]

        self.assertIsNone(second["views_delta"])
        self.assertIsNone(second["views_per_hour"])

    def test_observation_identity_is_deterministic_for_replay(self):
        update = self._make_update()
        first = es.build_snapshots_from_updates(self.spark, [update]).collect()[0]
        replay = es.build_snapshots_from_updates(self.spark, [update]).collect()[0]

        self.assertEqual(first["observation_id"], replay["observation_id"])
        self.assertEqual(len(first["observation_id"]), 64)

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
            "metrics_refresh_status",
            "producer_name",
            "producer_run_id",
            "coverage_json",
            "view_count_available",
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

    def test_snapshot_write_uses_insert_only_merge(self):
        source = (ROOT / "spark" / "jobs" / "batch" / "engagement_snapshots.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('dropDuplicates(["observation_id"])', source)
        self.assertIn("MERGE INTO lakehouse.silver.engagement_snapshots", source)
        self.assertIn("WHEN NOT MATCHED THEN", source)
        self.assertNotIn("WHEN MATCHED THEN", source)
        self.assertNotIn('join(existing_ids, ["observation_id"], "left_anti")', source)
        self.assertNotIn("DELETE FROM lakehouse.silver.engagement_snapshots", source)

    def test_snapshot_migration_preserves_a_backup_for_duplicate_cleanup(self):
        source = (
            ROOT / "spark" / "jobs" / "maintenance" / "migrate_engagement_snapshots.py"
        ).read_text(encoding="utf-8")

        self.assertIn('mode.add_argument("--dry-run"', source)
        self.assertIn('mode.add_argument("--apply"', source)
        self.assertIn("validated_staging_switch", source)
        self.assertIn("engagement_snapshots_backup_", source)
        self.assertNotIn("DROP TABLE", source)


class YouTubeVelocityAvailabilityTests(unittest.TestCase):
    SNAPSHOT_SCHEMA = StructType(
        [
            StructField("source", StringType(), False),
            StructField("platform_event_id", StringType(), False),
            StructField("observed_at", TimestampType(), False),
            StructField("view_count", LongType(), True),
            StructField("like_count", LongType(), True),
            StructField("comment_count", LongType(), True),
            StructField("views_delta", LongType(), True),
            StructField("likes_delta", LongType(), True),
            StructField("comments_delta", LongType(), True),
            StructField("views_per_hour", DoubleType(), True),
            StructField("likes_per_hour", DoubleType(), True),
            StructField("comments_per_hour", DoubleType(), True),
            StructField("like_rate", DoubleType(), True),
            StructField("comment_rate", DoubleType(), True),
            StructField("engagement_rate", DoubleType(), True),
            StructField("views_acceleration", DoubleType(), True),
        ]
    )

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("YouTubeVelocityAvailabilityTests")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _snapshot(self, **overrides):
        row = {
            "source": "youtube",
            "platform_event_id": "video-1",
            "observed_at": datetime(2026, 7, 20, 1, 0, 0),
            "view_count": 100,
            "like_count": 10,
            "comment_count": 1,
            "views_delta": 10,
            "likes_delta": 1,
            "comments_delta": 1,
            "views_per_hour": 10.0,
            "likes_per_hour": 1.0,
            "comments_per_hour": 1.0,
            "like_rate": 0.1,
            "comment_rate": 0.01,
            "engagement_rate": 0.11,
            "views_acceleration": 2.0,
        }
        row.update(overrides)
        return row

    def test_unknown_input_keeps_virality_unknown(self):
        snapshots = self.spark.createDataFrame(
            [self._snapshot(engagement_rate=None)],
            schema=self.SNAPSHOT_SCHEMA,
        )

        row = velocity.build_latest_velocity(snapshots, threshold=8.0).collect()[0]

        self.assertIsNone(row["virality_score"])
        self.assertIsNone(row["is_viral"])

    def test_known_zero_inputs_produce_a_real_zero_score(self):
        snapshots = self.spark.createDataFrame(
            [
                self._snapshot(
                    views_per_hour=0.0,
                    engagement_rate=0.0,
                    views_acceleration=0.0,
                )
            ],
            schema=self.SNAPSHOT_SCHEMA,
        )

        row = velocity.build_latest_velocity(snapshots, threshold=8.0).collect()[0]

        self.assertEqual(row["virality_score"], 0.0)
        self.assertFalse(row["is_viral"])


if __name__ == "__main__":
    unittest.main()
