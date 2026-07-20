import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.youtube_state import YouTubeStateStore
from common.youtube_usage_state import QuotaPolicy, decide_quota, quota_cost


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 20, 12, tzinfo=UTC)
QUOTA_ENV = {
    "YOUTUBE_DAILY_QUOTA_UNITS": "100",
    "YOUTUBE_RECENT_SNAPSHOT_RESERVE_UNITS": "20",
    "YOUTUBE_QUOTA_PRESSURE_RATIO": "0.80",
    "YOUTUBE_QUOTA_CRITICAL_RATIO": "0.95",
}


def _load_monitoring_job():
    path = ROOT / "spark" / "jobs" / "maintenance" / "youtube_api_usage.py"
    spec = importlib.util.spec_from_file_location("youtube_api_usage", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class QuotaPolicyTests(unittest.TestCase):
    def test_official_endpoint_costs_are_units_not_call_counts(self):
        self.assertEqual(quota_cost("search.list"), 100)
        self.assertEqual(quota_cost("videos.list"), 1)
        self.assertEqual(quota_cost("yt-dlp.extract_info"), 0)

    def test_recent_snapshots_can_spend_the_reserved_capacity(self):
        policy = QuotaPolicy(100, 20, 0.80, 0.95)

        secondary = decide_quota(
            policy,
            endpoint="commentThreads.list",
            workload="comments",
            requested_calls=10,
            used_units=75,
            recent_snapshot_units=0,
        )
        snapshots = decide_quota(
            policy,
            endpoint="videos.list",
            workload="recent_metrics",
            requested_calls=10,
            used_units=75,
            recent_snapshot_units=0,
        )

        self.assertEqual(secondary.allowed_calls, 5)
        self.assertEqual(secondary.reason, "recent_snapshot_reserve")
        self.assertEqual(snapshots.allowed_calls, 10)

    def test_secondary_workloads_are_suspended_at_pressure_threshold(self):
        policy = QuotaPolicy(100, 20, 0.80, 0.95)

        for workload in ("comments", "channels", "descriptive_metadata"):
            decision = decide_quota(
                policy,
                endpoint="commentThreads.list",
                workload=workload,
                requested_calls=1,
                used_units=80,
                recent_snapshot_units=0,
            )
            self.assertEqual(decision.allowed_calls, 0, workload)
            self.assertEqual(decision.reason, "secondary_workload_suspended")

    def test_invalid_policy_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "must not exceed"):
            QuotaPolicy.from_env(
                {
                    "YOUTUBE_DAILY_QUOTA_UNITS": "10",
                    "YOUTUBE_RECENT_SNAPSHOT_RESERVE_UNITS": "11",
                }
            )
        with self.assertRaisesRegex(ValueError, "must be >="):
            QuotaPolicy.from_env(
                {
                    "YOUTUBE_DAILY_QUOTA_UNITS": "100",
                    "YOUTUBE_RECENT_SNAPSHOT_RESERVE_UNITS": "10",
                    "YOUTUBE_QUOTA_PRESSURE_RATIO": "0.9",
                    "YOUTUBE_QUOTA_CRITICAL_RATIO": "0.8",
                }
            )


class MonitoringStateTests(unittest.TestCase):
    def test_old_usage_schema_is_migrated_with_derived_quota_units(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.sqlite"
            connection = sqlite3.connect(path)
            connection.execute(
                """
                CREATE TABLE youtube_api_usage (
                  usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
                  usage_date TEXT NOT NULL,
                  endpoint TEXT NOT NULL,
                  request_count INTEGER NOT NULL,
                  resource_count INTEGER NOT NULL,
                  success_count INTEGER NOT NULL,
                  error_count INTEGER NOT NULL,
                  quota_bucket TEXT NOT NULL,
                  observed_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO youtube_api_usage (
                  usage_date, endpoint, request_count, resource_count,
                  success_count, error_count, quota_bucket, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-07-20",
                    "search.list",
                    2,
                    20,
                    2,
                    0,
                    "discovery",
                    OBSERVED_AT.isoformat(),
                ),
            )
            connection.commit()
            connection.close()

            with YouTubeStateStore(path) as state:
                row = state.connection.execute("SELECT * FROM youtube_api_usage").fetchone()
                self.assertEqual(row["provider"], "youtube")
                self.assertEqual(row["operation"], "search.list")
                self.assertEqual(row["quota_cost_per_request"], 100)
                self.assertEqual(row["quota_units"], 200)
                self.assertEqual(row["producer_run_id"], "legacy")

    def test_usage_persists_budget_reserve_cache_retry_and_latency(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict("os.environ", QUOTA_ENV, clear=False),
        ):
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                state.record_api_usage(
                    endpoint="videos.list",
                    request_count=2,
                    resource_count=80,
                    success_count=2,
                    error_count=0,
                    quota_bucket="recent_metrics",
                    observed_at=OBSERVED_AT,
                    cache_hit_count=3,
                    cache_miss_count=1,
                    retry_count=2,
                    latency_ms=12.5,
                    queue_depth=4,
                    producer_run_id="run-1",
                )
                row = state.connection.execute("SELECT * FROM youtube_api_usage").fetchone()

                self.assertEqual(row["quota_units"], 2)
                self.assertEqual(row["daily_budget_units"], 100)
                self.assertEqual(row["remaining_units"], 98)
                self.assertEqual(row["reserved_units"], 20)
                self.assertEqual(row["reserve_remaining_units"], 18)
                self.assertEqual(row["cache_hit_count"], 3)
                self.assertEqual(row["retry_count"], 2)
                self.assertEqual(row["latency_ms"], 12.5)
                self.assertEqual(row["producer_run_id"], "run-1")

    def test_worker_health_uses_only_its_own_outbox_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                for worker_name, event_id in (
                    ("youtube_metrics", "a" * 64),
                    ("youtube_comment", "b" * 64),
                ):
                    state.enqueue_outbox(
                        worker_name=worker_name,
                        aggregate_id=worker_name,
                        topic="youtube.results",
                        event={
                            "event_id": event_id,
                            "platform_event_id": worker_name,
                        },
                        created_at=OBSERVED_AT - timedelta(minutes=10),
                    )
                state.record_worker_health(
                    worker_name="youtube_metrics",
                    observed_at=OBSERVED_AT,
                    status="success",
                    processed_count=1,
                    success_count=1,
                    error_count=0,
                )
                row = state.connection.execute("SELECT * FROM youtube_worker_health").fetchone()

                self.assertEqual(row["queue_depth"], 1)
                self.assertEqual(row["oldest_queue_age_seconds"], 600.0)


class MonitoringPersistenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = _load_monitoring_job()

    def test_loader_accepts_pre_migration_sqlite_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite"
            connection = sqlite3.connect(path)
            connection.execute(
                """
                CREATE TABLE youtube_api_usage (
                  usage_id INTEGER PRIMARY KEY,
                  usage_date TEXT NOT NULL,
                  endpoint TEXT NOT NULL,
                  request_count INTEGER NOT NULL,
                  resource_count INTEGER NOT NULL,
                  success_count INTEGER NOT NULL,
                  error_count INTEGER NOT NULL,
                  quota_bucket TEXT NOT NULL,
                  observed_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT INTO youtube_api_usage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    1,
                    "2026-07-20",
                    "search.list",
                    1,
                    5,
                    1,
                    0,
                    "discovery",
                    OBSERVED_AT.isoformat(),
                ),
            )
            connection.commit()
            connection.close()

            row = self.job.load_usage(path)[0]

            self.assertEqual(row["operation"], "search.list")
            self.assertEqual(row["quota_cost_per_request"], 100)
            self.assertEqual(row["quota_units"], 100)
            self.assertEqual(row["producer_run_id"], "legacy")
            self.assertEqual(len(row["external_usage_id"]), 64)

    def test_monitoring_job_uses_insert_only_merges_for_new_tables(self):
        source = (ROOT / "spark" / "jobs" / "maintenance" / "youtube_api_usage.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("lakehouse.monitoring.external_api_usage", source)
        self.assertIn("lakehouse.monitoring.pipeline_health", source)
        self.assertIn("MERGE INTO", source)
        self.assertIn("WHEN NOT MATCHED THEN INSERT *", source)
        self.assertIn("bronze_silver_gap", source)
        self.assertNotIn("left_anti\n    if not new_rows", source)

    def test_example_configuration_exposes_validated_monitoring_controls(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        names = (
            "YOUTUBE_DAILY_QUOTA_UNITS",
            "YOUTUBE_RECENT_SNAPSHOT_RESERVE_UNITS",
            "YOUTUBE_QUOTA_PRESSURE_RATIO",
            "YOUTUBE_QUOTA_CRITICAL_RATIO",
            "PIPELINE_QUEUE_WARNING_AGE_SECONDS",
            "PIPELINE_BRONZE_LAG_WARNING_SECONDS",
            "PIPELINE_SILVER_LAG_WARNING_SECONDS",
        )

        for name in names:
            self.assertIn(name, env_example)
            self.assertGreaterEqual(compose.count(name), 2, name)


if __name__ == "__main__":
    unittest.main()
