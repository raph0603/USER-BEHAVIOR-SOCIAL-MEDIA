import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_PATH = ROOT / "dashboard"
sys.path.insert(0, str(DASHBOARD_PATH))

try:
    from streamlit.testing.v1 import AppTest
except ModuleNotFoundError:
    AppTest = None


def silver_events_fixture():
    observed_at = pd.Timestamp("2026-07-20T11:00:00Z")
    return pd.DataFrame(
        [
            {
                "source": "youtube",
                "author_hash": "author-1",
                "url": "https://www.youtube.com/watch?v=video-1",
                "text": "A deterministic dashboard fixture",
                "created_at": pd.Timestamp("2026-07-18T10:00:00Z"),
                "event_date": observed_at.date(),
                "error": pd.NA,
                "platform_event_id": "video-1",
                "metadata_refreshed_at": observed_at,
                "owner_channel_id": "channel-1",
                "collaborator_channel_ids": [],
                "view_count": 0,
                "like_count": pd.NA,
                "comment_count": 0,
                "view_count_available": True,
                "like_count_available": False,
                "comment_count_available": True,
                "text_len_chars": 33,
                "text_len_words": 4,
                "has_question": False,
            }
        ]
    )


def optional_tables_fixture():
    content_id = "video-1"
    snapshot_at = pd.Timestamp("2026-07-20T11:00:00Z")
    contents = pd.DataFrame(
        [
            {
                "content_id": content_id,
                "source": "youtube",
                "content_type": "youtube_video",
                "created_at": pd.Timestamp("2026-07-18T10:00:00Z"),
                "title": "A deterministic dashboard fixture",
                "text": "A deterministic dashboard fixture",
                "url": "https://www.youtube.com/watch?v=video-1",
                "youtube_channel_id": "channel-1",
                "youtube_channel_name": "Fixture Channel",
                "thumbnail_url": pd.NA,
                "metadata_status": "success",
                "comments_status": "success",
                "metadata_available": True,
                "comments_available": True,
                "last_discovered_at": pd.Timestamp("2026-07-18T12:00:00Z"),
                "last_enriched_at": pd.Timestamp("2026-07-20T10:00:00Z"),
            }
        ]
    )
    snapshots = pd.DataFrame(
        [
            {
                "content_id": content_id,
                "source": "youtube",
                "platform_event_id": "video-1",
                "observation_id": "observation-1",
                "observed_at": snapshot_at,
                "snapshot_at": snapshot_at,
                "age_minutes": 2_940,
                "view_count": 0,
                "like_count": pd.NA,
                "comment_count": 0,
                "view_count_available": True,
                "like_count_available": False,
                "comment_count_available": True,
                "producer_name": "youtube_metrics_worker",
                "producer_run_id": "run-smoke",
                "collection_method": "youtube_data_api",
                "api_endpoint": "videos.list",
                "provenance_json": (
                    '{"producer_name":"youtube_metrics_worker",'
                    '"producer_run_id":"run-smoke",'
                    '"collection_method":"youtube_data_api",'
                    '"api_endpoint":"videos.list"}'
                ),
                "coverage_json": ('{"view_count":true,"like_count":false,"comment_count":true}'),
            }
        ]
    )
    transcripts = pd.DataFrame(
        [
            {
                "video_id": "video-1",
                "content_id": content_id,
                "transcript_status": "success",
                "transcript_lifecycle_status": "available",
                "requested_language_code": "en",
                "obtained_language_code": "en",
                "transcript_text": "deterministic transcript",
                "generation_type": "manual",
                "is_generated": False,
                "is_translated": False,
                "provider": "youtube_transcript_api",
                "attempt_count": 1,
                "last_attempt_at": pd.Timestamp("2026-07-20T10:30:00Z"),
                "next_attempt_at": pd.NaT,
                "updated_at": pd.Timestamp("2026-07-20T10:30:00Z"),
            }
        ]
    )
    content_stats = pd.DataFrame(
        [
            {
                "content_id": content_id,
                "latest_snapshot_at": snapshot_at,
                "latest_view_count": 0,
                "latest_like_count": pd.NA,
                "latest_comment_count": 0,
                "latest_view_count_available": True,
                "latest_like_count_available": False,
                "latest_comment_count_available": True,
            }
        ]
    )
    return {
        ("silver", "contents"): contents,
        ("silver", "interactions"): pd.DataFrame(),
        ("silver", "engagement_snapshots"): snapshots,
        ("silver", "transcripts"): transcripts,
        ("gold", "content_stats"): content_stats,
        ("gold", "user_evolution"): pd.DataFrame(),
    }


@unittest.skipIf(AppTest is None, "Streamlit testing runtime is not installed")
class DashboardStreamlitSmokeTests(unittest.TestCase):
    def test_dashboard_renders_chart_table_and_youtube_na_semantics(self):
        optional_tables = optional_tables_fixture()

        def load_optional(namespace, table_name, *_args, **_kwargs):
            return optional_tables.get((namespace, table_name), pd.DataFrame()), None

        with (
            patch("loaders.load_iceberg_data", return_value=silver_events_fixture()),
            patch("loaders.load_optional_iceberg_table", side_effect=load_optional),
            patch(
                "airflow_monitoring.AirflowClient.load_status",
                return_value={
                    "active_runs": [],
                    "next_runs": [],
                    "checked_at": datetime(2026, 7, 20, 12, tzinfo=UTC),
                },
            ),
            patch(
                "airflow_monitoring.AirflowClient.load_recent_collector_runs",
                return_value=[],
            ),
        ):
            app = AppTest.from_file(str(DASHBOARD_PATH / "app.py"))
            app.run(timeout=30)

        self.assertEqual(list(app.exception), [])
        self.assertGreater(len(app.get("plotly_chart")), 0)
        self.assertGreater(len(app.dataframe), 0)
        captions = [str(element.value) for element in app.caption]
        self.assertTrue(any("Views: 0" in value and "Likes: N/A" in value for value in captions))
        self.assertTrue(
            any("Coverage: 2/3 snapshot metrics observed" in value for value in captions)
        )


if __name__ == "__main__":
    unittest.main()
