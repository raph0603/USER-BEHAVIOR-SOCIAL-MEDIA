import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

from common.youtube_pipeline import utc_now
from common.youtube_state import YouTubeStateStore


ROOT = Path(__file__).resolve().parents[2]
PLAYWRIGHT_DIR = ROOT / "playwright"
SPARK_BATCH_DIR = ROOT / "spark" / "jobs" / "batch"
sys.path.insert(0, str(PLAYWRIGHT_DIR))
sys.path.insert(0, str(SPARK_BATCH_DIR))

googleapiclient = types.ModuleType("googleapiclient")
googleapiclient_discovery = types.ModuleType("googleapiclient.discovery")
googleapiclient_discovery.build = lambda *_args, **_kwargs: None
googleapiclient.discovery = googleapiclient_discovery
sys.modules.setdefault("googleapiclient", googleapiclient)
sys.modules.setdefault("googleapiclient.discovery", googleapiclient_discovery)

import youtube_transcript_metadata_backfill as metadata_backfill
import youtube_transcript_request_backfill as request_backfill


class YouTubeTranscriptMetadataBackfillTests(unittest.TestCase):
    def test_iso_duration_supports_hours_minutes_seconds_and_days(self):
        self.assertEqual(metadata_backfill.parse_iso8601_duration("PT1H2M3S"), 3723)
        self.assertEqual(metadata_backfill.parse_iso8601_duration("P1DT2S"), 86402)
        self.assertEqual(metadata_backfill.parse_iso8601_duration("PT0.5S"), 0.5)
        self.assertIsNone(metadata_backfill.parse_iso8601_duration("invalid"))

    def test_metadata_update_keeps_only_bounded_fields(self):
        updated = metadata_backfill.metadata_request_update(
            {"video_id": "abcdefghijk", "language": "en"},
            {
                "id": "abcdefghijk",
                "contentDetails": {"duration": "PT1M30S", "regionRestriction": {"x": "y"}},
                "status": {"privacyStatus": "public", "embeddable": True},
                "snippet": {"title": "not persisted"},
            },
        )

        self.assertEqual(updated["duration_seconds"], 90)
        self.assertEqual(updated["video_availability"], "public")
        self.assertNotIn("contentDetails", updated)
        self.assertNotIn("snippet", updated)

    def test_missing_video_becomes_terminal_and_is_not_selected_again(self):
        with tempfile.TemporaryDirectory() as directory:
            with YouTubeStateStore(Path(directory) / "state.sqlite") as state:
                now = utc_now()
                state.enqueue_transcript_request(
                    video_id="abcdefghijk",
                    correlation_id="content",
                    first_seen_at=now,
                    published_at=now,
                    request={
                        "video_id": "abcdefghijk",
                        "transcript_requested_language_code": "en",
                    },
                )
                rows = metadata_backfill._candidate_rows(state, 10)
                self.assertEqual(len(rows), 1)

                updated, unavailable = metadata_backfill._persist_updates(state, rows, {})

                self.assertEqual((updated, unavailable), (1, 1))
                lifecycle = state.connection.execute(
                    """
                    SELECT transcript_lifecycle_status, transcript_status, request_json
                    FROM youtube_transcript_lifecycle
                    """
                ).fetchone()
                self.assertEqual(lifecycle["transcript_lifecycle_status"], "unavailable")
                self.assertEqual(lifecycle["transcript_status"], "not_available")
                self.assertEqual(
                    json.loads(lifecycle["request_json"])["video_availability"],
                    "unavailable",
                )
                self.assertEqual(metadata_backfill._candidate_rows(state, 10), [])

    def test_duration_error_is_requeued_after_metadata_repair(self):
        with tempfile.TemporaryDirectory() as directory:
            with YouTubeStateStore(Path(directory) / "state.sqlite") as state:
                now = utc_now()
                state.enqueue_transcript_request(
                    video_id="abcdefghijk",
                    correlation_id="content",
                    first_seen_at=now,
                    published_at=now,
                    request={
                        "video_id": "abcdefghijk",
                        "transcript_requested_language_code": "en",
                    },
                )
                state.connection.execute(
                    """
                    UPDATE youtube_transcript_lifecycle
                    SET transcript_lifecycle_status = 'disabled',
                        transcript_status = 'disabled',
                        next_attempt_at = NULL,
                        error_code = 'gemini_duration_unknown'
                    """
                )
                state.connection.commit()
                rows = metadata_backfill._candidate_rows(state, 10)

                metadata_backfill._persist_updates(
                    state,
                    rows,
                    {
                        "abcdefghijk": {
                            "id": "abcdefghijk",
                            "contentDetails": {"duration": "PT45S"},
                            "status": {"privacyStatus": "public"},
                        }
                    },
                )

                lifecycle = state.connection.execute(
                    """
                    SELECT transcript_lifecycle_status, transcript_status,
                           next_attempt_at, error_code, request_json
                    FROM youtube_transcript_lifecycle
                    """
                ).fetchone()
                self.assertEqual(lifecycle["transcript_lifecycle_status"], "pending")
                self.assertEqual(lifecycle["transcript_status"], "pending")
                self.assertIsNotNone(lifecycle["next_attempt_at"])
                self.assertIsNone(lifecycle["error_code"])
                self.assertEqual(json.loads(lifecycle["request_json"])["duration_seconds"], 45)


class YouTubeScheduledTranscriptBackfillContractTests(unittest.TestCase):
    def test_silver_candidate_becomes_provider_neutral_request(self):
        request = request_backfill.candidate_request(
            {
                "root_content_id": "abcdefghijk",
                "content_id": "content-id",
                "created_at": "2026-07-21T00:00:00Z",
                "language": "fr",
                "requested_language": "en",
                "requested_language_code": "en",
                "duration_seconds": 90.0,
            }
        )

        self.assertEqual(request["video_id"], "abcdefghijk")
        self.assertEqual(request["correlation_id"], "content-id")
        self.assertEqual(request["duration_seconds"], 90.0)
        self.assertEqual(request["transcript_requested_language_code"], "en")
        self.assertEqual(request["published_at"], "2026-07-21T00:00:00+00:00")

    def test_hourly_dag_seeds_enriches_and_processes_missing_transcripts(self):
        dag = (ROOT / "orchestrator" / "dags" / "lakehouse_dag_factory.py").read_text(
            encoding="utf-8"
        )
        for task_id in (
            "seed_missing_youtube_transcripts",
            "backfill_missing_youtube_metadata",
            "process_missing_youtube_transcripts",
        ):
            self.assertIn(f'task_id="{task_id}"', dag)
        self.assertIn(
            "release_pipeline_lock\n"
            "            >> seed_missing_youtube_transcripts\n"
            "            >> backfill_missing_youtube_metadata\n"
            "            >> process_missing_youtube_transcripts",
            dag,
        )
        self.assertIn(
            "[\n            run_youtube_metadata,\n"
            "            run_x_collection,\n            run_reddit_collection,\n"
            "        ] >> acquire_pipeline_lock",
            dag,
        )

    def test_runtime_limits_are_exposed_to_airflow_and_collector(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        defaults = {
            "YOUTUBE_TRANSCRIPT_REQUEST_BACKFILL_LIMIT": "5000",
            "YOUTUBE_TRANSCRIPT_METADATA_BACKFILL_LIMIT": "500",
            "YOUTUBE_TRANSCRIPT_BACKFILL_BATCH_SIZE": "25",
        }
        for name, default in defaults.items():
            with self.subTest(name=name):
                self.assertIn(f"{name}={default}", env_example)
                self.assertGreaterEqual(
                    compose.count(f"{name}: ${{{name}:-{default}}}"),
                    1,
                )
                self.assertIn(f"{name}=${{{name}:-{default}}}", compose)

    def test_images_include_both_scheduled_backfill_workers(self):
        spark_dockerfile = (ROOT / "spark" / "master" / "Dockerfile").read_text(encoding="utf-8")
        collector_dockerfile = (ROOT / "playwright" / "Dockerfile").read_text(encoding="utf-8")

        self.assertIn("COPY spark/jobs/ /opt/spark/jobs/", spark_dockerfile)
        self.assertIn(
            "youtube_transcript_metadata_backfill.py /app/youtube_transcript_metadata_backfill.py",
            collector_dockerfile,
        )


if __name__ == "__main__":
    unittest.main()
