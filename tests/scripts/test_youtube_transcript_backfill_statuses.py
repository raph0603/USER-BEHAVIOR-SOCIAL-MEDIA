import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = ROOT / "spark" / "jobs" / "batch"
sys.path.insert(0, str(BATCH_DIR))

import youtube_transcripts as transcripts
from common.collection import OperationResult
from common.transcripts import TranscriptPayload


class YouTubeTranscriptBackfillUnitTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
        self.candidate = {
            "content_id": "content-1",
            "root_content_id": "video123",
            "conversation_id": None,
            "platform_event_id": "video123",
            "url": "https://www.youtube.com/watch?v=video123",
            "language": "en",
            "duration_seconds": 125.0,
            "created_at": self.now,
            "existing_attempt_count": 2,
        }

    def test_video_id_parser_supports_common_youtube_urls(self):
        cases = {
            "https://www.youtube.com/watch?v=abc_123": "abc_123",
            "https://youtu.be/abc_123?t=4": "abc_123",
            "https://www.youtube.com/shorts/abc_123": "abc_123",
            "https://www.youtube.com/embed/abc_123": "abc_123",
            "https://www.youtube.com/live/abc_123": "abc_123",
        }
        for url, expected in cases.items():
            with self.subTest(url=url):
                self.assertEqual(transcripts._video_id_from_url(url), expected)

    def test_retryability_respects_terminal_status_text_and_attempt_cap(self):
        for status in transcripts.TERMINAL_TRANSCRIPT_STATUSES:
            with self.subTest(status=status):
                self.assertFalse(
                    transcripts.is_retryable_transcript(status, None, 0, 5)
                )
        self.assertFalse(
            transcripts.is_retryable_transcript("failed", "existing text", 1, 5)
        )
        self.assertFalse(transcripts.is_retryable_transcript("failed", None, 5, 5))
        self.assertTrue(transcripts.is_retryable_transcript("failed", None, 4, 5))
        self.assertTrue(transcripts.is_retryable_transcript(None, None, 0, 5))

    def test_candidate_sort_key_prioritizes_fewer_and_older_attempts(self):
        candidates = [
            {
                "content_id": "c",
                "existing_attempt_count": 2,
                "existing_last_attempt_at": "2026-01-01T00:00:00Z",
            },
            {
                "content_id": "b",
                "existing_attempt_count": 1,
                "existing_last_attempt_at": "2026-02-01T00:00:00Z",
            },
            {
                "content_id": "a",
                "existing_attempt_count": 1,
                "existing_last_attempt_at": "2026-01-01T00:00:00Z",
            },
        ]
        ordered = sorted(candidates, key=transcripts.candidate_sort_key)
        self.assertEqual([row["content_id"] for row in ordered], ["a", "b", "c"])

    def test_success_result_preserves_rich_metadata(self):
        result = {
            "status": "success",
            "payload": {
                "language": "English",
                "language_code": "en",
                "is_generated": False,
                "is_translated": True,
                "source_language": "Spanish",
                "source_language_code": "es",
                "source": "translated_manual",
                "selection_strategy": "manual_other_language_translation",
                "text": "one two three",
                "segments": [
                    {"text": "one two", "start": 0.0, "duration": 1.0},
                    {"text": "three", "start": 1.0, "duration": 1.5},
                ],
                "segment_count": 2,
                "word_count": 3,
                "available_languages": [{"language_code": "es"}],
                "covered_duration_seconds": 2.5,
                "collected_at": self.now,
            },
            "completed_at": self.now,
            "error_code": None,
            "error_message": None,
        }

        row = transcripts._result_to_row(
            self.candidate,
            "video123",
            result,
            now=self.now,
        )

        self.assertEqual(row["transcript_status"], "success")
        self.assertEqual(row["attempt_count"], 3)
        self.assertEqual(row["language_code"], "en")
        self.assertEqual(row["source_language_code"], "es")
        self.assertFalse(row["is_generated"])
        self.assertTrue(row["is_translated"])
        self.assertEqual(row["segment_count"], 2)
        self.assertEqual(row["covered_duration_seconds"], 2.5)
        self.assertIn('"language_code": "es"', row["available_languages_json"])
        self.assertIn('"text": "one two"', row["segments_json"])

    def test_unavailable_result_is_persisted_as_terminal_outcome(self):
        result = {
            "status": "not_available",
            "payload": None,
            "completed_at": self.now,
            "error_code": "NoTranscriptFound",
            "error_message": "No transcript is available.",
        }
        row = transcripts._result_to_row(
            self.candidate,
            "video123",
            result,
            now=self.now,
        )
        self.assertEqual(row["transcript_status"], "not_available")
        self.assertEqual(row["error_code"], "NoTranscriptFound")
        self.assertIsNone(row["transcript_text"])
        self.assertEqual(row["attempt_count"], 3)

    def test_common_operation_result_integrates_without_shape_translation(self):
        payload = TranscriptPayload(
            video_id="video123",
            language="English",
            language_code="en",
            is_generated=True,
            is_translated=False,
            source_language="English",
            source_language_code="en",
            source="youtube_transcript_api",
            selection_strategy="generated_preferred",
            text="shared result",
            segments=(
                {"text": "shared result", "start": 0.0, "duration": 1.0},
            ),
            segment_count=1,
            word_count=2,
            available_languages=({"language_code": "en"},),
            covered_duration_seconds=1.0,
            collected_at=self.now,
        )
        result = OperationResult.success(
            payload,
            attempt_count=3,
            started_at=self.now,
            completed_at=self.now,
        )

        row = transcripts._result_to_row(
            self.candidate,
            "video123",
            result,
            now=self.now,
        )

        self.assertEqual(row["transcript_status"], "success")
        self.assertEqual(row["language_code"], "en")
        self.assertEqual(row["selection_strategy"], "generated_preferred")
        self.assertEqual(row["attempt_count"], 3)

    def test_empty_success_becomes_retryable_partial_outcome(self):
        result = {
            "status": "success",
            "payload": {"text": "", "segments": []},
            "completed_at": self.now,
        }
        row = transcripts._result_to_row(
            self.candidate,
            "video123",
            result,
            now=self.now,
        )
        self.assertEqual(row["transcript_status"], "partial")
        self.assertEqual(row["error_code"], "empty_transcript")

    def test_every_external_attempt_is_paced_even_when_fetch_raises(self):
        sleeps = []

        def failing_fetcher(video_id, preferred_languages, *, attempt_count):
            raise RuntimeError("network failure")

        row = transcripts._attempt_transcript_row(
            self.candidate,
            ("en",),
            0.5,
            fetcher=failing_fetcher,
            clock=lambda: self.now,
            sleeper=sleeps.append,
        )
        self.assertEqual(row["transcript_status"], "failed")
        self.assertEqual(row["error_code"], "runtime_error")
        self.assertEqual(sleeps, [0.5])

    def test_missing_video_id_is_persisted_and_paced(self):
        sleeps = []
        candidate = dict(self.candidate, root_content_id=None, platform_event_id="bad")
        candidate["url"] = "https://example.com/not-youtube"
        row = transcripts._attempt_transcript_row(
            candidate,
            ("en",),
            0.25,
            clock=lambda: self.now,
            sleeper=sleeps.append,
        )
        self.assertEqual(row["transcript_status"], "failed")
        self.assertEqual(row["error_code"], "missing_video_id")
        self.assertEqual(sleeps, [0.25])


class YouTubeTranscriptBackfillIntegrationContractTests(unittest.TestCase):
    def test_backfill_materializes_events_and_preserves_success_on_merge(self):
        source = (BATCH_DIR / "youtube_transcripts.py").read_text(encoding="utf-8")
        self.assertIn("_embedded_transcript_dataframe(events)", source)
        self.assertIn("t.transcript_text = COALESCE(s.transcript_text, t.transcript_text)", source)
        self.assertIn("WHEN t.transcript_status IN ('success', 'not_available', 'disabled')", source)
        self.assertIn("YOUTUBE_TRANSCRIPT_BACKFILL_RETRY_COOLDOWN_SECONDS", source)

    def test_both_dags_forward_retry_settings(self):
        for name in (
            "user_behavior_lakehouse.py",
            "user_behavior_lakehouse_no_row_checks.py",
        ):
            source = (ROOT / "orchestrator" / "dags" / name).read_text(
                encoding="utf-8"
            )
            with self.subTest(name=name):
                self.assertIn("YOUTUBE_TRANSCRIPT_BACKFILL_MAX_ATTEMPTS", source)
                self.assertIn(
                    "YOUTUBE_TRANSCRIPT_BACKFILL_RETRY_COOLDOWN_SECONDS",
                    source,
                )
                self.assertIn("YOUTUBE_TRANSCRIPT_BACKFILL_STOP_ON_RATE_LIMIT", source)
                self.assertIn("YOUTUBE_TRANSCRIPT_BACKFILL_FAIL_ON_RETRYABLE", source)

    def test_backfill_runtime_settings_are_exposed_to_airflow(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        settings = {
            "YOUTUBE_TRANSCRIPT_BACKFILL_LIMIT": "500",
            "YOUTUBE_TRANSCRIPT_BACKFILL_SLEEP_SECONDS": "0.25",
            "YOUTUBE_TRANSCRIPT_BACKFILL_MAX_ATTEMPTS": "5",
            "YOUTUBE_TRANSCRIPT_BACKFILL_RETRY_COOLDOWN_SECONDS": "3600",
            "YOUTUBE_TRANSCRIPT_BACKFILL_STOP_ON_RATE_LIMIT": "true",
            "YOUTUBE_TRANSCRIPT_BACKFILL_FAIL_ON_RETRYABLE": "true",
        }
        for name, default in settings.items():
            with self.subTest(name=name):
                self.assertIn(f"{name}={default}", env_example)
                self.assertIn(f"{name}: ${{{name}:-{default}}}", compose)

    def test_no_row_checks_dag_is_manual_and_refreshes_content_analytics(self):
        source = (
            ROOT
            / "orchestrator"
            / "dags"
            / "user_behavior_lakehouse_no_row_checks.py"
        ).read_text(encoding="utf-8")
        self.assertIn('"LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES",\n        "0"', source)
        self.assertIn("backfill_youtube_transcripts >> update_content_analytics", source)
        self.assertIn("update_content_analytics >> update_balancing_report", source)
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES=0", env_example)
        self.assertIn(
            "LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES: "
            "${LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES:-0}",
            compose,
        )

    def test_dashboard_distinguishes_transcript_statuses(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")
        for value in (
            "TRANSCRIPT_STATUS_PRESENTATION",
            "rate_limited",
            "not_available",
            "Transcript collection has not been attempted",
        ):
            with self.subTest(value=value):
                self.assertIn(value, source)


if __name__ == "__main__":
    unittest.main()
