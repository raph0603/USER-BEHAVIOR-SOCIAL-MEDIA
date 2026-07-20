import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "spark" / "jobs"))

from common.transcripts import (
    TRANSCRIPT_LIFECYCLE_STATUSES,
    TranscriptPayload,
    is_retryable_transcript_lifecycle,
    is_terminal_transcript_lifecycle,
    legacy_transcript_status,
    preferred_transcript_language_code,
    transcript_lifecycle_status,
)
from common.youtube_state import YouTubeStateStore
from event_contract import EVENT_FIELD_TYPES


UTC = timezone.utc


class TranscriptLifecycleTests(unittest.TestCase):
    def test_all_canonical_states_have_explicit_retry_semantics(self):
        terminal = {"available", "unavailable", "disabled", "permanent_error"}
        retryable = {"pending", "rate_limited", "blocked", "retryable_error"}

        self.assertEqual(TRANSCRIPT_LIFECYCLE_STATUSES, terminal | retryable)
        for status in terminal:
            with self.subTest(status=status):
                self.assertTrue(is_terminal_transcript_lifecycle(status))
                self.assertFalse(is_retryable_transcript_lifecycle(status))
        for status in retryable:
            with self.subTest(status=status):
                self.assertFalse(is_terminal_transcript_lifecycle(status))
                self.assertTrue(is_retryable_transcript_lifecycle(status))

    def test_collector_and_legacy_outcomes_map_without_conflating_failures(self):
        cases = (
            ({"status": "pending"}, "pending"),
            ({"status": "success", "has_text": True}, "available"),
            ({"status": "not_available"}, "unavailable"),
            ({"status": "disabled"}, "disabled"),
            ({"status": "rate_limited"}, "rate_limited"),
            ({"status": "rate_limited", "error_code": "ip_blocked"}, "blocked"),
            ({"status": "failed", "error_code": "timeout_error"}, "retryable_error"),
            ({"status": "failed", "error_code": "missing_video_id"}, "permanent_error"),
        )
        for arguments, expected in cases:
            with self.subTest(arguments=arguments):
                self.assertEqual(transcript_lifecycle_status(**arguments), expected)

        self.assertEqual(
            transcript_lifecycle_status(
                "failed",
                error_code="timeout_error",
                attempt_count=5,
                max_attempts=5,
            ),
            "permanent_error",
        )

    def test_legacy_status_mapping_remains_additive(self):
        expected = {
            "pending": "pending",
            "available": "success",
            "unavailable": "not_available",
            "disabled": "disabled",
            "rate_limited": "rate_limited",
            "blocked": "rate_limited",
            "retryable_error": "failed",
            "permanent_error": "failed",
        }
        self.assertEqual(
            {status: legacy_transcript_status(status) for status in expected},
            expected,
        )

    def test_language_policy_is_per_video_and_content_version_is_stable(self):
        self.assertEqual(preferred_transcript_language_code("vi-VN"), "vi")
        self.assertEqual(preferred_transcript_language_code("French"), "en")
        self.assertEqual(
            [
                preferred_transcript_language_code(language)
                for language in ("vi-VN", "en-US", "vi", None)
            ],
            ["vi", "en", "vi", "en"],
        )
        now = datetime(2026, 7, 20, tzinfo=UTC)
        arguments = {
            "video_id": "video-1",
            "language": "English",
            "language_code": "en",
            "is_generated": False,
            "is_translated": False,
            "source_language": "English",
            "source_language_code": "en",
            "source": "youtube_transcript_api",
            "selection_strategy": "manual_preferred",
            "text": "stable text",
            "segments": ({"text": "stable text", "start": 0.0, "duration": 1.0},),
            "segment_count": 1,
            "word_count": 2,
            "available_languages": ({"language_code": "en"},),
            "covered_duration_seconds": 1.0,
            "collected_at": now,
            "requested_language": "English",
            "requested_language_code": "en-US",
        }
        first = TranscriptPayload(**arguments)
        second = TranscriptPayload(**{**arguments, "collected_at": now + timedelta(days=1)})

        self.assertEqual(first.content_version, second.content_version)
        self.assertEqual(first.requested_language_code, "en-us")
        self.assertEqual(first.generation_type, "manual")


class TranscriptLifecycleStateTests(unittest.TestCase):
    def test_legacy_sqlite_rows_are_migrated_without_losing_attempt_details(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.sqlite"
            connection = sqlite3.connect(path)
            connection.execute(
                """
                CREATE TABLE youtube_transcript_state (
                  video_id TEXT PRIMARY KEY,
                  correlation_id TEXT NOT NULL,
                  first_seen_at TEXT NOT NULL,
                  published_at TEXT,
                  request_json TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'pending',
                  attempt_count INTEGER NOT NULL DEFAULT 0,
                  last_attempt_at TEXT,
                  next_attempt_at TEXT,
                  error_class TEXT,
                  error_message TEXT,
                  result_json TEXT
                )
                """
            )
            result = {
                "status": "success",
                "payload": {
                    "text": "xin chao",
                    "language": "Vietnamese",
                    "language_code": "vi",
                    "source": "youtube_transcript_api",
                    "content_version": "a" * 64,
                },
            }
            connection.execute(
                "INSERT INTO youtube_transcript_state VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "video-1",
                    "correlation-1",
                    "2026-07-20T00:00:00+00:00",
                    None,
                    json.dumps({"language": "vi-VN"}),
                    "available",
                    2,
                    "2026-07-20T02:00:00+00:00",
                    None,
                    None,
                    None,
                    json.dumps(result),
                ),
            )
            connection.commit()
            connection.close()

            with YouTubeStateStore(path) as state:
                row = state.connection.execute(
                    "SELECT * FROM youtube_transcript_lifecycle"
                ).fetchone()

                self.assertEqual(row["requested_language_code"], "vi")
                self.assertEqual(row["transcript_lifecycle_status"], "available")
                self.assertEqual(row["transcript_status"], "success")
                self.assertEqual(row["migrated_legacy_status"], "available")
                self.assertEqual(row["attempt_count"], 2)
                self.assertEqual(json.loads(row["result_json"]), result)

    def test_requests_and_results_are_persisted_per_requested_language(self):
        observed = datetime(2026, 7, 20, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as directory:
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                for code in ("en", "fr"):
                    state.enqueue_transcript_request(
                        video_id="video-1",
                        correlation_id=f"correlation-{code}",
                        first_seen_at=observed,
                        published_at=None,
                        request={"transcript_requested_language_code": code},
                    )

                due = state.due_transcript_requests(now=observed, limit=10)
                self.assertEqual(
                    {(row["video_id"], row["requested_language_code"]) for row in due},
                    {("video-1", "en"), ("video-1", "fr")},
                )

                next_attempt = observed + timedelta(hours=6)
                state.record_transcript_result(
                    video_id="video-1",
                    requested_language_code="en",
                    lifecycle_status="retryable_error",
                    attempt_count=1,
                    attempted_at=observed,
                    next_attempt_at=next_attempt,
                    result={"status": "failed"},
                    requested_language="en",
                    provider="youtube_transcript_api",
                    error_code="timeout_error",
                    error_message="temporary timeout",
                )
                state.record_transcript_result(
                    video_id="video-1",
                    requested_language_code="en",
                    lifecycle_status="available",
                    attempt_count=2,
                    attempted_at=next_attempt,
                    next_attempt_at=None,
                    result={"status": "success", "payload": {"text": "hello"}},
                    requested_language="en",
                    obtained_language="English",
                    obtained_language_code="en",
                    available_languages=[{"language_code": "en"}],
                    generation_type="manual",
                    is_generated=False,
                    is_translated=False,
                    provider="youtube_transcript_api",
                    collected_at=next_attempt,
                    recovered_at=next_attempt,
                    content_version="b" * 64,
                )
                row = state.connection.execute(
                    """
                    SELECT * FROM youtube_transcript_lifecycle
                    WHERE video_id = 'video-1' AND requested_language_code = 'en'
                    """
                ).fetchone()
                legacy = state.connection.execute(
                    "SELECT * FROM youtube_transcript_state WHERE video_id = 'video-1'"
                ).fetchone()

                self.assertEqual(row["transcript_lifecycle_status"], "available")
                self.assertEqual(row["transcript_status"], "success")
                self.assertEqual(row["attempt_count"], 2)
                self.assertIsNone(row["next_attempt_at"])
                self.assertEqual(row["recovered_at"], next_attempt.isoformat())
                self.assertEqual(legacy["status"], "success")
                self.assertEqual(legacy["attempt_count"], 2)


class TranscriptLifecycleContractTests(unittest.TestCase):
    def test_avro_and_spark_contracts_expose_additive_lifecycle_fields(self):
        schema = json.loads(
            (ROOT / "schemas" / "playwright_event.avsc").read_text(encoding="utf-8")
        )
        fields = {field["name"]: field for field in schema["fields"]}
        expected = {
            "transcript_lifecycle_status": "string",
            "transcript_requested_language_code": "string",
            "transcript_obtained_language_code": "string",
            "transcript_generation_type": "string",
            "transcript_provider": "string",
            "transcript_attempt_count": "int",
            "transcript_last_attempt_at": "string",
            "transcript_next_attempt_at": "string",
            "transcript_recovered_at": "string",
            "transcript_content_version": "string",
        }
        for name, spark_type in expected.items():
            with self.subTest(name=name):
                self.assertIn(name, fields)
                self.assertIsNone(fields[name]["default"])
                self.assertEqual(EVENT_FIELD_TYPES[name], spark_type)

    def test_worker_attempt_limit_is_explicitly_configured(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("YOUTUBE_TRANSCRIPT_MAX_ATTEMPTS=5", env_example)
        self.assertIn(
            "YOUTUBE_TRANSCRIPT_MAX_ATTEMPTS: ${YOUTUBE_TRANSCRIPT_MAX_ATTEMPTS:-5}",
            compose,
        )

    def test_migration_cli_applies_transcript_lifecycle_backfill(self):
        source = (
            ROOT / "spark" / "jobs" / "maintenance" / "migrate_pipeline_reliability.py"
        ).read_text(encoding="utf-8")

        self.assertIn("ensure_transcript_table(spark)", source)
        self.assertIn("would_migrate_transcript_lifecycle_rows", source)


if __name__ == "__main__":
    unittest.main()
