import contextlib
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from confluent_kafka import KafkaError, KafkaException


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "playwright"))

from common.youtube_outbox import event_json_size_bytes
from common.youtube_pipeline import parse_hour_offsets
from common.youtube_state import YouTubeStateStore
from youtube_pipeline_events import drain_outbox, youtube_kafka_max_event_bytes
from scripts.youtube_outbox_cli import run as run_outbox_cli


UTC = timezone.utc


class _Producer:
    def __init__(
        self,
        *,
        fail: bool = False,
        kafka_error: BaseException | None = None,
        max_event_bytes: int = 900_000,
    ):
        self.fail = fail
        self.kafka_error = kafka_error
        self.max_event_bytes = max_event_bytes
        self.published = []

    def serialized_size_bytes(self, _topic, event):
        return len(json.dumps(event, ensure_ascii=False, sort_keys=True).encode("utf-8"))

    def publish(self, topic, events):
        if self.kafka_error:
            raise self.kafka_error
        if self.fail:
            raise RuntimeError("temporary Kafka outage")
        self.published.extend((topic, event) for event in events)
        return len(events)


class YouTubeOutboxTests(unittest.TestCase):
    def _event(self):
        return {
            "event_id": "a" * 64,
            "event_type": "youtube.metadata.observed",
            "video_id": "video-1",
            "platform_event_id": "video-1",
            "source": "youtube",
            "collected_at": "2026-07-20T00:00:00+00:00",
        }

    def test_state_store_uses_configured_sqlite_lock_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {"YOUTUBE_STATE_LOCK_TIMEOUT_SECONDS": "123.5"},
            ):
                with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                    timeout = state.connection.execute("PRAGMA busy_timeout").fetchone()[0]

        self.assertEqual(timeout, 123_500)

    def test_transaction_rolls_back_state_and_outbox_together(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.sqlite"
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            with YouTubeStateStore(path) as state:
                with self.assertRaisesRegex(RuntimeError, "abort"):
                    with state.transaction():
                        state.enqueue_outbox(
                            worker_name="youtube_metadata",
                            aggregate_id="video-1",
                            topic="youtube.metadata.events",
                            event=self._event(),
                            created_at=observed,
                        )
                        raise RuntimeError("abort")

            with YouTubeStateStore(path) as state:
                self.assertEqual(
                    state.pending_outbox(
                        now=observed,
                        limit=10,
                        include_deferred=True,
                    ),
                    [],
                )

    def test_worker_success_and_publish_intent_commit_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.sqlite"
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            with YouTubeStateStore(path) as state:
                state.record_discovery(
                    video_id="video-1",
                    query_id="query-1",
                    first_seen_at=observed,
                    published_at=observed,
                    correlation_id="correlation-1",
                )
                with state.transaction():
                    state.record_metadata_success(
                        video_id="video-1",
                        observed_at=observed,
                        metadata={"title": "A title"},
                        offsets=parse_hour_offsets("6,24"),
                    )
                    state.enqueue_outbox(
                        worker_name="youtube_metadata",
                        aggregate_id="video-1",
                        topic="youtube.metadata.events",
                        event=self._event(),
                        created_at=observed,
                    )

            with YouTubeStateStore(path) as state:
                self.assertEqual(
                    state.metadata_state("video-1")["metadata_status"],
                    "success",
                )
                self.assertEqual(state.outbox_health(now=observed)["pending_count"], 1)

    def test_failed_ack_stays_pending_and_restart_redrains(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.sqlite"
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            with YouTubeStateStore(path) as state:
                state.enqueue_outbox(
                    worker_name="youtube_metadata",
                    aggregate_id="video-1",
                    topic="youtube.metadata.events",
                    event=self._event(),
                    created_at=observed,
                )
                with self.assertRaisesRegex(RuntimeError, "Kafka outage"):
                    drain_outbox(
                        state,
                        _Producer(fail=True),
                        include_deferred=True,
                        now=observed,
                    )
                health = state.outbox_health(now=observed + timedelta(seconds=1))
                self.assertEqual(health["pending_count"], 1)
                self.assertEqual(health["delivery_attempts"], 1)

            producer = _Producer()
            with YouTubeStateStore(path) as state:
                self.assertEqual(
                    drain_outbox(
                        state,
                        producer,
                        include_deferred=True,
                        now=observed,
                    ),
                    1,
                )
                self.assertEqual(state.outbox_health(now=observed)["pending_count"], 0)
                self.assertEqual(
                    drain_outbox(
                        state,
                        producer,
                        include_deferred=True,
                        now=observed,
                    ),
                    0,
                )
            self.assertEqual(len(producer.published), 1)

    def test_delivery_failure_honors_backoff_until_restart_redrain(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                state.enqueue_outbox(
                    worker_name="youtube_metadata",
                    aggregate_id="video-1",
                    topic="youtube.metadata.events",
                    event=self._event(),
                    created_at=observed,
                )
                with self.assertRaises(RuntimeError):
                    drain_outbox(
                        state,
                        _Producer(fail=True),
                        include_deferred=True,
                        now=observed,
                    )

                self.assertEqual(
                    drain_outbox(state, _Producer(), now=observed + timedelta(seconds=1)),
                    0,
                )
                self.assertEqual(
                    drain_outbox(
                        state,
                        _Producer(),
                        include_deferred=True,
                        now=observed + timedelta(seconds=1),
                    ),
                    1,
                )

    def test_duplicate_publish_intent_is_deduplicated(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                ids = [
                    state.enqueue_outbox(
                        worker_name="youtube_metadata",
                        aggregate_id="video-1",
                        topic="youtube.metadata.events",
                        event=self._event(),
                        created_at=observed,
                    )
                    for _ in range(2)
                ]

                self.assertEqual(ids[0], ids[1])
                self.assertEqual(state.outbox_health(now=observed)["pending_count"], 1)

    def test_metrics_observation_replay_is_idempotent(self):
        update = {
            "observation_id": "b" * 64,
            "platform_event_id": "video-1",
            "metadata_refreshed_at": "2026-07-20T00:00:00+00:00",
            "metrics_refresh_status": "available",
            "payload_fingerprint": "c" * 64,
            "view_count": 0,
            "view_count_available": True,
        }
        with tempfile.TemporaryDirectory() as directory:
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                state.record_metrics_observation(update)
                state.record_metrics_observation(update)
                count = state.connection.execute(
                    "SELECT COUNT(*) FROM youtube_metrics_state"
                ).fetchone()[0]

                self.assertEqual(count, 1)

    def test_oversized_event_is_retained_quarantined_and_does_not_block_next_row(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            producer = _Producer(max_event_bytes=500)
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                oversized = {**self._event(), "event_id": "large", "payload_json": "é" * 500}
                small = {**self._event(), "event_id": "small", "video_id": "video-2"}
                large_id = state.enqueue_outbox(
                    worker_name="youtube_transcript",
                    aggregate_id="video-1",
                    topic="youtube.transcript.results",
                    event=oversized,
                    created_at=observed,
                )
                state.enqueue_outbox(
                    worker_name="youtube_metadata",
                    aggregate_id="video-2",
                    topic="youtube.metadata.events",
                    event=small,
                    created_at=observed + timedelta(seconds=1),
                )
                stats = {}

                self.assertEqual(
                    drain_outbox(
                        state,
                        producer,
                        include_deferred=True,
                        now=observed,
                        stats=stats,
                    ),
                    1,
                )
                retained = dict(
                    state.connection.execute(
                        "SELECT * FROM youtube_worker_outbox WHERE outbox_id = ?",
                        (large_id,),
                    ).fetchone()
                )

                self.assertEqual(retained["status"], "failed_terminal")
                self.assertEqual(retained["failure_reason"], "message_size_too_large")
                self.assertIsNone(retained["delivered_at"])
                self.assertIn('"event_id":"large"', retained["event_json"])
                self.assertEqual(stats, {"published": 1, "quarantined": 1})
                self.assertEqual([event["event_id"] for _, event in producer.published], ["small"])
                self.assertEqual(
                    drain_outbox(
                        state,
                        producer,
                        include_deferred=True,
                        now=observed,
                    ),
                    0,
                )

    def test_all_oversized_events_are_quarantined_before_failure_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                state.enqueue_outbox(
                    worker_name="youtube_transcript",
                    aggregate_id="video-1",
                    topic="youtube.transcript.results",
                    event={**self._event(), "payload_json": "x" * 2000},
                    created_at=observed,
                )
                with self.assertRaisesRegex(RuntimeError, "were quarantined"):
                    drain_outbox(
                        state,
                        _Producer(max_event_bytes=500),
                        include_deferred=True,
                        now=observed,
                    )
                self.assertEqual(state.outbox_health(now=observed)["terminal_count"], 1)
                self.assertEqual(state.outbox_health(now=observed)["pending_count"], 0)

    def test_native_kafka_message_too_large_is_terminal(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            kafka_error = KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                outbox_id = state.enqueue_outbox(
                    worker_name="youtube_metadata",
                    aggregate_id="video-1",
                    topic="youtube.metadata.events",
                    event=self._event(),
                    created_at=observed,
                )
                with self.assertRaisesRegex(RuntimeError, "were quarantined"):
                    drain_outbox(
                        state,
                        _Producer(kafka_error=kafka_error),
                        include_deferred=True,
                        now=observed,
                    )
                row = state.connection.execute(
                    "SELECT status, failure_reason FROM youtube_worker_outbox WHERE outbox_id = ?",
                    (outbox_id,),
                ).fetchone()
                self.assertEqual(tuple(row), ("failed_terminal", "message_size_too_large"))

    def test_non_size_kafka_exception_remains_retryable_and_propagates(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            kafka_error = KafkaException(KafkaError(KafkaError._TRANSPORT))
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                outbox_id = state.enqueue_outbox(
                    worker_name="youtube_metadata",
                    aggregate_id="video-1",
                    topic="youtube.metadata.events",
                    event=self._event(),
                    created_at=observed,
                )
                with self.assertRaises(KafkaException):
                    drain_outbox(
                        state,
                        _Producer(kafka_error=kafka_error),
                        include_deferred=True,
                        now=observed,
                    )
                row = state.connection.execute(
                    "SELECT status, delivery_attempts FROM youtube_worker_outbox WHERE outbox_id = ?",
                    (outbox_id,),
                ).fetchone()
                self.assertEqual(tuple(row), ("pending", 1))

    def test_utf8_size_counts_encoded_bytes(self):
        payload = json.dumps({"text": "électrique"}, ensure_ascii=False)
        self.assertEqual(event_json_size_bytes(payload), len(payload.encode("utf-8")))
        self.assertGreater(event_json_size_bytes(payload), len(payload))

    def test_invalid_max_event_bytes_is_rejected(self):
        for value in ("not-a-number", "0", "100000001"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    youtube_kafka_max_event_bytes(value)
        with patch.dict(os.environ, {"YOUTUBE_KAFKA_MAX_EVENT_BYTES": "invalid"}):
            with self.assertRaises(ValueError):
                youtube_kafka_max_event_bytes()

    def test_existing_outbox_schema_is_migrated_compatibly(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.sqlite"
            connection = sqlite3.connect(path)
            connection.execute(
                """
                CREATE TABLE youtube_worker_outbox (
                  outbox_id TEXT PRIMARY KEY,
                  worker_name TEXT NOT NULL,
                  aggregate_id TEXT NOT NULL,
                  topic TEXT NOT NULL,
                  message_key TEXT NOT NULL,
                  event_json TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  available_at TEXT NOT NULL,
                  delivery_attempts INTEGER NOT NULL DEFAULT 0,
                  last_attempt_at TEXT,
                  delivered_at TEXT,
                  last_error TEXT
                )
                """
            )
            connection.execute(
                """
                INSERT INTO youtube_worker_outbox (
                  outbox_id, worker_name, aggregate_id, topic, message_key,
                  event_json, created_at, available_at
                ) VALUES ('legacy', 'youtube_metadata', 'video-1', 'topic',
                          'video-1', '{}', '2026-07-20T00:00:00+00:00',
                          '2026-07-20T00:00:00+00:00')
                """
            )
            connection.execute(
                """
                INSERT INTO youtube_worker_outbox (
                  outbox_id, worker_name, aggregate_id, topic, message_key,
                  event_json, created_at, available_at, delivered_at
                ) VALUES ('legacy-delivered', 'youtube_metadata', 'video-2', 'topic',
                          'video-2', '{}', '2026-07-20T00:00:00+00:00',
                          '2026-07-20T00:00:00+00:00',
                          '2026-07-20T00:01:00+00:00')
                """
            )
            connection.commit()
            connection.close()

            with YouTubeStateStore(path) as state:
                rows = state.connection.execute(
                    "SELECT outbox_id, status, failure_reason, failed_at "
                    "FROM youtube_worker_outbox ORDER BY outbox_id"
                ).fetchall()
                self.assertEqual(
                    [tuple(row) for row in rows],
                    [
                        ("legacy", "pending", None, None),
                        ("legacy-delivered", "delivered", None, None),
                    ],
                )

    def test_quarantine_cli_dry_run_does_not_modify_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.sqlite"
            observed = datetime(2026, 7, 20, tzinfo=UTC)
            with YouTubeStateStore(path) as state:
                state.enqueue_outbox(
                    worker_name="youtube_transcript",
                    aggregate_id="video-1",
                    topic="youtube.transcript.results",
                    event={**self._event(), "payload_json": "x" * 1000},
                    created_at=observed,
                )
            args = type(
                "Args",
                (),
                {
                    "state_db": str(path),
                    "max_event_bytes": 1024,
                    "include_terminal": False,
                    "command": "quarantine",
                    "dry_run": True,
                },
            )()
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(run_outbox_cli(args), 0)
            self.assertIn('"matched": 1', output.getvalue())
            self.assertIn('"quarantined": 0', output.getvalue())
            with YouTubeStateStore(path) as state:
                self.assertEqual(state.outbox_health(now=observed)["pending_count"], 1)


if __name__ == "__main__":
    unittest.main()
