import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "playwright"))

from common.youtube_pipeline import parse_hour_offsets
from common.youtube_state import YouTubeStateStore
from youtube_pipeline_events import drain_outbox


UTC = timezone.utc


class _Producer:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.published = []

    def publish(self, topic, events):
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


if __name__ == "__main__":
    unittest.main()
