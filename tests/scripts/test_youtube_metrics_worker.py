import contextlib
import importlib.util
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
PLAYWRIGHT_DIR = ROOT / "playwright"
sys.path.insert(0, str(PLAYWRIGHT_DIR))


def _load_worker():
    path = PLAYWRIGHT_DIR / "youtube_metrics_worker.py"
    spec = importlib.util.spec_from_file_location("youtube_metrics_worker", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Request:
    def __init__(self, response):
        self.response = response

    def execute(self):
        return self.response


class _VideosResource:
    def __init__(self, statistics_by_id):
        self.statistics_by_id = statistics_by_id
        self.calls = []

    def list(self, *, part, id):
        video_ids = id.split(",")
        self.calls.append((part, video_ids))
        return _Request(
            {
                "items": [
                    {"id": video_id, "statistics": self.statistics_by_id[video_id]}
                    for video_id in video_ids
                    if video_id in self.statistics_by_id
                ]
            }
        )


class _YouTube:
    def __init__(self, statistics_by_id):
        self.resource = _VideosResource(statistics_by_id)

    def videos(self):
        return self.resource


class _Producer:
    def __init__(self):
        self.published = []

    def publish(self, topic, events):
        materialized = list(events)
        self.published.extend((topic, event) for event in materialized)
        return len(materialized)


class YouTubeMetricsWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.worker = _load_worker()
        cls.observed_at = datetime(2026, 7, 20, 1, 2, 3, tzinfo=timezone.utc)

    def _target(self, video_id):
        return {
            "source": "youtube",
            "platform_event_id": video_id,
            "user_id": "user-1",
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "event_ts": "2026-07-19T00:00:00+00:00",
            "metrics_refresh_count": 0,
        }

    def test_videos_list_requests_are_batched_at_fifty(self):
        targets = [self._target(f"video-{index}") for index in range(51)]
        youtube = _YouTube({target["platform_event_id"]: {"viewCount": "1"} for target in targets})

        updates = self.worker.collect_metrics(
            targets,
            youtube=youtube,
            now=lambda: self.observed_at,
        )

        self.assertEqual(len(updates), 51)
        self.assertEqual([len(call[1]) for call in youtube.resource.calls], [50, 1])
        self.assertTrue(all(call[0] == "statistics" for call in youtube.resource.calls))

    def test_stateless_request_limit_preserves_daily_quota_budget(self):
        targets = [self._target(f"video-{index}") for index in range(51)]
        youtube = _YouTube({target["platform_event_id"]: {"viewCount": "1"} for target in targets})

        updates = self.worker.collect_metrics(
            targets,
            youtube=youtube,
            max_requests=1,
            now=lambda: self.observed_at,
        )

        self.assertEqual(len(updates), 50)
        self.assertEqual([len(call[1]) for call in youtube.resource.calls], [50])

    def test_known_zero_and_missing_metric_have_distinct_coverage(self):
        youtube = _YouTube({"video-1": {"viewCount": "0", "commentCount": "0"}})

        update = self.worker.collect_metrics(
            [self._target("video-1")],
            youtube=youtube,
            now=lambda: self.observed_at,
        )[0]
        coverage = json.loads(update["coverage_json"])

        self.assertEqual(update["view_count"], 0)
        self.assertTrue(update["view_count_available"])
        self.assertEqual(update["comment_count"], 0)
        self.assertTrue(update["comment_count_available"])
        self.assertIsNone(update["like_count"])
        self.assertFalse(update["like_count_available"])
        self.assertTrue(coverage["view_count"])
        self.assertFalse(coverage["like_count"])

    def test_missing_video_is_an_explicit_unavailable_observation(self):
        update = self.worker.collect_metrics(
            [self._target("private-video")],
            youtube=_YouTube({}),
            now=lambda: self.observed_at,
        )[0]

        self.assertEqual(update["metrics_refresh_status"], "unavailable")
        self.assertEqual(update["metrics_error_code"], "videos_list_not_returned")
        self.assertFalse(update["view_count_available"])

    def test_replaying_same_observation_has_stable_identity(self):
        youtube = _YouTube({"video-1": {"viewCount": "10"}})
        first = self.worker.collect_metrics(
            [self._target("video-1")],
            youtube=youtube,
            now=lambda: self.observed_at,
        )[0]
        replay = self.worker.collect_metrics(
            [self._target("video-1")],
            youtube=youtube,
            now=lambda: self.observed_at,
        )[0]

        self.assertEqual(first["observation_id"], replay["observation_id"])
        self.assertEqual(first["payload_fingerprint"], replay["payload_fingerprint"])

    def test_sqlite_lock_after_collection_does_not_prevent_kafka_delivery(self):
        producer = _Producer()
        youtube = _YouTube({"video-1": {"viewCount": "10", "likeCount": "2"}})

        def locked_state(*_args, **_kwargs):
            self.assertEqual(len(producer.published), 1)
            raise sqlite3.OperationalError("database is locked")

        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            targets_path = directory_path / "targets.jsonl"
            output_path = directory_path / "youtube.jsonl"
            targets_path.write_text(json.dumps(self._target("video-1")) + "\n", encoding="utf-8")
            environment = {
                "INSIGHT_REFRESH_TARGETS_PATH": str(targets_path),
                "INSIGHT_REFRESH_OUTPUT_PATH": str(output_path),
                "YOUTUBE_PIPELINE_STATE_DB": str(directory_path / "state.sqlite"),
                "YOUTUBE_API_KEY": "test-key",
                "YOUTUBE_METRICS_MAX_REQUESTS_PER_RUN": "1",
            }
            with (
                patch.dict(os.environ, environment, clear=False),
                patch.object(self.worker, "EventProducer", return_value=producer),
                patch.object(self.worker, "YouTubeStateStore", side_effect=locked_state),
                patch("googleapiclient.discovery.build", return_value=youtube),
                contextlib.redirect_stdout(io.StringIO()) as output,
            ):
                self.worker.main()

            summary = json.loads(output.getvalue())
            persisted_updates = [json.loads(line) for line in output_path.read_text().splitlines()]

        self.assertEqual(summary["delivery_mode"], "kafka_first")
        self.assertEqual(summary["kafka_published"], 1)
        self.assertFalse(summary["state_persisted"])
        self.assertEqual(persisted_updates[0]["view_count"], 10)
        self.assertEqual(producer.published[0][0], "youtube.engagement.snapshots")

    def test_worker_does_not_depend_on_browser_or_watch_navigation(self):
        source = (PLAYWRIGHT_DIR / "youtube_metrics_worker.py").read_text(encoding="utf-8")

        self.assertNotIn("from playwright", source.lower())
        self.assertNotIn("sync_playwright", source.lower())
        self.assertNotIn(".goto(", source)
        self.assertNotIn("/watch", source)
        self.assertIn('.list(part="statistics"', source)
        self.assertIn("producer.publish(topic, events)", source)
        self.assertNotIn("enqueue_outbox(", source)


if __name__ == "__main__":
    unittest.main()
