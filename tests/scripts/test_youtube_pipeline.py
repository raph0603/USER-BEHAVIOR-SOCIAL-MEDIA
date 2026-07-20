import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.youtube_pipeline import (
    SearchQuery,
    canonical_metadata,
    changed_metadata_fields,
    finalize_worker_summary,
    metadata_hash,
    metrics_refresh_interval,
    next_metadata_refresh_at,
    parse_hour_offsets,
    parse_search_queries,
    published_after,
)
from common.youtube_state import YouTubeStateStore


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[2]


class YouTubeQueryConfigurationTests(unittest.TestCase):
    def test_structured_queries_keep_their_language(self):
        specs = parse_search_queries(
            json.dumps(
                [
                    {"query": "electric vehicle", "language": "en"},
                    {"query": "xe điện", "language": "vi"},
                ]
            )
        )

        self.assertEqual(
            [(spec.query, spec.language) for spec in specs],
            [("electric vehicle", "en"), ("xe điện", "vi")],
        )

    def test_legacy_queries_are_paired_not_crossed(self):
        specs = parse_search_queries(
            None,
            ["electric vehicle", "xe điện"],
            ["en", "vi"],
        )

        self.assertEqual(len(specs), 2)
        self.assertEqual([spec.language for spec in specs], ["en", "vi"])

    def test_overlap_is_subtracted_from_watermark(self):
        result = published_after(
            "2026-07-17T08:00:00+00:00",
            overlap=timedelta(hours=2),
            initial_lookback=timedelta(days=1),
            now=datetime(2026, 7, 17, 12, tzinfo=UTC),
        )

        self.assertEqual(result, datetime(2026, 7, 17, 6, tzinfo=UTC))


class YouTubeMetadataTests(unittest.TestCase):
    def test_hash_ignores_set_like_order_and_nulls(self):
        first = {
            "title": "  Example   title ",
            "tags": ["beta", "alpha"],
            "categories": ["Cars", None],
        }
        second = {
            "title": "Example title",
            "tags": ["alpha", "beta"],
            "categories": ["Cars"],
            "description": None,
        }

        self.assertEqual(metadata_hash(first), metadata_hash(second))

    def test_chapter_order_remains_significant(self):
        first = {"chapters": [{"start_time": 0}, {"start_time": 10}]}
        second = {"chapters": [{"start_time": 10}, {"start_time": 0}]}

        self.assertNotEqual(metadata_hash(first), metadata_hash(second))

    def test_changed_fields_are_precise(self):
        previous = {"title": "Old", "tags": ["a"]}
        current = {"title": "New", "tags": ["a"], "description": "Added"}

        self.assertEqual(
            changed_metadata_fields(previous, current),
            ["description", "title"],
        )

    def test_canonical_metadata_excludes_engagement(self):
        result = canonical_metadata({"title": "Video", "view_count": 10, "like_count": 2})

        self.assertEqual(result, {"title": "Video"})

    def test_metadata_schedule_uses_configured_offsets(self):
        offsets = parse_hour_offsets("6,24,72")
        first_seen = datetime(2026, 7, 17, tzinfo=UTC)

        self.assertEqual(
            next_metadata_refresh_at(first_seen, 0, offsets),
            first_seen + timedelta(hours=6),
        )
        self.assertIsNone(next_metadata_refresh_at(first_seen, 3, offsets))


class YouTubeSchedulingTests(unittest.TestCase):
    def test_metrics_intervals_decrease_with_recency(self):
        cases = (
            (timedelta(hours=2), timedelta(minutes=30)),
            (timedelta(hours=12), timedelta(hours=1)),
            (timedelta(days=2), timedelta(hours=3)),
            (timedelta(days=5), timedelta(hours=6)),
            (timedelta(days=15), timedelta(days=1)),
            (timedelta(days=45), timedelta(days=7)),
        )
        for age, expected in cases:
            with self.subTest(age=age):
                self.assertEqual(metrics_refresh_interval(age), expected)

    def test_worker_summary_reports_average_processing_time(self):
        result = finalize_worker_summary({"due": 2}, elapsed_seconds=5.0, processed=2)

        self.assertEqual(result["elapsed_seconds"], 5.0)
        self.assertEqual(result["avg_seconds_per_video"], 2.5)


class YouTubeStateTests(unittest.TestCase):
    def test_discovery_and_watermark_are_persistent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.sqlite"
            spec = SearchQuery.create("electric vehicle", "en")
            observed = datetime(2026, 7, 17, tzinfo=UTC)
            with YouTubeStateStore(path) as state:
                self.assertTrue(
                    state.record_discovery(
                        video_id="video-1",
                        query_id=spec.query_id,
                        first_seen_at=observed,
                        published_at=observed,
                        correlation_id="correlation-1",
                    )
                )
                self.assertFalse(
                    state.record_discovery(
                        video_id="video-1",
                        query_id=spec.query_id,
                        first_seen_at=observed,
                        published_at=observed,
                        correlation_id="correlation-2",
                    )
                )
                state.record_search_success(
                    spec,
                    searched_at=observed,
                    last_published_at_seen=observed,
                )

            with YouTubeStateStore(path) as state:
                self.assertTrue(state.is_discovered("video-1"))
                self.assertEqual(
                    state.watermark(spec.query_id)["last_published_at_seen"],
                    observed.isoformat(),
                )
                self.assertEqual(len(state.due_metadata(observed, 10)), 1)

    def test_unchanged_metadata_does_not_report_changed_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.sqlite"
            observed = datetime(2026, 7, 17, tzinfo=UTC)
            offsets = parse_hour_offsets("6,24")
            with YouTubeStateStore(path) as state:
                state.record_discovery(
                    video_id="video-1",
                    query_id="query",
                    first_seen_at=observed,
                    published_at=observed,
                    correlation_id="correlation",
                )
                first_hash, previous_hash, changed = state.record_metadata_success(
                    video_id="video-1",
                    observed_at=observed,
                    metadata={"title": "Example", "tags": ["a", "b"]},
                    offsets=offsets,
                )
                second_hash, previous_hash, changed = state.record_metadata_success(
                    video_id="video-1",
                    observed_at=observed + timedelta(hours=6),
                    metadata={"title": "Example", "tags": ["b", "a"]},
                    offsets=offsets,
                )

                self.assertEqual(first_hash, second_hash)
                self.assertEqual(previous_hash, first_hash)
                self.assertEqual(changed, [])

    def test_circuit_breaker_respects_cooldown(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = datetime(2026, 7, 17, tzinfo=UTC)
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                state.open_breaker(
                    "transcript",
                    now=observed,
                    cooldown=timedelta(hours=1),
                    reason="ip blocked",
                )
                self.assertTrue(state.breaker_open("transcript", observed))
                self.assertFalse(state.breaker_open("transcript", observed + timedelta(hours=2)))

    def test_request_state_and_api_usage_are_persistent(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = datetime(2026, 7, 17, tzinfo=UTC)
            with YouTubeStateStore(Path(directory) / "youtube.sqlite") as state:
                self.assertTrue(
                    state.enqueue_request(
                        "transcript",
                        video_id="video-1",
                        correlation_id="correlation",
                        first_seen_at=observed,
                        published_at=observed,
                        request={"language": "en"},
                    )
                )
                self.assertEqual(
                    len(state.due_requests("transcript", now=observed, limit=10)),
                    1,
                )
                state.record_request_result(
                    "transcript",
                    video_id="video-1",
                    status="available",
                    attempted_at=observed,
                    next_attempt_at=None,
                    result={"text": "hello"},
                )
                self.assertEqual(
                    state.due_requests("transcript", now=observed, limit=10),
                    [],
                )
                state.record_api_usage(
                    endpoint="videos.list",
                    request_count=1,
                    resource_count=50,
                    success_count=1,
                    error_count=0,
                    quota_bucket="recent_metrics",
                    observed_at=observed,
                )
                self.assertEqual(
                    state.api_requests_today("videos.list", observed),
                    1,
                )

    def test_channel_cache_is_persistent_and_deduplicated(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = datetime(2026, 7, 17, tzinfo=UTC)
            path = Path(directory) / "youtube.sqlite"
            with YouTubeStateStore(path) as state:
                self.assertTrue(
                    state.enqueue_channel(
                        channel_id="channel-1",
                        first_seen_at=observed,
                        last_video_published_at=observed,
                    )
                )
                self.assertFalse(
                    state.enqueue_channel(
                        channel_id="channel-1",
                        first_seen_at=observed,
                        last_video_published_at=observed,
                    )
                )
                state.record_channel_success(
                    channel_id="channel-1",
                    observed_at=observed,
                    subscriber_count=123,
                    hidden_subscriber_count=False,
                    active_after=observed - timedelta(days=30),
                    active_interval=timedelta(days=1),
                    inactive_interval=timedelta(days=7),
                )

            with YouTubeStateStore(path) as state:
                row = state.channel_state("channel-1")
                self.assertEqual(row["subscriber_count"], 123)
                self.assertEqual(
                    row["next_refresh_at"],
                    (observed + timedelta(days=1)).isoformat(),
                )


class YouTubeSchemaCompatibilityTests(unittest.TestCase):
    def test_new_avro_fields_are_nullable_with_null_defaults(self):
        schema = json.loads(
            (ROOT / "schemas" / "playwright_event.avsc").read_text(encoding="utf-8")
        )
        fields = {field["name"]: field for field in schema["fields"]}
        for field_name in (
            "event_type",
            "event_version",
            "video_id",
            "correlation_id",
            "metadata_hash",
            "changed_fields",
            "next_metrics_refresh_at",
        ):
            self.assertIn("null", fields[field_name]["type"])
            self.assertIsNone(fields[field_name]["default"])


if __name__ == "__main__":
    unittest.main()
