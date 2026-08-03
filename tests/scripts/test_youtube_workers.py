import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PLAYWRIGHT_DIR = ROOT / "playwright"
sys.path.insert(0, str(PLAYWRIGHT_DIR))

googleapiclient = types.ModuleType("googleapiclient")
googleapiclient_discovery = types.ModuleType("googleapiclient.discovery")
googleapiclient_discovery.build = lambda *_args, **_kwargs: None
googleapiclient_errors = types.ModuleType("googleapiclient.errors")
googleapiclient_errors.HttpError = RuntimeError
googleapiclient.discovery = googleapiclient_discovery
googleapiclient.errors = googleapiclient_errors
sys.modules.setdefault("googleapiclient", googleapiclient)
sys.modules.setdefault("googleapiclient.discovery", googleapiclient_discovery)
sys.modules.setdefault("googleapiclient.errors", googleapiclient_errors)

import youtube_comment_worker
import youtube_channel_worker
import youtube_discovery


class FakeRequest:
    def __init__(self, payload):
        self.payload = payload

    def execute(self):
        return self.payload


class FakeCommentThreads:
    def __init__(self, pages, calls):
        self.pages = pages
        self.calls = calls

    def list(self, **kwargs):
        self.calls.append(kwargs)
        return FakeRequest(self.pages[len(self.calls) - 1])


class FakeYouTube:
    def __init__(self, pages):
        self.calls = []
        self._threads = FakeCommentThreads(pages, self.calls)

    def commentThreads(self):
        return self._threads


class FakeChannels:
    def __init__(self, calls):
        self.calls = calls

    def list(self, **kwargs):
        self.calls.append(kwargs)
        ids = kwargs["id"].split(",")
        return FakeRequest(
            {
                "items": [
                    {"id": channel_id, "statistics": {"subscriberCount": "10"}}
                    for channel_id in ids
                ]
            }
        )


class FakeChannelYouTube:
    def __init__(self):
        self.calls = []
        self._channels = FakeChannels(self.calls)

    def channels(self):
        return self._channels


class FakeSearch:
    def __init__(self, calls):
        self.calls = calls

    def list(self, **kwargs):
        self.calls.append(kwargs)
        index = len(self.calls)
        return FakeRequest(
            {
                "items": [{"id": {"videoId": f"video-{index}"}}],
                "nextPageToken": f"page-{index + 1}",
            }
        )


class FakeSearchYouTube:
    def __init__(self):
        self.calls = []
        self._search = FakeSearch(self.calls)

    def search(self):
        return self._search


def thread(comment_id):
    return {
        "snippet": {
            "topLevelComment": {
                "id": comment_id,
                "snippet": {"textDisplay": comment_id},
            }
        }
    }


class YouTubeCommentWorkerTests(unittest.TestCase):
    def test_pagination_stops_when_known_comment_is_seen(self):
        youtube = FakeYouTube(
            [
                {"items": [thread("new-1")], "nextPageToken": "page-2"},
                {"items": [thread("known"), thread("older")]},
            ]
        )

        comments, pages, stopped = youtube_comment_worker.fetch_incremental_comments(
            youtube,
            "video-1",
            known_comment_ids={"known"},
            max_pages=5,
        )

        self.assertEqual([comment["id"] for comment in comments], ["new-1"])
        self.assertEqual(pages, 2)
        self.assertTrue(stopped)

    def test_comment_pages_are_bounded(self):
        youtube = FakeYouTube(
            [
                {"items": [thread("one")], "nextPageToken": "page-2"},
                {"items": [thread("two")], "nextPageToken": "page-3"},
            ]
        )

        _comments, pages, _stopped = youtube_comment_worker.fetch_incremental_comments(
            youtube,
            "video-1",
            known_comment_ids=set(),
            max_pages=2,
        )

        self.assertEqual(pages, 2)


class YouTubeChannelWorkerTests(unittest.TestCase):
    def test_channel_statistics_are_fetched_in_one_batch(self):
        youtube = FakeChannelYouTube()
        channel_ids = [f"channel-{index}" for index in range(50)]

        statistics = youtube_channel_worker.fetch_channel_statistics(youtube, channel_ids)

        self.assertEqual(len(youtube.calls), 1)
        self.assertEqual(youtube.calls[0]["part"], "statistics")
        self.assertEqual(len(statistics), 50)

    def test_channel_batches_never_exceed_fifty(self):
        batches = list(
            youtube_channel_worker.batched((f"channel-{index}" for index in range(101)), size=100)
        )

        self.assertEqual([len(batch) for batch in batches], [50, 50, 1])

    def test_channel_worker_does_not_open_watch_pages(self):
        source = (PLAYWRIGHT_DIR / "youtube_channel_worker.py").read_text(encoding="utf-8")

        self.assertNotIn("youtube.com/watch", source)
        self.assertIn("channels.list", source)


class YouTubeDiscoveryWorkerTests(unittest.TestCase):
    def test_continuous_discovery_reads_one_page(self):
        youtube = FakeSearchYouTube()
        spec = youtube_discovery.SearchQuery.create("electric vehicle", "en")

        _items, calls = youtube_discovery.search_query(
            youtube,
            spec,
            published_after_value="2026-07-17T00:00:00Z",
            backfill=False,
            max_pages=10,
            order="date",
        )

        self.assertEqual(calls, 1)
        self.assertEqual(youtube.calls[0]["maxResults"], 50)

    def test_explicit_backfill_respects_page_limit(self):
        youtube = FakeSearchYouTube()
        spec = youtube_discovery.SearchQuery.create("electric vehicle", "en")

        _items, calls = youtube_discovery.search_query(
            youtube,
            spec,
            published_after_value="2026-07-17T00:00:00Z",
            backfill=True,
            max_pages=3,
            order="date",
        )

        self.assertEqual(calls, 3)


class YouTubeWorkerArchitectureTests(unittest.TestCase):
    def test_state_dependent_worker_results_are_published_through_the_outbox(self):
        for name in (
            "youtube_discovery.py",
            "youtube_metadata_worker.py",
            "youtube_transcript_worker.py",
            "youtube_comment_worker.py",
            "youtube_channel_worker.py",
        ):
            source = (PLAYWRIGHT_DIR / name).read_text(encoding="utf-8")

            self.assertNotIn("producer.publish(", source, name)
            self.assertIn("enqueue_outbox(", source, name)
            self.assertIn("drain_outbox(", source, name)

    def test_metrics_worker_publishes_before_auxiliary_sqlite_state(self):
        source = (PLAYWRIGHT_DIR / "youtube_metrics_worker.py").read_text(encoding="utf-8")

        publish_position = source.index("producer.publish(topic, events)")
        state_position = source.index("_persist_state_after_kafka(", publish_position)
        self.assertLess(publish_position, state_position)
        self.assertNotIn("enqueue_outbox(", source)

    def test_kafka_producer_enables_idempotent_delivery(self):
        source = (PLAYWRIGHT_DIR / "youtube_pipeline_events.py").read_text(encoding="utf-8")
        collector_source = (PLAYWRIGHT_DIR / "producer.py").read_text(encoding="utf-8")

        self.assertIn('"enable.idempotence": True', source)
        self.assertIn('"acks": "all"', source)
        self.assertIn('"enable.idempotence": True', collector_source)
        self.assertIn('"acks": "all"', collector_source)

    def test_engagement_topic_is_consumed_into_the_lakehouse(self):
        source = (ROOT / "orchestrator" / "dags" / "lakehouse_dag_factory.py").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            "youtube.channel.results,youtube.engagement.snapshots",
            source,
        )
        self.assertIn('required_source_variable="YOUTUBE_ENGAGEMENT_TOPIC"', source)

    def test_yt_dlp_worker_never_downloads_media(self):
        source = (PLAYWRIGHT_DIR / "youtube_metadata_worker.py").read_text(encoding="utf-8")

        self.assertIn('"skip_download": True', source)
        self.assertIn("download=False", source)
        self.assertNotIn("download=True", source)

    def test_yt_dlp_events_compact_caption_track_urls(self):
        source = (PLAYWRIGHT_DIR / "youtube_metadata_worker.py").read_text(encoding="utf-8")

        self.assertIn("def compact_yt_dlp_event_payload", source)
        self.assertIn('for field in ("subtitles", "automatic_captions")', source)
        self.assertIn('"storage_uri": str(diagnostic_path)', source)
        self.assertIn('"sha256": hashlib.sha256(', source)
        self.assertNotIn(
            "raw_source_payload=json.dumps(\n                                event_raw,", source
        )

    def test_worker_events_do_not_duplicate_heavy_api_results(self):
        discovery = (PLAYWRIGHT_DIR / "youtube_discovery.py").read_text(encoding="utf-8")
        comments = (PLAYWRIGHT_DIR / "youtube_comment_worker.py").read_text(encoding="utf-8")
        transcripts = (PLAYWRIGHT_DIR / "youtube_transcript_worker.py").read_text(encoding="utf-8")

        self.assertNotIn('"search_snippet": snippet', discovery)
        self.assertIn('"comment_count": len(comments)', comments)
        self.assertNotIn(
            "payload_json=json.dumps(result, ensure_ascii=False, sort_keys=True)",
            comments,
        )
        self.assertIn("transcript_segments_json=None", transcripts)
        self.assertNotIn(
            "payload_json=json.dumps(result, ensure_ascii=False, sort_keys=True)",
            transcripts,
        )

    def test_transcript_and_comment_workers_have_separate_topics(self):
        transcript = (PLAYWRIGHT_DIR / "youtube_transcript_worker.py").read_text(encoding="utf-8")
        comments = (PLAYWRIGHT_DIR / "youtube_comment_worker.py").read_text(encoding="utf-8")

        self.assertIn("youtube.transcript.requests", transcript)
        self.assertIn("youtube.transcript.results", transcript)
        self.assertIn("youtube.comment.requests", comments)
        self.assertIn("youtube.comment.results", comments)

    def test_workers_commit_only_after_polling_events(self):
        worker_paths = (
            "youtube_channel_worker.py",
            "youtube_comment_worker.py",
            "youtube_metadata_worker.py",
            "youtube_transcript_worker.py",
        )

        for worker_path in worker_paths:
            with self.subTest(worker=worker_path):
                source = (PLAYWRIGHT_DIR / worker_path).read_text(encoding="utf-8")
                self.assertRegex(source, r"if events:\s+consumer\.commit\(\)")

    def test_lakehouse_dags_use_independent_youtube_tasks(self):
        source = (ROOT / "orchestrator" / "dags" / "lakehouse_dag_factory.py").read_text(
            encoding="utf-8"
        )
        for task_id in (
            "discover_youtube_videos",
            "enrich_youtube_metadata",
            "process_youtube_transcript_requests",
            "process_youtube_comment_requests",
            "refresh_youtube_channel_statistics",
            "append_youtube_metadata_versions",
            "persist_youtube_api_usage",
        ):
            self.assertIn(f'task_id="{task_id}"', source)
        self.assertNotIn('task_id="collect_youtube_api_events"', source)


if __name__ == "__main__":
    unittest.main()
