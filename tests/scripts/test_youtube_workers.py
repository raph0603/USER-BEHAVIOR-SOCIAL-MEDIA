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

        statistics = youtube_channel_worker.fetch_channel_statistics(
            youtube, channel_ids
        )

        self.assertEqual(len(youtube.calls), 1)
        self.assertEqual(youtube.calls[0]["part"], "statistics")
        self.assertEqual(len(statistics), 50)

    def test_channel_batches_never_exceed_fifty(self):
        batches = list(
            youtube_channel_worker.batched(
                (f"channel-{index}" for index in range(101)), size=100
            )
        )

        self.assertEqual([len(batch) for batch in batches], [50, 50, 1])

    def test_channel_worker_does_not_open_watch_pages(self):
        source = (PLAYWRIGHT_DIR / "youtube_channel_worker.py").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("youtube.com/watch", source)
        self.assertIn("channels.list", source)


class YouTubeWorkerArchitectureTests(unittest.TestCase):
    def test_yt_dlp_worker_never_downloads_media(self):
        source = (PLAYWRIGHT_DIR / "youtube_metadata_worker.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('"skip_download": True', source)
        self.assertIn("download=False", source)
        self.assertNotIn("download=True", source)

    def test_transcript_and_comment_workers_have_separate_topics(self):
        transcript = (PLAYWRIGHT_DIR / "youtube_transcript_worker.py").read_text(
            encoding="utf-8"
        )
        comments = (PLAYWRIGHT_DIR / "youtube_comment_worker.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("youtube.transcript.requests", transcript)
        self.assertIn("youtube.transcript.results", transcript)
        self.assertIn("youtube.comment.requests", comments)
        self.assertIn("youtube.comment.results", comments)

    def test_lakehouse_dags_use_independent_youtube_tasks(self):
        for name in (
            "user_behavior_lakehouse.py",
            "user_behavior_lakehouse_no_row_checks.py",
        ):
            source = (ROOT / "orchestrator" / "dags" / name).read_text(
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
