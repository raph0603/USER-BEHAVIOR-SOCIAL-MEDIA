import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch


import sys
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "playwright"))
MODULE_PATH = ROOT / "playwright" / "youtube_authors.py"
SPEC = importlib.util.spec_from_file_location("youtube_authors", MODULE_PATH)
YOUTUBE_AUTHORS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(YOUTUBE_AUTHORS)


OWNER_ID = "UCOwnerChannel00000000001"
COLLABORATOR_IDS = (
    "UCCollaborator0000000001",
    "UCCollaborator0000000002",
)


def _channel_item(channel_id):
    return {
        "listItemViewModel": {
            "title": {
                "commandRuns": [
                    {
                        "onTap": {
                            "innertubeCommand": {
                                "browseEndpoint": {
                                    "browseId": channel_id,
                                }
                            }
                        }
                    }
                ]
            }
        }
    }


def _watch_html(channel_ids=None):
    navigation_endpoint = {
        "browseEndpoint": {
            "browseId": OWNER_ID,
        }
    }
    if channel_ids is not None:
        navigation_endpoint = {
            "showDialogCommand": {
                "panelLoadingStrategy": {
                    "inlineContent": {
                        "dialogViewModel": {
                            "customContent": {
                                "listViewModel": {
                                    "listItems": [
                                        _channel_item(channel_id)
                                        for channel_id in channel_ids
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }

    initial_data = {
        "contents": {
            "videoOwnerRenderer": {
                "navigationEndpoint": navigation_endpoint,
            }
        }
    }
    return f"var ytInitialData = {json.dumps(initial_data)};"


class YouTubeAuthorTests(unittest.TestCase):
    def test_video_without_collaborators_returns_empty_list(self):
        self.assertEqual(
            YOUTUBE_AUTHORS.extract_youtube_collaborator_channel_ids(
                _watch_html(),
                OWNER_ID,
            ),
            [],
        )

    def test_video_with_one_collaborator_excludes_owner(self):
        self.assertEqual(
            YOUTUBE_AUTHORS.extract_youtube_collaborator_channel_ids(
                _watch_html([OWNER_ID, COLLABORATOR_IDS[0]]),
                OWNER_ID,
            ),
            [COLLABORATOR_IDS[0]],
        )

    def test_video_with_multiple_collaborators_preserves_order(self):
        self.assertEqual(
            YOUTUBE_AUTHORS.extract_youtube_collaborator_channel_ids(
                _watch_html(
                    [
                        OWNER_ID,
                        COLLABORATOR_IDS[0],
                        COLLABORATOR_IDS[1],
                        COLLABORATOR_IDS[0],
                    ]
                ),
                OWNER_ID,
            ),
            list(COLLABORATOR_IDS),
        )

    def test_unavailable_page_returns_none(self):
        self.assertIsNone(
            YOUTUBE_AUTHORS.extract_youtube_collaborator_channel_ids(
                "<html>Video unavailable</html>",
                OWNER_ID,
            )
        )

    def test_parallel_fetch_preserves_each_video_result(self):
        owners = {
            "video-one": OWNER_ID,
            "video-two": OWNER_ID,
        }

        def fake_fetch(video_id, owner_channel_id, timeout_seconds):
            self.assertEqual(owner_channel_id, OWNER_ID)
            self.assertEqual(timeout_seconds, 7)
            return [f"UC-{video_id}"]

        with patch.object(
            YOUTUBE_AUTHORS,
            "fetch_youtube_collaborator_channel_ids",
            side_effect=fake_fetch,
        ):
            self.assertEqual(
                YOUTUBE_AUTHORS.fetch_youtube_collaborators(
                    owners,
                    timeout_seconds=7,
                    max_workers=2,
                ),
                {
                    "video-one": ["UC-video-one"],
                    "video-two": ["UC-video-two"],
                },
            )

    def test_author_metadata_is_propagated_and_preserved(self):
        paths = [
            ROOT / "schemas" / "playwright_event.avsc",
            ROOT / "playwright" / "producer.py",
            ROOT / "playwright" / "insight_refresh.py",
            ROOT
            / "spark"
            / "jobs"
            / "pipeline"
            / "collector_stream_pipeline.py",
            ROOT
            / "spark"
            / "jobs"
            / "streaming"
            / "kafka_to_iceberg_bronze.py",
            ROOT / "spark" / "jobs" / "batch" / "bronze_to_silver.py",
            ROOT
            / "spark"
            / "jobs"
            / "batch"
            / "bronze_to_silver_from_kafka.py",
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "apply_insight_updates.py",
        ]
        for path in paths:
            source = path.read_text(encoding="utf-8")
            with self.subTest(path=path):
                self.assertIn("owner_channel_id", source)
                self.assertIn("collaborator_channel_ids", source)

        for path in paths[-4:]:
            source = path.read_text(encoding="utf-8")
            with self.subTest(coalesce_path=path):
                self.assertIn("COALESCE", source)

    def test_extract_youtube_subscriber_count_success(self):
        initial_data = {
            "videoOwnerRenderer": {
                "subscriberCountText": {
                    "simpleText": "1.23M subscribers"
                }
            }
        }
        html = f"var ytInitialData = {json.dumps(initial_data)};"
        self.assertEqual(
            YOUTUBE_AUTHORS.extract_youtube_subscriber_count(html),
            1230000
        )


if __name__ == "__main__":
    unittest.main()
