import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "dashboard"))

from youtube_presentation import (  # noqa: E402
    build_youtube_display_rows,
    build_youtube_freshness_table,
    coverage_summary,
    format_available_metric,
    freshness_warning,
    merge_youtube_silver_event_fallback,
    provenance_summary,
    transcript_lifecycle_status,
    transcript_provenance_label,
    transcript_retry_warning,
    transcript_status_presentation,
    youtube_data_completeness,
    youtube_thumbnail_display_url,
)


class DashboardMetricAvailabilityTests(unittest.TestCase):
    def test_silver_events_fill_stale_youtube_card_values(self):
        display_rows = pd.DataFrame(
            [
                {
                    "content_id": "content-1",
                    "platform_content_id": "comment-1",
                    "url": "https://www.youtube.com/watch?v=video-1",
                    "title": "",
                    "latest_view_count": None,
                    "latest_view_count_available": False,
                }
            ]
        )
        silver_events = pd.DataFrame(
            [
                {
                    "source": "youtube",
                    "platform_event_id": "comment-1",
                    "conversation_id": "video-1",
                    "title": "Available directly in Silver",
                    "view_count": 1250,
                    "like_count": 42,
                    "comment_count": 7,
                    "event_ts": "2026-08-03T00:00:00Z",
                    "coverage_json": '{"view_count_available":true}',
                }
            ]
        )

        result = merge_youtube_silver_event_fallback(display_rows, silver_events).iloc[0]

        self.assertEqual(result["title"], "Available directly in Silver")
        self.assertEqual(result["latest_view_count"], 1250)
        self.assertEqual(result["latest_like_count"], 42)
        self.assertEqual(result["latest_comment_count"], 7)
        self.assertTrue(result["latest_view_count_available"])
        self.assertEqual(coverage_summary(result), "3/3 snapshot metrics observed")

    def test_unsafe_or_missing_thumbnail_is_not_exposed_as_complete(self):
        for value in (None, "https://example.test/thumbnail.jpg"):
            with self.subTest(value=value):
                _, _, checks = youtube_data_completeness({"thumbnail_url": value})
                self.assertFalse(checks["thumbnail"])

    def test_missing_thumbnail_uses_safe_video_id_fallback(self):
        row = {"thumbnail_url": None, "platform_content_id": "wDWFGSq3jz4"}

        self.assertEqual(
            youtube_thumbnail_display_url(row),
            "https://img.youtube.com/vi/wDWFGSq3jz4/default.jpg",
        )
        _, _, checks = youtube_data_completeness(row)
        self.assertTrue(checks["thumbnail"])

    def test_unsafe_thumbnail_is_replaced_by_safe_fallback(self):
        row = {
            "thumbnail_url": "https://example.test/thumbnail.jpg",
            "platform_content_id": "wDWFGSq3jz4",
        }

        self.assertEqual(
            youtube_thumbnail_display_url(row),
            "https://img.youtube.com/vi/wDWFGSq3jz4/default.jpg",
        )

    def test_known_zero_is_not_rendered_as_unknown(self):
        self.assertEqual(format_available_metric(0, True), "0")
        self.assertEqual(format_available_metric(0, None), "0")
        self.assertEqual(format_available_metric(0, False), "N/A")
        self.assertEqual(format_available_metric(pd.NA, True), "N/A")

    def test_completeness_uses_known_comment_count_and_legacy_metadata_status(self):
        _, _, checks = youtube_data_completeness(
            {
                "metadata_status": "success",
                "comments_available": False,
                "latest_comment_count": 7,
                "latest_comment_count_available": True,
            }
        )

        self.assertTrue(checks["metadata"])
        self.assertTrue(checks["comments"])

    def test_coverage_prefers_explicit_flags(self):
        row = {
            "latest_view_count": 0,
            "latest_like_count": 12,
            "latest_comment_count": None,
            "latest_view_count_available": True,
            "latest_like_count_available": False,
            "latest_snapshot_coverage_json": '{"comment_count": true}',
        }

        self.assertEqual(
            coverage_summary(row),
            "1/3 snapshot metrics observed; N/A: likes, comments",
        )

    def test_provenance_uses_columns_then_json_fallback(self):
        row = {
            "latest_snapshot_producer_name": "youtube_metrics_worker",
            "latest_snapshot_provenance_json": (
                '{"collection_method":"youtube_data_api",'
                '"api_endpoint":"videos.list",'
                '"producer_run_id":"run-7"}'
            ),
        }

        self.assertEqual(
            provenance_summary(row),
            "youtube_metrics_worker · youtube_data_api · videos.list · run-7",
        )

    def test_malformed_coverage_payload_is_safe(self):
        self.assertEqual(
            coverage_summary({"latest_snapshot_coverage_json": "not-json"}),
            "0/3 snapshot metrics observed; N/A: views, likes, comments",
        )


class DashboardTranscriptLifecycleTests(unittest.TestCase):
    def test_transcript_provenance_labels_distinguish_gemini_from_youtube(self):
        self.assertEqual(
            transcript_provenance_label(
                {
                    "transcript_lifecycle_status": "available",
                    "provider": "gemini",
                    "generation_type": "model_generated",
                }
            ),
            "Transcription générée depuis la vidéo avec Gemini",
        )
        self.assertEqual(
            transcript_provenance_label(
                {
                    "transcript_lifecycle_status": "available",
                    "provider": "youtube_transcript_api",
                    "generation_type": "automatic",
                }
            ),
            "Sous-titres YouTube automatiques",
        )

    def test_all_canonical_lifecycle_states_are_preserved(self):
        statuses = (
            "pending",
            "available",
            "unavailable",
            "disabled",
            "rate_limited",
            "blocked",
            "retryable_error",
            "permanent_error",
        )
        for status in statuses:
            with self.subTest(status=status):
                row = {"transcript_lifecycle_status": status}
                self.assertEqual(transcript_lifecycle_status(row), status)
                self.assertEqual(transcript_status_presentation(row)[0], status)

    def test_legacy_status_is_mapped_without_hiding_collected_text(self):
        self.assertEqual(
            transcript_lifecycle_status({"transcript_status": "not_available"}),
            "unavailable",
        )
        self.assertEqual(
            transcript_lifecycle_status(
                {
                    "transcript_status": "failed",
                    "transcript_text": "collected text",
                }
            ),
            "available",
        )

    def test_only_retryable_overdue_attempts_warn(self):
        now = datetime(2026, 7, 20, 12, tzinfo=UTC)
        retryable = {
            "transcript_lifecycle_status": "retryable_error",
            "next_attempt_at": "2026-07-20T09:00:00Z",
        }
        terminal = {
            "transcript_lifecycle_status": "permanent_error",
            "next_attempt_at": "2026-07-20T09:00:00Z",
        }

        self.assertEqual(
            transcript_retry_warning(retryable, now=now),
            "Transcript retry is overdue by 3.0 h.",
        )
        self.assertIsNone(transcript_retry_warning(terminal, now=now))


class DashboardFreshnessTableTests(unittest.TestCase):
    def test_newer_incomplete_snapshot_does_not_erase_known_comment_count(self):
        contents = pd.DataFrame([{"content_id": "video-1", "title": "Video"}])
        snapshots = pd.DataFrame(
            [
                {
                    "content_id": "video-1",
                    "snapshot_at": "2026-07-19T00:00:00Z",
                    "observation_id": "known",
                    "comment_count": 7,
                    "comment_count_available": True,
                },
                {
                    "content_id": "video-1",
                    "snapshot_at": "2026-07-20T00:00:00Z",
                    "observation_id": "incomplete",
                    "comment_count": None,
                    "comment_count_available": False,
                },
            ]
        )

        result = build_youtube_display_rows(
            contents,
            pd.DataFrame(),
            pd.DataFrame(),
            snapshots,
        )

        self.assertEqual(result.iloc[0]["latest_comment_count"], 7)
        self.assertTrue(result.iloc[0]["latest_comment_count_available"])
        self.assertEqual(
            result.iloc[0]["latest_snapshot_at"],
            "2026-07-20T00:00:00Z",
        )

    def test_mixed_grain_joins_do_not_duplicate_video_cards(self):
        contents = pd.DataFrame(
            [
                {
                    "content_id": "video-1",
                    "title": "Video",
                    "created_at": "2026-07-18T00:00:00Z",
                }
            ]
        )
        stats = pd.DataFrame(
            [
                {
                    "content_id": "video-1",
                    "latest_snapshot_at": "2026-07-19T00:00:00Z",
                    "latest_view_count": 5,
                },
                {
                    "content_id": "video-1",
                    "latest_snapshot_at": "2026-07-20T00:00:00Z",
                    "latest_view_count": 0,
                    "latest_view_count_available": True,
                },
            ]
        )
        transcripts = pd.DataFrame(
            [
                {
                    "content_id": "video-1",
                    "requested_language_code": "vi",
                    "transcript_lifecycle_status": "available",
                    "transcript_text": "xin chao",
                    "last_attempt_at": "2026-07-19T00:00:00Z",
                },
                {
                    "content_id": "video-1",
                    "requested_language_code": "en",
                    "transcript_lifecycle_status": "retryable_error",
                    "last_attempt_at": "2026-07-20T01:00:00Z",
                },
            ]
        )

        result = build_youtube_display_rows(
            contents,
            transcripts,
            stats,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["latest_view_count"], 0)
        self.assertEqual(
            result.iloc[0]["latest_transcript_lifecycle_status"],
            "retryable_error",
        )
        self.assertTrue(result.iloc[0]["transcript_available_any"])
        self.assertTrue(youtube_data_completeness(result.iloc[0])[2]["transcript"])

    def test_table_exposes_freshness_coverage_and_na_semantics(self):
        now = datetime(2026, 7, 20, 12, tzinfo=UTC)
        rows = pd.DataFrame(
            [
                {
                    "title": "Known zero",
                    "youtube_channel_name": "Channel",
                    "url": "https://www.youtube.com/watch?v=one",
                    "last_discovered_at": "2026-07-18T12:00:00Z",
                    "last_enriched_at": "2026-07-10T12:00:00Z",
                    "latest_snapshot_at": "2026-07-20T11:00:00Z",
                    "latest_view_count": 0,
                    "latest_view_count_available": True,
                    "latest_like_count": None,
                    "latest_like_count_available": False,
                    "latest_comment_count": 0,
                    "latest_comment_count_available": True,
                    "latest_transcript_lifecycle_status": "rate_limited",
                    "latest_transcript_requested_language_code": "vi",
                    "latest_transcript_attempt_count": 2,
                    "latest_transcript_last_attempt_at": "2026-07-20T10:00:00Z",
                    "latest_transcript_next_attempt_at": "2026-07-20T11:00:00Z",
                }
            ]
        )

        result = build_youtube_freshness_table(
            rows,
            enrichment_stale_hours=24,
            snapshot_stale_hours=6,
            now=now,
        )

        self.assertEqual(result.loc[0, "Views"], "0")
        self.assertEqual(result.loc[0, "Likes"], "N/A")
        self.assertEqual(result.loc[0, "Comments"], "0")
        self.assertEqual(result.loc[0, "Transcript lifecycle"], "rate limited")
        self.assertIn("Metadata enrichment is stale", result.loc[0, "Freshness warning"])
        self.assertIn("Transcript retry is overdue", result.loc[0, "Freshness warning"])

    def test_unknown_timestamp_is_na_without_inventing_age(self):
        self.assertIsNone(
            freshness_warning(
                "Engagement snapshot",
                None,
                stale_after_hours=24,
            )
        )

    def test_freshness_thresholds_are_configured_for_compose(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("DASHBOARD_YOUTUBE_ENRICHMENT_STALE_HOURS=168", env_example)
        self.assertIn("DASHBOARD_YOUTUBE_SNAPSHOT_STALE_HOURS=24", env_example)
        self.assertIn("DASHBOARD_YOUTUBE_ENRICHMENT_STALE_HOURS", compose)
        self.assertIn("DASHBOARD_YOUTUBE_SNAPSHOT_STALE_HOURS", compose)


if __name__ == "__main__":
    unittest.main()
