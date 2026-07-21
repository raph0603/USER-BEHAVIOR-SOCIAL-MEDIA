import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.transcripts import TranscriptPayload
from common.youtube_state import YouTubeStateStore


NOW = datetime(2026, 7, 21, 8, tzinfo=timezone.utc)


def gemini_payload():
    return TranscriptPayload(
        video_id="dQw4w9WgXcQ",
        language="en",
        language_code="en",
        is_generated=None,
        is_translated=False,
        source_language="en",
        source_language_code="en",
        source="gemini",
        selection_strategy="gemini_youtube_url_fallback",
        text="hello world",
        segments=({"start": 0.0, "end": 1.0, "text": "hello world"},),
        segment_count=1,
        word_count=2,
        available_languages=(),
        covered_duration_seconds=1.0,
        collected_at=NOW,
        requested_language="en",
        requested_language_code="en",
        model="gemini-3.5-flash",
        prompt_version="youtube-transcript-v1",
        generated_by_model=True,
        generation_type_override="model_generated",
    )


class GeminiTranscriptStateTests(unittest.TestCase):
    def test_usage_follows_the_gemini_pacific_quota_day(self):
        before_reset = datetime(2026, 7, 21, 6, 59, tzinfo=timezone.utc)
        after_reset = before_reset + timedelta(minutes=2)
        with tempfile.TemporaryDirectory() as directory:
            with YouTubeStateStore(Path(directory) / "state.sqlite") as state:
                for index, (observed_at, minutes) in enumerate(
                    ((before_reset, 10.0), (after_reset, 20.0))
                ):
                    state.record_api_usage(
                        endpoint="transcripts.generate_from_youtube_url",
                        request_count=1,
                        resource_count=1,
                        success_count=0,
                        error_count=1,
                        quota_bucket="transcript_fallback",
                        observed_at=observed_at,
                        provider="gemini",
                        video_minutes=minutes,
                    )
                    state.record_transcript_provider_attempt(
                        attempt_id=f"attempt-{index}",
                        video_id=f"video-{index}",
                        requested_language_code="en",
                        provider="gemini",
                        model="gemini-3.5-flash",
                        attempt_count=1,
                        attempted_at=observed_at,
                        latency_ms=1,
                        status="failed",
                        error_code="gemini_invalid_response",
                        fallback_reason="no_transcript_found",
                        result={"status": "failed"},
                    )

                state.record_transcript_provider_attempt(
                    attempt_id="fallback-model-attempt",
                    video_id="video-fallback",
                    requested_language_code="en",
                    provider="gemini",
                    model="gemini-3.1-flash-lite",
                    attempt_count=2,
                    attempted_at=after_reset,
                    latency_ms=1,
                    status="failed",
                    error_code="gemini_timeout",
                    fallback_reason="no_transcript_found",
                    result={"status": "failed"},
                )

                self.assertEqual(state.gemini_video_minutes_today(NOW), 20.0)
                self.assertEqual(state.gemini_requests_current_quota_day(NOW), 3)
                self.assertEqual(
                    state.gemini_requests_current_quota_day(NOW, "gemini-3.5-flash"),
                    1,
                )
                self.assertEqual(
                    state.gemini_requests_current_quota_day(
                        NOW,
                        "gemini-3.1-flash-lite",
                    ),
                    2,
                )

    def test_cache_usage_and_attempt_replay_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            with YouTubeStateStore(Path(directory) / "state.sqlite") as state:
                payload = gemini_payload()
                state.cache_gemini_transcript(
                    "cache-key",
                    payload,
                    1.5,
                    source_content_version="metadata-v1",
                )
                state.cache_gemini_transcript(
                    "cache-key",
                    payload,
                    1.5,
                    source_content_version="metadata-v1",
                )
                cached = state.cached_gemini_transcript("cache-key", accessed_at=NOW)
                self.assertEqual(cached.content_version, payload.content_version)
                self.assertEqual(cached.generation_type, "model_generated")
                self.assertEqual(
                    state.connection.execute(
                        "SELECT COUNT(*) FROM gemini_transcript_cache"
                    ).fetchone()[0],
                    1,
                )

                state.record_api_usage(
                    endpoint="transcripts.generate_from_youtube_url",
                    request_count=1,
                    resource_count=1,
                    success_count=1,
                    error_count=0,
                    quota_bucket="transcript_fallback",
                    observed_at=NOW,
                    provider="gemini",
                    video_minutes=1.5,
                    daily_video_minutes_budget=10,
                    remaining_video_minutes=8.5,
                )
                self.assertEqual(state.gemini_video_minutes_today(NOW), 1.5)

                result = {"status": "success", "payload": payload.to_dict()}
                arguments = {
                    "attempt_id": "stable-attempt",
                    "video_id": payload.video_id,
                    "requested_language_code": "en",
                    "provider": "gemini",
                    "model": payload.model,
                    "attempt_count": 1,
                    "attempted_at": NOW,
                    "latency_ms": 10,
                    "status": "success",
                    "error_code": None,
                    "fallback_reason": "no_transcript_found",
                    "result": result,
                }
                state.record_transcript_provider_attempt(**arguments)
                state.record_transcript_provider_attempt(**arguments)
                self.assertEqual(
                    state.connection.execute(
                        "SELECT COUNT(*) FROM youtube_transcript_provider_attempts"
                    ).fetchone()[0],
                    1,
                )


if __name__ == "__main__":
    unittest.main()
