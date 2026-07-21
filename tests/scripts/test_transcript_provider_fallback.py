import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from common.collection import OperationResult
from common.gemini_transcripts import (
    GeminiTranscriptConfig,
    GeminiTranscriptProvider,
)
from common.transcripts import (
    TranscriptPayload,
    TranscriptProviderChain,
    TranscriptRequest,
)


NOW = datetime(2026, 7, 21, 8, tzinfo=timezone.utc)
VIDEO_ID = "dQw4w9WgXcQ"


def payload(source="youtube_transcript_api"):
    generated = source == "gemini"
    return TranscriptPayload(
        video_id=VIDEO_ID,
        language="en",
        language_code="en",
        is_generated=None if generated else False,
        is_translated=False,
        source_language="en",
        source_language_code="en",
        source=source,
        selection_strategy=("gemini_youtube_url_fallback" if generated else "manual_preferred"),
        text="hello world",
        segments=({"start": 0.0, "end": 1.0, "text": "hello world"},),
        segment_count=1,
        word_count=2,
        available_languages=(),
        covered_duration_seconds=1.0,
        collected_at=NOW,
        generated_by_model=generated,
        generation_type_override="model_generated" if generated else None,
    )


class FakeProvider:
    def __init__(self, name, result):
        self.name = name
        self.result = result
        self.calls = 0

    def fetch(self, request):
        self.calls += 1
        return self.result


class TranscriptProviderChainTests(unittest.TestCase):
    def request(self, **changes):
        values = {
            "video_id": VIDEO_ID,
            "requested_language_code": "en",
            "attempt_count": 1,
            "max_primary_attempts": 3,
            "duration_seconds": 60,
            "video_availability": "public",
        }
        values.update(changes)
        return TranscriptRequest(**values)

    def chain(self, primary_result, fallback_result=None, enabled=True):
        primary = FakeProvider("youtube_transcript_api", primary_result)
        fallback = FakeProvider(
            "gemini", fallback_result or OperationResult.success(payload("gemini"))
        )
        return (
            TranscriptProviderChain(primary, fallback, fallback_enabled=enabled),
            primary,
            fallback,
        )

    def test_primary_success_never_calls_gemini(self):
        chain, primary, fallback = self.chain(OperationResult.success(payload()))
        result = chain.collect(self.request())
        self.assertEqual(result.final_result.payload.source, "youtube_transcript_api")
        self.assertEqual(primary.calls, 1)
        self.assertEqual(fallback.calls, 0)

    def test_unavailable_disabled_and_blocked_primary_use_gemini(self):
        results = (
            OperationResult.unavailable(error_code="no_transcript_found"),
            OperationResult.disabled(error_code="transcripts_disabled"),
            OperationResult.rate_limited(error_code="ip_blocked"),
        )
        for primary_result in results:
            with self.subTest(code=primary_result.error_code):
                chain, _, fallback = self.chain(primary_result)
                result = chain.collect(self.request())
                self.assertTrue(result.used_fallback)
                self.assertEqual(result.final_result.payload.source, "gemini")
                self.assertEqual(
                    result.final_result.payload.fallback_reason, primary_result.error_code
                )
                self.assertEqual(fallback.calls, 1)

    def test_open_primary_circuit_uses_gemini_without_calling_primary(self):
        chain, primary, fallback = self.chain(OperationResult.failed(error_code="unused"))
        result = chain.collect(self.request(), primary_circuit_open=True)
        self.assertEqual(primary.calls, 0)
        self.assertEqual(fallback.calls, 1)
        self.assertEqual(result.fallback_reason, "primary_circuit_open")

    def test_transient_primary_error_waits_for_retry_threshold(self):
        chain, _, fallback = self.chain(OperationResult.failed(error_code="timeout_error"))
        result = chain.collect(self.request(attempt_count=2, max_primary_attempts=3))
        self.assertFalse(result.used_fallback)
        self.assertEqual(fallback.calls, 0)

    def test_invalid_or_private_video_never_calls_gemini(self):
        for request in (
            self.request(video_id="invalid"),
            self.request(video_availability="private"),
        ):
            with self.subTest(request=request):
                chain, _, fallback = self.chain(
                    OperationResult.unavailable(error_code="no_transcript_found")
                )
                self.assertFalse(chain.collect(request).used_fallback)
                self.assertEqual(fallback.calls, 0)

    def test_disabled_fallback_preserves_primary_result(self):
        primary_result = OperationResult.unavailable(error_code="no_transcript_found")
        chain, _, fallback = self.chain(primary_result, enabled=False)
        result = chain.collect(self.request())
        self.assertIs(result.final_result, primary_result)
        self.assertEqual(fallback.calls, 0)


class FakeInteraction:
    def __init__(self, output_text):
        self.output_text = output_text


class FakeInteractions:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return FakeInteraction(response)


class FakeClient:
    def __init__(self, responses):
        self.interactions = FakeInteractions(responses)
        self.closed = False

    def close(self):
        self.closed = True


def response_json():
    return json.dumps(
        {
            "detected_language": "en",
            "text": "hello world",
            "segments": [{"start_seconds": 0, "end_seconds": 1.25, "text": "hello world"}],
            "covered_duration_seconds": 1.25,
            "warnings": [],
        }
    )


class GeminiProviderTests(unittest.TestCase):
    def test_provider_sends_public_url_without_downloading_video_or_audio(self):
        source = (
            Path(__file__).resolve().parents[2] / "common" / "gemini_transcripts.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("yt_dlp", source)
        self.assertNotIn("urlretrieve", source)
        self.assertNotIn("requests.get", source)
        self.assertNotIn("open(request.youtube_url", source)

    def config(self, **changes):
        values = {
            "api_key": "test-only",
            "enabled": True,
            "model": "gemini-3.5-flash",
            "fallback_models": (),
            "max_attempts": 1,
            "timeout_seconds": 5,
            "max_duration_minutes": 10,
            "daily_video_minutes_budget": 10,
            "daily_request_budget": 20,
            "cooldown_seconds": 30,
        }
        values.update(changes)
        return GeminiTranscriptConfig(**values)

    def request(self, **changes):
        values = {
            "video_id": VIDEO_ID,
            "requested_language_code": "en",
            "duration_seconds": 90,
            "video_availability": "public",
            "source_content_version": "metadata-v1",
        }
        values.update(changes)
        return TranscriptRequest(**values)

    def test_missing_key_and_exhausted_budget_do_not_create_client(self):
        for config, used, expected in (
            (self.config(api_key=""), 0, "gemini_api_key_missing"),
            (self.config(daily_video_minutes_budget=1), 1, "gemini_budget_exhausted"),
        ):
            calls = []
            provider = GeminiTranscriptProvider(
                config,
                client_factory=lambda _config: calls.append(True),
                used_video_minutes=lambda _now, value=used: value,
                clock=lambda: NOW,
            )
            ready, reason = provider.readiness(self.request())
            self.assertFalse(ready)
            self.assertEqual(reason, expected)
            self.assertEqual(calls, [])

    def test_exhausted_daily_request_budget_does_not_create_client(self):
        calls = []
        provider = GeminiTranscriptProvider(
            self.config(daily_request_budget=20),
            client_factory=lambda _config: calls.append(True),
            used_requests=lambda _now, _model: 20,
            clock=lambda: NOW,
        )

        ready, reason = provider.readiness(self.request())

        self.assertFalse(ready)
        self.assertEqual(reason, "gemini_request_budget_exhausted")
        self.assertEqual(calls, [])

    def test_retries_cannot_cross_remaining_daily_request_budget(self):
        client = FakeClient([TimeoutError("timed out"), response_json()])
        provider = GeminiTranscriptProvider(
            self.config(max_attempts=2, daily_request_budget=20),
            client_factory=lambda _config: client,
            used_requests=lambda _now, _model: 19,
            clock=lambda: NOW,
        )

        result = provider.fetch(self.request())

        self.assertEqual(result.error_code, "gemini_timeout")
        self.assertEqual(result.attempt_count, 1)
        self.assertEqual(len(client.interactions.calls), 1)

    def test_retries_cannot_cross_remaining_video_minutes_budget(self):
        client = FakeClient([TimeoutError("timed out"), response_json()])
        provider = GeminiTranscriptProvider(
            self.config(
                max_attempts=2,
                max_duration_minutes=20,
                daily_video_minutes_budget=30,
            ),
            client_factory=lambda _config: client,
            used_video_minutes=lambda _now: 0,
            clock=lambda: NOW,
        )

        result = provider.fetch(self.request(duration_seconds=20 * 60))

        self.assertEqual(result.error_code, "gemini_timeout")
        self.assertEqual(result.attempt_count, 1)
        self.assertEqual(len(client.interactions.calls), 1)

    def test_timeout_and_invalid_json_are_bounded_retryable_errors(self):
        for response, expected in (
            (TimeoutError("timed out"), "gemini_timeout"),
            ("not-json", "gemini_invalid_response"),
        ):
            client = FakeClient([response])
            provider = GeminiTranscriptProvider(
                self.config(),
                client_factory=lambda _config, value=client: value,
                clock=lambda: NOW,
            )
            result = provider.fetch(self.request())
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error_code, expected)
            self.assertEqual(len(client.interactions.calls), 1)
            self.assertTrue(client.closed)

    def test_rate_limited_primary_model_falls_back_to_next_model(self):
        client = FakeClient([RuntimeError("resource exhausted"), response_json()])
        provider = GeminiTranscriptProvider(
            self.config(fallback_models=("gemini-3.1-flash-lite", "gemini-2.5-flash")),
            client_factory=lambda _config: client,
            used_requests=lambda _now, model: 20 if model == "gemini-2.5-flash" else 0,
            clock=lambda: NOW,
        )

        result = provider.fetch(self.request())

        self.assertEqual(result.status, "success")
        self.assertEqual(result.payload.model, "gemini-3.1-flash-lite")
        self.assertEqual(result.attempt_count, 2)
        self.assertEqual(
            [call["model"] for call in client.interactions.calls],
            ["gemini-3.5-flash", "gemini-3.1-flash-lite"],
        )
        self.assertEqual(
            [model for model, _result in provider.model_results],
            ["gemini-3.5-flash", "gemini-3.1-flash-lite"],
        )

    def test_missing_sdk_is_a_clean_provider_error(self):
        provider = GeminiTranscriptProvider(
            self.config(),
            client_factory=lambda _config: (_ for _ in ()).throw(
                ModuleNotFoundError("google.genai")
            ),
            clock=lambda: NOW,
        )
        result = provider.fetch(self.request())
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error_code, "gemini_dependency_missing")
        self.assertEqual(result.attempt_count, 0)

    def test_segments_and_model_generated_provenance_are_normalized(self):
        client = FakeClient([response_json()])
        provider = GeminiTranscriptProvider(
            self.config(),
            client_factory=lambda _config: client,
            clock=lambda: NOW,
        )
        result = provider.fetch(self.request())
        self.assertEqual(result.status, "success")
        self.assertEqual(result.payload.segments[0]["duration"], 1.25)
        self.assertTrue(result.payload.generated_by_model)
        self.assertEqual(result.payload.generation_type, "model_generated")
        self.assertIsNone(result.payload.is_generated)
        call = client.interactions.calls[0]
        self.assertEqual(call["input"][0], {"type": "video", "uri": self.request().youtube_url})
        self.assertEqual(call["response_format"]["mime_type"], "application/json")

    def test_cached_success_replays_without_duplicate_call(self):
        cache = {}
        client = FakeClient([response_json()])
        provider = GeminiTranscriptProvider(
            self.config(),
            client_factory=lambda _config: client,
            cache_get=cache.get,
            cache_put=lambda key, value, _minutes: cache.setdefault(key, value),
            clock=lambda: NOW,
        )
        first = provider.fetch(self.request())
        second = provider.fetch(self.request())
        self.assertEqual(first.payload.content_version, second.payload.content_version)
        self.assertEqual(len(client.interactions.calls), 1)
        self.assertTrue(provider.last_cache_hit)


if __name__ == "__main__":
    unittest.main()
