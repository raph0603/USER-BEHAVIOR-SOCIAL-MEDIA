import unittest
from datetime import datetime, timezone

from common.collection import (
    STATUS_DISABLED,
    STATUS_FAILED,
    STATUS_NOT_AVAILABLE,
    STATUS_PARTIAL,
    STATUS_RATE_LIMITED,
    STATUS_SUCCESS,
)
from common.transcripts import (
    TRANSCRIPT_SOURCE,
    classify_transcript_error,
    fetch_transcript,
    fetch_youtube_transcript,
)


FIXED_TIME = datetime(2026, 7, 10, 8, 30, tzinfo=timezone.utc)


class FakeSegment:
    def __init__(self, text, start, duration):
        self.text = text
        self.start = start
        self.duration = duration


class FakeFetched:
    def __init__(self, segments, language=None, language_code=None, raw=False):
        self.segments = segments
        self.language = language
        self.language_code = language_code
        self.raw = raw

    def __iter__(self):
        return iter(self.segments)

    def to_raw_data(self):
        if not self.raw:
            raise AttributeError("raw conversion is disabled")
        return self.segments


class FakeTrack:
    def __init__(
        self,
        language,
        language_code,
        *,
        is_generated=False,
        segments=None,
        fetch_error=None,
        translations=None,
        translate_error=None,
    ):
        self.language = language
        self.language_code = language_code
        self.is_generated = is_generated
        self._segments = segments if segments is not None else [
            {"text": f"text in {language_code}", "start": 0, "duration": 1}
        ]
        self._fetch_error = fetch_error
        self._translations = translations or {}
        self._translate_error = translate_error
        self.is_translatable = bool(translations or translate_error)
        self.translation_languages = [
            {
                "language": track.language,
                "language_code": code,
            }
            for code, track in self._translations.items()
        ]
        self.fetch_calls = 0
        self.translate_calls = []

    def fetch(self):
        self.fetch_calls += 1
        if self._fetch_error is not None:
            raise self._fetch_error
        return self._segments

    def translate(self, language_code):
        self.translate_calls.append(language_code)
        if self._translate_error is not None:
            raise self._translate_error
        return self._translations[language_code]


class FakeApi:
    def __init__(self, tracks=None, error=None):
        self.tracks = tracks or []
        self.error = error
        self.calls = []

    def list(self, video_id):
        self.calls.append(video_id)
        if self.error is not None:
            raise self.error
        return self.tracks


class TranscriptsDisabled(Exception):
    pass


class NoTranscriptFound(Exception):
    pass


class VideoUnavailable(Exception):
    pass


class IpBlocked(Exception):
    pass


class RequestBlocked(Exception):
    pass


class TranslationFailure(Exception):
    pass


class HttpFailure(Exception):
    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


class TranscriptSelectionTests(unittest.TestCase):
    def test_manual_preferred_language_wins_before_generated(self):
        generated_vi = FakeTrack("Vietnamese", "vi", is_generated=True)
        manual_en = FakeTrack("English", "en-US")
        manual_vi = FakeTrack("Vietnamese", "vi")

        result = fetch_transcript(
            "video-1",
            ["vi", "en"],
            api=FakeApi([generated_vi, manual_en, manual_vi]),
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(result.status, STATUS_SUCCESS)
        self.assertEqual(result.payload.language_code, "vi")
        self.assertEqual(result.payload.selection_strategy, "manual_preferred")
        self.assertFalse(result.payload.is_generated)
        self.assertEqual(manual_vi.fetch_calls, 1)
        self.assertEqual(generated_vi.fetch_calls, 0)

    def test_generated_preferred_language_wins_before_foreign_manual(self):
        foreign_manual = FakeTrack("French", "fr")
        generated_en = FakeTrack("English", "en", is_generated=True)

        result = fetch_transcript(
            "video-2",
            ["en"],
            api=FakeApi([foreign_manual, generated_en]),
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(result.status, STATUS_SUCCESS)
        self.assertEqual(result.payload.selection_strategy, "generated_preferred")
        self.assertTrue(result.payload.is_generated)
        self.assertEqual(generated_en.fetch_calls, 1)
        self.assertEqual(foreign_manual.fetch_calls, 0)

    def test_foreign_manual_track_is_translated_with_complete_provenance(self):
        translated = FakeTrack(
            "English",
            "en",
            segments=[
                {"text": " first  segment ", "start": 0, "duration": 2},
                FakeSegment("second segment", 1, 3),
                {"text": "last", "start": 5, "duration": 1},
                {"text": "  ", "start": 10, "duration": 2},
            ],
        )
        manual_fr = FakeTrack(
            "French",
            "fr",
            translations={"en": translated},
        )
        generated_de = FakeTrack("German", "de", is_generated=True)

        result = fetch_transcript(
            "video-3",
            ["en-US"],
            api=FakeApi([generated_de, manual_fr]),
            attempt_count=3,
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(result.status, STATUS_SUCCESS)
        self.assertEqual(result.attempt_count, 3)
        self.assertEqual(result.started_at, FIXED_TIME)
        self.assertEqual(result.completed_at, FIXED_TIME)
        payload = result.payload
        self.assertEqual(payload.video_id, "video-3")
        self.assertEqual(payload.language, "English")
        self.assertEqual(payload.language_code, "en")
        self.assertEqual(payload.source_language, "French")
        self.assertEqual(payload.source_language_code, "fr")
        self.assertTrue(payload.is_translated)
        self.assertFalse(payload.is_generated)
        self.assertEqual(payload.source, TRANSCRIPT_SOURCE)
        self.assertEqual(payload.selection_strategy, "translated_manual_fallback")
        self.assertEqual(payload.text, "first segment second segment last")
        self.assertEqual(payload.segment_count, 3)
        self.assertEqual(payload.word_count, 5)
        self.assertEqual(payload.covered_duration_seconds, 5.0)
        self.assertEqual(payload.duration_seconds, 5.0)
        self.assertEqual(manual_fr.translate_calls, ["en"])
        self.assertEqual(
            [item["language_code"] for item in payload.available_languages],
            ["de", "fr"],
        )
        self.assertIn('"end":2.0', payload.segments_json)
        self.assertEqual(payload.to_dict()["collected_at"], "2026-07-10T08:30:00Z")

    def test_generated_fallback_can_be_kept_without_translation(self):
        generated_fr = FakeTrack("French", "fr", is_generated=True)

        result = fetch_transcript(
            "video-4",
            ["en"],
            api=FakeApi([generated_fr]),
            allow_translation=False,
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(result.status, STATUS_SUCCESS)
        self.assertEqual(result.payload.selection_strategy, "generated_fallback")
        self.assertFalse(result.payload.is_translated)
        self.assertTrue(result.payload.has_auto_captions)

    def test_fallback_selection_is_deterministic_across_api_order(self):
        manual_zh = FakeTrack("Chinese", "zh")
        manual_fr = FakeTrack("French", "fr")

        first = fetch_transcript(
            "video-5",
            ["en"],
            api=FakeApi([manual_zh, manual_fr]),
            allow_translation=False,
            clock=lambda: FIXED_TIME,
        )
        second = fetch_transcript(
            "video-5",
            ["en"],
            api=FakeApi([manual_fr, manual_zh]),
            allow_translation=False,
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(first.payload.language_code, "fr")
        self.assertEqual(second.payload.language_code, "fr")

    def test_translation_failure_returns_partial_original_transcript(self):
        manual_fr = FakeTrack(
            "French",
            "fr",
            translate_error=TranslationFailure(
                "translation failed at https://example.test/?token=private"
            ),
        )

        result = fetch_transcript(
            "video-6",
            ["en"],
            api=FakeApi([manual_fr]),
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(result.status, STATUS_PARTIAL)
        self.assertTrue(result.is_retryable)
        self.assertEqual(result.payload.language_code, "fr")
        self.assertEqual(result.payload.selection_strategy, "manual_fallback")
        self.assertFalse(result.payload.is_translated)
        self.assertEqual(result.error_code, "translation_translation_failure")
        self.assertNotIn("token=private", result.error_message)
        self.assertEqual(manual_fr.fetch_calls, 1)

    def test_translated_fetch_failure_falls_back_to_original(self):
        translated = FakeTrack(
            "English",
            "en",
            fetch_error=TimeoutError("translated fetch timed out"),
        )
        manual_fr = FakeTrack(
            "French",
            "fr",
            translations={"en": translated},
        )

        result = fetch_transcript(
            "video-7",
            ["en"],
            api=FakeApi([manual_fr]),
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(result.status, STATUS_PARTIAL)
        self.assertEqual(result.payload.language_code, "fr")
        self.assertEqual(result.error_code, "translation_timeout_error")
        self.assertEqual(translated.fetch_calls, 1)
        self.assertEqual(manual_fr.fetch_calls, 1)

    def test_empty_selected_track_is_retryable_partial_result(self):
        empty = FakeTrack("English", "en", segments=[])

        result = fetch_transcript(
            "video-8",
            ["en"],
            api=FakeApi([empty]),
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(result.status, STATUS_PARTIAL)
        self.assertEqual(result.error_code, "empty_transcript")
        self.assertEqual(result.payload.text, "")
        self.assertEqual(result.payload.segment_count, 0)


class TranscriptFailureTests(unittest.TestCase):
    def _fetch_error(self, error):
        return fetch_transcript(
            "failure-video",
            ["en"],
            api=FakeApi(error=error),
            clock=lambda: FIXED_TIME,
        )

    def test_disabled_and_unavailable_errors_are_terminal(self):
        cases = (
            (TranscriptsDisabled("captions disabled"), STATUS_DISABLED, "transcripts_disabled"),
            (NoTranscriptFound("none"), STATUS_NOT_AVAILABLE, "no_transcript_found"),
            (VideoUnavailable("private video"), STATUS_NOT_AVAILABLE, "video_unavailable"),
        )
        for error, status, code in cases:
            with self.subTest(error=type(error).__name__):
                result = self._fetch_error(error)
                self.assertEqual(result.status, status)
                self.assertEqual(result.error_code, code)
                self.assertTrue(result.is_terminal)
                self.assertFalse(result.is_retryable)
                self.assertIsNone(result.payload)

    def test_blocking_and_http_429_errors_are_retryable(self):
        cases = (
            (IpBlocked("IP has been blocked"), "ip_blocked"),
            (RequestBlocked("request blocked"), "request_blocked"),
            (HttpFailure(429), "rate_limited"),
        )
        for error, code in cases:
            with self.subTest(error=type(error).__name__):
                result = self._fetch_error(error)
                self.assertEqual(result.status, STATUS_RATE_LIMITED)
                self.assertEqual(result.error_code, code)
                self.assertTrue(result.is_retryable)

    def test_transport_and_dependency_errors_are_failed(self):
        timeout = self._fetch_error(TimeoutError("timed out"))
        dependency = fetch_transcript(
            "failure-video",
            ["en"],
            api_factory=lambda: (_ for _ in ()).throw(
                ModuleNotFoundError("youtube_transcript_api")
            ),
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(timeout.status, STATUS_FAILED)
        self.assertEqual(timeout.error_code, "timeout_error")
        self.assertEqual(dependency.status, STATUS_FAILED)
        self.assertEqual(dependency.error_code, "dependency_missing")

    def test_error_classification_reads_http_status(self):
        classification = classify_transcript_error(HttpFailure(503))

        self.assertEqual(classification.status, STATUS_FAILED)
        self.assertEqual(classification.error_code, "http_503")
        self.assertTrue(classification.is_retryable)

    def test_no_tracks_is_explicit_terminal_unavailable(self):
        result = fetch_transcript(
            "empty-video",
            ["en"],
            api=FakeApi([]),
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(result.status, STATUS_NOT_AVAILABLE)
        self.assertEqual(result.error_code, "no_transcript_found")
        self.assertTrue(result.is_terminal)

    def test_factory_injection_and_alias_are_supported(self):
        api = FakeApi([FakeTrack("English", "en")])

        result = fetch_youtube_transcript(
            "factory-video",
            ["en"],
            api_factory=lambda: api,
            clock=lambda: FIXED_TIME,
        )

        self.assertEqual(result.status, STATUS_SUCCESS)
        self.assertEqual(api.calls, ["factory-video"])

    def test_invalid_arguments_are_rejected_before_collection(self):
        with self.assertRaisesRegex(ValueError, "video_id"):
            fetch_transcript("", api=FakeApi())
        with self.assertRaisesRegex(ValueError, "not both"):
            fetch_transcript(
                "video",
                api=FakeApi(),
                api_factory=lambda: FakeApi(),
            )


if __name__ == "__main__":
    unittest.main()
