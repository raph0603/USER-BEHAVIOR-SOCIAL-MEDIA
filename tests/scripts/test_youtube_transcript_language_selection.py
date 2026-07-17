import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "spark" / "jobs" / "batch" / "youtube_transcripts.py"
PRODUCER_PATH = ROOT / "playwright" / "producer.py"
SPEC = importlib.util.spec_from_file_location("youtube_transcripts", MODULE_PATH)
TRANSCRIPTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRANSCRIPTS)


class YouTubeTranscriptLanguageSelectionTests(unittest.TestCase):
    def test_vietnamese_videos_request_only_vietnamese(self):
        self.assertEqual(
            TRANSCRIPTS._preferred_languages_for_candidate({"language": "vi-VN"}),
            ("vi",),
        )

    def test_other_videos_request_only_english(self):
        for language in ("en", "hi", None, ""):
            with self.subTest(language=language):
                self.assertEqual(
                    TRANSCRIPTS._preferred_languages_for_candidate({"language": language}),
                    ("en",),
                )

    def test_backfill_requires_a_preferred_language(self):
        source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertIn("require_preferred_language=True", source)

    def test_collector_uses_the_same_per_video_language_rule(self):
        source = PRODUCER_PATH.read_text(encoding="utf-8")

        self.assertIn("def _preferred_youtube_transcript_languages", source)
        self.assertIn("require_preferred_language=True", source)
        self.assertIn('return ["vi"]', source)
        self.assertIn('return ["en"]', source)


if __name__ == "__main__":
    unittest.main()
