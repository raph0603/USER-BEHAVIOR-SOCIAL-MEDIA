import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "playwright"))

from youtube_transcript_worker import (  # noqa: E402
    _gemini_candidate_minutes,
    prioritize_due_transcript_requests,
)


def request_row(
    video_id: str,
    duration_minutes: float | None,
    *,
    availability: str | None = None,
) -> dict:
    request = {"video_id": video_id}
    if duration_minutes is not None:
        request["duration_seconds"] = duration_minutes * 60
    if availability is not None:
        request["video_availability"] = availability
    return {"video_id": video_id, "request_json": json.dumps(request)}


class YouTubeTranscriptPriorityTests(unittest.TestCase):
    def test_daily_request_budget_is_exposed_to_runtime_services(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("GEMINI_TRANSCRIPT_DAILY_REQUEST_BUDGET=20", env_example)
        self.assertGreaterEqual(
            compose.count(
                "GEMINI_TRANSCRIPT_DAILY_REQUEST_BUDGET: "
                "${GEMINI_TRANSCRIPT_DAILY_REQUEST_BUDGET:-20}"
            ),
            1,
        )
        self.assertIn(
            "GEMINI_TRANSCRIPT_DAILY_REQUEST_BUDGET=${GEMINI_TRANSCRIPT_DAILY_REQUEST_BUDGET:-20}",
            compose,
        )

    def test_selects_longest_combination_that_fits_remaining_budget(self):
        rows = [
            request_row("short", 1),
            request_row("longest", 20),
            request_row("large", 15),
            request_row("best-fit", 10),
            request_row("almost-fit", 9.5),
        ]

        selected = prioritize_due_transcript_requests(
            rows,
            limit=25,
            max_duration_minutes=20,
            remaining_video_minutes=30,
            remaining_request_count=20,
            primary_circuit_open=True,
        )

        self.assertEqual([row["video_id"] for row in selected], ["longest", "best-fit"])

    def test_excludes_unknown_over_limit_and_non_public_videos(self):
        rows = [
            request_row("unknown", None),
            request_row("over-limit", 21),
            request_row("private", 19, availability="private"),
            request_row("eligible", 18, availability="public"),
        ]

        selected = prioritize_due_transcript_requests(
            rows,
            limit=25,
            max_duration_minutes=20,
            remaining_video_minutes=30,
            remaining_request_count=20,
            primary_circuit_open=True,
        )

        self.assertEqual([row["video_id"] for row in selected], ["eligible"])
        self.assertIsNone(_gemini_candidate_minutes(rows[1], max_duration_minutes=20))

    def test_does_not_consume_attempts_when_fallback_budget_is_empty(self):
        selected = prioritize_due_transcript_requests(
            [request_row("eligible", 20)],
            limit=25,
            max_duration_minutes=20,
            remaining_video_minutes=0,
            remaining_request_count=20,
            primary_circuit_open=True,
        )

        self.assertEqual(selected, [])

    def test_fills_batch_in_original_order_when_primary_is_available(self):
        rows = [
            request_row("old-short", 1),
            request_row("long", 20),
            request_row("over-limit", 30),
        ]

        selected = prioritize_due_transcript_requests(
            rows,
            limit=3,
            max_duration_minutes=20,
            remaining_video_minutes=20,
            remaining_request_count=20,
            primary_circuit_open=False,
        )

        self.assertEqual(
            [row["video_id"] for row in selected],
            ["long", "old-short", "over-limit"],
        )

    def test_daily_request_budget_caps_selected_fallbacks(self):
        rows = [
            request_row("long", 20),
            request_row("medium", 10),
        ]

        selected = prioritize_due_transcript_requests(
            rows,
            limit=25,
            max_duration_minutes=20,
            remaining_video_minutes=30,
            remaining_request_count=1,
            primary_circuit_open=True,
        )

        self.assertEqual([row["video_id"] for row in selected], ["long"])


if __name__ == "__main__":
    unittest.main()
