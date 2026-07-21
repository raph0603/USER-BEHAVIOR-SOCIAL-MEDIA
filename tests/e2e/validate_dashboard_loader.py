"""Exercise the production DuckDB/Iceberg dashboard loader against E2E MinIO."""

from __future__ import annotations

import json

import pandas as pd  # type: ignore[import-untyped]

from loaders import load_iceberg_data, load_optional_iceberg_table  # type: ignore[import-not-found]
from youtube_presentation import transcript_provenance_label


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    events = load_iceberg_data()
    _require(len(events) == 7, "dashboard loader did not read the Silver current projection")

    youtube = events.loc[events["source"] == "youtube"].set_index("platform_event_id")
    _require(
        set(youtube.index) == {"video-en", "video-vi", "video-private"},
        "dashboard lost a YouTube transcript lifecycle row",
    )
    _require(int(youtube.loc["video-en", "view_count"]) == 0, "known zero was not loaded")
    _require(
        bool(youtube.loc["video-en", "view_count_available"]),
        "known zero lost its availability flag",
    )
    _require(pd.isna(youtube.loc["video-vi", "view_count"]), "unknown view count became zero")
    _require(
        not bool(youtube.loc["video-vi", "view_count_available"]),
        "unknown view count became available",
    )
    _require(
        youtube["coverage_json"].notna().all(),
        "dashboard loader did not expose snapshot coverage",
    )

    contents, contents_error = load_optional_iceberg_table("silver", "contents")
    transcripts, transcripts_error = load_optional_iceberg_table("silver", "transcripts")
    examples, examples_error = load_optional_iceberg_table("gold", "training_examples")
    _require(contents_error is None and len(contents) == 5, "dashboard contents loader failed")
    _require(
        transcripts_error is None and len(transcripts) == 3,
        "dashboard transcripts loader failed",
    )
    transcript_rows = transcripts.set_index("video_id")
    _require(
        transcript_provenance_label(transcript_rows.loc["video-vi"])
        == "Transcription générée depuis la vidéo avec Gemini",
        "dashboard did not expose Gemini transcript provenance",
    )
    _require(
        transcript_provenance_label(transcript_rows.loc["video-private"])
        == "Indisponible",
        "dashboard misrepresented an ineligible private video",
    )
    _require(examples_error is None and len(examples) == 2, "dashboard ML loader failed")

    print(
        json.dumps(
            {
                "dashboard_events": len(events),
                "loaded_contents": len(contents),
                "loaded_transcripts": len(transcripts),
                "loaded_training_examples": len(examples),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
