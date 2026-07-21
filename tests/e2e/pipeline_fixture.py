"""Generate the deterministic Kafka and Schema Registry fixtures for pipeline E2E."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


PUBLISHED_AT = {
    "video-en": "2026-07-18T00:00:00+00:00",
    "video-vi": "2026-07-18T00:30:00+00:00",
    "video-private": "2026-07-18T00:45:00+00:00",
}


def _identifier(kind: str, label: str) -> str:
    return hashlib.sha256(f"pipeline-e2e:{kind}:{label}".encode()).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _base_event(
    label: str,
    *,
    source: str,
    platform_event_id: str,
    user_id: str,
    url: str,
    title: str,
    text: str,
    published_at: str,
    collected_at: str,
    content_id: str,
    root_content_id: str | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "stage": "clean",
        "event_id": _identifier("event", label),
        "source": source,
        "platform_event_id": platform_event_id,
        "user_id": user_id,
        "url": url,
        "title": title,
        "raw_text": text,
        "clean_text": text,
        "text_for_model": text.lower(),
        "timestamp": published_at,
        "published_at": published_at,
        "collected_at": collected_at,
        "updated_at": collected_at,
        "observed_at": collected_at,
        "content_id": content_id,
        "root_content_id": root_content_id or content_id,
        "collection_status": "success",
        "producer_name": "pipeline-e2e-fixture",
        "producer_run_id": "pipeline-e2e-run",
        "collection_method": "deterministic_fixture",
        "provenance_json": _json(
            {
                "fixture": "pipeline-reliability-v1",
                "label": label,
                "source": source,
            }
        ),
        "coverage_json": _json({"fixture_complete": True}),
        "event_version": "1.0",
        "collector_version": "e2e-v1",
        "source_payload_version": "e2e-v1",
    }
    payload.update(overrides)
    return payload


def _youtube_event(
    label: str,
    *,
    video_id: str,
    language: str,
    collected_at: str,
    event_type: str,
    **overrides: Any,
) -> dict[str, Any]:
    content_id = _identifier("content", video_id)
    text = (
        "Reliable electric vehicle review in English"
        if language == "en"
        else "Đánh giá xe điện đáng tin cậy bằng tiếng Việt"
    )
    return _base_event(
        label,
        source="youtube",
        platform_event_id=video_id,
        user_id=f"channel-{language}",
        url=f"https://www.youtube.com/watch?v={video_id}",
        title=f"Pipeline E2E {language.upper()} video",
        text=text,
        published_at=PUBLISHED_AT[video_id],
        collected_at=collected_at,
        content_id=content_id,
        event_type=event_type,
        video_id=video_id,
        channel_id=f"channel-{language}",
        owner_channel_id=f"channel-{language}",
        youtube_channel_name=f"E2E Channel {language.upper()}",
        language=language,
        content_type="youtube_video",
        **overrides,
    )


def build_valid_events() -> list[dict[str, Any]]:
    events = [
        _youtube_event(
            "video-en-discovery",
            video_id="video-en",
            language="en",
            collected_at="2026-07-18T00:01:00+00:00",
            event_type="youtube.discovery.discovered",
            query_id="query-en",
        ),
        _youtube_event(
            "video-en-metadata",
            video_id="video-en",
            language="en",
            collected_at="2026-07-18T00:10:00+00:00",
            event_type="youtube.metadata.observed",
            metadata_status="success",
            metadata_available=True,
            metadata_collected_at="2026-07-18T00:10:00+00:00",
            metadata_source="youtube_data_api",
            metadata_schema_version="v1",
            canonical_metadata=_json({"category": "autos", "language": "en"}),
        ),
        _youtube_event(
            "video-en-transcript",
            video_id="video-en",
            language="en",
            collected_at="2026-07-18T00:20:00+00:00",
            event_type="youtube.transcript.result",
            transcript_status="success",
            transcript_lifecycle_status="available",
            transcript_available=True,
            transcript_text="A complete English transcript for the first video.",
            transcript_segments_json=_json(
                [{"duration": 4.0, "start": 0.0, "text": "A complete English transcript."}]
            ),
            duration_seconds=4.0,
            has_auto_captions=False,
            transcript_requested_language="English",
            transcript_requested_language_code="en",
            transcript_obtained_language="English",
            transcript_obtained_language_code="en",
            transcript_language="English",
            transcript_language_code="en",
            transcript_source_language="English",
            transcript_source_language_code="en",
            transcript_is_generated=False,
            transcript_is_translated=False,
            transcript_generation_type="manual",
            transcript_provider="youtube_transcript_api",
            transcript_source="youtube",
            transcript_selection_strategy="requested_exact",
            transcript_segment_count=1,
            transcript_available_languages=["en"],
            transcript_available_languages_json=_json(["en"]),
            transcript_covered_duration_seconds=4.0,
            transcript_collected_at="2026-07-18T00:20:00+00:00",
            transcript_attempt_count=1,
            transcript_last_attempt_at="2026-07-18T00:20:00+00:00",
            transcript_content_version=_identifier("transcript", "video-en-en"),
        ),
        _youtube_event(
            "video-en-snapshot-1",
            video_id="video-en",
            language="en",
            collected_at="2026-07-18T01:00:00+00:00",
            event_type="youtube.engagement.snapshot",
            observation_id=_identifier("observation", "video-en-1h"),
            view_count=5,
            view_count_available=True,
            like_count=None,
            like_count_available=False,
            comment_count=0,
            comment_count_available=True,
            subscriber_count=0,
            subscriber_count_available=True,
            metrics_refresh_status="success",
            last_metrics_refresh_at="2026-07-18T01:00:00+00:00",
            collection_method="youtube_data_api",
            api_endpoint="videos.list",
            coverage_json=_json({"comment_count": True, "like_count": False, "view_count": True}),
        ),
        _youtube_event(
            "video-en-snapshot-2",
            video_id="video-en",
            language="en",
            collected_at="2026-07-18T02:00:00+00:00",
            event_type="youtube.engagement.snapshot",
            observation_id=_identifier("observation", "video-en-2h"),
            view_count=0,
            view_count_available=True,
            like_count=1,
            like_count_available=True,
            comment_count=None,
            comment_count_available=False,
            subscriber_count=0,
            subscriber_count_available=True,
            metrics_refresh_status="success",
            last_metrics_refresh_at="2026-07-18T02:00:00+00:00",
            collection_method="youtube_data_api",
            api_endpoint="videos.list",
            coverage_json=_json({"comment_count": False, "like_count": True, "view_count": True}),
        ),
        _youtube_event(
            "video-vi-discovery",
            video_id="video-vi",
            language="vi",
            collected_at="2026-07-18T00:31:00+00:00",
            event_type="youtube.discovery.discovered",
            query_id="query-vi",
        ),
        _youtube_event(
            "video-vi-metadata",
            video_id="video-vi",
            language="vi",
            collected_at="2026-07-18T00:40:00+00:00",
            event_type="youtube.metadata.observed",
            metadata_status="success",
            metadata_available=True,
            metadata_collected_at="2026-07-18T00:40:00+00:00",
            metadata_source="youtube_data_api",
            metadata_schema_version="v1",
            canonical_metadata=_json({"category": "autos", "language": "vi"}),
        ),
        _youtube_event(
            "video-vi-transcript",
            video_id="video-vi",
            language="vi",
            collected_at="2026-07-18T00:50:00+00:00",
            event_type="youtube.transcript.result",
            transcript_status="success",
            transcript_lifecycle_status="available",
            transcript_available=True,
            transcript_text="Bản chép lời tiếng Việt do mô hình tạo.",
            transcript_segments_json=_json(
                [{"duration": 3.0, "start": 0.0, "text": "Bản chép lời tiếng Việt."}]
            ),
            duration_seconds=3.0,
            transcript_requested_language="Vietnamese",
            transcript_requested_language_code="vi",
            transcript_obtained_language="Vietnamese",
            transcript_obtained_language_code="vi",
            transcript_language="Vietnamese",
            transcript_language_code="vi",
            transcript_available_languages=[],
            transcript_available_languages_json=_json([]),
            transcript_provider="gemini",
            transcript_model="gemini-3.5-flash",
            transcript_source="gemini",
            transcript_selection_strategy="gemini_youtube_url_fallback",
            transcript_fallback_reason="no_transcript_found",
            transcript_prompt_version="youtube-transcript-v1",
            transcript_generated_by_model=True,
            transcript_generation_type="model_generated",
            transcript_is_generated=None,
            transcript_is_translated=False,
            transcript_segment_count=1,
            transcript_covered_duration_seconds=3.0,
            transcript_attempt_count=2,
            transcript_primary_attempt_count=1,
            transcript_fallback_attempt_count=1,
            transcript_last_attempt_at="2026-07-18T00:50:00+00:00",
            transcript_primary_last_attempt_at="2026-07-18T00:50:00+00:00",
            transcript_fallback_last_attempt_at="2026-07-18T00:50:00+00:00",
            transcript_collected_at="2026-07-18T00:50:00+00:00",
            transcript_primary_result_json=_json(
                {"status": "not_available", "error_code": "no_transcript_found"}
            ),
            transcript_fallback_result_json=_json({"status": "success"}),
            transcript_content_version=_identifier("transcript", "video-vi-vi"),
        ),
        _youtube_event(
            "video-private-transcript",
            video_id="video-private",
            language="en",
            collected_at="2026-07-18T00:55:00+00:00",
            event_type="youtube.transcript.result",
            video_availability="private",
            transcript_status="not_available",
            transcript_lifecycle_status="unavailable",
            transcript_available=False,
            transcript_requested_language="English",
            transcript_requested_language_code="en",
            transcript_provider="youtube_transcript_api",
            transcript_source="youtube_transcript_api",
            transcript_selection_strategy="requested_exact",
            transcript_attempt_count=1,
            transcript_primary_attempt_count=1,
            transcript_fallback_attempt_count=0,
            transcript_error_code="video_private",
            transcript_error_message="The video is not public",
            transcript_content_version=_identifier("transcript", "video-private-en"),
        ),
        _youtube_event(
            "video-vi-snapshot-1",
            video_id="video-vi",
            language="vi",
            collected_at="2026-07-18T01:30:00+00:00",
            event_type="youtube.engagement.snapshot",
            observation_id=_identifier("observation", "video-vi-1h"),
            view_count=None,
            view_count_available=False,
            like_count=0,
            like_count_available=True,
            comment_count=0,
            comment_count_available=True,
            subscriber_count=None,
            subscriber_count_available=False,
            metrics_refresh_status="partial",
            last_metrics_refresh_at="2026-07-18T01:30:00+00:00",
            collection_method="youtube_data_api",
            api_endpoint="videos.list",
            coverage_json=_json({"comment_count": True, "like_count": True, "view_count": False}),
        ),
        _youtube_event(
            "video-vi-snapshot-2",
            video_id="video-vi",
            language="vi",
            collected_at="2026-07-18T02:30:00+00:00",
            event_type="youtube.engagement.snapshot",
            observation_id=_identifier("observation", "video-vi-2h"),
            view_count=None,
            view_count_available=False,
            like_count=0,
            like_count_available=True,
            comment_count=1,
            comment_count_available=True,
            subscriber_count=None,
            subscriber_count_available=False,
            metrics_refresh_status="partial",
            last_metrics_refresh_at="2026-07-18T02:30:00+00:00",
            collection_method="youtube_data_api",
            api_endpoint="videos.list",
            coverage_json=_json({"comment_count": True, "like_count": True, "view_count": False}),
        ),
    ]

    reddit_root = _identifier("content", "reddit-post-1")
    events.extend(
        [
            _base_event(
                "reddit-root",
                source="reddit",
                platform_event_id="reddit-post-1",
                user_id="reddit-author-root",
                url="https://www.reddit.com/r/electricvehicles/comments/reddit-post-1/e2e_post/",
                title="Deterministic Reddit thread",
                text="A deterministic root post about electric vehicles.",
                published_at="2026-07-18T03:00:00+00:00",
                collected_at="2026-07-18T03:05:00+00:00",
                content_id=reddit_root,
                relation_type="root",
                content_type="reddit_post",
                conversation_id="reddit-post-1",
                subreddit="electricvehicles",
                score=0,
                score_available=True,
                comment_count=1,
                comment_count_available=True,
                subreddit_member_count=0,
                subreddit_member_count_available=True,
            ),
            _base_event(
                "reddit-comment",
                source="reddit",
                platform_event_id="reddit-comment-1",
                user_id="reddit-author-comment",
                url="https://www.reddit.com/r/electricvehicles/comments/reddit-post-1/e2e_post/comment-1/",
                title="Reddit reply",
                text="A deterministic Reddit reply.",
                published_at="2026-07-18T03:10:00+00:00",
                collected_at="2026-07-18T03:11:00+00:00",
                content_id=_identifier("content", "reddit-comment-1"),
                root_content_id=reddit_root,
                parent_content_id=reddit_root,
                relation_type="reply",
                content_type="reddit_comment",
                conversation_id="reddit-post-1",
                depth=1,
                position_in_thread=1,
                score=0,
                score_available=True,
            ),
        ]
    )

    x_root = _identifier("content", "x-post-1001")
    events.extend(
        [
            _base_event(
                "x-root",
                source="x",
                platform_event_id="1001",
                user_id="x-author-root",
                url="https://x.com/e2e/status/1001",
                title="Deterministic X conversation",
                text="A deterministic root post on X.",
                published_at="2026-07-18T04:00:00+00:00",
                collected_at="2026-07-18T04:05:00+00:00",
                content_id=x_root,
                relation_type="root",
                content_type="x_post",
                conversation_id="1001",
                x_account="e2e",
                view_count=10,
                view_count_available=True,
                like_count=0,
                like_count_available=True,
                reply_count=1,
                reply_count_available=True,
                retweet_count=0,
                retweet_count_available=True,
                bookmark_count=0,
                bookmark_count_available=True,
                follower_count=0,
                follower_count_available=True,
            ),
            _base_event(
                "x-reply",
                source="x",
                platform_event_id="1002",
                user_id="x-author-reply",
                url="https://x.com/e2e/status/1002",
                title="X reply",
                text="A deterministic reply on X.",
                published_at="2026-07-18T04:10:00+00:00",
                collected_at="2026-07-18T04:11:00+00:00",
                content_id=_identifier("content", "x-reply-1002"),
                root_content_id=x_root,
                parent_content_id=x_root,
                relation_type="reply",
                content_type="x_reply",
                conversation_id="1001",
                parent_interaction_id=None,
                depth=1,
                position_in_thread=1,
                like_count=0,
                like_count_available=True,
                reply_count=0,
                reply_count_available=True,
            ),
        ]
    )
    return events


def build_fixture_messages() -> list[str]:
    events = build_valid_events()
    duplicate = next(
        event
        for event in events
        if event["event_id"] == _identifier("event", "video-en-snapshot-1")
    )
    malformed = '{"stage":"clean","source":"youtube","token":"must-never-be-persisted"'
    return [*(_json(event) for event in events), _json(duplicate), malformed]


def write_fixture(
    *,
    events_output: Path,
    schema_path: Path,
    schema_payload_output: Path,
) -> None:
    events_output.parent.mkdir(parents=True, exist_ok=True)
    schema_payload_output.parent.mkdir(parents=True, exist_ok=True)
    events_output.write_text("\n".join(build_fixture_messages()) + "\n", encoding="utf-8")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    registry_payload = {"schemaType": "AVRO", "schema": _json(schema)}
    schema_payload_output.write_text(_json(registry_payload) + "\n", encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events-output", type=Path, required=True)
    parser.add_argument("--schema-path", type=Path, required=True)
    parser.add_argument("--schema-payload-output", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    write_fixture(
        events_output=args.events_output,
        schema_path=args.schema_path,
        schema_payload_output=args.schema_payload_output,
    )
    print(
        _json(
            {
                "fixture_messages": len(build_fixture_messages()),
                "unique_valid_events": len(build_valid_events()),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
