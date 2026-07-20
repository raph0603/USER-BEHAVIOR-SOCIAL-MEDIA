import json

from tests.e2e.pipeline_fixture import build_fixture_messages, build_valid_events


def test_fixture_is_deterministic_and_has_one_replay_and_one_malformed_event():
    first = build_fixture_messages()
    second = build_fixture_messages()

    assert first == second
    assert len(first) == 16
    valid_messages = [json.loads(message) for message in first[:-1]]
    assert len({event["event_id"] for event in valid_messages}) == 14
    assert valid_messages[-1] == valid_messages[3]
    assert "must-never-be-persisted" in first[-1]


def test_fixture_covers_snapshots_languages_relations_and_missingness():
    events = build_valid_events()
    youtube_snapshots = [
        event for event in events if event.get("event_type") == "youtube.engagement.snapshot"
    ]
    transcripts = [
        event for event in events if event.get("event_type") == "youtube.transcript.result"
    ]

    assert len(events) == 14
    assert len(youtube_snapshots) == 4
    assert {event["platform_event_id"] for event in youtube_snapshots} == {
        "video-en",
        "video-vi",
    }
    assert {event["transcript_requested_language_code"] for event in transcripts} == {
        "en",
        "vi",
    }
    assert {event["transcript_lifecycle_status"] for event in transcripts} == {
        "available",
        "unavailable",
    }
    assert any(
        event.get("view_count") == 0 and event.get("view_count_available") is True
        for event in youtube_snapshots
    )
    assert any(
        event.get("view_count") is None and event.get("view_count_available") is False
        for event in youtube_snapshots
    )
    assert any(
        event["source"] == "reddit" and event.get("relation_type") == "reply" for event in events
    )
    assert any(event["source"] == "x" and event.get("relation_type") == "reply" for event in events)
