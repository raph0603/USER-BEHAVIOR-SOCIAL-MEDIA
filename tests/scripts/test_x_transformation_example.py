from scripts.finalize_x_transformation_example import (
    _transformations,
    latex_escape,
)
from spark.jobs.maintenance.inspect_x_transformation_candidates import (
    _score_candidate,
)


def test_latex_escape_preserves_privacy_tokens_and_escapes_special_characters():
    rendered = latex_escape("<USER> #Data user_id & 100%")

    assert r"\texttt{\textless USER\textgreater}" in rendered
    assert r"\#Data" in rendered
    assert r"user\_id" in rendered
    assert r"\&" in rendered
    assert r"100\%" in rendered


def test_transformations_report_redaction_spacing_lowercase_and_features():
    raw = {
        "raw_text": "Hello @Alice , #Data!",
        "url": "https://x.com/Alice/status/1",
        "x_account": "Alice",
    }
    clean = {
        "clean_text": "Hello <USER>, #Data!",
        "text_for_model": "hello <USER>, #data!",
        "url": "https://x.com/i/status/1",
        "x_account": "hashed-alice",
    }
    features = {
        "mention_token_count": 1,
        "hashtag_count": 1,
        "word_count": 3,
    }

    transformations = _transformations(raw, clean, features)
    types = [item["type"] for item in transformations]

    assert {
        "privacy_redaction",
        "whitespace_normalization",
        "lowercase_normalization",
        "url_canonicalization",
        "identity_hashing",
        "feature_extraction",
    }.issubset(types)
    assert {
        "input": "@Alice",
        "output": "<USER>",
        "type": "privacy_redaction",
    } in transformations


def test_candidate_score_uses_requested_weights():
    candidate = _score_candidate(
        {
            "platform_event_id": "123",
            "raw_text": "Hi @Alice, mail a@example.com #Data 🚀",
            "x_account": "author",
            "_kafka_topic": "x.raw.events",
            "_kafka_partition": 0,
            "_kafka_offset": 1,
        },
        None,
        bronze_available=False,
    )

    assert candidate["score"] == 8
    assert candidate["transformation_type_count"] == 4
    assert candidate["silver_token_counts"]["user_count"] == 1
    assert candidate["silver_token_counts"]["email_count"] == 1
    assert candidate["hashtag_count"] == 1
    assert candidate["emoji_count"] == 1
