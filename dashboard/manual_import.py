import csv
import hashlib
import io
import json
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs, urlparse


SUPPORTED_SOURCES = ("youtube", "x", "reddit")
DEFAULT_TOPIC_BY_SOURCE = {
    "youtube": "manual.youtube.raw.events",
    "x": "manual.x.raw.events",
    "reddit": "manual.reddit.raw.events",
}
MANUAL_IMPORT_DAG_ID = "manual_file_import_lakehouse"
EVENT_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1] / "schemas" / "playwright_event.avsc"
)


def get_manual_import_config():
    return {
        "bootstrap_servers": os.getenv(
            "DASHBOARD_KAFKA_BOOTSTRAP",
            "localhost:9092",
        ),
        "topic_by_source": {
            "youtube": os.getenv(
                "DASHBOARD_MANUAL_YOUTUBE_TOPIC",
                DEFAULT_TOPIC_BY_SOURCE["youtube"],
            ),
            "x": os.getenv(
                "DASHBOARD_MANUAL_X_TOPIC",
                DEFAULT_TOPIC_BY_SOURCE["x"],
            ),
            "reddit": os.getenv(
                "DASHBOARD_MANUAL_REDDIT_TOPIC",
                DEFAULT_TOPIC_BY_SOURCE["reddit"],
            ),
        },
    }


def _clean_value(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value or value.lower() in {"none", "nan", "null", "<na>"}:
        return None
    return value


def _first_value(row, *names):
    for name in names:
        if name in row:
            value = _clean_value(row.get(name))
            if value is not None:
                return value
    return None


def _parse_count(value):
    value = _clean_value(value)
    if value is None:
        return None
    text = value.replace(",", "")
    suffix = text[-1:].upper()
    multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(
        suffix,
        1,
    )
    if multiplier != 1:
        text = text[:-1]
    try:
        result = float(text) * multiplier
        import math
        if math.isinf(result) or math.isnan(result):
            return None
        return int(result)
    except (ValueError, OverflowError):
        return None


def _parse_timestamp(value, fallback=None):
    value = _clean_value(value)
    if value is None:
        return fallback or datetime.now(timezone.utc).isoformat()
    try:
        numeric_value = float(value)
    except ValueError:
        return value
    return datetime.fromtimestamp(numeric_value, timezone.utc).isoformat()


def _hash_identity(value):
    return hashlib.sha256((value or "anonymous").encode("utf-8")).hexdigest()


def _source_user_id(source, row, *fallback_names):
    user_id = _first_value(row, "user_id")
    if user_id:
        return user_id
    identity = _first_value(row, "author_hash", *fallback_names)
    if not identity:
        identity = "anonymous"
    return f"{source}-{_hash_identity(identity)}"


def _youtube_video_id(row):
    video_id = _first_value(row, "video_id")
    if video_id:
        return video_id
    url = _first_value(row, "url", "video_url", "comment_url")
    if not url:
        return None
    return parse_qs(urlparse(url).query).get("v", [None])[0]


def _x_status_id_from_url(url):
    url = _clean_value(url)
    if not url:
        return None
    match = re.search(r"/status/(\d+)", url)
    return match.group(1) if match else None


def _reddit_post_id_from_url(url):
    url = _clean_value(url)
    if not url:
        return None
    match = re.search(r"/comments/([^/]+)", url)
    return match.group(1) if match else None


def _subreddit_from_url(url):
    url = _clean_value(url)
    if not url:
        return None
    match = re.search(r"/r/([^/]+)", url, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _json_or_text(value):
    value = _clean_value(value)
    if value is None:
        return None, None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return value, None
    if isinstance(parsed, list):
        text = " ".join(
            str(item.get("text", "")).strip()
            for item in parsed
            if isinstance(item, dict) and str(item.get("text", "")).strip()
        )
        return text or None, json.dumps(parsed, ensure_ascii=False)
    if isinstance(parsed, dict):
        text = _clean_value(parsed.get("text") or parsed.get("transcript_text"))
        return text, json.dumps(parsed, ensure_ascii=False)
    return value, None


def _parse_collaborators(value):
    if isinstance(value, (list, tuple)):
        collaborators = [
            str(item).strip()
            for item in value
            if item is not None and str(item).strip()
        ]
        return collaborators or None
    value = _clean_value(value)
    if value is None:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = re.split(r"[,;]", value)
    if isinstance(parsed, str):
        parsed = [parsed]
    if not isinstance(parsed, list):
        return None
    collaborators = [str(item).strip() for item in parsed if str(item).strip()]
    return collaborators or None


def _parse_boolean(value):
    value = _clean_value(value)
    if value is None:
        return None
    normalized = value.lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return None


@lru_cache(maxsize=1)
def _event_schema_fields():
    try:
        schema = json.loads(EVENT_SCHEMA_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"unable to load the canonical event schema: {EVENT_SCHEMA_PATH}"
        ) from exc
    return tuple(schema["fields"])


def _nullable_avro_type(field):
    data_type = field["type"]
    if isinstance(data_type, list):
        data_type = next(item for item in data_type if item != "null")
    if isinstance(data_type, dict):
        return data_type["type"]
    return data_type


def _coerce_canonical_value(value, field):
    data_type = _nullable_avro_type(field)
    if data_type in {"int", "long"}:
        return _parse_count(value)
    if data_type == "double":
        value = _clean_value(value)
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if data_type == "boolean":
        return _parse_boolean(value)
    if data_type == "array":
        return _parse_collaborators(value)
    return _clean_value(value)


def _is_canonical_event(row):
    return all(field in row for field in ("user_id", "timestamp", "source"))


def _normalize_canonical_event(row, source=None):
    """Preserve every canonical field during an export/import transfer."""
    fields = _event_schema_fields()
    event = {
        field["name"]: _coerce_canonical_value(row.get(field["name"]), field)
        for field in fields
    }
    timestamp = _first_value(
        row,
        "timestamp",
        "event_ts",
        "created_at",
        "collected_at",
        "published_at",
    )
    event["timestamp"] = _parse_timestamp(timestamp) if timestamp else None
    selected_source = source or event.get("source")
    if selected_source:
        event["source"] = selected_source.lower()
    return event


def detect_source(row):
    source = _clean_value(row.get("source"))
    if source and source.lower() in SUPPORTED_SOURCES:
        return source.lower()
    columns = set(row)
    if {"tweet_text", "status_id"} & columns:
        return "x"
    if {"comment_text", "post_url", "created_iso"} & columns:
        return "reddit"
    if {"video_id", "video_title", "comment_published_at"} & columns:
        return "youtube"
    raise ValueError(
        "unable to detect source; choose YouTube, X or Reddit manually"
    )


def normalize_event(row, source=None):
    if _is_canonical_event(row):
        return _normalize_canonical_event(row, source=source)
    source = (source or detect_source(row)).lower()
    if source == "youtube":
        return _normalize_youtube(row)
    if source == "x":
        return _normalize_x(row)
    if source == "reddit":
        return _normalize_reddit(row)
    raise ValueError(f"unsupported source: {source}")


def _normalize_youtube(row):
    video_id = _youtube_video_id(row)
    transcript_text, transcript_segments_json = _json_or_text(
        _first_value(
            row,
            "transcript_segments_json",
            "video_transcript",
            "transcript",
            "transcript_text",
        )
    )
    platform_event_id = _first_value(
        row,
        "platform_event_id",
        "comment_id",
        "thread_id",
        "video_id",
    )
    url = _first_value(row, "url", "comment_url", "video_url")
    if not url and video_id:
        url = f"https://www.youtube.com/watch?v={video_id}"
    return {
        "user_id": _source_user_id(
            "youtube",
            row,
            "author",
            "comment_id",
            "video_id",
        ),
        "url": url,
        "title": _first_value(row, "title", "text", "comment_text", "video_title"),
        "raw_text": _first_value(row, "title", "text", "comment_text", "video_title"),
        "clean_text": None,
        "text_for_model": None,
        "timestamp": _parse_timestamp(
            _first_value(
                row,
                "timestamp",
                "created_at",
                "event_ts",
                "comment_published_at",
                "video_published_at",
            )
        ),
        "source": "youtube",
        "error": _first_value(row, "error"),
        "platform_event_id": platform_event_id,
        "owner_channel_id": _first_value(row, "owner_channel_id"),
        "subreddit": None,
        "x_account": None,
        "youtube_channel_name": _first_value(
            row,
            "youtube_channel_name",
            "channel_title",
            "channel_name",
        ),
        "language": _first_value(row, "language", "lang", "default_language"),
        "parent_interaction_id": _first_value(row, "parent_interaction_id"),
        "conversation_id": _first_value(row, "conversation_id", "video_id") or video_id,
        "transcript_text": transcript_text,
        "transcript_segments_json": transcript_segments_json,
        "duration_seconds": _parse_count(_first_value(row, "duration_seconds")),
        "has_auto_captions": None,
        "collaborator_channel_ids": _parse_collaborators(
            row.get("collaborator_channel_ids")
        ),
        "like_count": _parse_count(
            _first_value(row, "like_count", "comment_like_count", "video_like_count")
        ),
        "view_count": _parse_count(_first_value(row, "view_count", "video_view_count")),
        "follower_count": None,
        "subscriber_count": _parse_count(_first_value(row, "subscriber_count")),
        "subreddit_member_count": None,
    }


def _normalize_x(row):
    status_id = _first_value(row, "platform_event_id", "status_id")
    root_status_id = (
        _first_value(row, "conversation_id", "root_status_id", "root_content_id")
        or _x_status_id_from_url(_first_value(row, "page_url"))
        or status_id
    )
    is_reply = str(_first_value(row, "is_reply") or "").lower() in {"1", "true", "yes"}
    return {
        "user_id": _source_user_id(
            "x",
            row,
            "screen_name",
            "display_name",
            "status_id",
        ),
        "url": _first_value(row, "url", "tweet_url", "page_url"),
        "title": _first_value(row, "title", "tweet_text", "text"),
        "raw_text": _first_value(row, "title", "tweet_text", "text"),
        "clean_text": None,
        "text_for_model": None,
        "timestamp": _parse_timestamp(
            _first_value(
                row,
                "timestamp",
                "created_at",
                "event_ts",
                "tweet_time_iso",
                "tweet_time",
            )
        ),
        "source": "x",
        "error": _first_value(row, "error"),
        "platform_event_id": status_id,
        "owner_channel_id": None,
        "subreddit": None,
        "x_account": _first_value(row, "x_account", "screen_name"),
        "youtube_channel_name": None,
        "language": _first_value(row, "language", "lang"),
        "parent_interaction_id": _first_value(
            row,
            "parent_interaction_id",
            "reply_to_status_id",
            "reply_to_post_id",
        )
        or (root_status_id if is_reply and root_status_id != status_id else None),
        "conversation_id": root_status_id,
        "transcript_text": None,
        "transcript_segments_json": None,
        "duration_seconds": None,
        "has_auto_captions": None,
        "collaborator_channel_ids": None,
        "like_count": _parse_count(_first_value(row, "like_count")),
        "view_count": _parse_count(_first_value(row, "view_count")),
        "follower_count": _parse_count(_first_value(row, "follower_count")),
        "subscriber_count": None,
        "subreddit_member_count": None,
    }


def _normalize_reddit(row):
    url = _first_value(row, "url", "comment_permalink", "post_url")
    post_id = (
        _first_value(row, "conversation_id", "post_id", "root_content_id")
        or _reddit_post_id_from_url(url)
    )
    parent_id = _first_value(row, "parent_interaction_id", "parent_id")
    if parent_id and parent_id.startswith("t3_"):
        parent_id = None
    elif parent_id and parent_id.startswith("t1_"):
        parent_id = parent_id.removeprefix("t1_")
    return {
        "user_id": _source_user_id(
            "reddit",
            row,
            "author",
            "comment_id",
        ),
        "url": url,
        "title": _first_value(row, "comment_text", "text", "body", "title"),
        "raw_text": _first_value(row, "comment_text", "text", "body", "title"),
        "clean_text": None,
        "text_for_model": None,
        "timestamp": _parse_timestamp(
            _first_value(
                row,
                "timestamp",
                "created_at",
                "event_ts",
                "created_iso",
                "created_utc",
            )
        ),
        "source": "reddit",
        "error": _first_value(row, "error"),
        "platform_event_id": _first_value(row, "platform_event_id", "comment_id"),
        "owner_channel_id": None,
        "subreddit": _first_value(row, "subreddit") or _subreddit_from_url(url),
        "subreddit_title": _first_value(row, "subreddit_title", "community_title"),
        "subreddit_description": _first_value(
            row,
            "subreddit_description",
            "community_description",
        ),
        "subreddit_created_at": _first_value(row, "subreddit_created_at"),
        "subreddit_visibility": _first_value(
            row,
            "subreddit_visibility",
            "community_visibility",
        ),
        "subreddit_weekly_visitors": _parse_count(
            _first_value(row, "subreddit_weekly_visitors", "weekly_visitors")
        ),
        "subreddit_weekly_contributions": _parse_count(
            _first_value(
                row,
                "subreddit_weekly_contributions",
                "weekly_contributions",
            )
        ),
        "x_account": None,
        "youtube_channel_name": None,
        "language": _first_value(row, "language", "lang"),
        "parent_interaction_id": parent_id,
        "conversation_id": post_id,
        "transcript_text": None,
        "transcript_segments_json": None,
        "duration_seconds": None,
        "has_auto_captions": None,
        "collaborator_channel_ids": None,
        "like_count": None,
        "view_count": None,
        "follower_count": None,
        "subscriber_count": None,
        "subreddit_member_count": _parse_count(_first_value(row, "subreddit_member_count", "subreddit_subscribers")),
    }


def _decode_payload(payload):
    if isinstance(payload, str):
        return payload
    return payload.decode("utf-8-sig")


def _records_from_csv(payload):
    reader = csv.DictReader(io.StringIO(_decode_payload(payload)))
    return list(reader)


def _records_from_json(payload):
    parsed = json.loads(_decode_payload(payload))
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for key in ("events", "records", "data", "items"):
            if isinstance(parsed.get(key), list):
                return parsed[key]
        return [parsed]
    raise ValueError("JSON import must contain an object or a list of objects")


def _records_from_jsonl(payload):
    records = []
    for line in _decode_payload(payload).splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def load_import_events(file_name, payload, source="auto"):
    extension = Path(file_name).suffix.lower()
    if extension == ".csv":
        records = _records_from_csv(payload)
    elif extension == ".json":
        records = _records_from_json(payload)
    elif extension in {".jsonl", ".ndjson"}:
        records = _records_from_jsonl(payload)
    else:
        raise ValueError("supported formats are CSV, JSON, JSONL and NDJSON")

    selected_source = None if source == "auto" else source
    events = []
    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"record {index} is not an object")
        canonical_record = _is_canonical_event(record)
        event = normalize_event(record, selected_source)
        required_fields = ("user_id", "url", "timestamp", "source")
        if not canonical_record:
            required_fields += ("title",)
        missing = [
            field
            for field in required_fields
            if not event.get(field)
        ]
        if missing:
            raise ValueError(
                f"record {index} is missing required field(s): "
                + ", ".join(missing)
            )
        events.append(event)
    return events


def summarize_events(events):
    summary = {source: 0 for source in SUPPORTED_SOURCES}
    for event in events:
        summary[event["source"]] += 1
    return {source: count for source, count in summary.items() if count}


def publish_events(events, config=None):
    if not events:
        return {}
    try:
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as exc:
        raise RuntimeError(
            "confluent-kafka is required to publish imported files"
        ) from exc

    config = config or get_manual_import_config()
    topic_by_source = config["topic_by_source"]
    used_topics = {
        topic_by_source[event["source"]]
        for event in events
    }
    admin = AdminClient({"bootstrap.servers": config["bootstrap_servers"]})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=2, replication_factor=1) for topic in used_topics]
    )
    for future in futures.values():
        try:
            future.result(15)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise

    errors = []

    def delivery_report(error, _message):
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": config["bootstrap_servers"]})
    for event in events:
        producer.produce(
            topic=topic_by_source[event["source"]],
            key=event["user_id"].encode("utf-8"),
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

    undelivered = producer.flush(60)
    if undelivered or errors:
        raise RuntimeError(
            f"Kafka delivery failed: {undelivered} undelivered, {errors}"
        )
    return summarize_events(events)
