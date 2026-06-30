import csv
import hashlib
import io
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse


SUPPORTED_SOURCES = ("youtube", "x", "reddit")
DEFAULT_TOPIC_BY_SOURCE = {
    "youtube": "manual.youtube.raw.events",
    "x": "manual.x.raw.events",
    "reddit": "manual.reddit.raw.events",
}
MANUAL_IMPORT_DAG_ID = "manual_file_import_lakehouse"


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
        return int(float(text) * multiplier)
    except ValueError:
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


def _parse_collaborators(value):
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
        "timestamp": _parse_timestamp(
            _first_value(
                row,
                "timestamp",
                "comment_published_at",
                "video_published_at",
            )
        ),
        "source": "youtube",
        "error": _first_value(row, "error"),
        "platform_event_id": platform_event_id,
        "owner_channel_id": _first_value(row, "owner_channel_id"),
        "collaborator_channel_ids": _parse_collaborators(
            row.get("collaborator_channel_ids")
        ),
        "like_count": _parse_count(
            _first_value(row, "like_count", "comment_like_count", "video_like_count")
        ),
        "view_count": _parse_count(_first_value(row, "view_count", "video_view_count")),
    }


def _normalize_x(row):
    status_id = _first_value(row, "platform_event_id", "status_id")
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
        "timestamp": _parse_timestamp(
            _first_value(row, "timestamp", "tweet_time_iso", "tweet_time")
        ),
        "source": "x",
        "error": _first_value(row, "error"),
        "platform_event_id": status_id,
        "owner_channel_id": None,
        "collaborator_channel_ids": None,
        "like_count": _parse_count(_first_value(row, "like_count")),
        "view_count": _parse_count(_first_value(row, "view_count")),
    }


def _normalize_reddit(row):
    return {
        "user_id": _source_user_id(
            "reddit",
            row,
            "author",
            "comment_id",
        ),
        "url": _first_value(row, "url", "comment_permalink", "post_url"),
        "title": _first_value(row, "title", "comment_text", "text"),
        "timestamp": _parse_timestamp(
            _first_value(row, "timestamp", "created_iso", "created_utc")
        ),
        "source": "reddit",
        "error": _first_value(row, "error"),
        "platform_event_id": _first_value(row, "platform_event_id", "comment_id"),
        "owner_channel_id": None,
        "collaborator_channel_ids": None,
        "like_count": None,
        "view_count": None,
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
        event = normalize_event(record, selected_source)
        missing = [
            field
            for field in ("user_id", "url", "title", "timestamp", "source")
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
