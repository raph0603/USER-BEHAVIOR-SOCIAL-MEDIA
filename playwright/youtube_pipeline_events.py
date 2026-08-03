"""Kafka helpers for the independent YouTube pipeline workers."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from confluent_kafka import (
    DeserializingConsumer,
    KafkaError,
    KafkaException,
    SerializingProducer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)

from common.event_envelope import enrich_event_envelope
from common.youtube_outbox import MESSAGE_SIZE_TOO_LARGE, canonical_event_json


UTC = timezone.utc
LOGGER = logging.getLogger(__name__)
DEFAULT_YOUTUBE_KAFKA_MAX_EVENT_BYTES = 900_000
MIN_YOUTUBE_KAFKA_MAX_EVENT_BYTES = 1_024
MAX_YOUTUBE_KAFKA_MAX_EVENT_BYTES = 100_000_000
KAFKA_RECORD_MARGIN_BYTES = 100_000


class OversizedKafkaEvent(ValueError):
    def __init__(self, *, size_bytes: int, max_event_bytes: int) -> None:
        self.size_bytes = int(size_bytes)
        self.max_event_bytes = int(max_event_bytes)
        super().__init__(
            f"serialized event is {self.size_bytes} bytes; limit is {self.max_event_bytes} bytes"
        )


def youtube_kafka_max_event_bytes(value: str | int | None = None) -> int:
    raw_value = os.getenv("YOUTUBE_KAFKA_MAX_EVENT_BYTES") if value is None else value
    if raw_value is None or str(raw_value).strip() == "":
        return DEFAULT_YOUTUBE_KAFKA_MAX_EVENT_BYTES
    try:
        parsed = int(str(raw_value).strip())
    except ValueError as exc:
        raise ValueError("YOUTUBE_KAFKA_MAX_EVENT_BYTES must be an integer") from exc
    if not MIN_YOUTUBE_KAFKA_MAX_EVENT_BYTES <= parsed <= MAX_YOUTUBE_KAFKA_MAX_EVENT_BYTES:
        raise ValueError(
            "YOUTUBE_KAFKA_MAX_EVENT_BYTES must be between "
            f"{MIN_YOUTUBE_KAFKA_MAX_EVENT_BYTES} and "
            f"{MAX_YOUTUBE_KAFKA_MAX_EVENT_BYTES}"
        )
    return parsed


def _is_message_size_too_large(error: BaseException | object) -> bool:
    candidates = [error]
    if isinstance(error, KafkaException):
        candidates.extend(error.args)
    for candidate in candidates:
        code = getattr(candidate, "code", None)
        if callable(code):
            try:
                if code() == KafkaError.MSG_SIZE_TOO_LARGE:
                    return True
            except (TypeError, ValueError):
                pass
    return False


def _event_log_context(row: dict, event: dict, *, size_bytes: int) -> dict:
    return {
        "outbox_id": row.get("outbox_id"),
        "topic": row.get("topic"),
        "event_type": event.get("event_type"),
        "event_id": event.get("event_id"),
        "video_id": event.get("video_id") or event.get("platform_event_id"),
        "size_bytes": int(size_bytes),
        "delivery_attempts": int(row.get("delivery_attempts") or 0) + 1,
    }


_YOUTUBE_EVENT_PROVENANCE = {
    "discovery": ("youtube_discovery_worker", "youtube_data_api", "search.list"),
    "metadata": ("youtube_metadata_worker", "yt_dlp", "yt-dlp"),
    "engagement": ("youtube_metrics_worker", "youtube_data_api", "videos.list"),
    "transcript": (
        "youtube_transcript_worker",
        "youtube_transcript_api",
        "transcripts.fetch",
    ),
    "comment": ("youtube_comment_worker", "youtube_data_api", "commentThreads.list"),
    "channel": ("youtube_channel_worker", "youtube_data_api", "channels.list"),
}


def _default_provenance(event_type: str) -> tuple[str, str | None, str | None]:
    component = event_type.split(".", 2)[1] if event_type.count(".") >= 2 else "pipeline"
    return _YOUTUBE_EVENT_PROVENANCE.get(
        component,
        ("youtube_pipeline_worker", None, None),
    )


def pipeline_event(
    event_type: str,
    video_id: str,
    *,
    correlation_id: str | None = None,
    channel_id: str | None = None,
    collected_at: datetime | None = None,
    attempt_count: int = 1,
    producer_name: str | None = None,
    producer_run_id: str | None = None,
    collection_method: str | None = None,
    api_endpoint: str | None = None,
    **fields,
) -> dict:
    observed_at = (collected_at or datetime.now(UTC)).astimezone(UTC).isoformat()
    default_producer, default_method, default_endpoint = _default_provenance(event_type)
    event = {
        "event_type": event_type,
        "event_version": "1.0",
        "video_id": video_id,
        "platform_event_id": video_id,
        "channel_id": channel_id,
        "owner_channel_id": channel_id,
        "source": "youtube",
        "collected_at": observed_at,
        "observed_at": observed_at,
        "timestamp": fields.get("published_at") or observed_at,
        "correlation_id": correlation_id or str(uuid.uuid4()),
        "attempt_count": max(1, int(attempt_count)),
        "user_id": hashlib.sha256(f"youtube:{channel_id or video_id}".encode()).hexdigest(),
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "title": fields.get("title"),
        "raw_text": fields.get("description") or fields.get("title"),
        "content_type": "video",
        "relation_type": "root",
        "root_content_id": video_id,
        "conversation_id": video_id,
        "depth": 0,
        "collector_version": os.getenv("COLLECTOR_VERSION", "1"),
        "source_payload_version": "2",
        **fields,
    }
    return enrich_event_envelope(
        event,
        producer_name=producer_name or default_producer,
        producer_run_id=producer_run_id,
        collection_method=collection_method or default_method,
        api_endpoint=api_endpoint or default_endpoint,
    )


class EventProducer:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        schema_registry_url: str,
        schema_path: str | Path,
    ) -> None:
        schema_text = Path(schema_path).read_text(encoding="utf-8")
        schema = json.loads(schema_text)
        self.fields = tuple(field["name"] for field in schema["fields"])
        self.max_event_bytes = youtube_kafka_max_event_bytes()
        registry = SchemaRegistryClient({"url": schema_registry_url})
        self._key_serializer = StringSerializer("utf_8")
        self._value_serializer = AvroSerializer(registry, schema_text)
        self._producer = SerializingProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "message.max.bytes": self.max_event_bytes + KAFKA_RECORD_MARGIN_BYTES,
                "key.serializer": self._key_serializer,
                "value.serializer": self._value_serializer,
            }
        )

    def _value(self, event: dict) -> dict:
        return {field: event.get(field) for field in self.fields}

    def serialized_size_bytes(self, topic: str, event: dict) -> int:
        context = SerializationContext(topic, MessageField.VALUE)
        value_bytes = self._value_serializer(self._value(event), context) or b""
        key = event.get("video_id") or event.get("platform_event_id")
        key_bytes = str(key).encode("utf-8") if key is not None else b""
        return len(value_bytes) + len(key_bytes)

    def publish(self, topic: str, events: Iterable[dict]) -> int:
        count = 0
        errors: list[object] = []

        def delivery(error, _message) -> None:
            if error:
                errors.append(error)

        for event in events:
            size_bytes = self.serialized_size_bytes(topic, event)
            if size_bytes > self.max_event_bytes:
                raise OversizedKafkaEvent(
                    size_bytes=size_bytes,
                    max_event_bytes=self.max_event_bytes,
                )
            self._producer.produce(
                topic=topic,
                key=event.get("video_id") or event.get("platform_event_id"),
                value=self._value(event),
                on_delivery=delivery,
            )
            self._producer.poll(0)
            count += 1
        undelivered = self._producer.flush(60)
        if errors:
            first_error = errors[0]
            if _is_message_size_too_large(first_error):
                raise KafkaException(first_error)
            raise RuntimeError(
                f"Kafka delivery failed: {undelivered} undelivered, "
                f"{[str(error) for error in errors]}"
            )
        if undelivered:
            raise RuntimeError(f"Kafka delivery failed: {undelivered} undelivered")
        return count


def drain_outbox(
    state: Any,
    producer: EventProducer,
    *,
    limit: int = 1000,
    include_deferred: bool = False,
    now: datetime | None = None,
    stats: dict[str, int] | None = None,
) -> int:
    """Publish pending rows and acknowledge SQLite only after Kafka delivery."""

    drained = 0
    reference_time = (now or datetime.now(UTC)).astimezone(UTC)
    rows = state.pending_outbox(
        now=reference_time,
        limit=limit,
        include_deferred=include_deferred,
    )
    quarantined = 0
    for row in rows:
        attempted_at = datetime.now(UTC)
        event: dict = {}
        size_bytes = 0
        try:
            event = json.loads(row["event_json"])
            size_method = getattr(producer, "serialized_size_bytes", None)
            size_bytes = (
                int(size_method(row["topic"], event))
                if callable(size_method)
                else len(canonical_event_json(event).encode("utf-8"))
            )
            context = _event_log_context(row, event, size_bytes=size_bytes)
            LOGGER.info("youtube_outbox_publish_attempt %s", json.dumps(context, sort_keys=True))
            max_event_bytes = int(
                getattr(producer, "max_event_bytes", youtube_kafka_max_event_bytes())
            )
            if size_bytes > max_event_bytes:
                raise OversizedKafkaEvent(
                    size_bytes=size_bytes,
                    max_event_bytes=max_event_bytes,
                )
            producer.publish(row["topic"], [event])
        except BaseException as exc:
            if isinstance(exc, OversizedKafkaEvent) or _is_message_size_too_large(exc):
                if isinstance(exc, OversizedKafkaEvent):
                    size_bytes = exc.size_bytes
                context = _event_log_context(row, event, size_bytes=size_bytes)
                LOGGER.error(
                    "youtube_outbox_quarantined %s",
                    json.dumps(
                        {**context, "failure_reason": MESSAGE_SIZE_TOO_LARGE},
                        sort_keys=True,
                    ),
                )
                state.quarantine_outbox(
                    row["outbox_id"],
                    failed_at=attempted_at,
                    reason=MESSAGE_SIZE_TOO_LARGE,
                    error=exc,
                    payload_size_bytes=size_bytes or None,
                )
                quarantined += 1
                continue
            state.record_outbox_failure(
                row["outbox_id"],
                attempted_at=attempted_at,
                error=exc,
            )
            raise
        state.mark_outbox_delivered(
            row["outbox_id"],
            delivered_at=datetime.now(UTC),
        )
        drained += 1
    if stats is not None:
        stats["published"] = stats.get("published", 0) + drained
        stats["quarantined"] = stats.get("quarantined", 0) + quarantined
    if rows and drained == 0 and quarantined == len(rows):
        raise RuntimeError(f"All {quarantined} pending YouTube outbox event(s) were quarantined")
    return drained


class EventConsumer:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        group_id: str,
    ) -> None:
        registry = SchemaRegistryClient({"url": schema_registry_url})
        self._consumer = DeserializingConsumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "key.deserializer": StringDeserializer("utf_8"),
                "value.deserializer": AvroDeserializer(registry),
            }
        )
        self._consumer.subscribe([topic])

    def poll_batch(self, *, limit: int, idle_seconds: float = 5.0) -> list[dict]:
        events: list[dict] = []
        remaining_idle = idle_seconds
        while len(events) < max(1, int(limit)) and remaining_idle > 0:
            wait = min(1.0, remaining_idle)
            message = self._consumer.poll(wait)
            if message is None:
                remaining_idle -= wait
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            value = message.value()
            if isinstance(value, dict):
                events.append(value)
            remaining_idle = idle_seconds
        return events

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()

    def __enter__(self) -> "EventConsumer":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
