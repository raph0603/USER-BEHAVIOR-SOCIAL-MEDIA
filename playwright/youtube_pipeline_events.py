"""Kafka helpers for the independent YouTube pipeline workers."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer


UTC = timezone.utc


def pipeline_event(
    event_type: str,
    video_id: str,
    *,
    correlation_id: str | None = None,
    channel_id: str | None = None,
    collected_at: datetime | None = None,
    attempt_count: int = 1,
    **fields,
) -> dict:
    observed_at = (collected_at or datetime.now(UTC)).astimezone(UTC).isoformat()
    event = {
        "event_type": event_type,
        "event_version": "1.0",
        "video_id": video_id,
        "platform_event_id": video_id,
        "channel_id": channel_id,
        "owner_channel_id": channel_id,
        "source": "youtube",
        "collected_at": observed_at,
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
        **fields,
    }
    return event


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
        registry = SchemaRegistryClient({"url": schema_registry_url})
        self._producer = SerializingProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": AvroSerializer(registry, schema_text),
            }
        )
        self._errors: list[str] = []

    def publish(self, topic: str, events: Iterable[dict]) -> int:
        count = 0

        def delivery(error, _message) -> None:
            if error:
                self._errors.append(str(error))

        for event in events:
            value = {field: event.get(field) for field in self.fields}
            self._producer.produce(
                topic=topic,
                key=event.get("video_id") or event.get("platform_event_id"),
                value=value,
                on_delivery=delivery,
            )
            self._producer.poll(0)
            count += 1
        undelivered = self._producer.flush(60)
        if undelivered or self._errors:
            raise RuntimeError(f"Kafka delivery failed: {undelivered} undelivered, {self._errors}")
        return count


def drain_outbox(
    state: Any,
    producer: EventProducer,
    *,
    limit: int = 1000,
    include_deferred: bool = False,
    now: datetime | None = None,
) -> int:
    """Publish pending rows and acknowledge SQLite only after Kafka delivery."""

    drained = 0
    reference_time = (now or datetime.now(UTC)).astimezone(UTC)
    rows = state.pending_outbox(
        now=reference_time,
        limit=limit,
        include_deferred=include_deferred,
    )
    for row in rows:
        attempted_at = datetime.now(UTC)
        try:
            event = json.loads(row["event_json"])
            producer.publish(row["topic"], [event])
        except BaseException as exc:
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
