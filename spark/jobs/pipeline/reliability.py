"""Pure reliability helpers shared by Kafka and Iceberg pipeline stages."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any


EVENT_ID_VERSION = "v1"
TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
FALSE_VALUES = frozenset({"0", "false", "no", "off"})
EVENT_ID_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def parse_boolean(value: str | bool | None, *, name: str) -> bool:
    """Parse a strict environment-style boolean without accepting typos."""

    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be one of true/false, 1/0, yes/no, or on/off")


def fail_on_data_loss_option(
    configured: str | bool | None,
    *,
    allow_data_loss: str | bool | None = False,
) -> str:
    """Return Spark's option value and require an explicit unsafe override."""

    enabled = parse_boolean(
        "true" if configured is None else configured,
        name="KAFKA_FAIL_ON_DATA_LOSS",
    )
    if enabled:
        return "true"
    if not parse_boolean(allow_data_loss, name="ALLOW_KAFKA_DATA_LOSS"):
        raise ValueError(
            "KAFKA_FAIL_ON_DATA_LOSS=false requires ALLOW_KAFKA_DATA_LOSS=true"
        )
    return "false"


def canonical_payload_bytes(payload: Mapping[str, Any]) -> bytes:
    """Serialize an event deterministically for fingerprints and identities."""

    return json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def payload_fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_payload_bytes(payload)).hexdigest()


def deterministic_event_id(payload: Mapping[str, Any]) -> str:
    """Build a replay-stable ID while allowing distinct observations."""

    supplied = str(payload.get("event_id") or "").strip().lower()
    if EVENT_ID_PATTERN.fullmatch(supplied):
        return supplied
    fingerprint = payload_fingerprint(payload)
    identity = "\x1f".join(
        (
            EVENT_ID_VERSION,
            str(payload.get("source") or ""),
            supplied,
            str(payload.get("platform_event_id") or ""),
            str(payload.get("user_id") or ""),
            str(payload.get("url") or ""),
            str(payload.get("timestamp") or ""),
            str(payload.get("event_type") or ""),
            str(payload.get("event_version") or ""),
            str(payload.get("observed_at") or payload.get("collected_at") or ""),
            fingerprint,
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def protected_payload_envelope(raw_payload: str | bytes | None) -> str:
    """Describe a rejected payload without retaining or logging its contents."""

    if raw_payload is None:
        raw = b""
    elif isinstance(raw_payload, bytes):
        raw = raw_payload
    else:
        raw = raw_payload.encode("utf-8", errors="replace")
    return json.dumps(
        {
            "redacted": True,
            "byte_length": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
