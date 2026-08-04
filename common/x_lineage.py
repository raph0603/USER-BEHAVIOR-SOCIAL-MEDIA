"""Shared helpers for the single-event X lineage demonstration."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PRIVACY_TOKENS = ("<USER>", "<EMAIL>", "<PHONE>", "<IP>", "<URL>")

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+", re.UNICODE)
_URL_RE = re.compile(
    r"(?:https?://|www\.)[^\s<>\"']*[^\s<>\"'.,;:!?)}\]]",
    re.IGNORECASE,
)
_MENTION_RE = re.compile(r"(?<![\w@])@\w+", re.UNICODE)
_IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_PHONE_RE = re.compile(r"(?<!\d)\+?\d[\d\s\-.()]{6,}\d(?!\d)")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def redaction_summary(text: str | None) -> dict[str, int]:
    """Count sensitive values using the same precedence as the Spark cleaner."""

    remaining = str(text or "")
    counts: dict[str, int] = {}
    for name, pattern in (
        ("email_count", _EMAIL_RE),
        ("url_count", _URL_RE),
        ("user_count", _MENTION_RE),
        ("ip_count", _IP_RE),
        ("phone_count", _PHONE_RE),
    ):
        matches = pattern.findall(remaining)
        counts[name] = len(matches)
        remaining = pattern.sub(" ", remaining)
    return counts


def sensitive_values(text: str | None) -> dict[str, list[str]]:
    """Return the concrete values that must not survive beyond RAW."""

    remaining = str(text or "")
    values: dict[str, list[str]] = {}
    for name, pattern in (
        ("email", _EMAIL_RE),
        ("url", _URL_RE),
        ("user", _MENTION_RE),
        ("ip", _IP_RE),
        ("phone", _PHONE_RE),
    ):
        matches = [str(match) for match in pattern.findall(remaining)]
        values[name] = matches
        remaining = pattern.sub(" ", remaining)
    return values


def expected_clean_text(text: str | None) -> str:
    """Reference implementation used to validate the actual Spark result."""

    cleaned = str(text or "")
    for token, pattern in (
        ("<EMAIL>", _EMAIL_RE),
        ("<URL>", _URL_RE),
        ("<USER>", _MENTION_RE),
        ("<IP>", _IP_RE),
        ("<PHONE>", _PHONE_RE),
    ):
        cleaned = pattern.sub(f" {token} ", cleaned)
    cleaned = " ".join(cleaned.split())
    return re.sub(r"\s+([,.;:!?])", r"\1", cleaned)


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")


@dataclass
class RawCaptureWriter:
    """Write exact pre-privacy X events to a restricted, run-scoped directory."""

    enabled: bool
    directory: Path
    limit: int
    producer_name: str
    producer_run_id: str
    kafka_topic: str
    captured: int = 0

    @classmethod
    def from_environment(
        cls,
        *,
        producer_name: str,
        producer_run_id: str,
        kafka_topic: str,
    ) -> "RawCaptureWriter":
        enabled = os.getenv("X_RAW_CAPTURE_ENABLED", "false").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        directory = Path(os.getenv("X_RAW_CAPTURE_DIR", "/app/captures/x"))
        raw_limit = os.getenv("X_RAW_CAPTURE_LIMIT", "1").strip()
        try:
            limit = int(raw_limit)
        except ValueError as exc:
            raise ValueError("X_RAW_CAPTURE_LIMIT must be an integer") from exc
        if limit < 1:
            raise ValueError("X_RAW_CAPTURE_LIMIT must be at least 1")
        return cls(
            enabled=enabled,
            directory=directory,
            limit=limit,
            producer_name=producer_name,
            producer_run_id=producer_run_id,
            kafka_topic=kafka_topic,
        )

    def capture(self, event: dict[str, Any]) -> Path | None:
        if not self.enabled or self.captured >= self.limit:
            return None
        if str(event.get("source") or "").lower() != "x":
            return None

        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            self.directory.chmod(0o700)
        except OSError:
            pass

        filename = "raw.json" if self.limit == 1 else f"raw-{self.captured + 1}.json"
        destination = self.directory / filename
        if destination.exists():
            self.captured += 1
            return destination

        payload = {
            "capture_metadata": {
                "captured_at": utc_now_iso(),
                "producer_name": self.producer_name,
                "producer_run_id": self.producer_run_id,
                "kafka_topic": self.kafka_topic,
                "capture_stage": "before_privacy_cleaning",
            },
            "event": event,
        }
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_bytes(_json_bytes(payload))
        try:
            temporary.chmod(0o600)
        except OSError:
            pass
        temporary.replace(destination)
        self.captured += 1
        return destination
