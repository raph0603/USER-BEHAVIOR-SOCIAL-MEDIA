"""Shared collection outcomes, observability helpers, and safe serialization."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Generic, Iterable, Mapping, TypeVar


STATUS_PENDING = "pending"
STATUS_SUCCESS = "success"
STATUS_PARTIAL = "partial"
STATUS_NOT_AVAILABLE = "not_available"
STATUS_DISABLED = "disabled"
STATUS_RATE_LIMITED = "rate_limited"
STATUS_FAILED = "failed"

ALL_STATUSES = frozenset(
    {
        STATUS_PENDING,
        STATUS_SUCCESS,
        STATUS_PARTIAL,
        STATUS_NOT_AVAILABLE,
        STATUS_DISABLED,
        STATUS_RATE_LIMITED,
        STATUS_FAILED,
    }
)
TERMINAL_STATUSES = frozenset(
    {STATUS_SUCCESS, STATUS_NOT_AVAILABLE, STATUS_DISABLED}
)
RETRYABLE_STATUSES = frozenset(
    {STATUS_PENDING, STATUS_PARTIAL, STATUS_RATE_LIMITED, STATUS_FAILED}
)

REDACTED_VALUE = "[REDACTED]"
_SECRET_KEY_PARTS = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "authorization",
        "client_secret",
        "cookie",
        "credentials",
        "password",
        "refresh_token",
        "secret",
        "session",
        "session_id",
        "token",
    }
)
_BEARER_PATTERN = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_URL_SECRET_PATTERN = re.compile(
    r"(?i)([?&](?:access_token|api_key|apikey|key|password|secret|token)=)[^&#\s]+"
)

PayloadT = TypeVar("PayloadT")


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""

    return datetime.now(timezone.utc)


def ensure_utc(value: datetime | None) -> datetime | None:
    """Normalize a timestamp to UTC while treating naive values as UTC."""

    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def isoformat_utc(value: datetime | None) -> str | None:
    """Serialize a timestamp with a stable ``Z`` UTC suffix."""

    normalized = ensure_utc(value)
    if normalized is None:
        return None
    return normalized.isoformat().replace("+00:00", "Z")


def normalize_status(status: str) -> str:
    """Validate and normalize a collection status."""

    normalized = str(status or "").strip().lower()
    if normalized not in ALL_STATUSES:
        choices = ", ".join(sorted(ALL_STATUSES))
        raise ValueError(f"Unsupported collection status {status!r}; expected one of {choices}")
    return normalized


def is_terminal_status(status: str) -> bool:
    """Return whether a status should stop normal retry processing."""

    return normalize_status(status) in TERMINAL_STATUSES


def is_retryable_status(status: str) -> bool:
    """Return whether a status can be attempted again."""

    return normalize_status(status) in RETRYABLE_STATUSES


def overall_status(values: Iterable[str | "OperationResult[Any]"]) -> str:
    """Summarize component outcomes without hiding partial completion."""

    statuses = [
        normalize_status(value.status if isinstance(value, OperationResult) else value)
        for value in values
    ]
    if not statuses:
        return STATUS_PENDING
    unique = set(statuses)
    if len(unique) == 1:
        return statuses[0]
    if STATUS_PARTIAL in unique or STATUS_SUCCESS in unique:
        return STATUS_PARTIAL
    if STATUS_FAILED in unique:
        return STATUS_FAILED
    if STATUS_RATE_LIMITED in unique:
        return STATUS_RATE_LIMITED
    if STATUS_PENDING in unique:
        return STATUS_PENDING
    return STATUS_PARTIAL


def _normalized_secret_key(key: Any) -> str:
    split_camel_case = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(key).strip())
    normalized = re.sub(r"[^a-z0-9]+", "_", split_camel_case.lower()).strip("_")
    return normalized


def _is_secret_key(key: Any) -> bool:
    normalized = _normalized_secret_key(key)
    return normalized in _SECRET_KEY_PARTS or any(
        normalized.endswith(f"_{part}") for part in _SECRET_KEY_PARTS
    )


def sanitize_text(value: str, *, max_length: int | None = None) -> str:
    """Redact common credential forms from free-form text."""

    sanitized = _BEARER_PATTERN.sub("Bearer [REDACTED]", str(value))
    sanitized = _URL_SECRET_PATTERN.sub(r"\1[REDACTED]", sanitized)
    if max_length is not None and max_length >= 0 and len(sanitized) > max_length:
        if max_length <= 3:
            return sanitized[:max_length]
        return f"{sanitized[: max_length - 3]}..."
    return sanitized


def sanitize_error_message(value: Any, *, max_length: int = 1000) -> str | None:
    """Create a bounded, credential-redacted error message."""

    if value is None:
        return None
    message = " ".join(str(value).split())
    return sanitize_text(message, max_length=max_length) or None


def sanitize_json_value(
    value: Any,
    *,
    max_depth: int = 20,
    _depth: int = 0,
    _seen: set[int] | None = None,
) -> Any:
    """Convert arbitrary values to a bounded, JSON-compatible representation."""

    if _depth > max_depth:
        return "[MAX_DEPTH]"
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str):
        return sanitize_text(value)
    if isinstance(value, bytes):
        return sanitize_text(value.decode("utf-8", errors="replace"))
    if isinstance(value, datetime):
        return isoformat_utc(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return sanitize_json_value(
            value.value,
            max_depth=max_depth,
            _depth=_depth + 1,
            _seen=_seen,
        )

    seen = _seen if _seen is not None else set()
    track_identity = isinstance(value, (Mapping, list, tuple, set, frozenset)) or is_dataclass(value)
    identity = id(value)
    if track_identity:
        if identity in seen:
            return "[CYCLE]"
        seen.add(identity)

    try:
        if is_dataclass(value) and not isinstance(value, type):
            value = asdict(value)
        if isinstance(value, Mapping):
            result = {}
            for key, item in value.items():
                string_key = str(key)
                result[string_key] = (
                    REDACTED_VALUE
                    if _is_secret_key(key)
                    else sanitize_json_value(
                        item,
                        max_depth=max_depth,
                        _depth=_depth + 1,
                        _seen=seen,
                    )
                )
            return result
        if isinstance(value, (list, tuple)):
            return [
                sanitize_json_value(
                    item,
                    max_depth=max_depth,
                    _depth=_depth + 1,
                    _seen=seen,
                )
                for item in value
            ]
        if isinstance(value, (set, frozenset)):
            return [
                sanitize_json_value(
                    item,
                    max_depth=max_depth,
                    _depth=_depth + 1,
                    _seen=seen,
                )
                for item in sorted(value, key=lambda item: (type(item).__name__, repr(item)))
            ]
        return f"<{type(value).__name__}>"
    finally:
        if track_identity:
            seen.discard(identity)


def safe_json_dumps(value: Any, *, pretty: bool = False) -> str:
    """Serialize sanitized JSON with deterministic key ordering."""

    kwargs: dict[str, Any] = {
        "ensure_ascii": False,
        "sort_keys": True,
        "allow_nan": False,
    }
    if pretty:
        kwargs["indent"] = 2
    else:
        kwargs["separators"] = (",", ":")
    return json.dumps(sanitize_json_value(value), **kwargs)


@dataclass(frozen=True)
class OperationResult(Generic[PayloadT]):
    """A typed outcome for an independently retryable collection operation."""

    status: str
    payload: PayloadT | None = None
    error_code: str | None = None
    error_message: str | None = None
    attempt_count: int = 1
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", normalize_status(self.status))
        if self.attempt_count < 0:
            raise ValueError("attempt_count cannot be negative")
        object.__setattr__(self, "error_code", self.error_code or None)
        object.__setattr__(
            self,
            "error_message",
            sanitize_error_message(self.error_message),
        )
        object.__setattr__(self, "started_at", ensure_utc(self.started_at))
        object.__setattr__(self, "completed_at", ensure_utc(self.completed_at))

    @property
    def is_terminal(self) -> bool:
        return is_terminal_status(self.status)

    @property
    def is_retryable(self) -> bool:
        return is_retryable_status(self.status)

    @property
    def is_success(self) -> bool:
        return self.status == STATUS_SUCCESS

    @classmethod
    def pending(
        cls,
        payload: PayloadT | None = None,
        *,
        attempt_count: int = 0,
        started_at: datetime | None = None,
    ) -> "OperationResult[PayloadT]":
        return cls(
            status=STATUS_PENDING,
            payload=payload,
            attempt_count=attempt_count,
            started_at=started_at,
        )

    @classmethod
    def success(
        cls,
        payload: PayloadT,
        *,
        attempt_count: int = 1,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> "OperationResult[PayloadT]":
        return cls(
            status=STATUS_SUCCESS,
            payload=payload,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )

    @classmethod
    def partial(
        cls,
        payload: PayloadT,
        *,
        error_code: str,
        error_message: str | None = None,
        attempt_count: int = 1,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> "OperationResult[PayloadT]":
        return cls(
            status=STATUS_PARTIAL,
            payload=payload,
            error_code=error_code,
            error_message=error_message,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )

    @classmethod
    def unavailable(
        cls,
        *,
        error_code: str = STATUS_NOT_AVAILABLE,
        error_message: str | None = None,
        attempt_count: int = 1,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> "OperationResult[PayloadT]":
        return cls(
            status=STATUS_NOT_AVAILABLE,
            error_code=error_code,
            error_message=error_message,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )

    @classmethod
    def disabled(
        cls,
        *,
        error_code: str = STATUS_DISABLED,
        error_message: str | None = None,
        attempt_count: int = 1,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> "OperationResult[PayloadT]":
        return cls(
            status=STATUS_DISABLED,
            error_code=error_code,
            error_message=error_message,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )

    @classmethod
    def rate_limited(
        cls,
        *,
        error_code: str = STATUS_RATE_LIMITED,
        error_message: str | None = None,
        attempt_count: int = 1,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> "OperationResult[PayloadT]":
        return cls(
            status=STATUS_RATE_LIMITED,
            error_code=error_code,
            error_message=error_message,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )

    @classmethod
    def failed(
        cls,
        *,
        error_code: str = STATUS_FAILED,
        error_message: str | None = None,
        attempt_count: int = 1,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> "OperationResult[PayloadT]":
        return cls(
            status=STATUS_FAILED,
            error_code=error_code,
            error_message=error_message,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )

    def to_dict(self, *, include_payload: bool = True) -> dict[str, Any]:
        result = {
            "status": self.status,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "attempt_count": self.attempt_count,
            "started_at": isoformat_utc(self.started_at),
            "completed_at": isoformat_utc(self.completed_at),
        }
        if include_payload:
            result["payload"] = sanitize_json_value(self.payload)
        return result

    def as_event_fields(self, component: str) -> dict[str, Any]:
        """Return flattened audit fields for a named event component."""

        prefix = re.sub(r"[^a-z0-9]+", "_", component.strip().lower()).strip("_")
        if not prefix:
            raise ValueError("component must contain at least one letter or digit")
        return {
            f"{prefix}_status": self.status,
            f"{prefix}_error_code": self.error_code,
            f"{prefix}_error_message": self.error_message,
            f"{prefix}_attempt_count": self.attempt_count,
            f"{prefix}_last_attempt_at": isoformat_utc(
                self.completed_at or self.started_at
            ),
        }


def canonical_content_id(source: str, platform_content_id: str) -> str:
    """Build the stable canonical identifier used by relationship records."""

    normalized_source = str(source or "").strip().lower()
    normalized_id = str(platform_content_id or "").strip()
    if not normalized_source or not normalized_id:
        raise ValueError("source and platform_content_id are required")
    return hashlib.sha256(f"{normalized_source}:{normalized_id}".encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ContentRelationship:
    """Canonical identity and placement of one content item in a conversation."""

    content_id: str
    root_content_id: str
    conversation_id: str
    content_type: str
    relation_type: str
    parent_content_id: str | None = None
    depth: int = 0
    position_in_thread: int | None = None

    def __post_init__(self) -> None:
        required = {
            "content_id": self.content_id,
            "root_content_id": self.root_content_id,
            "conversation_id": self.conversation_id,
            "content_type": self.content_type,
            "relation_type": self.relation_type,
        }
        missing = [name for name, value in required.items() if not str(value or "").strip()]
        if missing:
            raise ValueError(f"Missing relationship fields: {', '.join(missing)}")
        if self.depth < 0:
            raise ValueError("depth cannot be negative")
        if self.position_in_thread is not None and self.position_in_thread < 0:
            raise ValueError("position_in_thread cannot be negative")
        if self.depth == 0 and self.parent_content_id is not None:
            raise ValueError("root content cannot have a parent_content_id")

    @classmethod
    def root(
        cls,
        *,
        source: str,
        platform_content_id: str,
        content_type: str,
        conversation_id: str | None = None,
    ) -> "ContentRelationship":
        content_id = canonical_content_id(source, platform_content_id)
        return cls(
            content_id=content_id,
            root_content_id=content_id,
            conversation_id=conversation_id or platform_content_id,
            content_type=content_type,
            relation_type="root",
        )

    @classmethod
    def child(
        cls,
        *,
        source: str,
        platform_content_id: str,
        parent_content_id: str,
        root_content_id: str,
        conversation_id: str,
        content_type: str,
        relation_type: str,
        depth: int,
        position_in_thread: int | None = None,
    ) -> "ContentRelationship":
        return cls(
            content_id=canonical_content_id(source, platform_content_id),
            parent_content_id=parent_content_id,
            root_content_id=root_content_id,
            conversation_id=conversation_id,
            content_type=content_type,
            relation_type=relation_type,
            depth=depth,
            position_in_thread=position_in_thread,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
