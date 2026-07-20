"""Scheduling, query parsing, and normalization for the YouTube pipeline."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence


UTC = timezone.utc
DESCRIPTIVE_METADATA_FIELDS = (
    "title",
    "description",
    "tags",
    "categories",
    "chapters",
    "thumbnails",
    "subtitles",
    "automatic_captions",
    "availability",
    "live_status",
    "language",
)
SET_LIKE_FIELDS = frozenset({"tags", "categories"})


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def parse_datetime(value: str | datetime | None) -> datetime | None:
    if isinstance(value, datetime):
        return ensure_utc(value)
    if not value:
        return None
    normalized = str(value).strip().replace("Z", "+00:00")
    try:
        return ensure_utc(datetime.fromisoformat(normalized))
    except ValueError:
        return None


def isoformat(value: datetime | None) -> str | None:
    return ensure_utc(value).isoformat() if value else None


@dataclass(frozen=True)
class SearchQuery:
    query: str
    language: str
    query_id: str

    @classmethod
    def create(cls, query: str, language: str) -> "SearchQuery":
        normalized_query = " ".join(str(query or "").split())
        normalized_language = str(language or "").strip().lower()
        if not normalized_query:
            raise ValueError("YouTube search query cannot be empty")
        identity = hashlib.sha256(
            f"{normalized_language}\n{normalized_query}".encode("utf-8")
        ).hexdigest()[:24]
        return cls(normalized_query, normalized_language, identity)


DEFAULT_SEARCH_QUERIES = (
    SearchQuery.create("electric vehicle review|EV charging battery range", "en"),
    SearchQuery.create("đánh giá xe điện|xe điện VinFast trạm sạc", "vi"),
)


def parse_search_queries(
    raw_json: str | None,
    legacy_queries: Sequence[str] = (),
    legacy_languages: Sequence[str] = (),
) -> list[SearchQuery]:
    """Parse query/language pairs without building a query-language product."""

    parsed: Any = None
    if raw_json and raw_json.strip():
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValueError("YOUTUBE_SEARCH_QUERIES_JSON must be valid JSON") from exc
        if not isinstance(parsed, list):
            raise ValueError("YOUTUBE_SEARCH_QUERIES_JSON must be a JSON list")

    specs: list[SearchQuery] = []
    if parsed is not None:
        for item in parsed:
            if isinstance(item, dict):
                specs.append(SearchQuery.create(item.get("query", ""), item.get("language", "")))
            elif isinstance(item, str):
                specs.append(SearchQuery.create(item, _legacy_language(item, (), "en")))
            else:
                raise ValueError("Each YouTube query must be an object or string")
    elif legacy_queries:
        languages = [str(value).strip() for value in legacy_languages if str(value).strip()]
        for index, query in enumerate(legacy_queries):
            paired = languages[index] if index < len(languages) else ""
            specs.append(
                SearchQuery.create(
                    query,
                    paired or _legacy_language(query, languages, "en"),
                )
            )
    else:
        specs.extend(DEFAULT_SEARCH_QUERIES)

    unique: dict[str, SearchQuery] = {}
    for spec in specs:
        unique.setdefault(spec.query_id, spec)
    return list(unique.values())


def _legacy_language(query: str, languages: Sequence[str], default: str) -> str:
    lowered = str(query).lower()
    vietnamese_markers = "ăâđêôơưáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ"
    if any(character in lowered for character in vietnamese_markers):
        return "vi"
    return languages[0] if languages else default


def published_after(
    watermark: str | datetime | None,
    *,
    overlap: timedelta,
    initial_lookback: timedelta,
    now: datetime | None = None,
) -> datetime:
    reference = ensure_utc(now or utc_now())
    previous = parse_datetime(watermark)
    if previous is None:
        return reference - initial_lookback
    return min(reference, previous - overlap)


def parse_hour_offsets(raw_value: str | None) -> tuple[timedelta, ...]:
    values = raw_value or "6,24,72,168,720"
    offsets: list[timedelta] = []
    for token in values.split(","):
        token = token.strip()
        if not token:
            continue
        hours = float(token)
        if hours <= 0:
            raise ValueError("Metadata refresh offsets must be positive")
        offsets.append(timedelta(hours=hours))
    if not offsets:
        raise ValueError("At least one metadata refresh offset is required")
    return tuple(offsets)


def next_metadata_refresh_at(
    first_seen_at: str | datetime,
    refresh_count: int,
    offsets: Sequence[timedelta],
) -> datetime | None:
    first_seen = parse_datetime(first_seen_at)
    if first_seen is None:
        raise ValueError("first_seen_at must be a valid timestamp")
    index = max(0, int(refresh_count))
    if index >= len(offsets):
        return None
    return first_seen + offsets[index]


def metrics_refresh_interval(age: timedelta) -> timedelta:
    hours = max(0.0, age.total_seconds() / 3600)
    if hours < 6:
        return timedelta(minutes=30)
    if hours < 24:
        return timedelta(hours=1)
    if hours < 72:
        return timedelta(hours=3)
    if hours < 168:
        return timedelta(hours=6)
    if hours < 720:
        return timedelta(hours=24)
    return timedelta(days=7)


def next_metrics_refresh_at(
    published_at: str | datetime,
    observed_at: str | datetime,
    *,
    growth_multiplier: float = 1.0,
) -> datetime:
    published = parse_datetime(published_at)
    observed = parse_datetime(observed_at)
    if published is None or observed is None:
        raise ValueError("published_at and observed_at must be valid timestamps")
    interval = metrics_refresh_interval(max(timedelta(0), observed - published))
    bounded_multiplier = min(1.0, max(0.25, float(growth_multiplier)))
    return observed + interval * bounded_multiplier


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, str):
        normalized = " ".join(value.split())
        return normalized or None
    if isinstance(value, datetime):
        return isoformat(value)
    return value


def _normalize_value(value: Any, *, field: str = "") -> Any:
    if isinstance(value, dict):
        normalized = {
            str(key): _normalize_value(item, field=str(key))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
        return {key: item for key, item in normalized.items() if item is not None}
    if isinstance(value, (list, tuple, set)):
        normalized_items = [_normalize_value(item, field=field) for item in value]
        normalized_items = [item for item in normalized_items if item is not None]
        if field in SET_LIKE_FIELDS:
            return sorted(
                {
                    json.dumps(item, sort_keys=True, ensure_ascii=False): item
                    for item in normalized_items
                }.values(),
                key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False),
            )
        if field in {"thumbnails", "subtitles", "automatic_captions"}:
            return sorted(
                normalized_items,
                key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False),
            )
        return normalized_items
    return _normalize_scalar(value)


def canonical_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    selected = {
        field: _normalize_value(metadata.get(field), field=field)
        for field in DESCRIPTIVE_METADATA_FIELDS
    }
    return {field: value for field, value in selected.items() if value is not None}


def metadata_hash(metadata: dict[str, Any]) -> str:
    payload = json.dumps(
        canonical_metadata(metadata),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def changed_metadata_fields(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> list[str]:
    before = canonical_metadata(previous or {})
    after = canonical_metadata(current)
    return sorted(
        field for field in set(before) | set(after) if before.get(field) != after.get(field)
    )


def retry_delay(
    attempt_count: int,
    *,
    base: timedelta,
    maximum: timedelta,
    jitter_seconds: float = 0,
) -> timedelta:
    exponential = base.total_seconds() * (2 ** max(0, int(attempt_count) - 1))
    bounded = min(maximum.total_seconds(), exponential)
    deterministic_jitter = 0.0
    if jitter_seconds > 0:
        digest = hashlib.sha256(str(attempt_count).encode("ascii")).digest()
        deterministic_jitter = int.from_bytes(digest[:2], "big") / 65535 * jitter_seconds
    return timedelta(seconds=bounded + deterministic_jitter)


def finalize_worker_summary(
    summary: dict[str, Any],
    *,
    elapsed_seconds: float,
    processed: int,
) -> dict[str, Any]:
    """Add consistent throughput fields to a structured worker summary."""
    result = dict(summary)
    elapsed = max(0.0, float(elapsed_seconds))
    result["elapsed_seconds"] = round(elapsed, 3)
    result["avg_seconds_per_video"] = round(elapsed / processed, 3) if processed > 0 else None
    return result
