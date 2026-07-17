"""Shared state, scheduling, and normalization for the YouTube pipeline."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


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
    SearchQuery.create(
        "electric vehicle review|EV charging battery range", "en"
    ),
    SearchQuery.create(
        "đánh giá xe điện|xe điện VinFast trạm sạc", "vi"
    ),
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
                specs.append(
                    SearchQuery.create(item.get("query", ""), item.get("language", ""))
                )
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
        normalized_items = [
            _normalize_value(item, field=field) for item in value
        ]
        normalized_items = [item for item in normalized_items if item is not None]
        if field in SET_LIKE_FIELDS:
            return sorted(
                {json.dumps(item, sort_keys=True, ensure_ascii=False): item for item in normalized_items}.values(),
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
        field
        for field in set(before) | set(after)
        if before.get(field) != after.get(field)
    )


class YouTubeStateStore:
    """SQLite state shared by discovery and bounded enrichment workers."""

    def __init__(self, path: str | Path) -> None:
        state_path = Path(path)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(state_path, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA busy_timeout = 30000")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self._migrate()

    def _migrate(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS youtube_search_watermarks (
              query_id TEXT PRIMARY KEY,
              query TEXT NOT NULL,
              language TEXT NOT NULL,
              last_successful_search_at TEXT,
              last_published_at_seen TEXT,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS youtube_discovered_videos (
              video_id TEXT PRIMARY KEY,
              query_id TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              published_at TEXT,
              correlation_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS youtube_metadata_state (
              video_id TEXT PRIMARY KEY,
              correlation_id TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              published_at TEXT,
              last_metadata_refresh_at TEXT,
              next_metadata_refresh_at TEXT,
              metadata_refresh_count INTEGER NOT NULL DEFAULT 0,
              current_metadata_hash TEXT,
              current_metadata_json TEXT,
              metadata_status TEXT NOT NULL DEFAULT 'pending',
              attempt_count INTEGER NOT NULL DEFAULT 0,
              last_attempt_at TEXT,
              next_attempt_at TEXT,
              error_class TEXT,
              error_message TEXT
            );
            CREATE TABLE IF NOT EXISTS youtube_known_comments (
              video_id TEXT NOT NULL,
              comment_id TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              PRIMARY KEY (video_id, comment_id)
            );
            CREATE TABLE IF NOT EXISTS youtube_circuit_breakers (
              breaker_name TEXT PRIMARY KEY,
              opened_at TEXT NOT NULL,
              cooldown_until TEXT NOT NULL,
              reason TEXT NOT NULL
            );
            """
        )
        self.connection.commit()

    def watermark(self, query_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM youtube_search_watermarks WHERE query_id = ?",
            (query_id,),
        ).fetchone()
        return dict(row) if row else None

    def record_search_success(
        self,
        spec: SearchQuery,
        *,
        searched_at: datetime,
        last_published_at_seen: str | datetime | None,
    ) -> None:
        now_value = isoformat(searched_at)
        published_value = isoformat(parse_datetime(last_published_at_seen))
        self.connection.execute(
            """
            INSERT INTO youtube_search_watermarks (
              query_id, query, language, last_successful_search_at,
              last_published_at_seen, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(query_id) DO UPDATE SET
              query = excluded.query,
              language = excluded.language,
              last_successful_search_at = excluded.last_successful_search_at,
              last_published_at_seen = COALESCE(
                excluded.last_published_at_seen,
                youtube_search_watermarks.last_published_at_seen
              ),
              updated_at = excluded.updated_at
            """,
            (
                spec.query_id,
                spec.query,
                spec.language,
                now_value,
                published_value,
                now_value,
            ),
        )
        self.connection.commit()

    def record_discovery(
        self,
        *,
        video_id: str,
        query_id: str,
        first_seen_at: datetime,
        published_at: str | datetime | None,
        correlation_id: str,
    ) -> bool:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO youtube_discovered_videos (
              video_id, query_id, first_seen_at, published_at, correlation_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                video_id,
                query_id,
                isoformat(first_seen_at),
                isoformat(parse_datetime(published_at)),
                correlation_id,
            ),
        )
        if cursor.rowcount:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO youtube_metadata_state (
                  video_id, correlation_id, first_seen_at, published_at,
                  next_metadata_refresh_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    video_id,
                    correlation_id,
                    isoformat(first_seen_at),
                    isoformat(parse_datetime(published_at)),
                    isoformat(first_seen_at),
                ),
            )
            self.connection.commit()
            return True
        return False

    def is_discovered(self, video_id: str) -> bool:
        return self.connection.execute(
            "SELECT 1 FROM youtube_discovered_videos WHERE video_id = ?",
            (video_id,),
        ).fetchone() is not None

    def due_metadata(self, now: datetime, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT * FROM youtube_metadata_state
            WHERE next_metadata_refresh_at IS NOT NULL
              AND next_metadata_refresh_at <= ?
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY next_metadata_refresh_at, first_seen_at
            LIMIT ?
            """,
            (isoformat(now), isoformat(now), max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]

    def record_metadata_success(
        self,
        *,
        video_id: str,
        observed_at: datetime,
        metadata: dict[str, Any],
        offsets: Sequence[timedelta],
    ) -> tuple[str, str | None, list[str]]:
        row = self.connection.execute(
            "SELECT * FROM youtube_metadata_state WHERE video_id = ?",
            (video_id,),
        ).fetchone()
        if row is None:
            raise KeyError(video_id)
        previous_hash = row["current_metadata_hash"]
        previous_metadata = json.loads(row["current_metadata_json"] or "{}")
        current_hash = metadata_hash(metadata)
        changed_fields = changed_metadata_fields(previous_metadata, metadata)
        refresh_count = int(row["metadata_refresh_count"] or 0) + 1
        next_refresh = next_metadata_refresh_at(
            row["first_seen_at"], refresh_count - 1, offsets
        )
        self.connection.execute(
            """
            UPDATE youtube_metadata_state SET
              last_metadata_refresh_at = ?,
              next_metadata_refresh_at = ?,
              metadata_refresh_count = ?,
              current_metadata_hash = ?,
              current_metadata_json = ?,
              metadata_status = 'success',
              attempt_count = attempt_count + 1,
              last_attempt_at = ?,
              next_attempt_at = NULL,
              error_class = NULL,
              error_message = NULL
            WHERE video_id = ?
            """,
            (
                isoformat(observed_at),
                isoformat(next_refresh),
                refresh_count,
                current_hash,
                json.dumps(
                    canonical_metadata(metadata),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                isoformat(observed_at),
                video_id,
            ),
        )
        self.connection.commit()
        return current_hash, previous_hash, changed_fields

    def record_metadata_failure(
        self,
        *,
        video_id: str,
        attempted_at: datetime,
        next_attempt_at: datetime | None,
        error: BaseException,
        permanent: bool = False,
    ) -> None:
        self.connection.execute(
            """
            UPDATE youtube_metadata_state SET
              metadata_status = ?,
              attempt_count = attempt_count + 1,
              last_attempt_at = ?,
              next_attempt_at = ?,
              next_metadata_refresh_at = CASE
                WHEN ? THEN NULL ELSE next_metadata_refresh_at END,
              error_class = ?,
              error_message = ?
            WHERE video_id = ?
            """,
            (
                "permanent_error" if permanent else "retryable_error",
                isoformat(attempted_at),
                isoformat(next_attempt_at),
                int(permanent),
                type(error).__name__,
                str(error)[:1000],
                video_id,
            ),
        )
        self.connection.commit()

    def known_comment_ids(self, video_id: str) -> set[str]:
        return {
            row[0]
            for row in self.connection.execute(
                "SELECT comment_id FROM youtube_known_comments WHERE video_id = ?",
                (video_id,),
            )
        }

    def record_comment_ids(
        self, video_id: str, comment_ids: Iterable[str], observed_at: datetime
    ) -> None:
        self.connection.executemany(
            """
            INSERT OR IGNORE INTO youtube_known_comments (
              video_id, comment_id, first_seen_at
            ) VALUES (?, ?, ?)
            """,
            [
                (video_id, comment_id, isoformat(observed_at))
                for comment_id in comment_ids
                if comment_id
            ],
        )
        self.connection.commit()

    def open_breaker(
        self, name: str, *, now: datetime, cooldown: timedelta, reason: str
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO youtube_circuit_breakers (
              breaker_name, opened_at, cooldown_until, reason
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(breaker_name) DO UPDATE SET
              opened_at = excluded.opened_at,
              cooldown_until = excluded.cooldown_until,
              reason = excluded.reason
            """,
            (name, isoformat(now), isoformat(now + cooldown), reason[:1000]),
        )
        self.connection.commit()

    def breaker_open(self, name: str, now: datetime) -> bool:
        row = self.connection.execute(
            "SELECT cooldown_until FROM youtube_circuit_breakers WHERE breaker_name = ?",
            (name,),
        ).fetchone()
        return bool(row and (parse_datetime(row[0]) or now) > now)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "YouTubeStateStore":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


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
