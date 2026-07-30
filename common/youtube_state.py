"""Persistent discovery and descriptive metadata state for YouTube."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from common.transcripts import (
    TranscriptPayload,
    legacy_transcript_status,
    transcript_content_version,
    transcript_lifecycle_status,
)
from common.youtube_pipeline import (
    SearchQuery,
    canonical_metadata,
    changed_metadata_fields,
    isoformat,
    metadata_hash,
    next_metadata_refresh_at,
    parse_datetime,
)
from common.youtube_request_state import YouTubeRequestStateMixin
from common.youtube_outbox import YouTubeOutboxMixin, canonical_event_json
from common.youtube_usage_state import YouTubeUsageStateMixin


_GEMINI_QUOTA_TIME_ZONE = ZoneInfo("America/Los_Angeles")


def _gemini_quota_window(now: datetime) -> tuple[str, str]:
    current = parse_datetime(now)
    if current is None:
        current = now.replace(tzinfo=timezone.utc)
    local_now = current.astimezone(_GEMINI_QUOTA_TIME_ZONE)
    local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    local_end = local_start + timedelta(days=1)
    return (
        isoformat(local_start.astimezone(timezone.utc)),
        isoformat(local_end.astimezone(timezone.utc)),
    )


class YouTubeStateStore(
    YouTubeRequestStateMixin,
    YouTubeUsageStateMixin,
    YouTubeOutboxMixin,
):
    """SQLite state shared by discovery and bounded enrichment workers."""

    def __init__(self, path: str | Path) -> None:
        state_path = Path(path)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(state_path, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA busy_timeout = 30000")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self._transaction_depth = 0
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
            CREATE TABLE IF NOT EXISTS youtube_transcript_state (
              video_id TEXT PRIMARY KEY,
              correlation_id TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              published_at TEXT,
              request_json TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending',
              attempt_count INTEGER NOT NULL DEFAULT 0,
              last_attempt_at TEXT,
              next_attempt_at TEXT,
              error_class TEXT,
              error_message TEXT,
              result_json TEXT
            );
            CREATE TABLE IF NOT EXISTS youtube_comment_state (
              video_id TEXT PRIMARY KEY,
              correlation_id TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              published_at TEXT,
              request_json TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending',
              attempt_count INTEGER NOT NULL DEFAULT 0,
              last_attempt_at TEXT,
              next_attempt_at TEXT,
              error_class TEXT,
              error_message TEXT,
              result_json TEXT
            );
            CREATE TABLE IF NOT EXISTS youtube_channel_state (
              channel_id TEXT PRIMARY KEY,
              first_seen_at TEXT NOT NULL,
              last_video_published_at TEXT,
              last_refresh_at TEXT,
              next_refresh_at TEXT NOT NULL,
              refresh_count INTEGER NOT NULL DEFAULT 0,
              subscriber_count INTEGER,
              hidden_subscriber_count INTEGER NOT NULL DEFAULT 0,
              status TEXT NOT NULL DEFAULT 'pending',
              attempt_count INTEGER NOT NULL DEFAULT 0,
              error_class TEXT,
              error_message TEXT
            );
            CREATE TABLE IF NOT EXISTS youtube_circuit_breakers (
              breaker_name TEXT PRIMARY KEY,
              opened_at TEXT NOT NULL,
              cooldown_until TEXT NOT NULL,
              reason TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS youtube_api_usage (
              usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
              usage_date TEXT NOT NULL,
              endpoint TEXT NOT NULL,
              request_count INTEGER NOT NULL,
              resource_count INTEGER NOT NULL,
              success_count INTEGER NOT NULL,
              error_count INTEGER NOT NULL,
              quota_bucket TEXT NOT NULL,
              observed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS youtube_worker_health (
              health_id INTEGER PRIMARY KEY AUTOINCREMENT,
              observed_at TEXT NOT NULL,
              producer_run_id TEXT NOT NULL,
              worker_name TEXT NOT NULL,
              status TEXT NOT NULL,
              processed_count INTEGER NOT NULL DEFAULT 0,
              success_count INTEGER NOT NULL DEFAULT 0,
              error_count INTEGER NOT NULL DEFAULT 0,
              retry_count INTEGER NOT NULL DEFAULT 0,
              cache_hit_count INTEGER NOT NULL DEFAULT 0,
              cache_miss_count INTEGER NOT NULL DEFAULT 0,
              latency_ms REAL,
              queue_depth INTEGER,
              oldest_queue_age_seconds REAL,
              circuit_open INTEGER NOT NULL DEFAULT 0,
              details_json TEXT
            );
            CREATE TABLE IF NOT EXISTS youtube_metrics_state (
              observation_id TEXT PRIMARY KEY,
              video_id TEXT NOT NULL,
              observed_at TEXT NOT NULL,
              status TEXT NOT NULL,
              payload_fingerprint TEXT NOT NULL,
              result_json TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS youtube_transcript_lifecycle (
              video_id TEXT NOT NULL,
              requested_language_code TEXT NOT NULL,
              correlation_id TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              published_at TEXT,
              request_json TEXT NOT NULL,
              transcript_lifecycle_status TEXT NOT NULL DEFAULT 'pending',
              transcript_status TEXT NOT NULL DEFAULT 'pending',
              migrated_legacy_status TEXT,
              requested_language TEXT,
              obtained_language TEXT,
              obtained_language_code TEXT,
              available_languages_json TEXT,
              generation_type TEXT,
              is_generated INTEGER,
              is_translated INTEGER,
              provider TEXT,
              selection_strategy TEXT,
              attempt_count INTEGER NOT NULL DEFAULT 0,
              last_attempt_at TEXT,
              next_attempt_at TEXT,
              collected_at TEXT,
              error_code TEXT,
              error_message TEXT,
              recovered_at TEXT,
              content_version TEXT,
              result_json TEXT,
              PRIMARY KEY (video_id, requested_language_code)
            );
            CREATE INDEX IF NOT EXISTS idx_youtube_transcript_lifecycle_due
              ON youtube_transcript_lifecycle (
                transcript_lifecycle_status, next_attempt_at, first_seen_at
              );
            CREATE TABLE IF NOT EXISTS youtube_transcript_provider_attempts (
              attempt_id TEXT PRIMARY KEY,
              video_id TEXT NOT NULL,
              requested_language_code TEXT NOT NULL,
              provider TEXT NOT NULL,
              model TEXT,
              attempt_count INTEGER NOT NULL,
              attempted_at TEXT NOT NULL,
              latency_ms REAL,
              status TEXT NOT NULL,
              error_code TEXT,
              fallback_reason TEXT,
              result_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_youtube_transcript_provider_attempts
              ON youtube_transcript_provider_attempts (
                provider, attempted_at, error_code, fallback_reason
              );
            CREATE TABLE IF NOT EXISTS gemini_transcript_cache (
              cache_key TEXT PRIMARY KEY,
              video_id TEXT NOT NULL,
              requested_language_code TEXT NOT NULL,
              model TEXT NOT NULL,
              prompt_version TEXT NOT NULL,
              source_content_version TEXT NOT NULL,
              content_version TEXT NOT NULL,
              video_minutes REAL NOT NULL DEFAULT 0,
              payload_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              last_accessed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS youtube_worker_outbox (
              outbox_id TEXT PRIMARY KEY,
              worker_name TEXT NOT NULL,
              aggregate_id TEXT NOT NULL,
              topic TEXT NOT NULL,
              message_key TEXT NOT NULL,
              event_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              available_at TEXT NOT NULL,
              delivery_attempts INTEGER NOT NULL DEFAULT 0,
              last_attempt_at TEXT,
              delivered_at TEXT,
              last_error TEXT,
              status TEXT NOT NULL DEFAULT 'pending',
              failure_reason TEXT,
              failed_at TEXT,
              payload_size_bytes INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_youtube_worker_outbox_pending
              ON youtube_worker_outbox (delivered_at, available_at, created_at);
            """
        )
        usage_columns = {
            "provider": "TEXT NOT NULL DEFAULT 'youtube'",
            "operation": "TEXT",
            "quota_units": "INTEGER NOT NULL DEFAULT 0",
            "quota_cost_per_request": "INTEGER NOT NULL DEFAULT 0",
            "daily_budget_units": "INTEGER",
            "reserved_units": "INTEGER",
            "remaining_units": "INTEGER",
            "reserve_remaining_units": "INTEGER",
            "priority": "TEXT",
            "cache_hit_count": "INTEGER NOT NULL DEFAULT 0",
            "cache_miss_count": "INTEGER NOT NULL DEFAULT 0",
            "retry_count": "INTEGER NOT NULL DEFAULT 0",
            "latency_ms": "REAL",
            "queue_depth": "INTEGER",
            "oldest_queue_age_seconds": "REAL",
            "circuit_open": "INTEGER NOT NULL DEFAULT 0",
            "status": "TEXT",
            "error_code": "TEXT",
            "producer_run_id": "TEXT",
            "video_minutes": "REAL NOT NULL DEFAULT 0",
            "daily_video_minutes_budget": "REAL",
            "remaining_video_minutes": "REAL",
        }
        current_usage_columns = {
            row[1] for row in self.connection.execute("PRAGMA table_info(youtube_api_usage)")
        }
        for column, data_type in usage_columns.items():
            if column not in current_usage_columns:
                self.connection.execute(
                    f"ALTER TABLE youtube_api_usage ADD COLUMN {column} {data_type}"
                )
        lifecycle_columns = {
            "model": "TEXT",
            "fallback_reason": "TEXT",
            "prompt_version": "TEXT",
            "generated_by_model": "INTEGER",
            "source_content_version": "TEXT",
            "primary_attempt_count": "INTEGER NOT NULL DEFAULT 0",
            "fallback_attempt_count": "INTEGER NOT NULL DEFAULT 0",
            "primary_last_attempt_at": "TEXT",
            "fallback_last_attempt_at": "TEXT",
            "primary_result_json": "TEXT",
            "fallback_result_json": "TEXT",
        }
        current_lifecycle_columns = {
            row[1]
            for row in self.connection.execute("PRAGMA table_info(youtube_transcript_lifecycle)")
        }
        for column, data_type in lifecycle_columns.items():
            if column not in current_lifecycle_columns:
                self.connection.execute(
                    f"ALTER TABLE youtube_transcript_lifecycle ADD COLUMN {column} {data_type}"
                )
        outbox_columns = {
            "status": "TEXT NOT NULL DEFAULT 'pending'",
            "failure_reason": "TEXT",
            "failed_at": "TEXT",
            "payload_size_bytes": "INTEGER",
        }
        current_outbox_columns = {
            row[1] for row in self.connection.execute("PRAGMA table_info(youtube_worker_outbox)")
        }
        for column, data_type in outbox_columns.items():
            if column not in current_outbox_columns:
                self.connection.execute(
                    f"ALTER TABLE youtube_worker_outbox ADD COLUMN {column} {data_type}"
                )
        self.connection.execute(
            """
            UPDATE youtube_worker_outbox
            SET status = CASE
              WHEN delivered_at IS NOT NULL THEN 'delivered'
              ELSE 'pending'
            END
            WHERE status IS NULL
               OR status = ''
               OR (delivered_at IS NOT NULL AND status = 'pending')
            """
        )
        self.connection.execute(
            """
            UPDATE youtube_api_usage
            SET operation = COALESCE(operation, endpoint),
                provider = COALESCE(provider, 'youtube'),
                quota_cost_per_request = CASE
                  WHEN quota_cost_per_request > 0 THEN quota_cost_per_request
                  WHEN endpoint = 'search.list' THEN 100
                  WHEN endpoint IN (
                    'videos.list', 'channels.list', 'commentThreads.list'
                  ) THEN 1
                  ELSE 0
                END,
                quota_units = CASE
                  WHEN quota_units > 0 THEN quota_units
                  WHEN endpoint = 'search.list' THEN request_count * 100
                  WHEN endpoint IN (
                    'videos.list', 'channels.list', 'commentThreads.list'
                  ) THEN request_count
                  ELSE 0
                END,
                producer_run_id = COALESCE(producer_run_id, 'legacy'),
                status = COALESCE(
                  status,
                  CASE WHEN error_count > 0 THEN 'error' ELSE 'success' END
                )
            WHERE operation IS NULL
               OR producer_run_id IS NULL
               OR status IS NULL
            """
        )
        self._backfill_transcript_lifecycle_state()
        self.connection.commit()

    def _backfill_transcript_lifecycle_state(self) -> None:
        """Copy legacy per-video transcript state into the language-aware table."""

        rows = self.connection.execute("SELECT * FROM youtube_transcript_state").fetchall()
        for source_row in rows:
            row = dict(source_row)
            try:
                request = json.loads(row.get("request_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                request = {}
            try:
                result = json.loads(row.get("result_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                result = {}
            if not isinstance(result, dict):
                result = {}
            payload = result.get("payload")
            payload = payload if isinstance(payload, dict) else {}
            requested_language, requested_code = self._requested_transcript_language(request)
            text = str(payload.get("text") or "").strip()
            lifecycle = transcript_lifecycle_status(
                row.get("status"),
                error_code=row.get("error_class") or result.get("error_code"),
                has_text=bool(text),
                attempt_count=int(row.get("attempt_count") or 0),
            )
            is_generated = payload.get("is_generated")
            generation_type = (
                None if is_generated is None else ("automatic" if bool(is_generated) else "manual")
            )
            available_languages = payload.get("available_languages")
            segments = payload.get("segments")
            if not isinstance(segments, (list, tuple)):
                segments = ()
            content_version = payload.get("content_version")
            if not content_version and text:
                content_version = transcript_content_version(
                    video_id=row["video_id"],
                    language_code=payload.get("language_code"),
                    text=text,
                    segments=segments,
                )
            self.connection.execute(
                """
                INSERT OR IGNORE INTO youtube_transcript_lifecycle (
                  video_id, requested_language_code, correlation_id,
                  first_seen_at, published_at, request_json,
                  transcript_lifecycle_status, transcript_status,
                  migrated_legacy_status, requested_language,
                  obtained_language, obtained_language_code,
                  available_languages_json, generation_type,
                  is_generated, is_translated, provider,
                  selection_strategy, attempt_count, last_attempt_at,
                  next_attempt_at, collected_at, error_code, error_message,
                  content_version, result_json
                ) VALUES (
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    row["video_id"],
                    requested_code,
                    row["correlation_id"],
                    row["first_seen_at"],
                    row.get("published_at"),
                    row["request_json"],
                    lifecycle,
                    legacy_transcript_status(lifecycle),
                    row.get("status"),
                    requested_language,
                    payload.get("language"),
                    payload.get("language_code"),
                    (
                        json.dumps(
                            available_languages,
                            ensure_ascii=False,
                            sort_keys=True,
                            default=str,
                        )
                        if available_languages is not None
                        else None
                    ),
                    generation_type,
                    None if is_generated is None else int(bool(is_generated)),
                    (
                        None
                        if payload.get("is_translated") is None
                        else int(bool(payload.get("is_translated")))
                    ),
                    payload.get("source"),
                    payload.get("selection_strategy"),
                    int(row.get("attempt_count") or 0),
                    row.get("last_attempt_at"),
                    row.get("next_attempt_at"),
                    payload.get("collected_at"),
                    row.get("error_class") or result.get("error_code"),
                    row.get("error_message") or result.get("error_message"),
                    content_version,
                    row.get("result_json"),
                ),
            )

    def _commit(self) -> None:
        if self._transaction_depth == 0:
            self.connection.commit()

    @contextmanager
    def transaction(self) -> Iterator["YouTubeStateStore"]:
        root = self._transaction_depth == 0
        if root:
            self.connection.execute("BEGIN IMMEDIATE")
        self._transaction_depth += 1
        try:
            yield self
        except BaseException:
            self._transaction_depth -= 1
            if root:
                self.connection.rollback()
            raise
        else:
            self._transaction_depth -= 1
            if root:
                self.connection.commit()

    def record_metrics_observation(self, update: dict[str, Any]) -> None:
        observation_id = str(update["observation_id"])
        result_json = canonical_event_json(update)
        self.connection.execute(
            """
            INSERT OR IGNORE INTO youtube_metrics_state (
              observation_id, video_id, observed_at, status,
              payload_fingerprint, result_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation_id,
                str(update["platform_event_id"]),
                str(update["metadata_refreshed_at"]),
                str(update["metrics_refresh_status"]),
                str(update["payload_fingerprint"]),
                result_json,
                str(update["metadata_refreshed_at"]),
            ),
        )
        row = self.connection.execute(
            """
            SELECT payload_fingerprint, result_json
            FROM youtube_metrics_state WHERE observation_id = ?
            """,
            (observation_id,),
        ).fetchone()
        if (
            row is None
            or row["payload_fingerprint"] != update["payload_fingerprint"]
            or row["result_json"] != result_json
        ):
            raise RuntimeError(f"Metrics observation collision: {observation_id}")
        self._commit()

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
        self._commit()

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
            self._commit()
            return True
        self._commit()
        return False

    def is_discovered(self, video_id: str) -> bool:
        return (
            self.connection.execute(
                "SELECT 1 FROM youtube_discovered_videos WHERE video_id = ?",
                (video_id,),
            ).fetchone()
            is not None
        )

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

    def metadata_state(self, video_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM youtube_metadata_state WHERE video_id = ?",
            (video_id,),
        ).fetchone()
        return dict(row) if row else None

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
        next_refresh = next_metadata_refresh_at(row["first_seen_at"], refresh_count - 1, offsets)
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
        self._commit()
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
        self._commit()

    def cached_gemini_transcript(
        self,
        cache_key: str,
        *,
        accessed_at: datetime | None = None,
    ) -> TranscriptPayload | None:
        """Return one validated cached success without contacting any provider."""

        row = self.connection.execute(
            "SELECT payload_json FROM gemini_transcript_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"])
            if not isinstance(payload, dict):
                return None
            collected_at = parse_datetime(payload.get("collected_at"))
            if collected_at is None:
                return None
            transcript = TranscriptPayload(
                video_id=payload["video_id"],
                language=payload.get("language"),
                language_code=payload.get("language_code"),
                is_generated=payload.get("is_generated"),
                is_translated=bool(payload.get("is_translated")),
                source_language=payload.get("source_language"),
                source_language_code=payload.get("source_language_code"),
                source=payload["source"],
                selection_strategy=payload["selection_strategy"],
                text=payload["text"],
                segments=tuple(payload.get("segments") or ()),
                segment_count=int(payload.get("segment_count") or 0),
                word_count=int(payload.get("word_count") or 0),
                available_languages=tuple(payload.get("available_languages") or ()),
                covered_duration_seconds=float(payload.get("covered_duration_seconds") or 0),
                collected_at=collected_at,
                requested_language=payload.get("requested_language"),
                requested_language_code=payload.get("requested_language_code"),
                content_version=payload.get("content_version") or "",
                model=payload.get("model"),
                fallback_reason=payload.get("fallback_reason"),
                prompt_version=payload.get("prompt_version"),
                generated_by_model=bool(payload.get("generated_by_model")),
                warnings=tuple(payload.get("warnings") or ()),
                generation_type_override=payload.get("generation_type_override"),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None
        self.connection.execute(
            "UPDATE gemini_transcript_cache SET last_accessed_at = ? WHERE cache_key = ?",
            (isoformat(accessed_at or collected_at), cache_key),
        )
        self._commit()
        return transcript

    def cache_gemini_transcript(
        self,
        cache_key: str,
        payload: TranscriptPayload,
        video_minutes: float,
        *,
        source_content_version: str | None,
    ) -> None:
        """Persist one idempotent Gemini success for its deterministic identity."""

        created_at = isoformat(payload.collected_at)
        self.connection.execute(
            """
            INSERT INTO gemini_transcript_cache (
              cache_key, video_id, requested_language_code, model,
              prompt_version, source_content_version, content_version,
              video_minutes, payload_json, created_at, last_accessed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
              last_accessed_at = excluded.last_accessed_at
            """,
            (
                cache_key,
                payload.video_id,
                payload.requested_language_code or "",
                payload.model or "",
                payload.prompt_version or "",
                source_content_version or "unknown",
                payload.content_version,
                max(0.0, float(video_minutes)),
                json.dumps(payload.to_dict(), ensure_ascii=False, sort_keys=True),
                created_at,
                created_at,
            ),
        )
        self._commit()

    def gemini_video_minutes_today(self, now: datetime) -> float:
        window_start, window_end = _gemini_quota_window(now)
        row = self.connection.execute(
            """
            SELECT COALESCE(SUM(video_minutes), 0)
            FROM youtube_api_usage
            WHERE provider = 'gemini'
              AND observed_at >= ?
              AND observed_at < ?
            """,
            (window_start, window_end),
        ).fetchone()
        return float(row[0] or 0.0)

    def gemini_requests_current_quota_day(
        self,
        now: datetime,
        model: str | None = None,
    ) -> int:
        window_start, window_end = _gemini_quota_window(now)
        row = self.connection.execute(
            """
            SELECT COALESCE(SUM(attempt_count), 0)
            FROM youtube_transcript_provider_attempts
            WHERE provider = 'gemini'
              AND attempted_at >= ?
              AND attempted_at < ?
              AND (? IS NULL OR model = ?)
            """,
            (window_start, window_end, model, model),
        ).fetchone()
        return int(row[0] or 0)

    def record_transcript_provider_attempt(
        self,
        *,
        attempt_id: str,
        video_id: str,
        requested_language_code: str,
        provider: str,
        model: str | None,
        attempt_count: int,
        attempted_at: datetime,
        latency_ms: float | None,
        status: str,
        error_code: str | None,
        fallback_reason: str | None,
        result: dict[str, Any],
    ) -> None:
        self.connection.execute(
            """
            INSERT OR IGNORE INTO youtube_transcript_provider_attempts (
              attempt_id, video_id, requested_language_code, provider, model,
              attempt_count, attempted_at, latency_ms, status, error_code,
              fallback_reason, result_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                attempt_id,
                video_id,
                requested_language_code,
                provider,
                model,
                max(0, int(attempt_count)),
                isoformat(attempted_at),
                latency_ms,
                status,
                error_code,
                fallback_reason,
                json.dumps(result, ensure_ascii=False, sort_keys=True),
            ),
        )
        self._commit()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "YouTubeStateStore":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
