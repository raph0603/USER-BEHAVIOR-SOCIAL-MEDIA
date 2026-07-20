"""Persistent discovery and descriptive metadata state for YouTube."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from common.transcripts import (
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
              last_error TEXT
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
        }
        current_usage_columns = {
            row[1] for row in self.connection.execute("PRAGMA table_info(youtube_api_usage)")
        }
        for column, data_type in usage_columns.items():
            if column not in current_usage_columns:
                self.connection.execute(
                    f"ALTER TABLE youtube_api_usage ADD COLUMN {column} {data_type}"
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

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "YouTubeStateStore":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
