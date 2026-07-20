"""Persistent discovery and descriptive metadata state for YouTube."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

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
            CREATE TABLE IF NOT EXISTS youtube_metrics_state (
              observation_id TEXT PRIMARY KEY,
              video_id TEXT NOT NULL,
              observed_at TEXT NOT NULL,
              status TEXT NOT NULL,
              payload_fingerprint TEXT NOT NULL,
              result_json TEXT NOT NULL,
              updated_at TEXT NOT NULL
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
        self.connection.commit()

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
