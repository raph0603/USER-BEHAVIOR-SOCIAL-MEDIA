"""Persistent transcript, comment, and channel enrichment request state."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Iterable

from common.youtube_pipeline import isoformat, parse_datetime


class YouTubeRequestStateMixin:
    """SQLite operations for independent secondary enrichment workers."""

    connection: sqlite3.Connection

    def known_comment_ids(self, video_id: str) -> set[str]:
        return {
            row[0]
            for row in self.connection.execute(
                "SELECT comment_id FROM youtube_known_comments WHERE video_id = ?",
                (video_id,),
            )
        }

    @staticmethod
    def _request_table(family: str) -> str:
        if family not in {"transcript", "comment"}:
            raise ValueError(f"Unsupported YouTube request family: {family}")
        return f"youtube_{family}_state"

    def enqueue_request(
        self,
        family: str,
        *,
        video_id: str,
        correlation_id: str,
        first_seen_at: datetime,
        published_at: str | datetime | None,
        request: dict[str, Any],
    ) -> bool:
        table = self._request_table(family)
        cursor = self.connection.execute(
            f"""
            INSERT OR IGNORE INTO {table} (
              video_id, correlation_id, first_seen_at, published_at,
              request_json, next_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                video_id,
                correlation_id,
                isoformat(first_seen_at),
                isoformat(parse_datetime(published_at)),
                json.dumps(request, ensure_ascii=False, sort_keys=True),
                isoformat(first_seen_at),
            ),
        )
        self.connection.commit()
        return bool(cursor.rowcount)

    def due_requests(
        self, family: str, *, now: datetime, limit: int
    ) -> list[dict[str, Any]]:
        table = self._request_table(family)
        terminal_statuses = (
            "('available', 'permanent_error')"
            if family == "transcript"
            else "('permanent_error')"
        )
        rows = self.connection.execute(
            f"""
            SELECT * FROM {table}
            WHERE status NOT IN {terminal_statuses}
              AND next_attempt_at IS NOT NULL
              AND next_attempt_at <= ?
            ORDER BY next_attempt_at, first_seen_at
            LIMIT ?
            """,
            (isoformat(now), max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]

    def record_request_result(
        self,
        family: str,
        *,
        video_id: str,
        status: str,
        attempted_at: datetime,
        next_attempt_at: datetime | None,
        result: dict[str, Any] | None = None,
        error_class: str | None = None,
        error_message: str | None = None,
    ) -> None:
        table = self._request_table(family)
        self.connection.execute(
            f"""
            UPDATE {table} SET
              status = ?,
              attempt_count = attempt_count + 1,
              last_attempt_at = ?,
              next_attempt_at = ?,
              error_class = ?,
              error_message = ?,
              result_json = ?
            WHERE video_id = ?
            """,
            (
                status,
                isoformat(attempted_at),
                isoformat(next_attempt_at),
                error_class,
                error_message[:1000] if error_message else None,
                json.dumps(result, ensure_ascii=False, sort_keys=True)
                if result is not None
                else None,
                video_id,
            ),
        )
        self.connection.commit()
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

    def enqueue_channel(
        self,
        *,
        channel_id: str,
        first_seen_at: datetime,
        last_video_published_at: str | datetime | None,
    ) -> bool:
        """Create one persistent refresh target per YouTube channel."""
        published_value = isoformat(parse_datetime(last_video_published_at))
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO youtube_channel_state (
              channel_id, first_seen_at, last_video_published_at, next_refresh_at
            ) VALUES (?, ?, ?, ?)
            """,
            (
                channel_id,
                isoformat(first_seen_at),
                published_value,
                isoformat(first_seen_at),
            ),
        )
        if not cursor.rowcount and published_value:
            self.connection.execute(
                """
                UPDATE youtube_channel_state SET
                  last_video_published_at = CASE
                    WHEN last_video_published_at IS NULL
                      OR last_video_published_at < ? THEN ?
                    ELSE last_video_published_at
                  END
                WHERE channel_id = ?
                """,
                (published_value, published_value, channel_id),
            )
        self.connection.commit()
        return bool(cursor.rowcount)

    def due_channels(self, *, now: datetime, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT * FROM youtube_channel_state
            WHERE next_refresh_at <= ?
              AND status != 'permanent_error'
            ORDER BY next_refresh_at, first_seen_at
            LIMIT ?
            """,
            (isoformat(now), max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]

    def channel_state(self, channel_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM youtube_channel_state WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()
        return dict(row) if row else None

    def record_channel_success(
        self,
        *,
        channel_id: str,
        observed_at: datetime,
        subscriber_count: int | None,
        hidden_subscriber_count: bool,
        active_after: datetime,
        active_interval: timedelta,
        inactive_interval: timedelta,
    ) -> None:
        row = self.channel_state(channel_id)
        if row is None:
            raise KeyError(channel_id)
        last_video = parse_datetime(row.get("last_video_published_at"))
        interval = (
            active_interval
            if last_video is not None and last_video >= active_after
            else inactive_interval
        )
        self.connection.execute(
            """
            UPDATE youtube_channel_state SET
              last_refresh_at = ?,
              next_refresh_at = ?,
              refresh_count = refresh_count + 1,
              subscriber_count = ?,
              hidden_subscriber_count = ?,
              status = 'success',
              attempt_count = attempt_count + 1,
              error_class = NULL,
              error_message = NULL
            WHERE channel_id = ?
            """,
            (
                isoformat(observed_at),
                isoformat(observed_at + interval),
                subscriber_count,
                int(hidden_subscriber_count),
                channel_id,
            ),
        )
        self.connection.commit()

    def record_channel_failure(
        self,
        *,
        channel_id: str,
        attempted_at: datetime,
        next_attempt_at: datetime | None,
        error_class: str,
        error_message: str,
        permanent: bool = False,
    ) -> None:
        self.connection.execute(
            """
            UPDATE youtube_channel_state SET
              next_refresh_at = COALESCE(?, next_refresh_at),
              status = ?,
              attempt_count = attempt_count + 1,
              error_class = ?,
              error_message = ?
            WHERE channel_id = ?
            """,
            (
                isoformat(next_attempt_at),
                "permanent_error" if permanent else "retryable_error",
                error_class,
                error_message[:1000],
                channel_id,
            ),
        )
        self.connection.commit()
