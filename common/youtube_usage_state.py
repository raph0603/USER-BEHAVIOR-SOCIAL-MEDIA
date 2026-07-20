"""Persistent YouTube API usage and circuit-breaker state."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Callable

from common.youtube_pipeline import ensure_utc, isoformat, parse_datetime


class YouTubeUsageStateMixin:
    """SQLite operations shared by quota budgets and cooldown circuits."""

    connection: sqlite3.Connection
    _commit: Callable[[], None]

    def open_breaker(self, name: str, *, now: datetime, cooldown: timedelta, reason: str) -> None:
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
        self._commit()

    def record_api_usage(
        self,
        *,
        endpoint: str,
        request_count: int,
        resource_count: int,
        success_count: int,
        error_count: int,
        quota_bucket: str,
        observed_at: datetime,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO youtube_api_usage (
              usage_date, endpoint, request_count, resource_count,
              success_count, error_count, quota_bucket, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ensure_utc(observed_at).date().isoformat(),
                endpoint,
                max(0, int(request_count)),
                max(0, int(resource_count)),
                max(0, int(success_count)),
                max(0, int(error_count)),
                quota_bucket,
                isoformat(observed_at),
            ),
        )
        self._commit()

    def api_requests_today(self, endpoint: str, now: datetime) -> int:
        row = self.connection.execute(
            """
            SELECT COALESCE(SUM(request_count), 0)
            FROM youtube_api_usage
            WHERE usage_date = ? AND endpoint = ?
            """,
            (ensure_utc(now).date().isoformat(), endpoint),
        ).fetchone()
        return int(row[0] or 0)

    def breaker_open(self, name: str, now: datetime) -> bool:
        row = self.connection.execute(
            "SELECT cooldown_until FROM youtube_circuit_breakers WHERE breaker_name = ?",
            (name,),
        ).fetchone()
        return bool(row and (parse_datetime(row[0]) or now) > now)
