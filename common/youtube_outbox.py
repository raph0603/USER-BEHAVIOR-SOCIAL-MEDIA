"""Transactional SQLite outbox shared by independent YouTube workers."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from common.youtube_pipeline import isoformat


UTC = timezone.utc


def canonical_event_json(event: dict[str, Any]) -> str:
    return json.dumps(
        event,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def deterministic_outbox_id(topic: str, event: dict[str, Any]) -> str:
    payload = f"{topic}\x1f{canonical_event_json(event)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class YouTubeOutboxMixin:
    """Durable publish intents committed with worker outcome state."""

    connection: sqlite3.Connection
    _commit: Callable[[], None]

    def enqueue_outbox(
        self,
        *,
        worker_name: str,
        aggregate_id: str,
        topic: str,
        event: dict[str, Any],
        created_at: datetime,
    ) -> str:
        event_json = canonical_event_json(event)
        outbox_id = deterministic_outbox_id(topic, event)
        message_key = str(event.get("video_id") or event.get("platform_event_id") or aggregate_id)
        self.connection.execute(
            """
            INSERT OR IGNORE INTO youtube_worker_outbox (
              outbox_id, worker_name, aggregate_id, topic, message_key,
              event_json, created_at, available_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outbox_id,
                worker_name,
                aggregate_id,
                topic,
                message_key,
                event_json,
                isoformat(created_at),
                isoformat(created_at),
            ),
        )
        row = self.connection.execute(
            "SELECT topic, event_json FROM youtube_worker_outbox WHERE outbox_id = ?",
            (outbox_id,),
        ).fetchone()
        if row is None or row["topic"] != topic or row["event_json"] != event_json:
            raise RuntimeError(f"Outbox identity collision: {outbox_id}")
        self._commit()
        return outbox_id

    def pending_outbox(
        self,
        *,
        now: datetime,
        limit: int,
        include_deferred: bool = False,
    ) -> list[dict[str, Any]]:
        availability = "" if include_deferred else "AND available_at <= ?"
        parameters: tuple[Any, ...] = (
            (max(1, int(limit)),) if include_deferred else (isoformat(now), max(1, int(limit)))
        )
        rows = self.connection.execute(
            f"""
            SELECT * FROM youtube_worker_outbox
            WHERE delivered_at IS NULL
              {availability}
            ORDER BY created_at, outbox_id
            LIMIT ?
            """,
            parameters,
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_outbox_delivered(self, outbox_id: str, *, delivered_at: datetime) -> None:
        self.connection.execute(
            """
            UPDATE youtube_worker_outbox SET
              delivered_at = ?,
              last_attempt_at = ?,
              last_error = NULL
            WHERE outbox_id = ? AND delivered_at IS NULL
            """,
            (isoformat(delivered_at), isoformat(delivered_at), outbox_id),
        )
        self._commit()

    def record_outbox_failure(
        self,
        outbox_id: str,
        *,
        attempted_at: datetime,
        error: BaseException,
    ) -> None:
        row = self.connection.execute(
            "SELECT delivery_attempts FROM youtube_worker_outbox WHERE outbox_id = ?",
            (outbox_id,),
        ).fetchone()
        attempt = int(row[0] or 0) + 1 if row else 1
        delay = timedelta(seconds=min(300, 2 ** min(attempt, 8)))
        self.connection.execute(
            """
            UPDATE youtube_worker_outbox SET
              delivery_attempts = ?,
              last_attempt_at = ?,
              available_at = ?,
              last_error = ?
            WHERE outbox_id = ? AND delivered_at IS NULL
            """,
            (
                attempt,
                isoformat(attempted_at),
                isoformat(attempted_at + delay),
                f"{type(error).__name__}: {error}"[:1000],
                outbox_id,
            ),
        )
        self._commit()

    def outbox_health(
        self,
        *,
        now: datetime,
        worker_name: str | None = None,
    ) -> dict[str, Any]:
        worker_filter = "" if worker_name is None else "AND worker_name = ?"
        parameters: tuple[str, ...] = () if worker_name is None else (worker_name,)
        row = self.connection.execute(
            f"""
            SELECT
              COUNT(*) AS pending_count,
              MIN(created_at) AS oldest_created_at,
              COALESCE(SUM(delivery_attempts), 0) AS delivery_attempts,
              COALESCE(SUM(CASE WHEN last_error IS NOT NULL THEN 1 ELSE 0 END), 0)
                AS error_count
            FROM youtube_worker_outbox
            WHERE delivered_at IS NULL
              {worker_filter}
            """,
            parameters,
        ).fetchone()
        oldest = row["oldest_created_at"] if row else None
        oldest_age_seconds = None
        if oldest:
            parsed = datetime.fromisoformat(oldest)
            oldest_age_seconds = max(
                0.0,
                (now.astimezone(UTC) - parsed.astimezone(UTC)).total_seconds(),
            )
        return {
            "pending_count": int(row["pending_count"] or 0) if row else 0,
            "oldest_created_at": oldest,
            "oldest_age_seconds": oldest_age_seconds,
            "delivery_attempts": int(row["delivery_attempts"] or 0) if row else 0,
            "error_count": int(row["error_count"] or 0) if row else 0,
        }
