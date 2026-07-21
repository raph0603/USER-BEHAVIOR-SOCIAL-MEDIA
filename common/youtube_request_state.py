"""Persistent transcript, comment, and channel enrichment request state."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable

from common.transcripts import (
    legacy_transcript_status,
    normalize_transcript_language_code,
    preferred_transcript_language_code,
)
from common.youtube_pipeline import isoformat, parse_datetime


class YouTubeRequestStateMixin:
    """SQLite operations for independent secondary enrichment workers."""

    connection: sqlite3.Connection
    _commit: Callable[[], None]

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
        self._commit()
        return bool(cursor.rowcount)

    @staticmethod
    def _requested_transcript_language(request: dict[str, Any]) -> tuple[str, str]:
        explicit_code = request.get("transcript_requested_language_code") or request.get(
            "requested_language_code"
        )
        requested_code = (
            normalize_transcript_language_code(explicit_code)
            if explicit_code
            else preferred_transcript_language_code(request.get("language"))
        )
        if not requested_code:
            requested_code = "en"
        requested_language = str(
            request.get("transcript_requested_language")
            or request.get("requested_language")
            or requested_code
        )
        return requested_language, requested_code

    def enqueue_transcript_request(
        self,
        *,
        video_id: str,
        correlation_id: str,
        first_seen_at: datetime,
        published_at: str | datetime | None,
        request: dict[str, Any],
    ) -> bool:
        """Persist one independent request for each video and requested language."""

        requested_language, requested_code = self._requested_transcript_language(request)
        request_payload = {
            **request,
            "transcript_requested_language": requested_language,
            "transcript_requested_language_code": requested_code,
        }
        serialized = json.dumps(request_payload, ensure_ascii=False, sort_keys=True)
        first_seen_value = isoformat(first_seen_at)
        published_value = isoformat(parse_datetime(published_at))
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO youtube_transcript_lifecycle (
              video_id, requested_language_code, correlation_id,
              first_seen_at, published_at, request_json,
              transcript_lifecycle_status, transcript_status,
              requested_language, next_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 'pending', ?, ?)
            """,
            (
                video_id,
                requested_code,
                correlation_id,
                first_seen_value,
                published_value,
                serialized,
                requested_language,
                first_seen_value,
            ),
        )
        self.connection.execute(
            """
            INSERT OR IGNORE INTO youtube_transcript_state (
              video_id, correlation_id, first_seen_at, published_at,
              request_json, next_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                video_id,
                correlation_id,
                first_seen_value,
                published_value,
                serialized,
                first_seen_value,
            ),
        )
        self._commit()
        return bool(cursor.rowcount)

    def due_transcript_requests(self, *, now: datetime, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT * FROM youtube_transcript_lifecycle
            WHERE transcript_lifecycle_status NOT IN (
              'available', 'unavailable', 'disabled', 'permanent_error'
            )
              AND next_attempt_at IS NOT NULL
              AND next_attempt_at <= ?
            ORDER BY next_attempt_at, first_seen_at, video_id, requested_language_code
            LIMIT ?
            """,
            (isoformat(now), max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]

    def record_transcript_result(
        self,
        *,
        video_id: str,
        requested_language_code: str,
        lifecycle_status: str,
        attempt_count: int,
        attempted_at: datetime,
        next_attempt_at: datetime | None,
        result: dict[str, Any],
        requested_language: str | None = None,
        obtained_language: str | None = None,
        obtained_language_code: str | None = None,
        available_languages: Any = None,
        generation_type: str | None = None,
        is_generated: bool | None = None,
        is_translated: bool | None = None,
        provider: str | None = None,
        selection_strategy: str | None = None,
        collected_at: datetime | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        recovered_at: datetime | None = None,
        content_version: str | None = None,
        model: str | None = None,
        fallback_reason: str | None = None,
        prompt_version: str | None = None,
        generated_by_model: bool | None = None,
        source_content_version: str | None = None,
        primary_attempt_count: int = 0,
        fallback_attempt_count: int = 0,
        primary_last_attempt_at: datetime | None = None,
        fallback_last_attempt_at: datetime | None = None,
        primary_result: dict[str, Any] | None = None,
        fallback_result: dict[str, Any] | None = None,
    ) -> None:
        requested_code = normalize_transcript_language_code(requested_language_code)
        legacy_status = legacy_transcript_status(lifecycle_status)
        result_json = json.dumps(result, ensure_ascii=False, sort_keys=True, default=str)
        available_json = (
            json.dumps(available_languages, ensure_ascii=False, sort_keys=True, default=str)
            if available_languages is not None
            else None
        )
        cursor = self.connection.execute(
            """
            UPDATE youtube_transcript_lifecycle SET
              transcript_lifecycle_status = ?,
              transcript_status = ?,
              requested_language = COALESCE(?, requested_language),
              obtained_language = ?,
              obtained_language_code = ?,
              available_languages_json = ?,
              generation_type = ?,
              is_generated = ?,
              is_translated = ?,
              provider = ?,
              selection_strategy = ?,
              model = ?,
              fallback_reason = ?,
              prompt_version = ?,
              generated_by_model = ?,
              source_content_version = ?,
              primary_attempt_count = MAX(primary_attempt_count, ?),
              fallback_attempt_count = MAX(fallback_attempt_count, ?),
              primary_last_attempt_at = COALESCE(?, primary_last_attempt_at),
              fallback_last_attempt_at = COALESCE(?, fallback_last_attempt_at),
              primary_result_json = COALESCE(?, primary_result_json),
              fallback_result_json = COALESCE(?, fallback_result_json),
              attempt_count = MAX(attempt_count, ?),
              last_attempt_at = ?,
              next_attempt_at = ?,
              collected_at = ?,
              error_code = ?,
              error_message = ?,
              recovered_at = COALESCE(?, recovered_at),
              content_version = ?,
              result_json = ?
            WHERE video_id = ? AND requested_language_code = ?
            """,
            (
                lifecycle_status,
                legacy_status,
                requested_language,
                obtained_language,
                obtained_language_code,
                available_json,
                generation_type,
                None if is_generated is None else int(is_generated),
                None if is_translated is None else int(is_translated),
                provider,
                selection_strategy,
                model,
                fallback_reason,
                prompt_version,
                None if generated_by_model is None else int(generated_by_model),
                source_content_version,
                max(0, int(primary_attempt_count)),
                max(0, int(fallback_attempt_count)),
                isoformat(primary_last_attempt_at),
                isoformat(fallback_last_attempt_at),
                (
                    json.dumps(primary_result, ensure_ascii=False, sort_keys=True)
                    if primary_result is not None
                    else None
                ),
                (
                    json.dumps(fallback_result, ensure_ascii=False, sort_keys=True)
                    if fallback_result is not None
                    else None
                ),
                max(0, int(attempt_count)),
                isoformat(attempted_at),
                isoformat(next_attempt_at),
                isoformat(collected_at),
                error_code,
                error_message[:1000] if error_message else None,
                isoformat(recovered_at),
                content_version,
                result_json,
                video_id,
                requested_code,
            ),
        )
        if not cursor.rowcount:
            raise KeyError((video_id, requested_code))
        self.connection.execute(
            """
            UPDATE youtube_transcript_state SET
              status = ?,
              attempt_count = MAX(attempt_count, ?),
              last_attempt_at = ?,
              next_attempt_at = ?,
              error_class = ?,
              error_message = ?,
              result_json = ?
            WHERE video_id = ?
            """,
            (
                legacy_status,
                max(0, int(attempt_count)),
                isoformat(attempted_at),
                isoformat(next_attempt_at),
                error_code,
                error_message[:1000] if error_message else None,
                result_json,
                video_id,
            ),
        )
        self._commit()

    def due_requests(self, family: str, *, now: datetime, limit: int) -> list[dict[str, Any]]:
        table = self._request_table(family)
        terminal_statuses = (
            "('available', 'permanent_error')" if family == "transcript" else "('permanent_error')"
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
        self._commit()

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
        self._commit()

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
        self._commit()
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
        self._commit()

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
        self._commit()
