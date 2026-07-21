"""Persistent YouTube API usage and circuit-breaker state."""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Mapping, Protocol, cast

from common.youtube_pipeline import ensure_utc, isoformat, parse_datetime


ENDPOINT_QUOTA_UNITS = {
    "search.list": 100,
    "videos.list": 1,
    "channels.list": 1,
    "commentThreads.list": 1,
}

WORKLOAD_PRIORITIES = {
    "recent_metrics": "critical",
    "discovery": "high",
    "transcript": "normal",
    "descriptive_metadata": "low",
    "channels": "low",
    "comments": "low",
}

LOW_PRIORITY_WORKLOADS = frozenset({"descriptive_metadata", "channels", "comments"})


class _OutboxHealthReader(Protocol):
    def outbox_health(
        self,
        *,
        now: datetime,
        worker_name: str | None = None,
    ) -> dict[str, Any]: ...


def _int_setting(
    values: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int = 0,
) -> int:
    raw = values.get(name, str(default))
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return parsed


def _ratio_setting(
    values: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    raw = values.get(name, str(default))
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc
    if not 0 <= parsed <= 1:
        raise ValueError(f"{name} must be between 0 and 1")
    return parsed


@dataclass(frozen=True)
class QuotaPolicy:
    daily_budget_units: int
    recent_snapshot_reserve_units: int
    pressure_ratio: float
    critical_ratio: float

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> "QuotaPolicy":
        settings = values if values is not None else os.environ
        budget = _int_setting(
            settings,
            "YOUTUBE_DAILY_QUOTA_UNITS",
            10_000,
            minimum=1,
        )
        reserve = _int_setting(
            settings,
            "YOUTUBE_RECENT_SNAPSHOT_RESERVE_UNITS",
            2_000,
        )
        if reserve > budget:
            raise ValueError(
                "YOUTUBE_RECENT_SNAPSHOT_RESERVE_UNITS must not exceed YOUTUBE_DAILY_QUOTA_UNITS"
            )
        pressure = _ratio_setting(
            settings,
            "YOUTUBE_QUOTA_PRESSURE_RATIO",
            0.80,
        )
        critical = _ratio_setting(
            settings,
            "YOUTUBE_QUOTA_CRITICAL_RATIO",
            0.95,
        )
        if critical < pressure:
            raise ValueError("YOUTUBE_QUOTA_CRITICAL_RATIO must be >= YOUTUBE_QUOTA_PRESSURE_RATIO")
        return cls(budget, reserve, pressure, critical)


@dataclass(frozen=True)
class QuotaDecision:
    endpoint: str
    workload: str
    priority: str
    requested_calls: int
    allowed_calls: int
    quota_cost_per_request: int
    allowed_units: int
    used_units: int
    remaining_units: int
    reserved_units: int
    reserve_remaining_units: int
    pressure_ratio: float
    throttled: bool
    reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def quota_cost(endpoint: str) -> int:
    """Return official quota units per request; non-quota clients cost zero."""

    return ENDPOINT_QUOTA_UNITS.get(endpoint, 0)


def decide_quota(
    policy: QuotaPolicy,
    *,
    endpoint: str,
    workload: str,
    requested_calls: int,
    used_units: int,
    recent_snapshot_units: int,
) -> QuotaDecision:
    """Reserve recent-metric capacity and degrade secondary work first."""

    requested = max(0, int(requested_calls))
    used = max(0, int(used_units))
    recent_used = max(0, int(recent_snapshot_units))
    remaining = max(0, policy.daily_budget_units - used)
    reserve_remaining = max(
        0,
        policy.recent_snapshot_reserve_units - recent_used,
    )
    pressure = min(1.0, used / float(policy.daily_budget_units))
    workload_allowed = True
    reason = None
    if pressure >= policy.critical_ratio and workload != "recent_metrics":
        workload_allowed = False
        reason = "critical_quota_pressure"
    elif pressure >= policy.pressure_ratio and workload in LOW_PRIORITY_WORKLOADS:
        workload_allowed = False
        reason = "secondary_workload_suspended"

    spendable = remaining if workload == "recent_metrics" else max(0, remaining - reserve_remaining)
    cost = quota_cost(endpoint)
    if not workload_allowed:
        allowed_calls = 0
    elif cost == 0:
        allowed_calls = requested
    else:
        allowed_calls = min(requested, spendable // cost)
        if allowed_calls < requested and reason is None:
            reason = "recent_snapshot_reserve" if reserve_remaining else "quota_exhausted"

    return QuotaDecision(
        endpoint=endpoint,
        workload=workload,
        priority=WORKLOAD_PRIORITIES.get(workload, "normal"),
        requested_calls=requested,
        allowed_calls=allowed_calls,
        quota_cost_per_request=cost,
        allowed_units=allowed_calls * cost,
        used_units=used,
        remaining_units=remaining,
        reserved_units=policy.recent_snapshot_reserve_units,
        reserve_remaining_units=reserve_remaining,
        pressure_ratio=pressure,
        throttled=allowed_calls < requested,
        reason=reason,
    )


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
        provider: str = "youtube",
        quota_units: int | None = None,
        quota_cost_per_request: int | None = None,
        priority: str | None = None,
        cache_hit_count: int = 0,
        cache_miss_count: int = 0,
        retry_count: int = 0,
        latency_ms: float | None = None,
        queue_depth: int | None = None,
        oldest_queue_age_seconds: float | None = None,
        circuit_open: bool = False,
        status: str | None = None,
        error_code: str | None = None,
        producer_run_id: str | None = None,
        video_minutes: float = 0.0,
        daily_video_minutes_budget: float | None = None,
        remaining_video_minutes: float | None = None,
    ) -> None:
        policy = QuotaPolicy.from_env()
        request_total = max(0, int(request_count))
        unit_cost = (
            quota_cost(endpoint)
            if quota_cost_per_request is None
            else max(0, int(quota_cost_per_request))
        )
        consumed_units = (
            request_total * unit_cost if quota_units is None else max(0, int(quota_units))
        )
        used_before = self.quota_units_today(observed_at)
        recent_before = self.quota_units_today(
            observed_at,
            quota_bucket="recent_metrics",
        )
        remaining = max(0, policy.daily_budget_units - used_before - consumed_units)
        recent_after = recent_before + (consumed_units if quota_bucket == "recent_metrics" else 0)
        reserve_remaining = max(
            0,
            policy.recent_snapshot_reserve_units - recent_after,
        )
        normalized_status = status or (
            "error"
            if error_count and not success_count
            else "partial"
            if error_count
            else "success"
        )
        self.connection.execute(
            """
            INSERT INTO youtube_api_usage (
              usage_date, endpoint, request_count, resource_count,
              success_count, error_count, quota_bucket, observed_at,
              provider, operation, quota_units, quota_cost_per_request,
              daily_budget_units, reserved_units, remaining_units,
              reserve_remaining_units, priority, cache_hit_count,
              cache_miss_count, retry_count, latency_ms, queue_depth,
              oldest_queue_age_seconds, circuit_open, status, error_code,
              producer_run_id, video_minutes, daily_video_minutes_budget,
              remaining_video_minutes
            ) VALUES (
              ?, ?, ?, ?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                ensure_utc(observed_at).date().isoformat(),
                endpoint,
                request_total,
                max(0, int(resource_count)),
                max(0, int(success_count)),
                max(0, int(error_count)),
                quota_bucket,
                isoformat(observed_at),
                provider,
                endpoint,
                consumed_units,
                unit_cost,
                policy.daily_budget_units,
                policy.recent_snapshot_reserve_units,
                remaining,
                reserve_remaining,
                priority or WORKLOAD_PRIORITIES.get(quota_bucket, "normal"),
                max(0, int(cache_hit_count)),
                max(0, int(cache_miss_count)),
                max(0, int(retry_count)),
                None if latency_ms is None else max(0.0, float(latency_ms)),
                None if queue_depth is None else max(0, int(queue_depth)),
                (
                    None
                    if oldest_queue_age_seconds is None
                    else max(0.0, float(oldest_queue_age_seconds))
                ),
                int(bool(circuit_open)),
                normalized_status,
                error_code,
                producer_run_id or os.getenv("PIPELINE_RUN_ID") or "standalone",
                max(0.0, float(video_minutes)),
                daily_video_minutes_budget,
                remaining_video_minutes,
            ),
        )
        self._commit()

    def quota_units_today(
        self,
        now: datetime,
        *,
        quota_bucket: str | None = None,
    ) -> int:
        bucket_filter = "" if quota_bucket is None else "AND quota_bucket = ?"
        parameters: tuple[str, ...] = (
            (ensure_utc(now).date().isoformat(),)
            if quota_bucket is None
            else (ensure_utc(now).date().isoformat(), quota_bucket)
        )
        row = self.connection.execute(
            f"""
            SELECT COALESCE(SUM(quota_units), 0)
            FROM youtube_api_usage
            WHERE usage_date = ? {bucket_filter}
            """,
            parameters,
        ).fetchone()
        return int(row[0] or 0)

    def quota_decision(
        self,
        *,
        endpoint: str,
        workload: str,
        requested_calls: int,
        now: datetime,
    ) -> QuotaDecision:
        return decide_quota(
            QuotaPolicy.from_env(),
            endpoint=endpoint,
            workload=workload,
            requested_calls=requested_calls,
            used_units=self.quota_units_today(now),
            recent_snapshot_units=self.quota_units_today(
                now,
                quota_bucket="recent_metrics",
            ),
        )

    def workload_allowed(self, workload: str, now: datetime) -> bool:
        decision = self.quota_decision(
            endpoint=workload,
            workload=workload,
            requested_calls=1,
            now=now,
        )
        return decision.allowed_calls == 1

    def record_worker_health(
        self,
        *,
        worker_name: str,
        observed_at: datetime,
        status: str,
        processed_count: int,
        success_count: int,
        error_count: int,
        retry_count: int = 0,
        cache_hit_count: int = 0,
        cache_miss_count: int = 0,
        latency_ms: float | None = None,
        circuit_open: bool = False,
        details: Mapping[str, Any] | None = None,
        producer_run_id: str | None = None,
    ) -> None:
        outbox_health = cast(_OutboxHealthReader, self).outbox_health(
            now=observed_at,
            worker_name=worker_name,
        )
        self.connection.execute(
            """
            INSERT INTO youtube_worker_health (
              observed_at, producer_run_id, worker_name, status,
              processed_count, success_count, error_count, retry_count,
              cache_hit_count, cache_miss_count, latency_ms, queue_depth,
              oldest_queue_age_seconds, circuit_open, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                isoformat(observed_at),
                producer_run_id or os.getenv("PIPELINE_RUN_ID") or "standalone",
                worker_name,
                status,
                max(0, int(processed_count)),
                max(0, int(success_count)),
                max(0, int(error_count)),
                max(0, int(retry_count)),
                max(0, int(cache_hit_count)),
                max(0, int(cache_miss_count)),
                None if latency_ms is None else max(0.0, float(latency_ms)),
                outbox_health["pending_count"],
                outbox_health["oldest_age_seconds"],
                int(bool(circuit_open)),
                (
                    json.dumps(
                        dict(details),
                        ensure_ascii=False,
                        sort_keys=True,
                        default=str,
                    )
                    if details is not None
                    else None
                ),
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
