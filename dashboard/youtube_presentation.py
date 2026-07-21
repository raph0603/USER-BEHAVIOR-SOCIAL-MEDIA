"""Pure presentation helpers for YouTube freshness and coverage surfaces."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from common.youtube_thumbnails import safe_youtube_thumbnail_url


TRANSCRIPT_LIFECYCLE_PRESENTATION = {
    "pending": ("info", "Transcript collection is pending for this video."),
    "available": ("success", "Transcript collected successfully."),
    "unavailable": (
        "info",
        "YouTube explicitly reports that no transcript is available for this video.",
    ),
    "disabled": ("info", "YouTube reports that transcripts are disabled."),
    "rate_limited": (
        "warning",
        "Transcript collection was rate limited and will resume after its cooldown.",
    ),
    "blocked": (
        "warning",
        "Transcript collection is temporarily blocked and remains retryable.",
    ),
    "retryable_error": (
        "warning",
        "Transcript collection failed temporarily and remains retryable.",
    ),
    "permanent_error": (
        "error",
        "Transcript collection reached a permanent error and will not be retried.",
    ),
}

LEGACY_TRANSCRIPT_STATUS_MAP = {
    "success": "available",
    "partial": "retryable_error",
    "not_available": "unavailable",
    "not_found": "unavailable",
    "age_restricted": "unavailable",
    "disabled": "disabled",
    "rate_limited": "rate_limited",
    "ip_blocked": "blocked",
    "failed": "retryable_error",
    "permanent_error": "permanent_error",
    "pending": "pending",
}

RETRYABLE_TRANSCRIPT_STATUSES = {
    "pending",
    "rate_limited",
    "blocked",
    "retryable_error",
}

SNAPSHOT_METRICS = (
    ("view_count", "views"),
    ("like_count", "likes"),
    ("comment_count", "comments"),
)
CONTENT_STATS_FRESHNESS_COLUMNS = (
    "latest_view_count",
    "latest_like_count",
    "latest_comment_count",
    "latest_reply_count",
    "latest_retweet_count",
    "latest_bookmark_count",
    "latest_snapshot_at",
    "latest_snapshot_observation_id",
    "latest_snapshot_producer_name",
    "latest_snapshot_producer_run_id",
    "latest_snapshot_collection_method",
    "latest_snapshot_api_endpoint",
    "latest_snapshot_provenance_json",
    "latest_snapshot_coverage_json",
    "latest_view_count_available",
    "latest_like_count_available",
    "latest_comment_count_available",
    "latest_reply_count_available",
    "latest_retweet_count_available",
    "latest_bookmark_count_available",
    "last_discovered_at",
    "last_enriched_at",
)
TRANSCRIPT_CARD_COLUMNS = (
    "transcript_status",
    "transcript_lifecycle_status",
    "transcript_text",
    "requested_language_code",
    "obtained_language_code",
    "generation_type",
    "is_translated",
    "provider",
    "model",
    "fallback_reason",
    "prompt_version",
    "generated_by_model",
    "selection_strategy",
    "attempt_count",
    "last_attempt_at",
    "next_attempt_at",
    "error_code",
)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return bool(missing) if isinstance(missing, bool) else False


def has_value(value: Any) -> bool:
    if is_missing(value):
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _row_value(row: Any, *names: str) -> Any:
    for name in names:
        try:
            value = row.get(name)
        except AttributeError:
            value = None
        if not is_missing(value):
            return value
    return None


def availability_flag(value: Any, *, fallback: bool = False) -> bool:
    if is_missing(value):
        return fallback
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "available"}:
            return True
        if normalized in {"false", "0", "no", "unavailable"}:
            return False
    try:
        return bool(value)
    except (TypeError, ValueError):
        return fallback


def metric_is_available(value: Any, available: Any = None) -> bool:
    return availability_flag(available, fallback=has_value(value)) and has_value(value)


def format_available_metric(value: Any, available: Any = None) -> str:
    """Format an observed count while preserving unknown-versus-zero semantics."""

    if not metric_is_available(value, available):
        return "N/A"
    numeric = pd.to_numeric(value, errors="coerce")
    if is_missing(numeric):
        return "N/A"
    return f"{int(numeric):,}"


def format_timestamp(value: Any) -> str:
    if is_missing(value):
        return "N/A"
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if is_missing(timestamp):
        return "N/A"
    return timestamp.strftime("%Y-%m-%d %H:%M UTC")


def _json_object(value: Any) -> dict[str, Any]:
    if not has_value(value):
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def transcript_lifecycle_status(row: Any) -> str:
    transcript_text = _row_value(
        row,
        "latest_transcript_text",
        "transcript_text",
    )
    if has_value(transcript_text):
        return "available"

    lifecycle = _row_value(
        row,
        "latest_transcript_lifecycle_status",
        "transcript_lifecycle_status",
    )
    if has_value(lifecycle):
        return str(lifecycle).strip().lower()

    legacy = _row_value(
        row,
        "latest_transcript_status",
        "transcript_status",
    )
    if not has_value(legacy):
        return "pending"
    normalized = str(legacy).strip().lower()
    return LEGACY_TRANSCRIPT_STATUS_MAP.get(normalized, normalized)


def transcript_status_presentation(row: Any) -> tuple[str, str, str]:
    status = transcript_lifecycle_status(row)
    level, message = TRANSCRIPT_LIFECYCLE_PRESENTATION.get(
        status,
        ("warning", "Transcript collection returned an unknown lifecycle status."),
    )
    error_code = _row_value(
        row,
        "latest_transcript_error_code",
        "error_code",
    )
    if has_value(error_code) and status != "available":
        message = f"{message} Error code: {str(error_code).strip()}."
    return status, level, message


def transcript_provenance_label(row: Any) -> str:
    """Return an explicit user-facing origin without implying official captions."""

    status = transcript_lifecycle_status(row)
    if status != "available":
        return {
            "pending": "En attente",
            "retryable_error": "Erreur réessayable",
            "rate_limited": "Erreur réessayable",
            "blocked": "Erreur réessayable",
        }.get(status, "Indisponible")
    provider = str(
        _row_value(row, "latest_transcript_provider", "provider") or ""
    ).strip().lower()
    if provider == "gemini":
        return "Transcription générée depuis la vidéo avec Gemini"
    translated = availability_flag(
        _row_value(row, "latest_transcript_is_translated", "is_translated")
    )
    generation_type = str(
        _row_value(row, "latest_transcript_generation_type", "generation_type") or ""
    ).strip().lower()
    if translated:
        return "Sous-titres YouTube traduits"
    if generation_type == "automatic":
        return "Sous-titres YouTube automatiques"
    return "Sous-titres YouTube manuels"


def latest_rows_by_content(
    dataframe: pd.DataFrame,
    sort_candidates: tuple[str, ...] = (
        "last_attempt_at",
        "updated_at",
        "collected_at",
        "created_at",
    ),
) -> pd.DataFrame:
    if dataframe.empty or "content_id" not in dataframe.columns:
        return pd.DataFrame()
    latest = dataframe.copy()
    sort_columns = [column for column in sort_candidates if column in latest.columns]
    if sort_columns:
        latest = latest.sort_values(
            sort_columns,
            ascending=False,
            na_position="last",
        )
    return latest.drop_duplicates("content_id")


def _merge_latest_snapshot_fallback(
    display_rows: pd.DataFrame,
    engagement_snapshots: pd.DataFrame,
) -> pd.DataFrame:
    latest_snapshots = latest_rows_by_content(
        engagement_snapshots,
        sort_candidates=("snapshot_at", "observed_at", "observation_id"),
    )
    if latest_snapshots.empty or "content_id" not in display_rows.columns:
        return display_rows

    source_to_target = {
        "snapshot_at": "latest_snapshot_at",
        "observation_id": "latest_snapshot_observation_id",
        "producer_name": "latest_snapshot_producer_name",
        "producer_run_id": "latest_snapshot_producer_run_id",
        "collection_method": "latest_snapshot_collection_method",
        "api_endpoint": "latest_snapshot_api_endpoint",
        "provenance_json": "latest_snapshot_provenance_json",
        "coverage_json": "latest_snapshot_coverage_json",
        "view_count": "latest_view_count",
        "like_count": "latest_like_count",
        "comment_count": "latest_comment_count",
        "reply_count": "latest_reply_count",
        "retweet_count": "latest_retweet_count",
        "bookmark_count": "latest_bookmark_count",
        "view_count_available": "latest_view_count_available",
        "like_count_available": "latest_like_count_available",
        "comment_count_available": "latest_comment_count_available",
        "reply_count_available": "latest_reply_count_available",
        "retweet_count_available": "latest_retweet_count_available",
        "bookmark_count_available": "latest_bookmark_count_available",
    }
    available_sources = [
        column for column in source_to_target if column in latest_snapshots.columns
    ]
    fallback = latest_snapshots[["content_id", *available_sources]].rename(
        columns={column: source_to_target[column] for column in available_sources}
    )
    merged = display_rows.merge(
        fallback,
        on="content_id",
        how="left",
        suffixes=("", "__snapshot"),
        validate="many_to_one",
    )
    for target in source_to_target.values():
        fallback_column = f"{target}__snapshot"
        if fallback_column not in merged.columns:
            continue
        if target in display_rows.columns:
            merged[target] = merged[target].combine_first(merged[fallback_column])
        else:
            merged[target] = merged[fallback_column]
        merged = merged.drop(columns=[fallback_column])
    return merged


def build_youtube_display_rows(
    youtube_contents: pd.DataFrame,
    transcripts: pd.DataFrame,
    content_stats: pd.DataFrame,
    engagement_snapshots: pd.DataFrame | None = None,
) -> pd.DataFrame:
    display_rows = youtube_contents.copy()
    latest_stats = latest_rows_by_content(
        content_stats,
        sort_candidates=(
            "latest_snapshot_at",
            "last_enriched_at",
            "last_discovered_at",
        ),
    )
    stat_columns = [
        column
        for column in ("content_id", *CONTENT_STATS_FRESHNESS_COLUMNS)
        if column in latest_stats.columns
    ]
    if not latest_stats.empty and "content_id" in display_rows.columns and stat_columns:
        overlapping = [
            column
            for column in stat_columns
            if column != "content_id" and column in display_rows.columns
        ]
        stat_rows = latest_stats[stat_columns].rename(
            columns={column: f"{column}__stats" for column in overlapping}
        )
        display_rows = display_rows.merge(
            stat_rows,
            on="content_id",
            how="left",
            validate="many_to_one",
        )
        for column in overlapping:
            stats_column = f"{column}__stats"
            display_rows[column] = display_rows[column].combine_first(display_rows[stats_column])
            display_rows = display_rows.drop(columns=[stats_column])

    if engagement_snapshots is not None and not engagement_snapshots.empty:
        display_rows = _merge_latest_snapshot_fallback(
            display_rows,
            engagement_snapshots,
        )

    latest_transcripts = latest_rows_by_content(transcripts)
    if not latest_transcripts.empty and "content_id" in display_rows.columns:
        transcript_columns = [
            column
            for column in ("content_id", *TRANSCRIPT_CARD_COLUMNS)
            if column in latest_transcripts.columns
        ]
        latest_transcripts = latest_transcripts[transcript_columns].rename(
            columns={
                column: (
                    f"latest_{column}"
                    if column.startswith("transcript_")
                    else f"latest_transcript_{column}"
                )
                for column in transcript_columns
                if column != "content_id"
            }
        )
        display_rows = display_rows.merge(
            latest_transcripts,
            on="content_id",
            how="left",
            validate="many_to_one",
        )

        lifecycle_by_content = transcripts.copy()
        lifecycle_by_content["_transcript_lifecycle"] = lifecycle_by_content.apply(
            transcript_lifecycle_status,
            axis=1,
        )
        availability = (
            lifecycle_by_content.groupby("content_id")["_transcript_lifecycle"]
            .agg(lambda statuses: bool((statuses == "available").any()))
            .rename("transcript_available_any")
            .reset_index()
        )
        display_rows = display_rows.merge(
            availability,
            on="content_id",
            how="left",
            validate="many_to_one",
        )

    if "created_at" in display_rows.columns:
        display_rows = display_rows.sort_values("created_at", ascending=False)
    return display_rows


def youtube_data_completeness(row: Any) -> tuple[int, int, dict[str, bool]]:
    transcript_available = availability_flag(
        _row_value(row, "transcript_available_any"),
        fallback=transcript_lifecycle_status(row) == "available",
    )
    metadata_available = has_value(_row_value(row, "last_enriched_at")) or (
        availability_flag(_row_value(row, "metadata_available"))
    )
    comments_status = _row_value(row, "comments_status")
    comments_available = availability_flag(
        _row_value(row, "comments_available"),
        fallback=(has_value(comments_status) and str(comments_status).strip().lower() == "success"),
    )
    checks = {
        "thumbnail": bool(
            safe_youtube_thumbnail_url(_row_value(row, "thumbnail_url"))
        ),
        "metadata": metadata_available,
        "transcript": transcript_available,
        "views": metric_is_available(
            _row_value(row, "latest_view_count"),
            _row_value(row, "latest_view_count_available"),
        ),
        "comments": comments_available,
    }
    return sum(checks.values()), len(checks), checks


def _coverage_flag(row: Any, metric: str, coverage: dict[str, Any]) -> bool:
    value = _row_value(row, f"latest_{metric}", metric)
    direct = _row_value(
        row,
        f"latest_{metric}_available",
        f"{metric}_available",
    )
    if not is_missing(direct):
        return metric_is_available(value, direct)
    encoded = coverage.get(f"{metric}_available", coverage.get(metric))
    return metric_is_available(value, encoded)


def coverage_summary(row: Any) -> str:
    coverage = _json_object(_row_value(row, "latest_snapshot_coverage_json", "coverage_json"))
    observed = []
    unknown = []
    for metric, label in SNAPSHOT_METRICS:
        if _coverage_flag(row, metric, coverage):
            observed.append(label)
        else:
            unknown.append(label)
    summary = f"{len(observed)}/{len(SNAPSHOT_METRICS)} snapshot metrics observed"
    if unknown:
        summary += "; N/A: " + ", ".join(unknown)
    return summary


def provenance_summary(row: Any) -> str:
    provenance = _json_object(_row_value(row, "latest_snapshot_provenance_json", "provenance_json"))
    candidates = (
        _row_value(row, "latest_snapshot_producer_name", "producer_name")
        or provenance.get("producer_name"),
        _row_value(row, "latest_snapshot_collection_method", "collection_method")
        or provenance.get("collection_method"),
        _row_value(row, "latest_snapshot_api_endpoint", "api_endpoint")
        or provenance.get("api_endpoint"),
        _row_value(row, "latest_snapshot_producer_run_id", "producer_run_id")
        or provenance.get("producer_run_id"),
    )
    values = []
    for value in candidates:
        if not has_value(value):
            continue
        normalized = str(value).strip()
        if normalized not in values:
            values.append(normalized)
    return " · ".join(values) if values else "N/A"


def _utc_now(now: datetime | pd.Timestamp | None = None) -> pd.Timestamp:
    if now is None:
        return pd.Timestamp(datetime.now(UTC))
    timestamp = pd.Timestamp(now)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def freshness_warning(
    label: str,
    value: Any,
    *,
    stale_after_hours: float,
    now: datetime | pd.Timestamp | None = None,
) -> str | None:
    if is_missing(value):
        return None
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if is_missing(timestamp):
        return None
    age_hours = (_utc_now(now) - timestamp).total_seconds() / 3600
    if age_hours < -0.1:
        return f"{label} timestamp is {abs(age_hours):.1f} h in the future."
    if age_hours <= stale_after_hours:
        return None
    return f"{label} is stale ({age_hours:.1f} h old; threshold {stale_after_hours:g} h)."


def transcript_retry_warning(
    row: Any,
    *,
    now: datetime | pd.Timestamp | None = None,
) -> str | None:
    if transcript_lifecycle_status(row) not in RETRYABLE_TRANSCRIPT_STATUSES:
        return None
    next_attempt = _row_value(
        row,
        "latest_transcript_next_attempt_at",
        "next_attempt_at",
    )
    if is_missing(next_attempt):
        return None
    timestamp = pd.to_datetime(next_attempt, errors="coerce", utc=True)
    if is_missing(timestamp) or timestamp >= _utc_now(now):
        return None
    overdue_hours = (_utc_now(now) - timestamp).total_seconds() / 3600
    return f"Transcript retry is overdue by {overdue_hours:.1f} h."


def build_youtube_freshness_table(
    dataframe: pd.DataFrame,
    *,
    enrichment_stale_hours: float,
    snapshot_stale_hours: float,
    now: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    columns = [
        "Title",
        "Channel",
        "Last discovery",
        "Last enrichment",
        "Last snapshot",
        "Views",
        "Likes",
        "Comments",
        "Coverage",
        "Provenance",
        "Transcript lifecycle",
        "Requested language",
        "Obtained language",
        "Attempts",
        "Last attempt",
        "Next attempt",
        "Freshness warning",
        "URL",
    ]
    records = []
    for _, row in dataframe.iterrows():
        warnings = [
            freshness_warning(
                "Metadata enrichment",
                row.get("last_enriched_at"),
                stale_after_hours=enrichment_stale_hours,
                now=now,
            ),
            freshness_warning(
                "Engagement snapshot",
                row.get("latest_snapshot_at"),
                stale_after_hours=snapshot_stale_hours,
                now=now,
            ),
            transcript_retry_warning(row, now=now),
        ]
        records.append(
            {
                "Title": _row_value(row, "title") or "N/A",
                "Channel": _row_value(row, "youtube_channel_name") or "N/A",
                "Last discovery": format_timestamp(row.get("last_discovered_at")),
                "Last enrichment": format_timestamp(row.get("last_enriched_at")),
                "Last snapshot": format_timestamp(row.get("latest_snapshot_at")),
                "Views": format_available_metric(
                    row.get("latest_view_count"),
                    row.get("latest_view_count_available"),
                ),
                "Likes": format_available_metric(
                    row.get("latest_like_count"),
                    row.get("latest_like_count_available"),
                ),
                "Comments": format_available_metric(
                    row.get("latest_comment_count"),
                    row.get("latest_comment_count_available"),
                ),
                "Coverage": coverage_summary(row),
                "Provenance": provenance_summary(row),
                "Transcript lifecycle": transcript_lifecycle_status(row).replace("_", " "),
                "Requested language": _row_value(row, "latest_transcript_requested_language_code")
                or "N/A",
                "Obtained language": _row_value(row, "latest_transcript_obtained_language_code")
                or "N/A",
                "Attempts": format_available_metric(row.get("latest_transcript_attempt_count")),
                "Last attempt": format_timestamp(row.get("latest_transcript_last_attempt_at")),
                "Next attempt": format_timestamp(row.get("latest_transcript_next_attempt_at")),
                "Freshness warning": " ".join(warning for warning in warnings if warning) or "None",
                "URL": _row_value(row, "url") or "N/A",
            }
        )
    return pd.DataFrame.from_records(records, columns=columns)
