"""Deterministic YouTube transcript selection with explicit outcomes."""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from .collection import (
    OperationResult,
    STATUS_DISABLED,
    STATUS_FAILED,
    STATUS_NOT_AVAILABLE,
    STATUS_RATE_LIMITED,
    ensure_utc,
    is_retryable_status,
    is_terminal_status,
    safe_json_dumps,
    sanitize_error_message,
    sanitize_json_value,
    utc_now,
)


TRANSCRIPT_SOURCE = "youtube_transcript_api"

TRANSCRIPT_PENDING = "pending"
TRANSCRIPT_AVAILABLE = "available"
TRANSCRIPT_UNAVAILABLE = "unavailable"
TRANSCRIPT_DISABLED = "disabled"
TRANSCRIPT_RATE_LIMITED = "rate_limited"
TRANSCRIPT_BLOCKED = "blocked"
TRANSCRIPT_RETRYABLE_ERROR = "retryable_error"
TRANSCRIPT_PERMANENT_ERROR = "permanent_error"

TRANSCRIPT_LIFECYCLE_STATUSES = frozenset(
    {
        TRANSCRIPT_PENDING,
        TRANSCRIPT_AVAILABLE,
        TRANSCRIPT_UNAVAILABLE,
        TRANSCRIPT_DISABLED,
        TRANSCRIPT_RATE_LIMITED,
        TRANSCRIPT_BLOCKED,
        TRANSCRIPT_RETRYABLE_ERROR,
        TRANSCRIPT_PERMANENT_ERROR,
    }
)
TERMINAL_TRANSCRIPT_LIFECYCLE_STATUSES = frozenset(
    {
        TRANSCRIPT_AVAILABLE,
        TRANSCRIPT_UNAVAILABLE,
        TRANSCRIPT_DISABLED,
        TRANSCRIPT_PERMANENT_ERROR,
    }
)
RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES = frozenset(
    {
        TRANSCRIPT_PENDING,
        TRANSCRIPT_RATE_LIMITED,
        TRANSCRIPT_BLOCKED,
        TRANSCRIPT_RETRYABLE_ERROR,
    }
)

_BLOCKED_ERROR_CODES = frozenset({"ip_blocked", "request_blocked"})
_PERMANENT_ERROR_CODES = frozenset(
    {
        "dependency_missing",
        "invalid_video_id",
        "missing_video_id",
    }
)
_LEGACY_LIFECYCLE_ALIASES = {
    "success": TRANSCRIPT_AVAILABLE,
    "partial": TRANSCRIPT_RETRYABLE_ERROR,
    "not_available": TRANSCRIPT_UNAVAILABLE,
    "age_restricted": TRANSCRIPT_UNAVAILABLE,
    "not_found": TRANSCRIPT_UNAVAILABLE,
    "ip_blocked": TRANSCRIPT_BLOCKED,
    "failed": TRANSCRIPT_RETRYABLE_ERROR,
}
_LIFECYCLE_TO_LEGACY_STATUS = {
    TRANSCRIPT_PENDING: "pending",
    TRANSCRIPT_AVAILABLE: "success",
    TRANSCRIPT_UNAVAILABLE: "not_available",
    TRANSCRIPT_DISABLED: "disabled",
    TRANSCRIPT_RATE_LIMITED: "rate_limited",
    TRANSCRIPT_BLOCKED: "rate_limited",
    TRANSCRIPT_RETRYABLE_ERROR: "failed",
    TRANSCRIPT_PERMANENT_ERROR: "failed",
}


def normalize_transcript_language_code(value: Any) -> str:
    return _normalize_language_code(value)


def preferred_transcript_language_code(value: Any) -> str:
    """Apply the canonical per-video language policy without retaining global state."""

    language = normalize_transcript_language_code(value)
    if language == "vi" or language.startswith("vi-") or "vietnam" in language:
        return "vi"
    return "en"


def transcript_lifecycle_status(
    status: Any,
    *,
    error_code: Any = None,
    has_text: bool = False,
    attempt_count: int = 0,
    max_attempts: int | None = None,
) -> str:
    """Map collector and legacy outcomes to the additive transcript lifecycle."""

    normalized = str(status or TRANSCRIPT_PENDING).strip().lower()
    normalized_error = str(error_code or "").strip().lower()
    if has_text:
        lifecycle = TRANSCRIPT_AVAILABLE
    elif normalized_error in _BLOCKED_ERROR_CODES:
        lifecycle = TRANSCRIPT_BLOCKED
    elif normalized_error in _PERMANENT_ERROR_CODES:
        lifecycle = TRANSCRIPT_PERMANENT_ERROR
    elif normalized in TRANSCRIPT_LIFECYCLE_STATUSES:
        lifecycle = normalized
    elif normalized == STATUS_RATE_LIMITED:
        lifecycle = TRANSCRIPT_RATE_LIMITED
    elif normalized == STATUS_DISABLED:
        lifecycle = TRANSCRIPT_DISABLED
    elif normalized == STATUS_NOT_AVAILABLE:
        lifecycle = TRANSCRIPT_UNAVAILABLE
    else:
        lifecycle = _LEGACY_LIFECYCLE_ALIASES.get(
            normalized,
            TRANSCRIPT_RETRYABLE_ERROR,
        )

    if (
        lifecycle in RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES
        and max_attempts is not None
        and int(attempt_count or 0) >= max(1, int(max_attempts))
    ):
        return TRANSCRIPT_PERMANENT_ERROR
    return lifecycle


def legacy_transcript_status(lifecycle_status: Any) -> str:
    """Return the compatibility status retained for existing consumers."""

    lifecycle = transcript_lifecycle_status(lifecycle_status)
    return _LIFECYCLE_TO_LEGACY_STATUS[lifecycle]


def is_terminal_transcript_lifecycle(status: Any) -> bool:
    return transcript_lifecycle_status(status) in TERMINAL_TRANSCRIPT_LIFECYCLE_STATUSES


def is_retryable_transcript_lifecycle(status: Any) -> bool:
    return transcript_lifecycle_status(status) in RETRYABLE_TRANSCRIPT_LIFECYCLE_STATUSES


def transcript_content_version(
    *,
    video_id: str,
    language_code: str | None,
    text: str,
    segments: Sequence[Mapping[str, Any]],
) -> str:
    """Build a stable content version that is independent of collection time."""

    del segments  # timing edits do not create a new textual content version
    canonical = "\u001f".join(
        (
            str(video_id),
            _normalize_language_code(language_code),
            str(text),
        )
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TranscriptErrorClassification:
    """Normalized handling policy for a transcript exception."""

    status: str
    error_code: str
    error_message: str | None

    @property
    def is_terminal(self) -> bool:
        return is_terminal_status(self.status)

    @property
    def is_retryable(self) -> bool:
        return is_retryable_status(self.status)


@dataclass(frozen=True)
class TranscriptPayload:
    """Canonical transcript content and provenance metadata."""

    video_id: str
    language: str | None
    language_code: str | None
    is_generated: bool | None
    is_translated: bool
    source_language: str | None
    source_language_code: str | None
    source: str
    selection_strategy: str
    text: str
    segments: tuple[dict[str, Any], ...]
    segment_count: int
    word_count: int
    available_languages: tuple[dict[str, Any], ...]
    covered_duration_seconds: float
    collected_at: datetime
    requested_language: str | None = None
    requested_language_code: str | None = None
    content_version: str = ""
    model: str | None = None
    fallback_reason: str | None = None
    prompt_version: str | None = None
    generated_by_model: bool = False
    warnings: tuple[str, ...] = ()
    generation_type_override: str | None = None

    def __post_init__(self) -> None:
        requested_code = _normalize_language_code(self.requested_language_code)
        if not requested_code and self.requested_language:
            requested_code = _normalize_language_code(self.requested_language)
        object.__setattr__(self, "requested_language_code", requested_code or None)
        if not self.content_version:
            object.__setattr__(
                self,
                "content_version",
                transcript_content_version(
                    video_id=self.video_id,
                    language_code=self.language_code,
                    text=self.text,
                    segments=self.segments,
                ),
            )

    @property
    def transcript_text(self) -> str:
        return self.text

    @property
    def duration_seconds(self) -> float:
        return self.covered_duration_seconds

    @property
    def has_auto_captions(self) -> bool | None:
        return self.is_generated

    @property
    def generation_type(self) -> str | None:
        if self.generation_type_override:
            return self.generation_type_override
        if self.is_generated is None:
            return None
        return "automatic" if self.is_generated else "manual"

    @property
    def segments_json(self) -> str:
        return safe_json_dumps(self.segments)

    def to_dict(self) -> dict[str, Any]:
        return sanitize_json_value(asdict(self))


@dataclass(frozen=True)
class _TrackChoice:
    track: Any
    strategy: str
    is_fallback: bool


@dataclass(frozen=True)
class TranscriptRequest:
    """Provider-neutral request and bounded fallback context."""

    video_id: str
    requested_language_code: str = "en"
    attempt_count: int = 1
    max_primary_attempts: int = 1
    duration_seconds: float | None = None
    video_availability: str | None = None
    source_content_version: str | None = None

    @property
    def youtube_url(self) -> str:
        return f"https://www.youtube.com/watch?v={self.video_id}"


class TranscriptProvider(Protocol):
    """Minimal boundary implemented by transcript sources and test fakes."""

    name: str

    def fetch(self, request: TranscriptRequest) -> OperationResult[TranscriptPayload]: ...


class YouTubeTranscriptProvider:
    """Adapter retaining ``youtube-transcript-api`` as the primary source."""

    name = TRANSCRIPT_SOURCE

    def __init__(
        self,
        fetcher: Callable[..., OperationResult[TranscriptPayload]] | None = None,
    ) -> None:
        self._fetcher = fetcher or fetch_transcript

    def fetch(self, request: TranscriptRequest) -> OperationResult[TranscriptPayload]:
        return self._fetcher(
            request.video_id,
            preferred_languages=[request.requested_language_code],
            require_preferred_language=True,
            attempt_count=request.attempt_count,
        )


@dataclass(frozen=True)
class TranscriptChainResult:
    """Final result plus independent evidence from each attempted provider."""

    final_result: OperationResult[TranscriptPayload]
    primary_result: OperationResult[TranscriptPayload]
    fallback_result: OperationResult[TranscriptPayload] | None = None
    fallback_reason: str | None = None

    @property
    def used_fallback(self) -> bool:
        return self.fallback_result is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "final_result": self.final_result.to_dict(),
            "primary_result": self.primary_result.to_dict(),
            "fallback_result": (
                self.fallback_result.to_dict() if self.fallback_result else None
            ),
            "fallback_reason": self.fallback_reason,
        }


_PUBLIC_VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
_FALLBACK_IMMEDIATE_CODES = frozenset(
    {
        "no_transcript_found",
        "transcripts_disabled",
        "preferred_language_not_available",
        "ip_blocked",
        "request_blocked",
    }
)
_FALLBACK_NEVER_CODES = frozenset(
    {
        "dependency_missing",
        "invalid_video_id",
        "missing_video_id",
        "video_deleted",
        "video_private",
        "video_unavailable",
        "video_unplayable",
        "age_restricted",
    }
)


def fallback_reason_for_primary(
    result: OperationResult[TranscriptPayload],
    request: TranscriptRequest,
    *,
    primary_circuit_open: bool = False,
) -> str | None:
    """Return a stable fallback reason only after the primary retry policy allows it."""

    if not _PUBLIC_VIDEO_ID.fullmatch(str(request.video_id or "").strip()):
        return None
    availability = str(request.video_availability or "").strip().lower()
    if availability and availability not in {"public", "available"}:
        return None
    code = str(result.error_code or "").strip().lower()
    if code in _FALLBACK_NEVER_CODES:
        return None
    if primary_circuit_open:
        return "primary_circuit_open"
    if code in _FALLBACK_IMMEDIATE_CODES:
        return code
    if result.status in {STATUS_FAILED, STATUS_RATE_LIMITED} and (
        request.attempt_count >= max(1, request.max_primary_attempts)
    ):
        return code or "primary_retry_exhausted"
    return None


class TranscriptProviderChain:
    """Deterministic primary/fallback orchestration with no hidden retries."""

    def __init__(
        self,
        primary: TranscriptProvider,
        fallback: TranscriptProvider | None,
        *,
        fallback_enabled: bool,
    ) -> None:
        self.primary = primary
        self.fallback = fallback
        self.fallback_enabled = fallback_enabled

    def collect(
        self,
        request: TranscriptRequest,
        *,
        primary_circuit_open: bool = False,
    ) -> TranscriptChainResult:
        if primary_circuit_open:
            now = utc_now()
            primary = OperationResult.rate_limited(
                error_code="ip_blocked",
                error_message="The primary transcript circuit breaker is open",
                attempt_count=0,
                started_at=now,
                completed_at=now,
            )
        else:
            primary = self.primary.fetch(request)

        if primary.payload and primary.payload.text.strip():
            return TranscriptChainResult(primary, primary)

        reason = fallback_reason_for_primary(
            primary,
            request,
            primary_circuit_open=primary_circuit_open,
        )
        if not reason or not self.fallback_enabled or self.fallback is None:
            return TranscriptChainResult(primary, primary, fallback_reason=reason)

        readiness = getattr(self.fallback, "readiness", None)
        if callable(readiness):
            ready, skip_reason = readiness(request)
            if not ready:
                if skip_reason in {
                    "gemini_fallback_disabled",
                    "gemini_api_key_missing",
                }:
                    return TranscriptChainResult(primary, primary, fallback_reason=reason)
                skipped = self.fallback.fetch(request)
                return TranscriptChainResult(
                    skipped,
                    primary,
                    fallback_result=skipped,
                    fallback_reason=reason,
                )

        fallback = self.fallback.fetch(request)
        if fallback.payload is not None:
            payload = fallback.payload
            if not payload.fallback_reason:
                object.__setattr__(payload, "fallback_reason", reason)
        return TranscriptChainResult(
            fallback,
            primary,
            fallback_result=fallback,
            fallback_reason=reason,
        )


def _snake_case(value: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()


def _exception_status_code(error: BaseException) -> int | None:
    candidates = [
        getattr(error, "status_code", None),
        getattr(error, "http_status", None),
        getattr(getattr(error, "response", None), "status_code", None),
    ]
    for value in candidates:
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def classify_transcript_error(error: BaseException) -> TranscriptErrorClassification:
    """Map library and transport failures to stable retry semantics."""

    names = {
        base.__name__.lower()
        for base in type(error).__mro__
        if base not in {BaseException, Exception, object}
    }
    compact_names = {re.sub(r"[^a-z0-9]", "", name) for name in names}
    message = sanitize_error_message(error)
    message_lower = (message or "").lower()
    status_code = _exception_status_code(error)

    if "modulenotfounderror" in compact_names or "importerror" in compact_names:
        return TranscriptErrorClassification(
            STATUS_FAILED,
            "dependency_missing",
            message,
        )

    if "transcriptsdisabled" in compact_names or "transcript disabled" in message_lower:
        return TranscriptErrorClassification(
            STATUS_DISABLED,
            "transcripts_disabled",
            message,
        )

    rate_limited_codes = {
        "ipblocked": "ip_blocked",
        "requestblocked": "request_blocked",
        "toomanyrequests": "rate_limited",
        "ratelimiterror": "rate_limited",
    }
    for name, code in rate_limited_codes.items():
        if name in compact_names:
            return TranscriptErrorClassification(STATUS_RATE_LIMITED, code, message)
    if status_code == 429 or any(
        phrase in message_lower
        for phrase in (
            "too many requests",
            "rate limit",
            "request is blocked",
            "request blocked",
            "ip has been blocked",
        )
    ):
        return TranscriptErrorClassification(
            STATUS_RATE_LIMITED,
            "rate_limited",
            message,
        )

    unavailable_codes = {
        "notranscriptfound": "no_transcript_found",
        "videounavailable": "video_unavailable",
        "videounplayable": "video_unplayable",
        "agerestricted": "age_restricted",
        "invalidvideoid": "invalid_video_id",
    }
    for name, code in unavailable_codes.items():
        if name in compact_names:
            return TranscriptErrorClassification(STATUS_NOT_AVAILABLE, code, message)
    if any(
        phrase in message_lower
        for phrase in (
            "no transcript was found",
            "no transcripts were found",
            "private video",
            "video is unavailable",
            "video unavailable",
            "age restricted",
        )
    ):
        return TranscriptErrorClassification(
            STATUS_NOT_AVAILABLE,
            "transcript_not_available",
            message,
        )

    error_code = _snake_case(type(error).__name__) or "transcript_fetch_failed"
    if status_code is not None:
        error_code = f"http_{status_code}"
    return TranscriptErrorClassification(STATUS_FAILED, error_code, message)


def _default_api_factory() -> Any:
    from youtube_transcript_api import YouTubeTranscriptApi

    return YouTubeTranscriptApi()


def _normalize_language_code(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _language_match_quality(language_code: str, preferred: str) -> int | None:
    code = _normalize_language_code(language_code)
    target = _normalize_language_code(preferred)
    if not code or not target:
        return None
    if code == target:
        return 0
    if code.split("-", 1)[0] == target.split("-", 1)[0]:
        return 1
    return None


def _preferred_key(
    track: Any, preferred_languages: Sequence[str]
) -> tuple[int, int, str, str] | None:
    language_code = _normalize_language_code(getattr(track, "language_code", None))
    matches = []
    for index, preferred in enumerate(preferred_languages):
        quality = _language_match_quality(language_code, preferred)
        if quality is not None:
            matches.append((index, quality))
    if not matches:
        return None
    index, quality = min(matches)
    return (
        index,
        quality,
        language_code,
        str(getattr(track, "language", "") or "").casefold(),
    )


def _fallback_key(track: Any) -> tuple[str, str, int]:
    return (
        _normalize_language_code(getattr(track, "language_code", None)),
        str(getattr(track, "language", "") or "").casefold(),
        int(bool(getattr(track, "is_generated", False))),
    )


def _select_track(
    tracks: Sequence[Any],
    preferred_languages: Sequence[str],
    *,
    require_preferred_language: bool = False,
) -> _TrackChoice | None:
    manual = [track for track in tracks if not bool(getattr(track, "is_generated", False))]
    generated = [track for track in tracks if bool(getattr(track, "is_generated", False))]

    for candidates, strategy in (
        (manual, "manual_preferred"),
        (generated, "generated_preferred"),
    ):
        preferred_candidates = [
            (key, track)
            for track in candidates
            if (key := _preferred_key(track, preferred_languages)) is not None
        ]
        if preferred_candidates:
            _, selected = min(preferred_candidates, key=lambda item: item[0])
            return _TrackChoice(selected, strategy, False)

    if require_preferred_language:
        for candidates, strategy in (
            (manual, "manual_translation_preferred"),
            (generated, "generated_translation_preferred"),
        ):
            translatable_candidates = [
                track
                for track in candidates
                if any(_can_translate(track, language) for language in preferred_languages)
            ]
            if translatable_candidates:
                return _TrackChoice(
                    min(translatable_candidates, key=_fallback_key),
                    strategy,
                    True,
                )
        return None

    if manual:
        return _TrackChoice(min(manual, key=_fallback_key), "manual_fallback", True)
    if generated:
        return _TrackChoice(
            min(generated, key=_fallback_key),
            "generated_fallback",
            True,
        )
    return None


def _translation_languages(track: Any) -> list[dict[str, str | None]]:
    result = []
    for item in getattr(track, "translation_languages", None) or []:
        if isinstance(item, Mapping):
            language = item.get("language")
            language_code = item.get("language_code")
        else:
            language = getattr(item, "language", None)
            language_code = getattr(item, "language_code", None)
        result.append(
            {
                "language": str(language) if language is not None else None,
                "language_code": (str(language_code) if language_code is not None else None),
            }
        )
    return sorted(
        result,
        key=lambda item: (
            _normalize_language_code(item["language_code"]),
            str(item["language"] or "").casefold(),
        ),
    )


def _track_descriptor(track: Any) -> dict[str, Any]:
    return {
        "language": getattr(track, "language", None),
        "language_code": getattr(track, "language_code", None),
        "is_generated": bool(getattr(track, "is_generated", False)),
        "is_translatable": bool(getattr(track, "is_translatable", False)),
        "translation_languages": _translation_languages(track),
    }


def _available_languages(tracks: Sequence[Any]) -> tuple[dict[str, Any], ...]:
    descriptors = [_track_descriptor(track) for track in tracks]
    descriptors.sort(
        key=lambda item: (
            _normalize_language_code(item["language_code"]),
            bool(item["is_generated"]),
            str(item["language"] or "").casefold(),
        )
    )
    return tuple(descriptors)


def _can_translate(track: Any, target_language: str) -> bool:
    if not bool(getattr(track, "is_translatable", False)):
        return False
    available = _translation_languages(track)
    if not available:
        return True
    return any(
        _language_match_quality(item["language_code"] or "", target_language) is not None
        for item in available
    )


def _translation_target(track: Any, requested_language: str) -> str:
    """Resolve a regional preference to the exact code accepted by the track."""

    requested = _normalize_language_code(requested_language)
    candidates = []
    for item in _translation_languages(track):
        language_code = _normalize_language_code(item["language_code"])
        quality = _language_match_quality(language_code, requested)
        if quality is not None:
            candidates.append((quality, language_code))
    return min(candidates)[1] if candidates else requested


def _preferred_translation_target(
    track: Any,
    preferred_languages: Sequence[str],
) -> str | None:
    for language in preferred_languages:
        if _can_translate(track, language):
            return _translation_target(track, language)
    return None


def _float_or_zero(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) and result >= 0 else 0.0


def _segment_value(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(name, default)
    return getattr(item, name, default)


def _raw_segments(fetched: Any) -> Iterable[Any]:
    to_raw_data = getattr(fetched, "to_raw_data", None)
    if callable(to_raw_data):
        return to_raw_data()
    return fetched or ()


def _clean_segments(fetched: Any) -> tuple[dict[str, Any], ...]:
    result = []
    for item in _raw_segments(fetched):
        text = " ".join(str(_segment_value(item, "text", "") or "").split())
        if not text:
            continue
        start = _float_or_zero(_segment_value(item, "start", 0.0))
        duration = _float_or_zero(_segment_value(item, "duration", 0.0))
        result.append(
            {
                "text": text,
                "start": start,
                "duration": duration,
                "end": start + duration,
            }
        )
    return tuple(result)


def _covered_duration(segments: Sequence[Mapping[str, Any]]) -> float:
    intervals = sorted(
        (
            _float_or_zero(segment.get("start")),
            _float_or_zero(segment.get("end")),
        )
        for segment in segments
        if _float_or_zero(segment.get("end")) > _float_or_zero(segment.get("start"))
    )
    if not intervals:
        return 0.0
    total = 0.0
    current_start, current_end = intervals[0]
    for start, end in intervals[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
        else:
            total += current_end - current_start
            current_start, current_end = start, end
    return total + current_end - current_start


def _completed(clock: Callable[[], datetime]) -> datetime:
    return ensure_utc(clock()) or utc_now()


def _result_from_error(
    error: BaseException,
    *,
    attempt_count: int,
    started_at: datetime,
    completed_at: datetime,
) -> OperationResult[TranscriptPayload]:
    classification = classify_transcript_error(error)
    if classification.status == STATUS_DISABLED:
        return OperationResult.disabled(
            error_code=classification.error_code,
            error_message=classification.error_message,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )
    if classification.status == STATUS_NOT_AVAILABLE:
        return OperationResult.unavailable(
            error_code=classification.error_code,
            error_message=classification.error_message,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )
    if classification.status == STATUS_RATE_LIMITED:
        return OperationResult.rate_limited(
            error_code=classification.error_code,
            error_message=classification.error_message,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )
    return OperationResult.failed(
        error_code=classification.error_code,
        error_message=classification.error_message,
        attempt_count=attempt_count,
        started_at=started_at,
        completed_at=completed_at,
    )


def fetch_transcript(
    video_id: str,
    preferred_languages: Sequence[str] = ("en",),
    *,
    api: Any | None = None,
    api_factory: Callable[[], Any] | None = None,
    allow_translation: bool = True,
    translation_language: str | None = None,
    require_preferred_language: bool = False,
    attempt_count: int = 1,
    clock: Callable[[], datetime] | None = None,
) -> OperationResult[TranscriptPayload]:
    """Fetch one transcript using a deterministic, provenance-preserving strategy."""

    if api is not None and api_factory is not None:
        raise ValueError("Pass api or api_factory, not both")
    normalized_video_id = str(video_id or "").strip()
    if not normalized_video_id:
        raise ValueError("video_id is required")
    languages = tuple(
        dict.fromkeys(
            _normalize_language_code(language)
            for language in preferred_languages
            if _normalize_language_code(language)
        )
    )
    now = clock or utc_now
    started_at = _completed(now)

    try:
        transcript_api = api or (api_factory or _default_api_factory)()
        tracks = tuple(transcript_api.list(normalized_video_id))
    except Exception as error:
        return _result_from_error(
            error,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=_completed(now),
        )

    choice = _select_track(
        tracks,
        languages,
        require_preferred_language=require_preferred_language,
    )
    if choice is None:
        completed_at = _completed(now)
        return OperationResult.unavailable(
            error_code=(
                "preferred_language_not_available"
                if require_preferred_language
                else "no_transcript_found"
            ),
            error_message=(
                f"No transcript in the preferred languages was available for {normalized_video_id}"
                if require_preferred_language
                else f"No transcript tracks were returned for {normalized_video_id}"
            ),
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )

    source_track = choice.track
    selected_track = source_track
    selection_strategy = choice.strategy
    is_translated = False
    translation_error: BaseException | None = None
    target_language: str | None = (
        _normalize_language_code(translation_language or (languages[0] if languages else ""))
        or None
    )

    if choice.is_fallback and require_preferred_language:
        target_language = _preferred_translation_target(source_track, languages)
        if not allow_translation or not target_language:
            completed_at = _completed(now)
            return OperationResult.unavailable(
                error_code="preferred_language_not_available",
                error_message=(
                    f"No transcript in the preferred languages was available for "
                    f"{normalized_video_id}"
                ),
                attempt_count=attempt_count,
                started_at=started_at,
                completed_at=completed_at,
            )
        try:
            selected_track = source_track.translate(target_language)
            is_translated = True
            selection_strategy = f"translated_{choice.strategy}"
        except Exception as error:
            return _result_from_error(
                error,
                attempt_count=attempt_count,
                started_at=started_at,
                completed_at=_completed(now),
            )
    elif (
        choice.is_fallback
        and allow_translation
        and target_language
        and _can_translate(source_track, target_language)
    ):
        try:
            selected_track = source_track.translate(
                _translation_target(source_track, target_language)
            )
            is_translated = True
            selection_strategy = f"translated_{choice.strategy}"
        except Exception as error:
            translation_error = error

    try:
        fetched = selected_track.fetch()
    except Exception as error:
        if is_translated:
            if require_preferred_language:
                return _result_from_error(
                    error,
                    attempt_count=attempt_count,
                    started_at=started_at,
                    completed_at=_completed(now),
                )
            translation_error = error
            selected_track = source_track
            is_translated = False
            selection_strategy = choice.strategy
            try:
                fetched = selected_track.fetch()
            except Exception as fallback_error:
                return _result_from_error(
                    fallback_error,
                    attempt_count=attempt_count,
                    started_at=started_at,
                    completed_at=_completed(now),
                )
        else:
            return _result_from_error(
                error,
                attempt_count=attempt_count,
                started_at=started_at,
                completed_at=_completed(now),
            )

    completed_at = _completed(now)
    segments = _clean_segments(fetched)
    transcript_text = " ".join(segment["text"] for segment in segments)
    payload = TranscriptPayload(
        video_id=normalized_video_id,
        language=(getattr(selected_track, "language", None) or getattr(fetched, "language", None)),
        language_code=(
            getattr(selected_track, "language_code", None)
            or getattr(fetched, "language_code", None)
        ),
        is_generated=bool(getattr(source_track, "is_generated", False)),
        is_translated=is_translated,
        source_language=getattr(source_track, "language", None),
        source_language_code=getattr(source_track, "language_code", None),
        source=TRANSCRIPT_SOURCE,
        selection_strategy=selection_strategy,
        text=transcript_text,
        segments=segments,
        segment_count=len(segments),
        word_count=len(transcript_text.split()),
        available_languages=_available_languages(tracks),
        covered_duration_seconds=_covered_duration(segments),
        collected_at=completed_at,
        requested_language=(languages[0] if languages else None),
        requested_language_code=(languages[0] if languages else None),
    )

    if not segments:
        return OperationResult.partial(
            payload,
            error_code="empty_transcript",
            error_message="The selected transcript did not contain usable segments",
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )
    if translation_error is not None:
        classification = classify_transcript_error(translation_error)
        return OperationResult.partial(
            payload,
            error_code=f"translation_{classification.error_code}",
            error_message=classification.error_message,
            attempt_count=attempt_count,
            started_at=started_at,
            completed_at=completed_at,
        )
    return OperationResult.success(
        payload,
        attempt_count=attempt_count,
        started_at=started_at,
        completed_at=completed_at,
    )


fetch_youtube_transcript = fetch_transcript
