"""Gemini transcript fallback using a public YouTube URL and structured output."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping

from .collection import OperationResult, sanitize_error_message, utc_now
from .transcripts import TranscriptPayload, TranscriptRequest, transcript_content_version


GEMINI_PROVIDER = "gemini"
GEMINI_SELECTION_STRATEGY = "gemini_youtube_url_fallback"
GEMINI_PROMPT_VERSION = "youtube-transcript-v1"

GEMINI_TRANSCRIPT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "detected_language",
        "text",
        "segments",
        "covered_duration_seconds",
        "warnings",
    ],
    "properties": {
        "detected_language": {"type": "string"},
        "text": {"type": "string"},
        "segments": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["start_seconds", "text"],
                "properties": {
                    "start_seconds": {"type": "number", "minimum": 0},
                    "end_seconds": {"type": ["number", "null"], "minimum": 0},
                    "text": {"type": "string"},
                },
            },
        },
        "covered_duration_seconds": {"type": "number", "minimum": 0},
        "warnings": {"type": "array", "items": {"type": "string"}},
    },
}

GEMINI_TRANSCRIPT_PROMPT = """Transcribe the spoken audio in this video faithfully.
Return only JSON matching the supplied schema. Do not summarize, paraphrase, or translate.
Keep the language actually spoken. Use [inaudible] when speech cannot be understood and
never invent ambiguous content. Provide timestamps for coherent segments only; do not
fabricate word-level timestamps. The full text must represent the segments in order.
The requested language preference is: {requested_language}.
"""


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except ValueError:
        return default


@dataclass(frozen=True)
class GeminiTranscriptConfig:
    """Bounded, environment-backed controls for the optional fallback."""

    api_key: str = ""
    enabled: bool = False
    model: str = "gemini-3.5-flash"
    max_attempts: int = 2
    timeout_seconds: int = 120
    max_duration_minutes: float = 60.0
    daily_video_minutes_budget: float = 120.0
    daily_request_budget: int = 20
    cooldown_seconds: int = 3600

    @classmethod
    def from_env(cls) -> "GeminiTranscriptConfig":
        return cls(
            api_key=os.getenv("GEMINI_API_KEY", "").strip(),
            enabled=_env_bool("GEMINI_TRANSCRIPT_FALLBACK_ENABLED"),
            model=os.getenv("GEMINI_TRANSCRIPT_MODEL", "gemini-3.5-flash").strip()
            or "gemini-3.5-flash",
            max_attempts=_env_int("GEMINI_TRANSCRIPT_MAX_ATTEMPTS", 2),
            timeout_seconds=_env_int("GEMINI_TRANSCRIPT_TIMEOUT_SECONDS", 120),
            max_duration_minutes=_env_float("GEMINI_TRANSCRIPT_MAX_DURATION_MINUTES", 60.0),
            daily_video_minutes_budget=_env_float(
                "GEMINI_TRANSCRIPT_DAILY_VIDEO_MINUTES_BUDGET", 120.0
            ),
            daily_request_budget=_env_int("GEMINI_TRANSCRIPT_DAILY_REQUEST_BUDGET", 20),
            cooldown_seconds=_env_int("GEMINI_TRANSCRIPT_COOLDOWN_SECONDS", 3600),
        )


def gemini_cache_key(request: TranscriptRequest, *, model: str) -> str:
    identity = "\u001f".join(
        (
            request.video_id,
            request.requested_language_code,
            model,
            GEMINI_PROMPT_VERSION,
            request.source_content_version or "unknown",
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _finite_nonnegative(value: Any, *, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return number


def _validated_response(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("Gemini returned invalid JSON") from exc
    if not isinstance(value, Mapping):
        raise ValueError("Gemini response must be a JSON object")
    expected = {
        "detected_language",
        "text",
        "segments",
        "covered_duration_seconds",
        "warnings",
    }
    if set(value) != expected:
        raise ValueError("Gemini response contains missing or unexpected fields")
    language = str(value.get("detected_language") or "").strip()
    text = " ".join(str(value.get("text") or "").split())
    if not language or not text:
        raise ValueError("Gemini response must contain a language and transcript text")
    warnings = value.get("warnings")
    if not isinstance(warnings, list) or not all(isinstance(item, str) for item in warnings):
        raise ValueError("Gemini warnings must be a string array")
    raw_segments = value.get("segments")
    if not isinstance(raw_segments, list) or not raw_segments:
        raise ValueError("Gemini response must contain transcript segments")
    segments = []
    for item in raw_segments:
        if not isinstance(item, Mapping) or set(item) - {
            "start_seconds",
            "end_seconds",
            "text",
        }:
            raise ValueError("Gemini segment has an invalid shape")
        segment_text = " ".join(str(item.get("text") or "").split())
        if not segment_text:
            raise ValueError("Gemini segment text cannot be empty")
        start = _finite_nonnegative(item.get("start_seconds"), name="segment start")
        end_value = item.get("end_seconds")
        end = None if end_value is None else _finite_nonnegative(end_value, name="segment end")
        if end is not None and end < start:
            raise ValueError("Gemini segment end precedes its start")
        segment = {"start": start, "text": segment_text}
        if end is not None:
            segment["end"] = end
            segment["duration"] = end - start
        segments.append(segment)
    covered = _finite_nonnegative(
        value.get("covered_duration_seconds"),
        name="covered duration",
    )
    return {
        "detected_language": language,
        "text": text,
        "segments": tuple(segments),
        "covered_duration_seconds": covered,
        "warnings": tuple(" ".join(item.split()) for item in warnings if item.strip()),
    }


def _default_client_factory(config: GeminiTranscriptConfig) -> Any:
    from google import genai
    from google.genai import types

    return genai.Client(
        api_key=config.api_key,
        http_options=types.HttpOptions(timeout=config.timeout_seconds * 1000),
    )


def _error_code(error: BaseException) -> tuple[str, str]:
    name = type(error).__name__.lower()
    message = (sanitize_error_message(error) or "").lower()
    status = getattr(error, "status_code", None) or getattr(error, "code", None)
    if isinstance(error, (ImportError, ModuleNotFoundError)):
        return "failed", "gemini_dependency_missing"
    if status == 429 or "rate limit" in message or "resource exhausted" in message:
        return "rate_limited", "gemini_rate_limited"
    if "timeout" in name or "timed out" in message or "deadline" in message:
        return "failed", "gemini_timeout"
    if status in {404, 503} or "model" in message and "unavailable" in message:
        return "failed", "gemini_model_unavailable"
    if status in {400, 403} and any(
        marker in message for marker in ("video", "youtube", "unsupported", "private")
    ):
        return "not_available", "gemini_video_not_supported"
    if status in {400, 401, 403}:
        return "failed", "gemini_permanent_error"
    return "failed", "gemini_retryable_error"


class GeminiTranscriptProvider:
    """Optional provider; callers inject cache/budget hooks in production and tests."""

    name = GEMINI_PROVIDER

    def __init__(
        self,
        config: GeminiTranscriptConfig | None = None,
        *,
        client_factory: Callable[[GeminiTranscriptConfig], Any] | None = None,
        cache_get: Callable[[str], TranscriptPayload | None] | None = None,
        cache_put: Callable[[str, TranscriptPayload, float], None] | None = None,
        used_video_minutes: Callable[[datetime], float] | None = None,
        used_requests: Callable[[datetime], int] | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.config = config or GeminiTranscriptConfig.from_env()
        self._client_factory = client_factory or _default_client_factory
        self._cache_get = cache_get or (lambda _key: None)
        self._cache_put = cache_put or (lambda _key, _payload, _minutes: None)
        self._used_video_minutes = used_video_minutes or (lambda _now: 0.0)
        self._used_requests = used_requests or (lambda _now: 0)
        self._clock = clock
        self.last_cache_hit = False

    def video_minutes(self, request: TranscriptRequest) -> float:
        return max(0.0, float(request.duration_seconds or 0.0)) / 60.0

    def readiness(self, request: TranscriptRequest) -> tuple[bool, str | None]:
        if not self.config.enabled:
            return False, "gemini_fallback_disabled"
        if not self.config.api_key:
            return False, "gemini_api_key_missing"
        if not re.fullmatch(r"[A-Za-z0-9_-]{11}", request.video_id):
            return False, "gemini_invalid_video_id"
        if request.video_availability and str(request.video_availability).lower() not in {
            "public",
            "available",
        }:
            return False, "gemini_video_not_public"
        try:
            duration_seconds = float(request.duration_seconds or 0)
        except (TypeError, ValueError):
            duration_seconds = 0
        if duration_seconds <= 0 or not math.isfinite(duration_seconds):
            return False, "gemini_duration_unknown"
        minutes = self.video_minutes(request)
        if minutes > self.config.max_duration_minutes:
            return False, "gemini_duration_limit_exceeded"
        if (
            self._used_video_minutes(self._clock()) + minutes
            > self.config.daily_video_minutes_budget
        ):
            return False, "gemini_budget_exhausted"
        if self._used_requests(self._clock()) >= self.config.daily_request_budget:
            return False, "gemini_request_budget_exhausted"
        return True, None

    def _payload(self, request: TranscriptRequest, response: Any) -> TranscriptPayload:
        value = _validated_response(response)
        language = value["detected_language"]
        content_version = transcript_content_version(
            video_id=request.video_id,
            language_code=language,
            text=value["text"],
            segments=value["segments"],
        )
        return TranscriptPayload(
            video_id=request.video_id,
            language=language,
            language_code=language,
            is_generated=None,
            is_translated=False,
            source_language=language,
            source_language_code=language,
            source=GEMINI_PROVIDER,
            selection_strategy=GEMINI_SELECTION_STRATEGY,
            text=value["text"],
            segments=value["segments"],
            segment_count=len(value["segments"]),
            word_count=len(value["text"].split()),
            available_languages=(),
            covered_duration_seconds=value["covered_duration_seconds"],
            collected_at=self._clock(),
            requested_language=request.requested_language_code,
            requested_language_code=request.requested_language_code,
            content_version=content_version,
            model=self.config.model,
            prompt_version=GEMINI_PROMPT_VERSION,
            generated_by_model=True,
            warnings=value["warnings"],
            generation_type_override="model_generated",
        )

    def fetch(self, request: TranscriptRequest) -> OperationResult[TranscriptPayload]:
        started_at = self._clock()
        ready, reason = self.readiness(request)
        if not ready:
            factory = {
                "gemini_budget_exhausted": OperationResult.rate_limited,
                "gemini_request_budget_exhausted": OperationResult.rate_limited,
                "gemini_video_not_public": OperationResult.unavailable,
                "gemini_duration_limit_exceeded": OperationResult.unavailable,
            }.get(reason or "", OperationResult.disabled)
            return factory(
                error_code=reason or "gemini_unavailable",
                attempt_count=0,
                started_at=started_at,
                completed_at=self._clock(),
            )

        key = gemini_cache_key(request, model=self.config.model)
        cached = self._cache_get(key)
        if cached is not None:
            self.last_cache_hit = True
            return OperationResult.success(
                cached,
                attempt_count=0,
                started_at=started_at,
                completed_at=self._clock(),
            )
        self.last_cache_hit = False

        client = None
        last_error: BaseException | None = None
        try:
            try:
                client = self._client_factory(self.config)
            except Exception as error:
                _, code = _error_code(error)
                return OperationResult.failed(
                    error_code=code,
                    error_message=str(error),
                    attempt_count=0,
                    started_at=started_at,
                    completed_at=self._clock(),
                )
            remaining_requests = max(
                0,
                self.config.daily_request_budget - self._used_requests(self._clock()),
            )
            video_minutes = self.video_minutes(request)
            remaining_video_minutes = max(
                0.0,
                self.config.daily_video_minutes_budget - self._used_video_minutes(self._clock()),
            )
            video_attempt_limit = math.floor((remaining_video_minutes + 1e-9) / video_minutes)
            attempt_limit = min(
                self.config.max_attempts,
                remaining_requests,
                video_attempt_limit,
            )
            if attempt_limit <= 0:
                return OperationResult.rate_limited(
                    error_code="gemini_request_budget_exhausted",
                    attempt_count=0,
                    started_at=started_at,
                    completed_at=self._clock(),
                )
            for attempt in range(1, attempt_limit + 1):
                try:
                    interaction = client.interactions.create(
                        model=self.config.model,
                        input=[
                            {"type": "video", "uri": request.youtube_url},
                            {
                                "type": "text",
                                "text": GEMINI_TRANSCRIPT_PROMPT.format(
                                    requested_language=request.requested_language_code
                                ),
                            },
                        ],
                        response_format={
                            "type": "text",
                            "mime_type": "application/json",
                            "schema": GEMINI_TRANSCRIPT_SCHEMA,
                        },
                    )
                    payload = self._payload(request, interaction.output_text)
                    self._cache_put(key, payload, self.video_minutes(request))
                    return OperationResult.success(
                        payload,
                        attempt_count=attempt,
                        started_at=started_at,
                        completed_at=self._clock(),
                    )
                except (ValueError, TypeError, json.JSONDecodeError) as error:
                    last_error = error
                    if attempt == self.config.max_attempts:
                        return OperationResult.failed(
                            error_code="gemini_invalid_response",
                            error_message=str(error),
                            attempt_count=attempt,
                            started_at=started_at,
                            completed_at=self._clock(),
                        )
                except Exception as error:
                    last_error = error
                    status, code = _error_code(error)
                    if attempt < attempt_limit and code in {
                        "gemini_timeout",
                        "gemini_model_unavailable",
                        "gemini_retryable_error",
                    }:
                        continue
                    factory = {
                        "rate_limited": OperationResult.rate_limited,
                        "not_available": OperationResult.unavailable,
                    }.get(status, OperationResult.failed)
                    return factory(
                        error_code=code,
                        error_message=str(error),
                        attempt_count=attempt,
                        started_at=started_at,
                        completed_at=self._clock(),
                    )
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

        return OperationResult.failed(
            error_code="gemini_retryable_error",
            error_message=str(last_error) if last_error else None,
            attempt_count=self.config.max_attempts,
            started_at=started_at,
            completed_at=self._clock(),
        )
