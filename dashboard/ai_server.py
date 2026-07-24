"""HTTP client for the remote AI inference server."""
from __future__ import annotations

import os
from typing import Any

import requests


class AIServerError(RuntimeError):
    """Raised when the AI server cannot return a usable response."""


def _base_url() -> str:
    return os.getenv("DASHBOARD_AI_SERVER_URL", "http://ai-server:8000").rstrip("/")


def _timeout() -> float:
    try:
        timeout = float(os.getenv("DASHBOARD_AI_SERVER_TIMEOUT_SECONDS", "30"))
    except ValueError:
        timeout = 30.0
    return timeout if timeout > 0 else 30.0


def _headers() -> dict[str, str]:
    token = os.getenv("DASHBOARD_AI_SERVER_TOKEN", "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _request(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    try:
        response = requests.request(
            method,
            f"{_base_url()}{path}",
            headers=_headers(),
            timeout=_timeout(),
            **kwargs,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise AIServerError(f"AI server request failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise AIServerError("AI server returned an invalid response")
    return payload


def get_ai_server_health() -> dict[str, Any]:
    return _request("GET", "/health")


def predict_post(
    text: str,
    source: str = "",
    audience: float | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"text": text, "source": source}
    if audience is not None:
        payload["audience"] = audience
    return _request("POST", "/predict", json=payload)
