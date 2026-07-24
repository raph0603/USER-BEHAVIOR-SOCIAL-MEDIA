"""Client for the internal ML inference API."""
from __future__ import annotations

import os
from typing import Any

import requests


class MLAPIError(RuntimeError):
    """Raised when the ML API cannot return a usable response."""


def _base_url() -> str:
    return os.getenv("DASHBOARD_ML_API_URL", "http://ml-api:8000").rstrip("/")


def _timeout() -> float:
    return float(os.getenv("DASHBOARD_ML_API_TIMEOUT_SECONDS", "30"))


def _headers() -> dict[str, str]:
    token = os.getenv("DASHBOARD_ML_API_TOKEN", "")
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
        raise MLAPIError(f"ML API request failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLAPIError("ML API returned an invalid response")
    return payload


def get_ml_health() -> dict[str, Any]:
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
