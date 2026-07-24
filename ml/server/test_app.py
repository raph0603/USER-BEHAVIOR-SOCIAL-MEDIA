"""Smoke tests for the AI-server - run without the real model (predict_fn stubbed).

    pip install fastapi httpx pytest
    pytest ml/server/test_app.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
import app as app_module  # noqa: E402


def _fake_predict(text: str, source: str, audience):
    return {
        "viral_score": 0.671,
        "label": "viral-likely",
        "confidence": 0.342,
        "top_factors": [{"feature": "content_score", "label": "Post content/topic",
                         "value": 0.74, "contribution": 0.81, "direction": "up"}],
        "explanation_text": f"stub for source={source!r}",
        "suggestions": ["Add a clear call to action (CTA)."],
    }


app_module.predict_fn = _fake_predict
client = TestClient(app_module.app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_predict_ok():
    r = client.post("/predict", json={"text": "great EV deal", "source": "youtube"})
    assert r.status_code == 200
    body = r.json()
    assert body["label"] == "viral-likely"
    assert 0.0 <= body["viral_score"] <= 1.0
    assert "source='youtube'" in body["explanation_text"]


def test_prediction_requires_configured_bearer_token(monkeypatch):
    monkeypatch.setenv("AI_SERVER_TOKEN", "server-secret")

    unauthenticated = client.post("/predict", json={"text": "hello", "source": "x"})
    authenticated = client.post(
        "/predict",
        json={"text": "hello", "source": "x"},
        headers={"Authorization": "Bearer server-secret"},
    )

    assert unauthenticated.status_code == 401
    assert unauthenticated.headers["www-authenticate"] == "Bearer"
    assert authenticated.status_code == 200


def test_health_remains_available_without_authentication(monkeypatch):
    monkeypatch.setenv("AI_SERVER_TOKEN", "server-secret")

    response = client.get("/health")

    assert response.status_code == 200


def test_predict_bad_source():
    r = client.post("/predict", json={"text": "hi", "source": "tiktok"})
    assert r.status_code == 422


def test_predict_empty_text():
    r = client.post("/predict", json={"text": "", "source": "x"})
    assert r.status_code == 422  # pydantic min_length


def test_batch():
    r = client.post("/predict/batch", json={"items": [
        {"text": "a", "source": "x"},
        {"text": "b", "source": "reddit"},
    ]})
    assert r.status_code == 200
    assert len(r.json()) == 2


def test_report_with_template_backend():
    app_module.REPORT_BACKEND = "template"
    r = client.post(
        "/report",
        json={"text": "great EV deal", "source": "youtube", "lang": "en"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["prediction"]["label"] == "viral-likely"
    assert "Viral likelihood" in body["report"]


def test_model_missing_returns_503():
    def _raise(*a, **k):
        raise FileNotFoundError("stage1_multisource.joblib")
    app_module.predict_fn = _raise
    r = client.post("/predict", json={"text": "x", "source": "x"})
    assert r.status_code == 503
    app_module.predict_fn = _fake_predict  # restore
