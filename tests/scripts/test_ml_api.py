import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from ml.service import create_app


RESULT = {
    "viral_score": 0.72,
    "label": "viral-likely",
    "confidence": 0.44,
    "top_factors": [
        {
            "feature": "content_score",
            "label": "Post content/topic",
            "value": 0.8,
            "contribution": 0.5,
            "direction": "up",
        }
    ],
    "explanation_text": "Likely viral.",
    "suggestions": ["Add proof."],
}


class FakePredictor:
    def explain(self, text, source="", audience=None):
        return {**RESULT, "explanation_text": f"{source}:{text}:{audience}"}


class MLAPITests(unittest.TestCase):
    def test_health_and_prediction_use_loaded_model(self):
        app = create_app(lambda: FakePredictor())
        with TestClient(app) as client:
            self.assertEqual(
                client.get("/health").json(),
                {"status": "ok", "model": "ready"},
            )
            response = client.post(
                "/predict",
                json={"text": "EV launch", "source": "youtube", "audience": 42},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["viral_score"], 0.72)
        self.assertEqual(response.json()["explanation_text"], "youtube:EV launch:42.0")

    def test_batch_prediction_preserves_item_order(self):
        app = create_app(lambda: FakePredictor())
        with TestClient(app) as client:
            response = client.post(
                "/predict/batch",
                json={
                    "items": [
                        {"text": "first", "source": "x"},
                        {"text": "second", "source": "reddit"},
                    ]
                },
            )

        self.assertEqual(response.status_code, 200)
        explanations = [
            item["explanation_text"] for item in response.json()["items"]
        ]
        self.assertEqual(explanations, ["x:first:None", "reddit:second:None"])

    def test_unavailable_model_returns_service_unavailable(self):
        def fail_loader():
            raise FileNotFoundError("model missing")

        app = create_app(fail_loader)
        with TestClient(app) as client:
            health = client.get("/health")
            prediction = client.post("/predict", json={"text": "test"})

        self.assertEqual(health.status_code, 503)
        self.assertEqual(prediction.status_code, 503)
        self.assertEqual(prediction.json()["detail"], "ML model is not ready")

    def test_optional_bearer_token_is_enforced(self):
        app = create_app(lambda: FakePredictor())
        with patch.dict(os.environ, {"ML_API_TOKEN": "secret"}, clear=False):
            with TestClient(app) as client:
                denied = client.get("/health")
                allowed = client.get(
                    "/health",
                    headers={"Authorization": "Bearer secret"},
                )

        self.assertEqual(denied.status_code, 401)
        self.assertEqual(allowed.status_code, 200)

    def test_invalid_source_is_rejected(self):
        app = create_app(lambda: FakePredictor())
        with TestClient(app) as client:
            response = client.post(
                "/predict",
                json={"text": "test", "source": "instagram"},
            )

        self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
