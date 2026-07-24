import unittest
from unittest.mock import Mock, patch

from dashboard.ai_server import AIServerError, get_ai_server_health, predict_post


class AIServerClientTests(unittest.TestCase):
    @patch("dashboard.ai_server.requests.request")
    def test_prediction_uses_configured_server_and_token(self, request):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"viral_score": 0.8}
        request.return_value = response

        with patch.dict(
            "os.environ",
            {
                "DASHBOARD_AI_SERVER_URL": "https://ml.internal/",
                "DASHBOARD_AI_SERVER_TOKEN": "secret",
                "DASHBOARD_AI_SERVER_TIMEOUT_SECONDS": "12",
            },
            clear=False,
        ):
            result = predict_post("EV launch", "youtube", 100)

        self.assertEqual(result, {"viral_score": 0.8})
        request.assert_called_once_with(
            "POST",
            "https://ml.internal/predict",
            headers={"Authorization": "Bearer secret"},
            timeout=12.0,
            json={"text": "EV launch", "source": "youtube", "audience": 100},
        )

    @patch("dashboard.ai_server.requests.request")
    def test_health_uses_internal_default(self, request):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"status": "ok", "model_loaded": False}
        request.return_value = response

        with patch.dict("os.environ", {}, clear=True):
            result = get_ai_server_health()

        self.assertFalse(result["model_loaded"])
        self.assertEqual(request.call_args.args[:2], ("GET", "http://ai-server:8000/health"))

    @patch("dashboard.ai_server.requests.request")
    def test_invalid_timeout_falls_back_to_default(self, request):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"status": "ok"}
        request.return_value = response

        with patch.dict(
            "os.environ",
            {"DASHBOARD_AI_SERVER_TIMEOUT_SECONDS": "invalid"},
            clear=False,
        ):
            get_ai_server_health()

        self.assertEqual(request.call_args.kwargs["timeout"], 30.0)

    @patch("dashboard.ai_server.requests.request")
    def test_non_object_response_is_rejected(self, request):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = []
        request.return_value = response

        with self.assertRaisesRegex(AIServerError, "invalid response"):
            get_ai_server_health()


if __name__ == "__main__":
    unittest.main()
