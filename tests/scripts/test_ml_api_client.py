import unittest
from unittest.mock import Mock, patch

from dashboard.ml_api import get_ml_health, predict_post


class MLAPIClientTests(unittest.TestCase):
    @patch("dashboard.ml_api.requests.request")
    def test_prediction_uses_configured_api_and_token(self, request):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"viral_score": 0.8}
        request.return_value = response

        with patch.dict(
            "os.environ",
            {
                "DASHBOARD_ML_API_URL": "https://ml.internal/",
                "DASHBOARD_ML_API_TOKEN": "secret",
                "DASHBOARD_ML_API_TIMEOUT_SECONDS": "12",
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

    @patch("dashboard.ml_api.requests.request")
    def test_health_uses_internal_default(self, request):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"status": "ok", "model": "ready"}
        request.return_value = response

        with patch.dict("os.environ", {}, clear=True):
            result = get_ml_health()

        self.assertEqual(result["model"], "ready")
        self.assertEqual(request.call_args.args[:2], ("GET", "http://ml-api:8000/health"))


if __name__ == "__main__":
    unittest.main()
