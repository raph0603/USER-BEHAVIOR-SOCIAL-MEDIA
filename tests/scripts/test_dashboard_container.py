import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class DashboardContainerTests(unittest.TestCase):
    def test_dashboard_has_container_entrypoint(self):
        dockerfile = (ROOT / "dashboard" / "Dockerfile").read_text(
            encoding="utf-8"
        )

        self.assertIn("FROM python:3.12-slim", dockerfile)
        self.assertIn("pip install --no-cache-dir -r requirements.txt", dockerfile)
        self.assertIn("streamlit", dockerfile)
        self.assertIn("--server.address=0.0.0.0", dockerfile)
        self.assertIn("--server.headless=true", dockerfile)

    def test_compose_starts_dashboard_with_internal_service_urls(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("dashboard:", compose)
        self.assertIn("context: ./dashboard", compose)
        self.assertIn("8501:8501", compose)
        self.assertIn("DASHBOARD_MINIO_ENDPOINT=http://minio:9000", compose)
        self.assertIn(
            "DASHBOARD_AIRFLOW_URL=http://airflow-webserver:8080",
            compose,
        )
        self.assertIn(
            "DASHBOARD_BALANCING_REPORT_PATH=/app/balancing/report.json",
            compose,
        )
        self.assertIn(
            "${HOST_PROJECT_DIR:-.}/data/balancing:/app/balancing:ro",
            compose,
        )


if __name__ == "__main__":
    unittest.main()
