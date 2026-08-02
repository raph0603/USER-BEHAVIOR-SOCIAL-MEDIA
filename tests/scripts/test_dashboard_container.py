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
        self.assertIn("/usr/local/bin/airflow-jobs", dockerfile)
        self.assertIn("/usr/local/bin/data-transfer", dockerfile)
        self.assertIn("airflow_jobs_cli.py", dockerfile)
        self.assertIn("data_cli.py", dockerfile)
        self.assertIn("--server.address=0.0.0.0", dockerfile)
        self.assertIn("--server.headless=true", dockerfile)

    def test_compose_starts_dashboard_with_internal_service_urls(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("dashboard:", compose)
        self.assertIn("dockerfile: dashboard/Dockerfile", compose)
        self.assertIn(
            "${HOST_BIND_ADDRESS:-127.0.0.1}:${DASHBOARD_PORT:-8501}:8501",
            compose,
        )
        self.assertIn("DASHBOARD_MINIO_ENDPOINT=http://minio:9000", compose)
        self.assertIn(
            "DASHBOARD_AIRFLOW_URL=http://airflow-webserver:8080",
            compose,
        )
        self.assertIn(
            "DASHBOARD_BALANCING_REPORT_PATH=/app/balancing/report.json",
            compose,
        )
        self.assertIn("DASHBOARD_KAFKA_BOOTSTRAP=kafka:9092", compose)
        self.assertIn(
            "DASHBOARD_MANUAL_YOUTUBE_TOPIC="
            "${MANUAL_YOUTUBE_KAFKA_TOPIC:-manual.youtube.raw.events}",
            compose,
        )
        self.assertIn(
            "balancing:/app/balancing:ro",
            compose,
        )

    def test_airflow_metadata_uses_docker_volume(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("airflow-postgres-data:/var/lib/postgresql/data", compose)
        self.assertIn("airflow-postgres-data:", compose)
        self.assertNotIn("data/airflow-postgres:/var/lib/postgresql/data", compose)

    def test_service_images_package_runtime_code(self):
        dashboard = (ROOT / "dashboard" / "Dockerfile").read_text(
            encoding="utf-8"
        )
        playwright = (ROOT / "playwright" / "Dockerfile").read_text(
            encoding="utf-8"
        )
        spark_master = (ROOT / "spark" / "master" / "Dockerfile").read_text(
            encoding="utf-8"
        )
        airflow = (ROOT / "orchestrator" / "airflow" / "Dockerfile").read_text(
            encoding="utf-8"
        )
        publish_workflow = (
            ROOT / ".github" / "workflows" / "publish-docker-hub.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("COPY dashboard/ .", dashboard)
        self.assertIn("COPY scripts/ /app/scripts/", dashboard)
        self.assertIn("COPY schemas/ /schemas/", dashboard)
        self.assertIn("COPY playwright/producer.py /app/producer.py", playwright)
        self.assertIn("COPY schemas/ /app/schemas/", playwright)
        self.assertIn("COPY API/ /app/api/", playwright)
        self.assertIn("COPY spark/jobs/ /opt/spark/jobs/", spark_master)
        self.assertIn("COPY schemas/ /opt/spark/schemas/", spark_master)
        self.assertIn("COPY tests/spark/ /opt/spark/tests/", spark_master)
        self.assertIn("COPY orchestrator/dags/ /opt/airflow/dags/", airflow)
        self.assertIn("COPY docker-compose.yml /workspace/docker-compose.yml", airflow)
        self.assertIn("/usr/local/bin/stack-transfer", airflow)
        self.assertIn("stack_transfer_cli.py", airflow)
        self.assertIn("COPY --from=docker-cli", airflow)
        self.assertIn("context: .", publish_workflow)


if __name__ == "__main__":
    unittest.main()
