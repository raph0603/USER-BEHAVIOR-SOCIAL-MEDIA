import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class DockerComposeBundleTests(unittest.TestCase):
    def test_bundle_removes_every_local_build_context(self):
        bundle = (ROOT / "deployment" / "compose.bundle.yaml").read_text(encoding="utf-8")
        expected_services = {
            "dashboard",
            "youtube-collector",
            "x-collector",
            "reddit-collector",
            "spark-master",
            "spark-worker",
            "airflow-init",
            "airflow-webserver",
            "airflow-scheduler",
            "ml-api",
            "ai-trainer",
        }

        for service in expected_services:
            self.assertIn(f"  {service}:\n    build: !reset null", bundle)

    def test_release_workflow_publishes_bundle_and_ml_image(self):
        workflow = (ROOT / ".github" / "workflows" / "publish-docker-hub.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn("user-behavior-social-media-ai-trainer", workflow)
        self.assertIn("user-behavior-social-media-ml-api", workflow)
        self.assertIn("publish-compose-bundle:", workflow)
        self.assertIn("gh release upload", workflow)

    def test_ml_services_are_not_started_by_default(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn('profiles: ["ml"]', compose)
        self.assertIn("DASHBOARD_ML_API_URL=${DASHBOARD_ML_API_URL:-http://ml-api:8000}", compose)

    def test_airflow_keeps_the_bundle_release_and_minio_credentials(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("PROJECT_IMAGE_TAG: ${PROJECT_IMAGE_TAG:-latest}", compose)
        self.assertIn("MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minioadmin}", compose)


if __name__ == "__main__":
    unittest.main()
