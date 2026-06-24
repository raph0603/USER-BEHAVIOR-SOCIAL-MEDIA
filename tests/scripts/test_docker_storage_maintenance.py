import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class DockerStorageMaintenanceTests(unittest.TestCase):
    def test_maintenance_dag_is_syntactically_valid(self):
        dag_path = (
            ROOT
            / "orchestrator"
            / "dags"
            / "docker_storage_maintenance.py"
        )
        ast.parse(dag_path.read_text(encoding="utf-8"))

    def test_maintenance_preserves_volumes(self):
        source = (
            ROOT
            / "orchestrator"
            / "dags"
            / "docker_storage_maintenance.py"
        ).read_text(encoding="utf-8")

        self.assertIn("docker container prune", source)
        self.assertIn("docker image prune", source)
        self.assertIn("docker builder prune", source)
        self.assertIn("--reserved-space", source)
        self.assertNotIn("docker volume prune", source)
        self.assertNotIn("--volumes", source)

    def test_compose_rotates_container_logs(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("x-default-logging", compose)
        self.assertIn("driver: local", compose)
        self.assertIn('max-size: "10m"', compose)
        self.assertIn('max-file: "3"', compose)


if __name__ == "__main__":
    unittest.main()
