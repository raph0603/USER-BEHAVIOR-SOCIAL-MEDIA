import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class SecurityHardeningTests(unittest.TestCase):
    def test_compose_binds_host_ports_to_loopback_by_default(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        for port_variable in (
            "MINIO_API_PORT",
            "KAFKA_HOST_PORT",
            "SCHEMA_REGISTRY_PORT",
            "DASHBOARD_PORT",
            "AIRFLOW_WEBSERVER_PORT",
        ):
            with self.subTest(port_variable=port_variable):
                self.assertIn(
                    f"${{HOST_BIND_ADDRESS:-127.0.0.1}}:${{{port_variable}",
                    compose,
                )

    def test_x_cdp_proxy_requires_and_propagates_access_token(self):
        proxy = (ROOT / "scripts" / "x_cdp_proxy.py").read_text(encoding="utf-8")
        starter = (ROOT / "scripts" / "start_x_browser.ps1").read_text(
            encoding="utf-8"
        )
        producer = (ROOT / "playwright" / "producer.py").read_text(encoding="utf-8")
        insight_refresh = (ROOT / "playwright" / "insight_refresh.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("hmac.compare_digest", proxy)
        self.assertIn("--access-token", proxy)
        self.assertIn("X-CDP-Token", proxy)
        self.assertIn("cdp-token.txt", starter)
        self.assertIn("--access-token", starter)
        self.assertIn("X_CDP_TOKEN_FILE", producer)
        self.assertIn("_with_x_cdp_token", producer)
        self.assertIn("X_CDP_TOKEN_FILE", insight_refresh)
        self.assertIn("_with_x_cdp_token", insight_refresh)

    def test_resilient_stack_generates_local_airflow_secrets(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        script = (ROOT / "scripts" / "ensure_resilient_stack.ps1").read_text(
            encoding="utf-8"
        )
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")

        self.assertIn("AIRFLOW_ADMIN_PASSWORD", compose)
        self.assertIn("airflow users reset-password", compose)
        self.assertIn("Ensure-SecretVariable", script)
        self.assertIn("HOST_BIND_ADDRESS", script)
        self.assertIn("replace-me-with-a-local-password", env_example)
        self.assertNotIn("DASHBOARD_AIRFLOW_PASSWORD=admin", env_example)


if __name__ == "__main__":
    unittest.main()
