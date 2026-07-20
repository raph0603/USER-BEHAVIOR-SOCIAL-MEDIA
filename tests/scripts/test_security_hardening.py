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
        starter = (ROOT / "scripts" / "start_x_browser.ps1").read_text(encoding="utf-8")
        producer = (ROOT / "playwright" / "producer.py").read_text(encoding="utf-8")
        insight_refresh = (ROOT / "playwright" / "insight_refresh.py").read_text(encoding="utf-8")

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
        script = (ROOT / "scripts" / "ensure_resilient_stack.ps1").read_text(encoding="utf-8")
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")

        self.assertIn("AIRFLOW_ADMIN_PASSWORD", compose)
        self.assertIn("airflow users reset-password", compose)
        self.assertIn("Ensure-SecretVariable", script)
        self.assertIn("HOST_BIND_ADDRESS", script)
        self.assertIn("replace-me-with-a-local-password", env_example)
        self.assertNotIn("DASHBOARD_AIRFLOW_PASSWORD=admin", env_example)

    def test_airflow_receives_collector_runtime_configuration(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        for variable in (
            "YOUTUBE_API_KEY: ${YOUTUBE_API_KEY:-}",
            "YOUTUBE_SEARCH_QUERIES:",
            "YOUTUBE_MAX_EVENTS:",
            "YOUTUBE_COMMENT_MAX_PAGES:",
            "YOUTUBE_TRANSCRIPT_MAX_FAILURES:",
            "YOUTUBE_COLLECTION_TIMEOUT_SECONDS:",
            "X_CDP_HOST:",
            "X_GOOGLE_EMAIL:",
            "X_MAX_EVENTS:",
            "REDDIT_SUBREDDITS:",
            "REDDIT_MAX_EVENTS:",
        ):
            with self.subTest(variable=variable):
                self.assertIn(variable, compose)

        self.assertIn("YOUTUBE_API_KEY=${YOUTUBE_API_KEY:-}", compose)
        self.assertNotIn("YOUTUBE_API_KEY=${YOUTUBE_API_KEY}", compose)

    def test_youtube_collection_is_bounded(self):
        producer = (ROOT / "playwright" / "producer.py").read_text(encoding="utf-8")
        source = (ROOT / "orchestrator" / "dags" / "lakehouse_dag_factory.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("timed_docker_compose", source)
        self.assertIn("YOUTUBE_COLLECTION_TIMEOUT_SECONDS", source)
        self.assertIn("label=com.docker.compose.project", source)
        self.assertIn("label=com.docker.compose.service={service}", source)
        self.assertIn("xargs -r docker stop", source)
        self.assertIn("execution_timeout=timedelta", source)

        self.assertIn("YOUTUBE_COMMENT_MAX_PAGES", producer)
        self.assertIn("YOUTUBE_TRANSCRIPT_MAX_FAILURES", producer)
        self.assertIn("transcripts_disabled", producer)
        self.assertIn("transcript_result.error_code", producer)
        self.assertIn('"ip_blocked"', producer)


if __name__ == "__main__":
    unittest.main()
