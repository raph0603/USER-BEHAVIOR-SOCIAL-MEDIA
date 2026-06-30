import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "dashboard" / "airflow_monitoring.py"
SPEC = importlib.util.spec_from_file_location("airflow_monitoring", MODULE_PATH)
MONITORING = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MONITORING)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.content = b"{}"
        self.text = payload if isinstance(payload, str) else ""

    def raise_for_status(self):
        return None

    def json(self):
        if isinstance(self.payload, str):
            raise ValueError("text response")
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.auth = None

    def request(self, method, url, params, json, timeout):
        path = url.split("/api/v1", 1)[1]
        return FakeResponse(self.responses[path])


class AirflowMonitoringTests(unittest.TestCase):
    def test_load_status_calculates_progress_and_next_run(self):
        responses = {
            "/dags": {
                "dags": [
                    {
                        "dag_id": "pipeline",
                        "is_paused": False,
                        "next_dagrun": "2026-06-15T10:30:00+00:00",
                        "next_dagrun_create_after": "2026-06-15T10:30:00+00:00",
                    }
                ]
            },
            "/dags/pipeline/dagRuns": {
                "dag_runs": [
                    {
                        "dag_run_id": "scheduled__run",
                        "state": "running",
                        "start_date": "2026-06-15T10:00:00+00:00",
                    }
                ]
            },
            "/dags/pipeline/dagRuns/scheduled__run/taskInstances": {
                "total_entries": 4,
                "task_instances": [
                    {"state": "success"},
                    {"state": "success"},
                    {"state": "running"},
                    {"state": None},
                ],
            },
        }
        client = MONITORING.AirflowClient(
            config={
                "base_url": "http://airflow",
                "username": "admin",
                "password": "admin",
                "timeout": 10,
            },
            session=FakeSession(responses),
        )

        status = client.load_status(
            now=datetime(2026, 6, 15, 10, 0, tzinfo=timezone.utc)
        )

        self.assertEqual(status["active_runs"][0]["progress"], 50)
        self.assertEqual(status["active_runs"][0]["completed_tasks"], 2)
        self.assertEqual(status["next_runs"][0]["countdown"], "30 min 0 s")

    def test_format_duration(self):
        self.assertEqual(MONITORING.format_duration(90), "1 min 30 s")
        self.assertEqual(MONITORING.format_duration(3660), "1 h 1 min")
        self.assertEqual(
            MONITORING.format_countdown(-90),
            "en attente depuis 1 min 30 s",
        )

    def test_recent_collector_runs_extract_soft_blocks_from_logs(self):
        responses = {
            "/dags/user_behavior_lakehouse/dagRuns": {
                "dag_runs": [
                    {
                        "dag_run_id": "scheduled__run",
                        "state": "success",
                        "start_date": "2026-06-15T10:00:00+00:00",
                        "end_date": "2026-06-15T10:02:00+00:00",
                    }
                ]
            },
            "/dags/user_behavior_lakehouse/dagRuns/scheduled__run/taskInstances": {
                "task_instances": [
                    {
                        "task_id": "collect_x_playwright_events",
                        "state": "success",
                        "try_number": 1,
                    }
                ]
            },
            (
                "/dags/user_behavior_lakehouse/dagRuns/scheduled__run/"
                "taskInstances/collect_x_playwright_events/logs/1"
            ): (
                "Collector soft-blocked: x collection blocked: "
                "temporarily limited"
            ),
            "/dags/user_behavior_lakehouse_no_row_checks/dagRuns": {
                "dag_runs": []
            },
        }
        client = MONITORING.AirflowClient(
            config={
                "base_url": "http://airflow",
                "username": "admin",
                "password": "admin",
                "timeout": 10,
            },
            session=FakeSession(responses),
        )

        rows = client.load_recent_collector_runs(limit=1)

        self.assertEqual(rows[0]["source"], "x")
        self.assertEqual(rows[0]["collector_status"], "blocked")
        self.assertIn("temporarily limited", rows[0]["message"])

    def test_crawler_configuration_allows_five_thousand_events_per_run(self):
        configuration_source = (
            ROOT / "dashboard" / "pages" / "1_Configuration.py"
        ).read_text(encoding="utf-8")
        crawler_source = (
            ROOT / "orchestrator" / "dags" / "crawler_configuration.py"
        ).read_text(encoding="utf-8")
        dag_source = (
            ROOT / "orchestrator" / "dags" / "user_behavior_lakehouse.py"
        ).read_text(encoding="utf-8")

        for source in (configuration_source, crawler_source):
            with self.subTest():
                self.assertIn('"youtube_event_count": 1000', source)
                self.assertIn('"x_event_count": 1000', source)
                self.assertIn('"reddit_event_count": 1000', source)

        self.assertIn('"max_events_per_source": 1000', configuration_source)
        self.assertIn('"max_events_per_source": 1000', crawler_source)
        self.assertIn('"reddit_comment_scan_limit": 1000', configuration_source)
        self.assertIn('"reddit_comment_scan_limit": 1000', crawler_source)
        self.assertIn("max_value=5000", configuration_source)
        self.assertIn("maximum=5000", dag_source)


if __name__ == "__main__":
    unittest.main()
