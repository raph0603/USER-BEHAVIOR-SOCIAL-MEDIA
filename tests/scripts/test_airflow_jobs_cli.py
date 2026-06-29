import io
import os
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts import airflow_jobs_cli as cli


class FakeClient:
    def __init__(self):
        self.requests = []

    def load_status(self):
        return {
            "checked_at": "2026-06-28T10:00:00+00:00",
            "active_runs": [
                {
                    "dag_id": "pipeline",
                    "run_id": "manual__run",
                    "state": "running",
                    "progress": 50,
                    "completed_tasks": 2,
                    "successful_tasks": 2,
                    "failed_tasks": 0,
                    "total_tasks": 4,
                    "started_at": "2026-06-28T09:55:00+00:00",
                }
            ],
            "next_runs": [
                {
                    "dag_id": "build_balanced_comment_dataset",
                    "next_run": cli.parse_datetime("2026-06-29T00:00:00+00:00"),
                    "seconds_until": 3600,
                }
            ],
        }

    def _get(self, path, params=None):
        self.requests.append((path, params))
        if path == "/dags":
            return {"dags": [{"dag_id": "pipeline"}]}
        if path == "/dags/pipeline/dagRuns":
            if params and params.get("state") == "failed":
                return {
                    "dag_runs": [
                        {
                            "dag_run_id": "scheduled__failed",
                            "state": "failed",
                            "run_type": "scheduled",
                            "start_date": "2026-06-28T08:00:00+00:00",
                            "end_date": "2026-06-28T08:05:00+00:00",
                        }
                    ]
                }
            if params and params.get("state") in {"running", "queued"}:
                return {"dag_runs": []}
            return {
                "dag_runs": [
                    {
                        "dag_run_id": "scheduled__failed",
                        "state": "failed",
                        "run_type": "scheduled",
                        "start_date": "2026-06-28T08:00:00+00:00",
                        "end_date": "2026-06-28T08:05:00+00:00",
                    },
                    {
                        "dag_run_id": "manual__run",
                        "state": "success",
                        "run_type": "manual",
                        "start_date": "2026-06-28T09:00:00+00:00",
                        "end_date": "2026-06-28T09:10:00+00:00",
                    }
                ]
            }
        if path == "/dags/pipeline/dagRuns/manual__run/taskInstances":
            return {
                "task_instances": [
                    {
                        "task_id": "start",
                        "state": "success",
                        "try_number": 1,
                        "start_date": "2026-06-28T09:00:00+00:00",
                        "end_date": "2026-06-28T09:01:00+00:00",
                    },
                    {
                        "task_id": "finish",
                        "state": "running",
                        "try_number": 1,
                        "start_date": "2026-06-28T09:01:00+00:00",
                        "end_date": None,
                    },
                ]
            }
        if path == "/dags/pipeline/dagRuns/scheduled__failed/taskInstances":
            return {
                "task_instances": [
                    {
                        "task_id": "broken_task",
                        "state": "failed",
                        "try_number": 2,
                        "start_date": "2026-06-28T08:00:00+00:00",
                        "end_date": "2026-06-28T08:05:00+00:00",
                    }
                ]
            }
        raise AssertionError(path)


class AirflowJobsCliTests(unittest.TestCase):
    def test_build_config_prefers_airflow_environment(self):
        args = cli.build_parser().parse_args(["status"])
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_URL": "http://airflow:8080",
                "AIRFLOW_USERNAME": "user",
                "AIRFLOW_PASSWORD": "secret",
                "AIRFLOW_TIMEOUT_SECONDS": "7",
            },
            clear=True,
        ):
            config = cli.build_config(args)

        self.assertEqual(config["base_url"], "http://airflow:8080")
        self.assertEqual(config["username"], "user")
        self.assertEqual(config["password"], "secret")
        self.assertEqual(config["timeout"], 7)

    def test_status_renders_active_and_next_runs(self):
        args = cli.build_parser().parse_args(["status"])
        output = io.StringIO()
        with redirect_stdout(output):
            cli.run_status(args, FakeClient())

        rendered = output.getvalue()
        self.assertIn("Active DAG runs", rendered)
        self.assertIn("pipeline", rendered)
        self.assertIn("manual__run", rendered)
        self.assertIn("Next scheduled DAG runs", rendered)

    def test_runs_renders_recent_runs(self):
        args = cli.build_parser().parse_args(["runs", "--dag-id", "pipeline"])
        output = io.StringIO()
        with redirect_stdout(output):
            cli.run_runs(args, FakeClient())

        rendered = output.getvalue()
        self.assertIn("manual__run", rendered)
        self.assertIn("success", rendered)

    def test_tasks_renders_task_progress(self):
        args = cli.build_parser().parse_args(
            ["tasks", "--dag-id", "pipeline", "--run-id", "manual__run"]
        )
        output = io.StringIO()
        with redirect_stdout(output):
            cli.run_tasks(args, FakeClient())

        rendered = output.getvalue()
        self.assertIn("Progress: 50%", rendered)
        self.assertIn("start", rendered)
        self.assertIn("finish", rendered)

    def test_failures_renders_failed_runs_and_tasks(self):
        args = cli.build_parser().parse_args(
            ["failures", "--dag-id", "pipeline", "--with-tasks"]
        )
        output = io.StringIO()
        with redirect_stdout(output):
            exit_code = cli.run_failures(args, FakeClient())

        rendered = output.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("scheduled__failed", rendered)
        self.assertIn("broken_task", rendered)

    def test_failures_can_exit_non_zero_when_found(self):
        args = cli.build_parser().parse_args(
            ["failures", "--dag-id", "pipeline", "--fail-on-found"]
        )
        output = io.StringIO()
        with redirect_stdout(output):
            exit_code = cli.run_failures(args, FakeClient())

        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
