import os
import json
from datetime import datetime, timezone
from urllib.parse import quote

import requests


TERMINAL_TASK_STATES = {
    "failed",
    "removed",
    "skipped",
    "success",
    "upstream_failed",
}
ACTIVE_RUN_STATES = {"queued", "running"}
CRAWLER_VARIABLE_KEY = "crawler_dashboard_config"
INSIGHT_VARIABLE_KEY = "insight_dashboard_config"


def get_airflow_config():
    return {
        "base_url": os.getenv(
            "DASHBOARD_AIRFLOW_URL",
            "http://localhost:8088",
        ).rstrip("/"),
        "username": os.getenv("DASHBOARD_AIRFLOW_USERNAME", "admin"),
        "password": os.getenv("DASHBOARD_AIRFLOW_PASSWORD", "admin"),
        "timeout": int(os.getenv("DASHBOARD_AIRFLOW_TIMEOUT_SECONDS", "10")),
    }


def _parse_datetime(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


def format_duration(seconds):
    seconds = max(0, int(seconds))
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    if days:
        return f"{days} j {hours} h"
    if hours:
        return f"{hours} h {minutes} min"
    if minutes:
        return f"{minutes} min {seconds} s"
    return f"{seconds} s"


def format_countdown(seconds):
    if seconds < 0:
        return f"en attente depuis {format_duration(abs(seconds))}"
    return format_duration(seconds)


class AirflowClient:
    def __init__(self, config=None, session=None):
        self.config = config or get_airflow_config()
        self.session = session or requests.Session()
        self.session.auth = (
            self.config["username"],
            self.config["password"],
        )

    def _request(self, method, path, params=None, json_body=None):
        response = self.session.request(
            method,
            f"{self.config['base_url']}/api/v1{path}",
            params=params,
            json=json_body,
            timeout=self.config["timeout"],
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    def _get(self, path, params=None):
        return self._request("GET", path, params=params)

    def get_variable(self, key, defaults):
        try:
            payload = self._get(f"/variables/{quote(key, safe='')}")
        except requests.HTTPError as exc:
            if exc.response.status_code == 404:
                return defaults.copy()
            raise
        try:
            stored_value = json.loads(payload.get("value", "{}"))
        except json.JSONDecodeError:
            return defaults.copy()
        if not defaults:
            return stored_value
        config = defaults.copy()
        config.update(
            {
                name: value
                for name, value in stored_value.items()
                if name in defaults
            }
        )
        return config

    def save_variable(self, key, value):
        encoded_key = quote(key, safe="")
        serialized_value = json.dumps(value, ensure_ascii=True)
        try:
            self._get(f"/variables/{encoded_key}")
        except requests.HTTPError as exc:
            if exc.response.status_code != 404:
                raise
            return self._request(
                "POST",
                "/variables",
                json_body={
                    "key": key,
                    "value": serialized_value,
                    "description": "Configuration managed from the dashboard",
                },
            )
        return self._request(
            "PATCH",
            f"/variables/{encoded_key}",
            json_body={
                "key": key,
                "value": serialized_value,
                "description": "Configuration managed from the dashboard",
            },
        )

    def trigger_dag(self, dag_id, conf):
        return self._request(
            "POST",
            f"/dags/{quote(dag_id, safe='')}/dagRuns",
            json_body={"conf": conf},
        )

    def load_status(self, now=None):
        now = now or datetime.now(timezone.utc)
        dags = self._get(
            "/dags",
            params={"limit": 100, "only_active": "true"},
        ).get("dags", [])
        active_runs = []
        next_runs = []

        for dag in dags:
            dag_id = dag["dag_id"]
            encoded_dag_id = quote(dag_id, safe="")
            run_response = self._get(
                f"/dags/{encoded_dag_id}/dagRuns",
                params={"limit": 20, "order_by": "-execution_date"},
            )
            runs = run_response.get("dag_runs", [])

            for run in runs:
                if run.get("state") not in ACTIVE_RUN_STATES:
                    continue
                run_id = run["dag_run_id"]
                encoded_run_id = quote(run_id, safe="")
                task_response = self._get(
                    f"/dags/{encoded_dag_id}/dagRuns/"
                    f"{encoded_run_id}/taskInstances",
                    params={"limit": 1000},
                )
                tasks = task_response.get("task_instances", [])
                total_tasks = task_response.get("total_entries", len(tasks))
                completed_tasks = sum(
                    task.get("state") in TERMINAL_TASK_STATES
                    for task in tasks
                )
                successful_tasks = sum(
                    task.get("state") == "success" for task in tasks
                )
                failed_tasks = sum(
                    task.get("state") in {"failed", "upstream_failed"}
                    for task in tasks
                )
                progress = (
                    round(completed_tasks / total_tasks * 100)
                    if total_tasks
                    else 0
                )
                active_runs.append(
                    {
                        "dag_id": dag_id,
                        "run_id": run_id,
                        "state": run.get("state"),
                        "progress": progress,
                        "completed_tasks": completed_tasks,
                        "successful_tasks": successful_tasks,
                        "failed_tasks": failed_tasks,
                        "total_tasks": total_tasks,
                        "started_at": _parse_datetime(run.get("start_date")),
                    }
                )

            next_run = _parse_datetime(dag.get("next_dagrun_create_after"))
            if not dag.get("is_paused") and next_run:
                seconds_until = (next_run - now).total_seconds()
                next_runs.append(
                    {
                        "dag_id": dag_id,
                        "next_run": next_run,
                        "seconds_until": seconds_until,
                        "countdown": format_countdown(seconds_until),
                    }
                )

        active_runs.sort(
            key=lambda run: run["started_at"] or datetime.min.replace(
                tzinfo=timezone.utc
            )
        )
        next_runs.sort(key=lambda run: run["next_run"])
        return {
            "active_runs": active_runs,
            "next_runs": next_runs,
            "checked_at": now,
        }
