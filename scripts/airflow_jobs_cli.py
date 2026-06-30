#!/usr/bin/env python3
"""Monitor Airflow DAG runs from the command line."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = ROOT / "dashboard"
for path in (ROOT, DASHBOARD_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

try:
    from dashboard.airflow_monitoring import AirflowClient
except ImportError:
    from airflow_monitoring import AirflowClient


ACTIVE_STATES = {"queued", "running"}
FAILED_STATES = {"failed", "upstream_failed"}
TERMINAL_STATES = {
    "failed",
    "removed",
    "skipped",
    "success",
    "upstream_failed",
}


def parse_datetime(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


def format_datetime(value):
    parsed = parse_datetime(value) if isinstance(value, str) else value
    if not parsed:
        return "-"
    return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def format_duration(seconds):
    seconds = max(0, int(seconds))
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def calculate_progress(tasks):
    total = len(tasks)
    completed = sum(task.get("state") in TERMINAL_STATES for task in tasks)
    failed = sum(task.get("state") in FAILED_STATES for task in tasks)
    success = sum(task.get("state") == "success" for task in tasks)
    progress = round(completed / total * 100) if total else 0
    return {
        "progress": progress,
        "completed_tasks": completed,
        "successful_tasks": success,
        "failed_tasks": failed,
        "total_tasks": total,
    }


def table(headers, rows):
    string_rows = [[str(cell) for cell in row] for row in rows]
    widths = [
        max(len(str(header)), *(len(row[index]) for row in string_rows))
        if string_rows
        else len(str(header))
        for index, header in enumerate(headers)
    ]
    header_line = "  ".join(
        str(header).ljust(widths[index]) for index, header in enumerate(headers)
    )
    separator = "  ".join("-" * width for width in widths)
    body = [
        "  ".join(row[index].ljust(widths[index]) for index in range(len(headers)))
        for row in string_rows
    ]
    return "\n".join([header_line, separator, *body])


def build_config(args):
    return {
        "base_url": (
            args.url
            or os.getenv("AIRFLOW_URL")
            or os.getenv("DASHBOARD_AIRFLOW_URL")
            or "http://localhost:8088"
        ).rstrip("/"),
        "username": (
            args.username
            or os.getenv("AIRFLOW_USERNAME")
            or os.getenv("DASHBOARD_AIRFLOW_USERNAME")
            or "admin"
        ),
        "password": (
            args.password
            or os.getenv("AIRFLOW_PASSWORD")
            or os.getenv("DASHBOARD_AIRFLOW_PASSWORD")
            or "admin"
        ),
        "timeout": int(
            args.timeout
            or os.getenv("AIRFLOW_TIMEOUT_SECONDS")
            or os.getenv("DASHBOARD_AIRFLOW_TIMEOUT_SECONDS")
            or "10"
        ),
    }


def get_runs(client, dag_id, limit, state=None):
    params = {"limit": limit, "order_by": "-execution_date"}
    if state:
        params["state"] = state
    payload = client._get(f"/dags/{quote(dag_id, safe='')}/dagRuns", params=params)
    return payload.get("dag_runs", [])


def get_tasks(client, dag_id, run_id):
    payload = client._get(
        f"/dags/{quote(dag_id, safe='')}/dagRuns/"
        f"{quote(run_id, safe='')}/taskInstances",
        params={"limit": 1000},
    )
    return payload.get("task_instances", [])


def run_status(args, client):
    status = client.load_status()
    if args.output == "json":
        print(json.dumps(status, default=str, indent=2))
        return

    print(f"Checked at: {format_datetime(status['checked_at'])}")
    active_runs = status["active_runs"]
    if active_runs:
        print("\nActive DAG runs")
        print(
            table(
                [
                    "DAG",
                    "Run",
                    "State",
                    "Progress",
                    "Tasks",
                    "Failed",
                    "Started",
                ],
                [
                    [
                        run["dag_id"],
                        run["run_id"],
                        run["state"],
                        f"{run['progress']}%",
                        f"{run['completed_tasks']}/{run['total_tasks']}",
                        run["failed_tasks"],
                        format_datetime(run["started_at"]),
                    ]
                    for run in active_runs
                ],
            )
        )
    else:
        print("\nNo active DAG runs.")

    next_runs = status["next_runs"]
    if next_runs:
        print("\nNext scheduled DAG runs")
        now = datetime.now(timezone.utc)
        print(
            table(
                ["DAG", "Next run", "In"],
                [
                    [
                        run["dag_id"],
                        format_datetime(run["next_run"]),
                        format_duration(
                            (run["next_run"] - now).total_seconds()
                        ),
                    ]
                    for run in next_runs
                ],
            )
        )


def run_runs(args, client):
    dag_ids = [args.dag_id] if args.dag_id else [
        dag["dag_id"]
        for dag in client._get(
            "/dags",
            params={"limit": 100, "only_active": "true"},
        ).get("dags", [])
    ]
    rows = []
    payload = []
    for dag_id in dag_ids:
        for run in get_runs(client, dag_id, args.limit, state=args.state):
            if args.state and run.get("state") != args.state:
                continue
            item = {
                "dag_id": dag_id,
                "run_id": run.get("dag_run_id"),
                "state": run.get("state"),
                "run_type": run.get("run_type"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
            }
            payload.append(item)
            rows.append(
                [
                    item["dag_id"],
                    item["run_id"],
                    item["state"],
                    item["run_type"],
                    format_datetime(item["start_date"]),
                    format_datetime(item["end_date"]),
                ]
            )
    if args.output == "json":
        print(json.dumps(payload, indent=2))
        return
    print(table(["DAG", "Run", "State", "Type", "Started", "Ended"], rows))


def run_tasks(args, client):
    tasks = get_tasks(client, args.dag_id, args.run_id)
    progress = calculate_progress(tasks)
    payload = {"summary": progress, "tasks": tasks}
    if args.output == "json":
        print(json.dumps(payload, indent=2, default=str))
        return
    print(
        "Progress: "
        f"{progress['progress']}% "
        f"({progress['completed_tasks']}/{progress['total_tasks']} complete, "
        f"{progress['failed_tasks']} failed)"
    )
    rows = [
        [
            task.get("task_id"),
            task.get("state") or "-",
            task.get("try_number", "-"),
            format_datetime(task.get("start_date")),
            format_datetime(task.get("end_date")),
        ]
        for task in tasks
    ]
    print(table(["Task", "State", "Try", "Started", "Ended"], rows))


def run_failures(args, client):
    dag_ids = [args.dag_id] if args.dag_id else [
        dag["dag_id"]
        for dag in client._get(
            "/dags",
            params={"limit": 100, "only_active": "true"},
        ).get("dags", [])
    ]
    failures = []
    for dag_id in dag_ids:
        candidate_runs = get_runs(client, dag_id, args.limit, state="failed")
        if args.include_running:
            candidate_runs.extend(get_runs(client, dag_id, args.limit, state="running"))
            candidate_runs.extend(get_runs(client, dag_id, args.limit, state="queued"))

        for run in candidate_runs:
            run_state = run.get("state")
            run_id = run.get("dag_run_id")
            include_tasks = args.with_tasks or args.include_running
            if not include_tasks:
                tasks = []
            else:
                tasks = get_tasks(client, dag_id, run_id)
            failed_tasks = [
                task for task in tasks if task.get("state") in FAILED_STATES
            ]
            if run_state in FAILED_STATES or failed_tasks:
                failures.append(
                    {
                        "dag_id": dag_id,
                        "run_id": run_id,
                        "run_state": run_state,
                        "run_type": run.get("run_type"),
                        "start_date": run.get("start_date"),
                        "end_date": run.get("end_date"),
                        "failed_tasks": [
                            {
                                "task_id": task.get("task_id"),
                                "state": task.get("state"),
                                "try_number": task.get("try_number"),
                                "start_date": task.get("start_date"),
                                "end_date": task.get("end_date"),
                            }
                            for task in failed_tasks
                        ],
                    }
                )

    if args.output == "json":
        print(json.dumps(failures, indent=2))
        return 1 if args.fail_on_found and failures else 0

    if not failures:
        print("No failed DAG runs or failed tasks found.")
        return 0

    rows = []
    for failure in failures:
        failed_task_names = ", ".join(
            task["task_id"] for task in failure["failed_tasks"]
        ) or "-"
        rows.append(
            [
                failure["dag_id"],
                failure["run_id"],
                failure["run_state"],
                failed_task_names,
                format_datetime(failure["start_date"]),
                format_datetime(failure["end_date"]),
            ]
        )
    print(
        table(
            ["DAG", "Run", "Run state", "Failed tasks", "Started", "Ended"],
            rows,
        )
    )
    return 1 if args.fail_on_found else 0


def run_watch(args, client):
    iterations = args.iterations
    count = 0
    while iterations is None or count < iterations:
        if count:
            print()
        run_status(args, client)
        count += 1
        if iterations is not None and count >= iterations:
            break
        time.sleep(args.interval)


def build_parser():
    parser = argparse.ArgumentParser(
        description="Monitor Airflow DAG runs for the lakehouse stack.",
    )
    parser.add_argument("--url", help="Airflow base URL.")
    parser.add_argument("--username", help="Airflow username.")
    parser.add_argument("--password", help="Airflow password.")
    parser.add_argument("--timeout", type=int, help="Request timeout in seconds.")
    parser.add_argument(
        "--output",
        choices=("table", "json"),
        default="table",
        help="Output format.",
    )

    def add_output_argument(command_parser):
        command_parser.add_argument(
            "--output",
            choices=("table", "json"),
            default=argparse.SUPPRESS,
            help="Output format.",
        )

    subparsers = parser.add_subparsers(dest="command", required=True)
    status_parser = subparsers.add_parser(
        "status",
        help="Show active and next scheduled DAG runs.",
    )
    add_output_argument(status_parser)

    runs_parser = subparsers.add_parser("runs", help="List recent DAG runs.")
    add_output_argument(runs_parser)
    runs_parser.add_argument("--dag-id", help="Restrict results to one DAG.")
    runs_parser.add_argument("--state", help="Restrict results to one run state.")
    runs_parser.add_argument("--limit", type=int, default=10)

    tasks_parser = subparsers.add_parser("tasks", help="Show tasks for one DAG run.")
    add_output_argument(tasks_parser)
    tasks_parser.add_argument("--dag-id", required=True)
    tasks_parser.add_argument("--run-id", required=True)

    failures_parser = subparsers.add_parser(
        "failures",
        help="Show failed DAG runs and failed tasks.",
    )
    add_output_argument(failures_parser)
    failures_parser.add_argument("--dag-id", help="Restrict results to one DAG.")
    failures_parser.add_argument("--limit", type=int, default=20)
    failures_parser.add_argument(
        "--include-running",
        action="store_true",
        help="Also inspect queued/running runs for failed tasks.",
    )
    failures_parser.add_argument(
        "--with-tasks",
        action="store_true",
        help="Fetch task details for failed DAG runs.",
    )
    failures_parser.add_argument(
        "--fail-on-found",
        action="store_true",
        help="Exit with code 1 when any failure is found.",
    )

    watch_parser = subparsers.add_parser("watch", help="Poll the Airflow status.")
    add_output_argument(watch_parser)
    watch_parser.add_argument("--interval", type=int, default=15)
    watch_parser.add_argument(
        "--iterations",
        type=int,
        help="Stop after N refreshes. Default: run until interrupted.",
    )

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    client = AirflowClient(config=build_config(args))

    try:
        if args.command == "status":
            run_status(args, client)
        elif args.command == "runs":
            run_runs(args, client)
        elif args.command == "tasks":
            run_tasks(args, client)
        elif args.command == "failures":
            return run_failures(args, client)
        elif args.command == "watch":
            run_watch(args, client)
        else:
            parser.error(f"Unsupported command: {args.command}")
    except Exception as exc:
        print(f"Airflow monitoring failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
