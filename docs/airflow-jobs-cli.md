# Airflow Jobs CLI

`scripts/airflow_jobs_cli.py` monitors Airflow DAG runs from a terminal. It uses
the Airflow REST API, so it can run from the host against the published Airflow
port or inside the dashboard container on the Compose network.

## Configuration

The CLI accepts flags or environment variables:

| Flag | Environment variable | Default |
|---|---|---|
| `--url` | `AIRFLOW_URL` or `DASHBOARD_AIRFLOW_URL` | `http://localhost:8088` |
| `--username` | `AIRFLOW_USERNAME` or `DASHBOARD_AIRFLOW_USERNAME` | `admin` |
| `--password` | `AIRFLOW_PASSWORD` or `DASHBOARD_AIRFLOW_PASSWORD` | `admin` |
| `--timeout` | `AIRFLOW_TIMEOUT_SECONDS` or `DASHBOARD_AIRFLOW_TIMEOUT_SECONDS` | `10` |

## Host usage

Start the stack, then run the CLI from the repository root:

```bash
docker compose up -d airflow-webserver airflow-scheduler
python scripts/airflow_jobs_cli.py status
```

List recent runs for one DAG:

```bash
python scripts/airflow_jobs_cli.py runs \
  --dag-id build_balanced_comment_dataset \
  --limit 5
```

Inspect tasks for a specific DAG run:

```bash
python scripts/airflow_jobs_cli.py tasks \
  --dag-id build_balanced_comment_dataset \
  --run-id scheduled__2026-06-27T00:00:00+00:00
```

Poll the current status every 15 seconds:

```bash
python scripts/airflow_jobs_cli.py watch --interval 15
```

## Docker usage

The dashboard image exposes the CLI as `airflow-jobs`.

```bash
docker exec dashboard airflow-jobs status
```

JSON output is available for automation:

```bash
docker exec dashboard airflow-jobs \
  runs --dag-id user_behavior_lakehouse --limit 3 \
  --output json
```

Check whether recent jobs failed:

```bash
docker exec dashboard airflow-jobs failures --limit 20
```

Include failed task names for each failed run:

```bash
docker exec dashboard airflow-jobs failures --with-tasks --limit 20
```

Use `--fail-on-found` in CI or scripts when failures should produce a non-zero
exit code:

```bash
docker exec dashboard airflow-jobs failures --fail-on-found
```

For one-shot usage without an existing dashboard container, run the image on
the Compose network and point it at the internal Airflow service:

```bash
docker run --rm --network user-behavior-social-media_default \
  raph0603/user-behavior-social-media-dashboard:latest \
  airflow-jobs --url http://airflow-webserver:8080 status
```

## Commands

| Command | Purpose |
|---|---|
| `status` | Show active DAG runs and the next scheduled DAG runs. |
| `runs` | List recent DAG runs for all DAGs or one DAG. |
| `tasks` | Show task state and progress for one DAG run. |
| `failures` | Show failed DAG runs and failed tasks. |
| `watch` | Re-run `status` on an interval until interrupted. |
