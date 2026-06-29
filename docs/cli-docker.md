# CLI Data Import / Export — Docker Usage

> `scripts/data_cli.py` connects to MinIO, Kafka, and Airflow.
> These services run inside the project's Docker Compose stack.
> The CLI **cannot** be used with a standalone Docker Hub image — it must run **inside the stack's Docker network**.

---

## ⚡ Quick Start — Pull data to local in one shot

Run these commands in sequence from the project root. Replace `csv` with `jsonl` or `parquet` as needed.

```bash
# 1. Make sure the stack is up
docker compose up -d

# 2. Run the export inside the dashboard container
docker exec dashboard python /app/scripts/data_cli.py \
  export --format csv --output /tmp/export.csv

# 3. Copy the file to your current local directory
docker cp dashboard:/tmp/export.csv ./export.csv
```

With filters (platform + date range + record limit):

```bash
docker exec dashboard python /app/scripts/data_cli.py \
  export --format jsonl \
  --source youtube \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --limit 5000 \
  --output /tmp/export.jsonl \
  && docker cp dashboard:/tmp/export.jsonl ./export.jsonl
```

> **Note:** The `&&` chains both commands so the copy only runs if the export succeeds.
> On PowerShell, replace `&&` with `;` or run the two commands separately.

---

## ⚡ Quick Start — Push a local file into the stack

Run these commands in sequence from the project root.

```bash
# 1. Make sure the stack is up
docker compose up -d

# 2. Copy your local file into the container
docker cp ./data.jsonl dashboard:/tmp/data.jsonl

# 3. Run the import (auto-detects source from filename)
docker exec dashboard python /app/scripts/data_cli.py \
  import --file /tmp/data.jsonl --source auto
```

With forced source and Airflow pipeline trigger:

```bash
docker cp ./data.csv dashboard:/tmp/data.csv \
  && docker exec dashboard python /app/scripts/data_cli.py \
     import --file /tmp/data.csv --source youtube --trigger-pipeline
```

> **Note:** The `&&` chains both commands so the import only runs if the copy succeeds.
> On PowerShell, replace `&&` with `;` or run the two commands separately.

---

## Prerequisites

Start the full stack:

```bash
docker compose up -d
```

Check that the required services are running:

```bash
docker compose ps
```

Required services: `minio`, `kafka`, `airflow-webserver`, `dashboard`.

---

## Recommended approach: `docker exec` on the `dashboard` container

The `dashboard` container has all Python dependencies and the correct network configuration.
The project directory is mounted into Airflow containers at `/workspace`, but **not** into `dashboard` by default.

### Mount `scripts/` into the dashboard container

Add the following volume under the `dashboard` service in `docker-compose.yml`:

```yaml
volumes:
  - ${HOST_PROJECT_DIR:-.}/scripts:/app/scripts:ro
```

Then restart the container:

```bash
docker compose up -d dashboard
```

---

## Export Commands

### Export as CSV

```bash
docker exec dashboard python /app/scripts/data_cli.py \
  export --format csv --output /tmp/export.csv
```

### Export as JSONL

```bash
docker exec dashboard python /app/scripts/data_cli.py \
  export --format jsonl --output /tmp/export.jsonl
```

### Export as Parquet

```bash
docker exec dashboard python /app/scripts/data_cli.py \
  export --format parquet --output /tmp/export.parquet
```

### Filtering options

| Option | Description | Example |
|---|---|---|
| `--source` | Filter by platform | `--source youtube` |
| `--start-date` | Start date (inclusive) | `--start-date 2025-01-01` |
| `--end-date` | End date (inclusive) | `--end-date 2025-12-31` |
| `--limit` | Maximum number of records | `--limit 1000` |

### Copy the exported file to the host

```bash
docker cp dashboard:/tmp/export.csv ./export.csv
```

---

## Import Commands

### Copy a file into the container

```bash
docker cp ./data.jsonl dashboard:/tmp/data.jsonl
```

### Import without triggering the pipeline

```bash
docker exec dashboard python /app/scripts/data_cli.py \
  import --file /tmp/data.jsonl --source auto
```

### Import and trigger the Airflow pipeline

```bash
docker exec dashboard python /app/scripts/data_cli.py \
  import --file /tmp/data.jsonl --source youtube --trigger-pipeline
```

### Supported `--source` values

| Value | Description |
|---|---|
| `auto` | Auto-detect source from filename |
| `youtube` | Force YouTube source |
| `x` | Force X (Twitter) source |
| `reddit` | Force Reddit source |

### Supported file formats

| Extension | Format |
|---|---|
| `.csv` | CSV with headers |
| `.json` | JSON array |
| `.jsonl` / `.ndjson` | JSON Lines (one object per line) |

---

## Environment Variables

The `dashboard` container reads its configuration from these variables (set in `docker-compose.yml`):

| Variable | Default | Role |
|---|---|---|
| `DASHBOARD_ICEBERG_TABLE_PATH` | `s3://lakehouse/warehouse/silver/events` | Iceberg table used by export |
| `DASHBOARD_MINIO_ENDPOINT` | `http://minio:9000` | MinIO endpoint |
| `DASHBOARD_KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker for import |
| `DASHBOARD_AIRFLOW_URL` | `http://airflow-webserver:8080` | Airflow API for `--trigger-pipeline` |

To override a variable without editing `docker-compose.yml`:

```bash
docker exec -e DASHBOARD_MINIO_ENDPOINT=http://minio:9000 dashboard \
  python /app/scripts/data_cli.py export --format csv --output /tmp/out.csv
```

---

## Full Example: Export → Import Round-Trip

```bash
# 1. Export current data
docker exec dashboard python /app/scripts/data_cli.py \
  export --format jsonl --source youtube --limit 500 --output /tmp/yt_export.jsonl

# 2. Copy to host
docker cp dashboard:/tmp/yt_export.jsonl ./yt_export.jsonl

# (edit the data if needed)

# 3. Copy back and re-import
docker cp ./yt_export.jsonl dashboard:/tmp/yt_export.jsonl

docker exec dashboard python /app/scripts/data_cli.py \
  import --file /tmp/yt_export.jsonl --source youtube --trigger-pipeline
```

---

## Troubleshooting

### `ModuleNotFoundError`

The `scripts/` volume is not mounted in the container.
Check the `volumes` section of the `dashboard` service in `docker-compose.yml`.

### `Connection refused` on Kafka or MinIO

The stack is not fully started. Wait for all services to be `healthy`:

```bash
docker compose ps
```

### `Airflow DAG trigger failed`

Verify that `airflow-webserver` is reachable from inside the container:

```bash
docker exec dashboard curl -s http://airflow-webserver:8080/health
```

### Stream container logs

```bash
docker logs -f dashboard
```
