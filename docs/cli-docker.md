# Portable data transfer CLIs

There are two complementary commands:

- `stack-transfer` creates an offline snapshot of every Docker volume belonging
  to the Compose project. Use it to move or back up the whole deployment.
- `data-transfer` exports selected rows from the Iceberg Silver event table and
  replays them through Kafka. Use it for a partial dataset or an interoperability
  file.

Both commands work in a source checkout and in the versioned Compose release
bundle.

## Transfer the complete deployment

`stack-transfer` is packaged in the Airflow image because that image already
contains the Docker CLI and mounts the Docker socket. It discovers volumes from
the actual Compose containers instead of relying on fixed Docker-generated
names. The snapshot includes:

- the complete MinIO volume, including Bronze, Silver, Gold, monitoring, Iceberg
  metadata, and every other object in the lakehouse bucket;
- named application volumes for collector SQLite state, insight refreshes,
  balancing reports, Airflow PostgreSQL, and Airflow logs;
- anonymous volumes such as Kafka data and Kafka or Schema Registry runtime
  state;
- any new named or anonymous volume attached to a project service in a later
  Compose version.

Host bind mounts, the Docker socket, `.env`, and external secret/config files are
intentionally excluded. Copy configuration and secrets separately through a
secure channel.

Create the archive from the directory containing the active Compose files. The
stack must be stopped so MinIO, Kafka, SQLite, and PostgreSQL are captured at one
consistent point in time:

```console
mkdir transfer
docker compose stop
docker compose run --rm --no-deps -T --volume ./transfer:/transfer --entrypoint stack-transfer airflow-init export --output /transfer/stack-backup.tar.gz
docker compose start
```

Inspect the portable manifest without restoring anything:

```console
docker compose run --rm --no-deps -T --volume ./transfer:/transfer --entrypoint stack-transfer airflow-init inspect --archive /transfer/stack-backup.tar.gz
```

On the target, use the same application release whenever possible. Create the
target containers and volumes, keep them stopped, then restore:

```console
mkdir transfer
# Copy stack-backup.tar.gz into ./transfer first.
docker compose create
docker compose stop
docker compose run --rm --no-deps -T --volume ./transfer:/transfer --entrypoint stack-transfer airflow-init restore --archive /transfer/stack-backup.tar.gz
docker compose up -d
```

Restoration refuses to modify any non-empty target volume. For a deliberate
replacement of an existing target deployment, add `--overwrite`; this clears
the exact discovered target volumes before extracting the snapshot. If a target
uses a different Compose version and intentionally lacks an archived volume,
`--skip-missing` permits a partial restore and reports every skipped mapping.

The source checkout also supports `python scripts/stack_transfer_cli.py ...`
when Docker is available. Archive paths then refer to the host directly.

## Transfer selected Silver events

The dashboard image contains `data-transfer`, which exports the complete
Iceberg Silver event schema and imports CSV, JSON, JSONL, or NDJSON files into
the manual Kafka topics.

JSONL is the recommended transfer format. It preserves nested arrays, streams
well, and can be imported without loading a spreadsheet application.

## Check the environment

After starting the stack, verify all required connections:

```console
docker exec dashboard data-transfer doctor
```

Run only the check needed by an operation:

```console
docker exec dashboard data-transfer doctor --check iceberg
docker exec dashboard data-transfer doctor --check kafka --check airflow
```

Use `--output json` when another program consumes the diagnostic.

## Export data

Write an export inside the container and copy it to the host. These commands are
portable across Linux, macOS, Windows PowerShell, and Windows Command Prompt:

```console
docker exec dashboard data-transfer export --format jsonl --output /tmp/events.jsonl
docker cp dashboard:/tmp/events.jsonl ./events.jsonl
```

Available export formats are `csv`, `jsonl`, and `parquet`. Optional filters:

```console
docker exec dashboard data-transfer export --format jsonl --output /tmp/youtube.jsonl --source youtube --start-date 2026-01-01 --end-date 2026-12-31 --limit 5000
docker cp dashboard:/tmp/youtube.jsonl ./youtube.jsonl
```

The export reads every Silver column. Transcript text, transcript provenance,
metadata status, identifiers, counters, and array fields therefore survive an
export/import round trip.

## Import data

Copy a file into the running dashboard container and publish it to Kafka:

```console
docker cp ./events.jsonl dashboard:/tmp/events.jsonl
docker exec dashboard data-transfer import --file /tmp/events.jsonl --source auto
```

Trigger the ingestion DAG after publication:

```console
docker exec dashboard data-transfer import --file /tmp/events.jsonl --source auto --trigger-pipeline
```

`--source auto` reads the source from canonical exports and detects common raw
YouTube, X, and Reddit layouts. Use `--source youtube`, `x`, or `reddit` when a
third-party file does not have enough columns for automatic detection.

Parquet is intentionally export-only. Use JSONL for a portable round trip.

## Stream between environments

`-` means standard output for exports and standard input for imports. Progress
messages are written to standard error, so they never corrupt streamed data.

On Linux, macOS, or a POSIX shell, a local file can be streamed without an
intermediate copy inside the container:

```bash
docker exec dashboard data-transfer export --format jsonl --output - > events.jsonl
docker exec -i dashboard data-transfer import --file - --format jsonl --source auto < events.jsonl
```

Two remote Docker hosts can be connected directly over SSH:

```bash
ssh source-server 'docker exec dashboard data-transfer export --format jsonl --output -' |
  ssh target-server 'docker exec -i dashboard data-transfer import --file - --format jsonl --source auto --trigger-pipeline'
```

For Windows PowerShell, `docker cp` is the safest option for arbitrary and
binary files. JSONL can also be streamed with PowerShell 7:

```powershell
docker exec dashboard data-transfer export --format jsonl --output - |
  Set-Content -Encoding utf8 ./events.jsonl
Get-Content -Raw -Encoding utf8 ./events.jsonl |
  docker exec -i dashboard data-transfer import --file - --format jsonl --source auto
```

## Run directly from a source checkout

Install the dashboard dependencies, expose or tunnel the service endpoints, and
run the same Python entry point:

```console
python -m pip install --requirement dashboard/requirements.txt
python scripts/data_cli.py doctor
python scripts/data_cli.py export --format jsonl --output ./events.jsonl
python scripts/data_cli.py import --file ./events.jsonl --source auto
```

The CLI accepts endpoint overrides such as `--minio-endpoint`, `--table-path`,
`--kafka-bootstrap`, and `--airflow-url`. Prefer environment variables for
credentials so secrets do not appear in shell history.

| Environment variable | Default on the host | Purpose |
|---|---|---|
| `DASHBOARD_ICEBERG_TABLE_PATH` | `s3://lakehouse/warehouse/silver/events` | Silver table |
| `DASHBOARD_MINIO_ENDPOINT` | `http://localhost:9000` | MinIO/S3 endpoint |
| `DASHBOARD_MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `DASHBOARD_MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `DASHBOARD_KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap server |
| `DASHBOARD_AIRFLOW_URL` | `http://localhost:8088` | Airflow base URL |
| `DASHBOARD_AIRFLOW_USERNAME` | `admin` | Airflow username |
| `DASHBOARD_AIRFLOW_PASSWORD` | `admin` | Airflow password |

Inside Compose, these values already point to `minio`, `kafka`, and
`airflow-webserver`; no host ports or source-code mounts are required.

## Failure behavior

The command exits with a non-zero status when validation, storage access, Kafka
delivery, or the optional Airflow trigger fails. An import validates the entire
file before publishing it. Keep the original export until the target pipeline
has completed successfully.
