# User Behavior Social Media — Docker Compose bundle

This bundle runs a versioned release without cloning the repository or building
local images. It requires Docker Desktop (or Docker Engine) with Docker Compose
v2.24.4 or later and access to Docker Hub.

## Start the stack

1. Copy `.env.example` to `.env`.
2. Replace the example Airflow and MinIO credentials before exposing any port
   beyond localhost. Set `YOUTUBE_API_KEY` only when YouTube collection is
   needed.
3. Start the release images:

   ```bash
   docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml up -d --pull always
   ```

The bundle pins `PROJECT_IMAGE_TAG` to its release tag. Do not change it to
`latest` if you need a reproducible deployment.

The dashboard is available at `http://localhost:8501` and Airflow at
`http://localhost:8088` by default. Use `docker compose -f compose.yaml -f
compose.bundle.yaml down` to stop the stack while preserving named volumes.

## Verify and transfer data

The bundle includes two transfer commands. `stack-transfer` moves the complete
deployment, while `data-transfer` exports or replays selected Silver events.

For a complete migration, stop the source stack and write the archive through a
host-mounted directory:

```bash
mkdir transfer
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml stop
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml run --rm --no-deps -T --volume ./transfer:/transfer --entrypoint stack-transfer airflow-init export --output /transfer/stack-backup.tar.gz
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml start
```

The archive contains every project Docker volume: the full MinIO lakehouse,
Kafka data, collector state, Airflow PostgreSQL and logs, and other application
state. It does not contain `.env`, secrets, or host bind mounts; transfer those
separately and securely.

On the target, copy the archive to `./transfer`, create the empty deployment,
and restore it before starting services:

```bash
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml create
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml stop
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml run --rm --no-deps -T --volume ./transfer:/transfer --entrypoint stack-transfer airflow-init restore --archive /transfer/stack-backup.tar.gz
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml up -d
```

The restore fails safely if a target volume is not empty. Add `--overwrite`
only when replacing a verified target deployment. Prefer the same release on
both servers.

For a selective event transfer, verify Iceberg, Kafka, and Airflow after
deployment:

```bash
docker exec dashboard data-transfer doctor
```

Export or import a JSONL transfer without installing Python on the server:

```bash
docker exec dashboard data-transfer export --format jsonl --output /tmp/events.jsonl
docker cp dashboard:/tmp/events.jsonl ./events.jsonl
docker cp ./events.jsonl dashboard:/tmp/events.jsonl
docker exec dashboard data-transfer import --file /tmp/events.jsonl --source auto --trigger-pipeline
```

## Optional ML profile

The `ml` profile is intentionally not started with the core stack. It needs a
host workspace for training input and generated model artifacts. Use the source
repository for that workflow, or provide an explicit host workspace before
running the profile.
