# USER-BAHAVIOR-SOCIAL-MEDIA

## Lakehouse (Bronze/Silver on Iceberg)

This stack includes MinIO (S3-compatible) and Spark jobs that write to Iceberg tables.

## Privacy gateway

The Bronze streaming job applies privacy safeguards before data lands in Iceberg:

- `user_id` is replaced by a deterministic SHA-256 hash salted with `PRIVACY_HASH_SALT`.
- URL query strings and fragments are stripped.
- Emails, phone numbers, and IP addresses are redacted from free-text fields.

Set `PRIVACY_HASH_SALT` to a non-default secret outside local development.

## Orchestration (Airflow)

The project includes a local Airflow orchestrator for the lakehouse flow.

When Airflow runs Docker Compose through the Docker socket, bind mounts must use a host-visible path. Keep a local `.env` based on `.env.example`; on this Windows Docker Desktop setup it uses:

```bash
HOST_PROJECT_DIR=/run/desktop/mnt/host/c/Users/rapha/OneDrive/Documents/USER-BEHAVIOR-SOCIAL-MEDIA
```

```bash
docker compose up -d airflow-init airflow-webserver airflow-scheduler
```

Airflow UI: http://localhost:8088

Default local login:

- user: `admin`
- password: `admin`

The DAG `user_behavior_lakehouse` starts the core stack, runs the Spark/Kafka probe and Bronze stream in parallel, emits bounded synthetic events, then runs Silver as a batch step after Bronze has data.

### Start services

```bash
docker compose up -d --build
```

### Run the Bronze streaming job

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
	/opt/spark/jobs/streaming/kafka_to_iceberg_bronze.py
```

### Run the Silver batch job

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
	/opt/spark/jobs/batch/bronze_to_silver.py
```
