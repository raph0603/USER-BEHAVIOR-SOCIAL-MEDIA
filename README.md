# USER-BAHAVIOR-SOCIAL-MEDIA

## Lakehouse (Bronze/Silver on Iceberg)

This stack includes MinIO (S3-compatible) and Spark jobs that write to Iceberg tables.

## Privacy gateway

The three source-specific cleaning jobs apply privacy safeguards before data
lands in Bronze:

- `user_id` is replaced by a deterministic SHA-256 hash salted with `PRIVACY_HASH_SALT`.
- URL fragments are stripped while useful query parameters such as YouTube
  video IDs are preserved.
- Emails, mentions, phone numbers, IP addresses, embedded URLs, HTML and
  control characters are removed or replaced in free-text fields.
- Invalid records are sent to a source-specific DLQ before Bronze.

Set `PRIVACY_HASH_SALT` to a non-default secret outside local development.

## Orchestration (Airflow)

The project includes a local Airflow orchestrator for the lakehouse flow.

When Airflow runs Docker Compose through the Docker socket, bind mounts must use a host-visible path. Keep local startup mounts relative, and configure a separate Docker Desktop path for Compose commands launched inside Airflow:

```bash
HOST_PROJECT_DIR=.
DOCKER_HOST_PROJECT_DIR=C:/Users/rapha/OneDrive/Documents/USER-BEHAVIOR-SOCIAL-MEDIA
YOUTUBE_KAFKA_TOPIC=youtube.raw.events
X_COLLECTION_ENABLED=true
X_KAFKA_TOPIC=x.raw.events
REDDIT_COLLECTION_ENABLED=true
REDDIT_KAFKA_TOPIC=reddit.raw.events
YOUTUBE_CLEAN_KAFKA_TOPIC=youtube.clean.events
X_CLEAN_KAFKA_TOPIC=x.clean.events
REDDIT_CLEAN_KAFKA_TOPIC=reddit.clean.events
YOUTUBE_DLQ_KAFKA_TOPIC=youtube.dlq.events
X_DLQ_KAFKA_TOPIC=x.dlq.events
REDDIT_DLQ_KAFKA_TOPIC=reddit.dlq.events
```

```bash
docker compose up -d airflow-init airflow-webserver airflow-scheduler
```

Airflow UI: http://localhost:8088

Default local login:

- user: `admin`
- password: `admin`

The DAG `user_behavior_lakehouse` runs the complete online pipeline:

1. collect YouTube, X and Reddit data into their raw Kafka topics;
2. clean, validate and anonymize the three raw streams in parallel, with
   separate clean topics, DLQ topics and checkpoints;
3. merge only records marked `stage=clean` into Iceberg Bronze;
4. publish the Bronze records to `lakehouse.bronze.for_silver`;
5. transmit and process those Bronze records into Iceberg Silver.

The Airflow task names describe the implemented transformations. In
particular, `sha256_hash_pii_redact_validate_*` applies salted SHA-256 user
hashing, regex-based PII redaction, text normalization, validation and DLQ
routing. This pipeline does not currently run a NER model.

The separate `social_clean_pipeline` DAG is retained for replaying the legacy
sample CSV files. It is not required for the online lakehouse flow.

The online DAG runs automatically every 60 minutes by default. Configure the
interval in `.env`:

```env
LAKEHOUSE_SCHEDULE_MINUTES=30
```

Use `LAKEHOUSE_SCHEDULE_MINUTES=0` to disable automatic runs. Airflow keeps at
most one active run, so intervals shorter than the pipeline duration do not
execute concurrently.

The `user_behavior_lakehouse_no_row_checks` DAG runs the same online pipeline
without `wait_bronze_rows` or `wait_silver_rows`. It succeeds when collectors
and Spark jobs finish without an execution error, even when a run produces no
new Bronze or Silver row. It also runs every 60 minutes by default. Its
schedule is configured separately:

```env
LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES=30
```

Both online DAGs use a shared pipeline lock. If their scheduled or manual runs
overlap, the second run waits before cleanup and Spark processing instead of
starting concurrent streams on the same checkpoints. Lock operations use
`flock`, and a waiting run checks the owner in the Airflow metadata database.
Locks owned by terminal or missing DAG runs are reclaimed automatically after
a short safety delay.

The lock timing can be configured in `.env`:

```env
PIPELINE_LOCK_POLL_SECONDS=10
PIPELINE_LOCK_MAX_WAIT_SECONDS=7200
PIPELINE_LOCK_STALE_GRACE_SECONDS=30
```

If the Airflow metadata lookup fails, the lock is preserved and the waiting
run retries later.

The independent `iceberg_parquet_compaction` DAG compacts the Parquet files
managed by the Bronze and Silver Iceberg tables. It runs every six hours by
default:

```env
ICEBERG_COMPACTION_SCHEDULE_MINUTES=360
ICEBERG_COMPACTION_TABLES=lakehouse.bronze.events,lakehouse.silver.events
ICEBERG_COMPACTION_RETRIES=3
```

Set the schedule to `0` to keep this DAG manual. Its trigger form exposes
`target_file_size_mb` (128 MB by default) and `min_input_files` (2 by default).
Iceberg uses `rewrite-all=false`, so files already compacted are skipped.

Each table rewrite is an atomic Iceberg snapshot transaction with partial
progress disabled. A failed attempt leaves the previous snapshot readable.
The job verifies that the record count is unchanged and retries three times by
default. The shared lock prevents compaction from overlapping Bronze or Silver
writes without creating a dependency between DAGs.

The separate `refresh_recent_engagement_insights` DAG refreshes engagement
metrics for events already stored in Silver. It runs once per day by default,
selects events from the previous 15 days, recollects the metrics available for
YouTube, X and Reddit, then updates the matching Bronze and Silver rows.
Rows are matched by `platform_event_id` when available, with the older
`source`, `user_id`, `url` and `event_ts` key kept as a fallback for legacy
records. Successful refreshes persist `metadata_refreshed_at`.

```env
INSIGHT_REFRESH_SCHEDULE_MINUTES=1440
```

The trigger form exposes `lookback_days` and `max_events_per_source`. X and
Reddit follow the existing `X_COLLECTION_ENABLED` and
`REDDIT_COLLECTION_ENABLED` switches. X also requires the authenticated CDP
browser session.

The `docker_storage_maintenance` DAG limits development-machine disk growth. It
runs daily, removes stopped containers and dangling images older than 24 hours,
keeps at most 1 GB of old build cache, and removes Airflow logs older than 14
days. Docker volumes are never pruned by this DAG.

```env
DOCKER_MAINTENANCE_SCHEDULE_MINUTES=1440
DOCKER_MAINTENANCE_BUILD_CACHE_KEEP=1GB
AIRFLOW_LOG_RETENTION_DAYS=14
```

Compose also uses Docker's compressed `local` logging driver with three 10 MB
files per container.

Docker Desktop's WSL disk does not always shrink after cleanup. To return its
free blocks to Windows, close Docker Desktop and run the following command from
an elevated PowerShell:

```powershell
.\scripts\compact_docker_disk.ps1
```

When manually triggering `user_behavior_lakehouse` from the Airflow interface,
the trigger form exposes three limits:

- `youtube_event_count`: maximum number of new YouTube videos, from 1 to 500;
- `x_event_count`: maximum number of new X posts, from 1 to 500;
- `reddit_event_count`: maximum number of new Reddit comments, from 1 to 500.

These are upper limits. A collector can return fewer events when the online
search does not contain enough unprocessed results. Scheduled runs use the
default value of 5 for each source.

The trigger form also exposes `x_headless`:

- `true` (default): Edge uses the persistent profile without displaying a
  window;
- `false`: Edge is visible, which is required to complete a Google login or
  an X challenge.

The Windows CDP proxy automatically restarts Edge when it has been closed.
YouTube is API-based, and Reddit remains headless inside its Docker container.

### Start services

```bash
docker compose up -d --build
```

This starts MinIO, Kafka, Spark, Airflow and the Streamlit dashboard. The
dashboard is exposed at http://localhost:8501 and uses the internal Docker
endpoints `http://minio:9000` and `http://airflow-webserver:8080`.

### Run the local dashboard

The Streamlit dashboard is containerized by default. For local dashboard
development without Docker, it can still be launched from the host. Start
MinIO before launching it:

```powershell
docker compose up -d minio minio-init
cd dashboard
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m streamlit run app.py
```

The dashboard is available at http://localhost:8501. Its Iceberg table,
MinIO endpoint, and credentials can be overridden with the
`DASHBOARD_ICEBERG_TABLE_PATH`, `DASHBOARD_MINIO_ENDPOINT`,
`DASHBOARD_MINIO_ACCESS_KEY`, and `DASHBOARD_MINIO_SECRET_KEY` environment
variables. The Airflow monitoring panel uses `DASHBOARD_AIRFLOW_URL`,
`DASHBOARD_AIRFLOW_USERNAME`, and `DASHBOARD_AIRFLOW_PASSWORD`. It displays
active DAG runs, task completion progress, and upcoming scheduled runs.

The separate `Configuration` dashboard page manages crawler limits and search
filters. Keywords are added and removed individually, then translated into
YouTube and X query syntax based on the selected language and filters. Reddit
uses its configured keyword list to filter recent comments from the selected
subreddits. Saved values are stored in Airflow Variables and become defaults
for later scheduled runs.

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

### Run YouTube collection

Set `YOUTUBE_API_KEY` in your `.env`, then run:

```bash
docker compose run --rm youtube-collector
```

The collector writes raw JSON under `API/yt_raw_json/` and emits YouTube
events to Kafka using the same event schema as the other producers.

### Engagement metadata

Collector events carry a nullable engagement contract through the privacy
cleaning topics, Iceberg Bronze and Iceberg Silver:

- stable matching field: `platform_event_id`;
- shared nullable fields: `like_count`, `comment_count`, `reply_count` and
  `view_count`;
- X-specific fields: `retweet_count` and `bookmark_count`;
- Reddit-specific field: `score`;
- YouTube videos populate likes, comments, replies found in fetched threads,
  and views;
- X posts populate likes, replies, views, retweets and bookmarks;
- Reddit comments populate direct replies and their native score.

Unsupported or unavailable metrics remain null, preserving the original
platform semantics for downstream score calculation. Existing Bronze and
Silver tables are evolved automatically when new columns are first used.
`metadata_refreshed_at` is null for initial collection rows and is set by the
refresh job when mutable metadata is recollected.

### Balanced dataset

The `build_balanced_comment_dataset` DAG builds a reproducible balanced sample
from `lakehouse.silver.events`. It writes the Iceberg table
`lakehouse.silver.balanced_events` and a JSON report to
`data/balancing/report.json`.

By default, balancing is done by `source` only, so YouTube, X and Reddit keep
the same number of rows in the balanced output. `engagement_band` and
`comment_type` are still derived and kept in the output for analysis, but they
are not part of the default balancing key. Sampling order is deterministic from
`BALANCE_SEED` and stable platform/event fields, so the same input and seed
produce the same output.

```env
BALANCE_DATASET_SCHEDULE_MINUTES=0
BALANCE_SEED=42
BALANCE_TARGET_PER_GROUP=0
BALANCE_DIMENSIONS=source
```

`BALANCE_TARGET_PER_GROUP=0` uses the smallest available source size. If a
requested target is larger than the smallest source, the job lowers the
effective target and records that constraint in the report instead of
duplicating rows.

### YouTube owners and collaborators

YouTube events store the publishing channel in `owner_channel_id` and accepted
creator collaborators in `collaborator_channel_ids`. The owner comes from the
stable `snippet.channelId` returned by the YouTube Data API. Collaborators are
read from the canonical public watch page because the Data API does not expose
the accepted collaborator list.

A confirmed video without collaborators stores an empty list. If the watch
page is unavailable, private, deleted, blocked by a consent or anti-bot page,
or its undocumented structure changes, the collaborator value remains null.
Bronze, Silver and the insight refresh job use `COALESCE`, so such failures do
not replace previously collected owner or collaborator metadata. The refresh
DAG retries this enrichment for recent YouTube events.

`YOUTUBE_WATCH_PAGE_TIMEOUT_SECONDS` controls the public page request timeout
and defaults to 20 seconds. `YOUTUBE_AUTHOR_FETCH_WORKERS` limits concurrent
watch-page requests and defaults to 8. Because collaborator extraction depends
on undocumented page data, it should be monitored when YouTube changes its
watch page.

### Run X collection

X is collected directly from the live website with Playwright. The collector
connects to an authenticated Chrome or Edge session through CDP and publishes
new posts to `x.raw.events`:

```bash
docker compose run --rm x-collector
```

Set `X_COLLECTION_ENABLED=true` to include this step in the Airflow DAG.
Start the browser with remote debugging enabled before running the DAG:

```powershell
.\scripts\start_x_browser.ps1
```

The script selects free ports for Edge and its Docker-accessible CDP proxy,
then writes the selected proxy port to `data/x-runtime/cdp-port.txt`. The
`x-collector` container reads that file automatically, so no fixed X port is
required in `.env` or Airflow. `X_CDP_URL` remains available only as a fallback
for an externally managed CDP endpoint.

Log in to X in that browser window. When triggering the DAG from the Airflow
UI, set `x_event_count` to the maximum number of new X posts to collect
(between 1 and 500). For a direct `docker compose run`, `X_MAX_EVENTS` in
`.env` provides the limit. If fewer new posts are available, the collector
publishes fewer events. When X collection is enabled, a crawler or CDP
failure fails the Airflow task and the DAG.

Authenticate to X with Google in the Edge window. The crawler reuses that
authenticated browser session. If the X login page reappears, it clicks the
Google login option and selects `X_GOOGLE_EMAIL` from the existing Edge
session. Password, CAPTCHA and MFA challenges still require completion in the
Edge window.

### Run Reddit collection

Reddit is collected directly from the live public pages with Playwright. New
comments from the configured subreddits are published to `reddit.raw.events`:

```bash
docker compose run --rm reddit-collector
```

Set `REDDIT_COLLECTION_ENABLED=true` to include this step in Airflow.
`REDDIT_SUBREDDITS`, `REDDIT_COMMENT_SCAN_LIMIT` and `REDDIT_MAX_EVENTS`
control the online collection. The collector scans up to 100 recent comments
per subreddit, sorts them globally by publication time and publishes the newest
unprocessed comments first.

Each collector stores processed source IDs in its own persistent SQLite file
under `data/collector-state/`. Existing topic contents are imported into this
state before collection, so previously processed posts, comments and videos
are not republished.

### Reset all pipeline data

Run this before an end-to-end test to delete Kafka events and schemas, MinIO
Bronze/Silver tables, collector deduplication state, and collected source
files:

```powershell
.\scripts\reset_pipeline_data.ps1
```

The command preserves the X/Google browser profile, Airflow metadata, and
Airflow logs. For non-interactive use, pass `-Force`.
