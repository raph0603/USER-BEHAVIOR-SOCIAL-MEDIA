# USER-BEHAVIOR-SOCIAL-MEDIA

## Lakehouse (Bronze/Silver on Iceberg)

This stack includes MinIO (S3-compatible) and Spark jobs that write to Iceberg tables.

Architecture and operations:

- [End-to-end architecture](docs/ARCHITECTURE.md)
- [Canonical event schema](docs/DATA_SCHEMA.md)
- [Collector behavior](docs/COLLECTORS.md)
- [Lakehouse data model](docs/data-model.md)
- [Metadata and caption troubleshooting](docs/TROUBLESHOOTING.md)

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

Local login:

- user: `admin`
- password: value of `AIRFLOW_ADMIN_PASSWORD` in `.env`

Airflow metadata is stored in the Docker volume `airflow-postgres-data`.
This keeps Postgres out of the OneDrive-backed project tree, which avoids
filesystem I/O errors that can make the dashboard lose the Airflow API.

The DAG `user_behavior_lakehouse` runs the complete online pipeline:

1. collect YouTube, X and Reddit data into their raw Kafka topics;
2. clean, validate and anonymize the three raw streams in parallel, with
   separate clean topics, DLQ topics and checkpoints;
3. merge only records marked `stage=clean` into Iceberg Bronze;
4. after the Bronze MERGE commits, publish that micro-batch to
   `lakehouse.bronze.for_silver`;
5. merge the handed-off records into Iceberg Silver.

The Bronze write and Kafka handoff run in one `foreachBatch` callback. A
handoff failure retries the idempotent Bronze MERGE before re-emitting the
batch, so Silver never consumes a record ahead of its Bronze commit.

The Airflow task names describe the implemented transformations. In
particular, `sha256_hash_pii_redact_validate_*` applies salted SHA-256 user
hashing, regex-based PII redaction, text normalization, validation and DLQ
routing. This pipeline does not currently run a NER model.

The separate `social_clean_pipeline` DAG is retained for replaying the legacy
sample CSV files. It is not required for the online lakehouse flow.

The `user_behavior_lakehouse_no_row_checks` online DAG runs automatically every
60 minutes by default. It succeeds when collectors and Spark jobs finish
without an execution error, even when a run produces no new Bronze or Silver
row. Configure its interval in `.env`:

```env
LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES=30
```

Use `LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES=0` to disable automatic runs.
Airflow keeps at most one active run, so intervals shorter than the pipeline
duration do not execute concurrently.

The `user_behavior_lakehouse` DAG retains `wait_bronze_rows` and
`wait_silver_rows`. It is a strict diagnostic/manual DAG by default; schedule
it only when a run must fail if it produces no new Bronze or Silver row:

```env
LAKEHOUSE_SCHEDULE_MINUTES=0
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
metrics for events already stored in Silver. It runs every 30 minutes by
default, but selects only rows whose source-specific next refresh is due. For
YouTube it batches up to 50 IDs in `videos.list` requests and collects only
view, like and comment counters. After validation, it appends an idempotent
historical observation, merges the latest values, and materializes current
velocity, acceleration and virality signals.
Rows are matched by `platform_event_id` when available, with the older
`source`, `user_id`, `url` and `event_ts` key kept as a fallback for legacy
records. Successful refreshes persist `metadata_refreshed_at`.

```env
INSIGHT_REFRESH_SCHEDULE_MINUTES=30
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

- `youtube_event_count`: maximum number of new YouTube videos, from 1 to 5000
  (default: 50 per Airflow run);
- `x_event_count`: maximum number of new X posts, from 1 to 5000;
- `reddit_event_count`: maximum number of new Reddit comments, from 1 to 5000.

These are upper limits. A collector can return fewer events when the online
search does not contain enough unprocessed results. Scheduled runs use the
default value of 1000 for each source.

The trigger form also exposes `x_headless`:

- `true` (default): Edge uses the persistent profile without displaying a
  window;
- `false`: Edge is visible, which is required to complete a Google login or
  an X challenge.

The Windows CDP proxy automatically restarts Edge when it has been closed.
YouTube is API-based, and Reddit remains headless inside its Docker container.

### Start services

```powershell
.\scripts\ensure_resilient_stack.ps1
```

This starts Docker Desktop if needed, keeps already published ports when
containers are running, selects free host ports when defaults are busy, writes
the resolved values to `.env`, starts MinIO, Kafka, Spark, Airflow and the
Streamlit dashboard, then verifies the main HTTP endpoints. Add
`-IncludeCollectors` to also start the online collectors after the core stack is
healthy.

Published service ports bind to `127.0.0.1` by default through
`HOST_BIND_ADDRESS`. Keep that default for local development; set
`HOST_BIND_ADDRESS=0.0.0.0` only when the stack must be reachable from another
machine and the Airflow, dashboard and MinIO credentials have been replaced.
The startup script also replaces weak local Airflow defaults in `.env` and
keeps dashboard credentials aligned with the generated Airflow password.

### Production release and Docker Hub

Production promotion runs through the GitHub Actions workflow that merges
`main` into `production`, then runs Release Please. When Release Please creates
a production release, the workflow publishes Docker images for the services
that are built from this repository. Docker Hub does not publish the whole
project workspace, the Compose file or local configuration as one standalone
artifact. Clone this repository to run the full stack.

Runtime code is packaged in the service images: the dashboard image includes
the Streamlit application and CLI helper scripts, the Playwright image includes
the collectors and Avro schema, the Spark images include the Spark jobs,
schemas and lakehouse checks, and the Airflow image includes the DAGs plus the
Compose project files needed for orchestration. Runtime state and generated
files are stored in Docker volumes.

See [CLI Data Import / Export - Docker Usage](docs/cli-docker.md) for the
commands that run data import and export inside the Compose stack.

| Image | Docker Hub |
|---|---|
| `user-behavior-social-media-dashboard` | <https://hub.docker.com/r/raph0603/user-behavior-social-media-dashboard> |
| `user-behavior-social-media-airflow` | published as `<namespace>/user-behavior-social-media-airflow` |
| `user-behavior-social-media-playwright` | published as `<namespace>/user-behavior-social-media-playwright` |
| `user-behavior-social-media-spark-master` | published as `<namespace>/user-behavior-social-media-spark-master` |
| `user-behavior-social-media-spark-worker` | published as `<namespace>/user-behavior-social-media-spark-worker` |
| `user-behavior-social-media-ai-trainer` | published as `<namespace>/user-behavior-social-media-ai-trainer` |
| `user-behavior-social-media-ml-api` | published as `<namespace>/user-behavior-social-media-ml-api` |

The currently published dashboard image under `raph0603` exposes these tags:

```bash
docker pull raph0603/user-behavior-social-media-dashboard:latest
docker pull raph0603/user-behavior-social-media-dashboard:production
docker pull raph0603/user-behavior-social-media-dashboard:v1.7.0
```

Configure these GitHub secrets before promoting to production:

- `DOCKERHUB_USERNAME`: Docker Hub account or organization login.
- `DOCKERHUB_TOKEN`: Docker Hub access token with permission to create and
  push repositories in the target namespace.

Optionally set the GitHub Actions variable `DOCKERHUB_NAMESPACE` when the image
namespace differs from `DOCKERHUB_USERNAME`. Each release is pushed with the
Release Please tag, `production`, and `latest`. The workflow creates the Docker
Hub repositories automatically when they do not already exist.

### Docker Compose bundle (without cloning the repository)

Every published production release also attaches a versioned
`user-behavior-social-media-compose-<tag>.zip` asset. It contains the Compose
configuration, a no-build override, an environment template and a short
deployment guide. The bundle pulls the matching immutable images from Docker
Hub; it never falls back to building source code locally.

After downloading and extracting the asset, copy `.env.example` to `.env`, set
strong Airflow and MinIO credentials, add `YOUTUBE_API_KEY` when YouTube
collection is required, then run:

```bash
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml up -d --pull always
```

The image tag in the bundled `.env.example` is already pinned to the release.
The `ml` profile remains opt-in because model training writes data and model
artifacts to a host-mounted workspace.

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
For terminal monitoring, see [Airflow Jobs CLI](docs/airflow-jobs-cli.md).

The separate `Configuration` dashboard page manages crawler limits and search
filters. Keywords are added and removed individually, then translated into
YouTube and X query syntax based on the selected language and filters. Reddit
uses its configured keyword list to filter recent comments from the selected
subreddits. Saved values are stored in Airflow Variables and become defaults
for later scheduled runs.

The main dashboard also includes an `Add data` panel for files that already
contain crawled data. It accepts CSV, JSON, JSONL, and NDJSON files, normalizes
YouTube, X, and Reddit rows into the common raw event contract, publishes them
to `manual.*.raw.events` Kafka topics, then can trigger the
`manual_file_import_lakehouse` DAG to clean, merge, and refresh Silver.

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
docker compose run --rm youtube-collector python /app/youtube_discovery.py
docker compose run --rm youtube-collector python /app/youtube_metadata_worker.py
docker compose run --rm youtube-collector python /app/youtube_transcript_worker.py
docker compose run --rm youtube-collector python /app/youtube_comment_worker.py
docker compose run --rm youtube-collector python /app/youtube_channel_worker.py
```

The online DAG invokes these independent workers automatically. Discovery uses
`search.list` only, with one structured query/language pair and one persistent
watermark per query. The metadata worker uses `yt-dlp` without downloading
media and retains both normalized and raw metadata. Transcripts, text comments
and cached channel statistics have independent queues, schedules and failure
states. See [.env.example](.env.example) for batch, budget, jitter, cooldown,
overlap and adaptive refresh settings.

### Engagement metadata

Collector events carry nullable engagement and outcome fields through the
privacy topics, Iceberg Bronze, and Iceberg Silver. `platform_event_id` is the
preferred merge identity. Common counters include likes, views, comments,
replies, reposts, bookmarks, score, followers, subscribers, and subreddit
members when a source exposes them.

Null means unknown or unavailable; zero means an observed count of zero.
Reddit score remains `score` and is not mapped to likes. Independent
`metadata_status`, `comments_status`, `transcript_status`, and
`storage_status` values explain why an enrichment is missing. Source
publication time remains separate from collection and retry timestamps.
Existing Bronze and Silver tables add missing nullable columns before a MERGE.
YouTube video rows include a low-resolution `thumbnail_url` when available.
For older rows, the pipeline derives the public
`https://img.youtube.com/vi/<video_id>/default.jpg` URL from the existing video
id, so thumbnail backfill does not consume YouTube Data API quota.

### Balanced dataset

The `build_balanced_comment_dataset` DAG builds a reproducible balanced sample
from `lakehouse.silver.events`. It writes the Iceberg table
`lakehouse.silver.balanced_events` and a JSON report to
`data/balancing/report.json`.
The crawl DAGs also refresh this balanced table and report automatically after
Silver has been updated, so the dashboard reflects the latest crawl results.

By default, balancing is done by `source` only, so YouTube, X and Reddit keep
the same number of rows in the balanced output. `engagement_band` and
`comment_type` are still derived and kept in the output for analysis, but they
are not part of the default balancing key. Sampling order is deterministic from
`BALANCE_SEED` and stable platform/event fields, so the same input and seed
produce the same output.

```env
BALANCE_DATASET_SCHEDULE_MINUTES=1440
BALANCE_SEED=42
BALANCE_TARGET_PER_GROUP=0
BALANCE_DIMENSIONS=source
```

`BALANCE_TARGET_PER_GROUP=0` uses the smallest available source size. If a
requested target is larger than the smallest source, the job lowers the
effective target and records that constraint in the report instead of
duplicating rows.

### Content analytics layer

`lakehouse.silver.events` remains the base monitoring table. The
`content_analytics.py` batch job derives entity-level tables for analysis:

- `lakehouse.silver.contents`: one row per main content item, such as a
  Reddit post, X post, or YouTube video.
- `lakehouse.silver.interactions`: comments and replies attached to a parent
  content item.
- `lakehouse.silver.engagement_snapshots`: append-only content engagement
  observations over time.
- `lakehouse.silver.transcripts`: YouTube transcript text and optional segment
  metadata when available.
- `lakehouse.gold.content_stats`: content-level aggregates such as interaction
  counts, unique interacting users, average interaction length, and latest
  engagement metrics.
- `lakehouse.gold.user_evolution`: anonymized user activity by day and source.

Collectors and manual imports propagate `content_id`, `parent_content_id`,
`root_content_id`, `conversation_id`, `content_type`, `relation_type`,
`depth`, and `position_in_thread` through Bronze and Silver. Root posts and
videos become content rows; only real comments and replies become
interactions. Existing YouTube rows can be materialized from event caption
fields or backfilled without YouTube Data API search quota.

Run the job manually with:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m \
  --executor-memory 512m \
  --conf spark.cores.max=2 \
  --conf spark.executor.cores=1 \
  /opt/spark/jobs/batch/content_analytics.py
```

To backfill transcripts for videos already present in Silver:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m \
  --executor-memory 512m \
  --conf spark.cores.max=2 \
  --conf spark.executor.cores=1 \
  /opt/spark/jobs/batch/youtube_transcripts.py
```

To backfill missing low-resolution thumbnails without YouTube API quota:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m \
  --executor-memory 512m \
  --conf spark.cores.max=2 \
  --conf spark.executor.cores=1 \
  /opt/spark/jobs/batch/youtube_thumbnail_backfill.py
```

Both online Airflow DAGs materialize or backfill captions and low-resolution
thumbnail URLs before refreshing content analytics after
`lakehouse.silver.events` has been updated. The
Streamlit dashboard exposes the derived tables in `Content Explorer`, with
Reddit, X, YouTube, and Users views. If an analytical table has not been
created yet, the dashboard shows a non-blocking availability message and keeps
the event view available for debugging.

### ML input freshness and label coverage

Training requires an explicit Silver export. The pipeline rejects a missing
export and, by default, any export older than 24 hours:

```bash
python ml/run_pipeline.py --export --report
```

Each export writes a timestamped, checksum-addressed copy under
`data/samples/exports/` plus `filtered_events.manifest.json`. Use
`--allow-stale-input` only for an intentional reproducibility run, or change
the limit with `--max-input-age-hours`. Engagement counters that were not
observed remain null; rows without any observed label metric are excluded
instead of becoming negative examples with an invented zero.

### YouTube owners and collaborators

YouTube events store the publishing channel in `owner_channel_id` and accepted
creator collaborators in `collaborator_channel_ids`. The owner comes from the
stable `snippet.channelId` returned by the YouTube Data API. Collaborators are
read from the canonical public watch page because the Data API does not expose
the accepted collaborator list.

A confirmed video without collaborators stores an empty list. If the watch
page is unavailable, private, deleted, blocked by a consent or anti-bot page,
or its undocumented structure changes, the collaborator value remains null.
Bronze and Silver use `COALESCE`, so such failures do not replace previously
collected owner or collaborator metadata. The engagement refresh never calls
this page enrichment. Collaborator access is opt-in and belongs only to an
initial or separately scheduled metadata enrichment.

`YOUTUBE_WATCH_PAGE_TIMEOUT_SECONDS` controls the public page request timeout
and defaults to 20 seconds. `YOUTUBE_AUTHOR_FETCH_WORKERS` limits concurrent
watch-page requests and defaults to 8. `YOUTUBE_COLLABORATOR_COLLECTION_ENABLED`
defaults to `false` so normal collection does not depend on an undocumented
YouTube watch-page endpoint that may return 429. Set it to `true` only when
collaborator enrichment is explicitly required and the endpoint is available.
Subscriber counts do not use this path: the channel worker deduplicates by
`channel_id`, persists its cache and calls the official `channels.list`
endpoint in batches of up to 50.

YouTube collection is bounded for scheduled DAG runs.
`YOUTUBE_COLLECTION_TIMEOUT_SECONDS` defaults to 900 seconds and stops
leftover `youtube-collector` containers when the limit is exceeded.
`YOUTUBE_COMMENT_MAX_PAGES` defaults to 3 comment pages per video.
`YOUTUBE_TRANSCRIPT_MAX_FAILURES` bounds consecutive systemic caption
failures. A confirmed unavailable caption track does not consume that budget;
rate limits and network restrictions remain visible and retryable.

## Development checks

Install the lightweight validation dependencies and run the same non-Spark
checks used by CI:

```bash
python -m pip install --requirement requirements-test.txt
python -m ruff check .
python -m pytest \
  --ignore=tests/scripts/test_engagement_snapshots.py \
  --ignore=tests/scripts/test_silver_post_features.py
```

Pytest uses importlib import mode so test files with the same basename in
different directories are collected independently. The two excluded tests
start a local Spark runtime and remain available for a Spark-enabled
validation environment.

### Run X collection

X is collected directly from the live website with Playwright. The collector
launches its own headless Chromium browser and publishes new posts to
`x.raw.events`:

```bash
docker compose run --rm x-collector
```

Set `X_COLLECTION_ENABLED=true` to include this step in the Airflow DAG.
The container stores its browser profile in the collector state volume through
`X_USER_DATA_DIR`, so cookies can be reused across runs. For non-interactive
headless runs, provide `X_AUTH_TOKEN` and optionally `X_CT0` in `.env` so the
collector can authenticate without opening a visible browser.

When triggering the DAG from the Airflow UI, set `x_event_count` to the maximum
number of new X posts to collect (between 1 and 5000). For a direct
`docker compose run`, `X_MAX_EVENTS` in `.env` provides the limit. If fewer new
posts are available, the collector publishes fewer events. X crawler,
authentication, and rate-limit failures fail the task by default so incomplete
runs cannot appear healthy. Set `X_FAIL_ON_ERROR=false` only for an intentional
best-effort collection run.

Authenticate to X with Google in a non-headless run if you need to seed the
profile manually. The crawler reuses that authenticated profile on later
headless runs. If the X login page reappears, it clicks the Google login
option and selects `X_GOOGLE_EMAIL` when available. Password, CAPTCHA and MFA
challenges still require a visible browser run.

### Run Reddit collection

Reddit is collected directly from the live public pages with Playwright. New
comments from the configured subreddits are published to `reddit.raw.events`:

```bash
docker compose run --rm reddit-collector
```

Set `REDDIT_COLLECTION_ENABLED=true` to include this step in Airflow.
`REDDIT_SUBREDDITS`, `REDDIT_COMMENT_SCAN_LIMIT` and `REDDIT_MAX_EVENTS`
control the online collection. The collector scans up to 100 recent comments
per Reddit page and follows pagination until it reaches the configured scan
limit for each subreddit. It then sorts comments globally by publication time
and publishes the newest unprocessed comments first.

If Reddit pages do not load or the expected comment markup is missing, the
collector fails instead of reporting a successful zero-event run. A zero-event
run is only valid after comments were actually scanned and every candidate was
already processed or filtered out by the configured keywords.

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

The command preserves the X/Google browser profile, the
`airflow-postgres-data` Docker volume, and Airflow logs. For non-interactive
use, pass `-Force`.
