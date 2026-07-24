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

## Optional ML profile

The `ml` profile is intentionally not started with the core stack. It contains
the batch trainer and the HTTP inference service. Copy the trained model
artifacts into `./ml/models` (or set `ML_MODEL_DIR`), configure matching
`ML_API_TOKEN` and `DASHBOARD_ML_API_TOKEN` values when authentication is
required, then start the API:

```bash
mkdir -p ml/models
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml --profile ml up -d ml-api
docker compose --env-file .env -f compose.yaml -f compose.bundle.yaml --profile ml ps
```

The Dashboard calls `http://ml-api:8000` on the private Compose network. The
host mapping defaults to `127.0.0.1:8000`; do not expose it publicly without TLS
and authentication. Training still needs a host workspace and input export, so
use the source repository for the `ai-trainer` workflow.
