# YouTube media references and transcript fallback

## Thumbnail URL audit

The discovery, metadata, Kafka/Avro, Bronze, Silver, analytics, dashboard, and
backfill paths persist thumbnail references only. `yt-dlp` runs with
`skip_download=true` and `download=false`; the URL backfill constructs an
`https://img.youtube.com/...` string. No pipeline component downloads or stores
JPG, PNG, WebP, base64, or other thumbnail bytes in local volumes or MinIO.

Metadata URLs take precedence. Only HTTPS URLs on the explicit YouTube image
host allowlist are retained or displayed. If none is safe, the pipeline builds
the deterministic `img.youtube.com` URL without making an HTTP request. The
browser loads the validated URL at display time; the server never probes it.

## Transcript provider chain

`youtube-transcript-api` remains the primary provider. Gemini is an optional
fallback that receives only the public YouTube watch URL. The pipeline never
downloads video or audio. Immediate fallback is limited to missing/disabled
captions, missing accepted languages, primary blocking, and an open primary
circuit. Ordinary transient errors exhaust the configured primary retry policy
first. Invalid IDs and explicitly non-public, deleted, unavailable, overlong,
or over-budget videos never invoke Gemini.

Gemini output must match the strict JSON schema and pass semantic validation.
The persisted row records the provider, model, selection strategy, fallback
reason, prompt version, model-generated flag, content version, provider attempt
counts, and the separate primary/fallback results. Gemini content uses the
distinct `model_generated` category and is never presented as an official
manual, automatic, or translated YouTube caption.

Successful results are cached by video ID, requested language, model, prompt
version, and source content version. Daily usage is bounded in video minutes;
usage, errors, latency, cache hits, budget, and circuit state use the existing
worker monitoring store.

## Configuration and activation

The fallback defaults to off and also stays off when `GEMINI_API_KEY` is empty.
Configure these values in the deployment secret/environment store (never in
Git):

```env
GEMINI_API_KEY=
GEMINI_TRANSCRIPT_FALLBACK_ENABLED=false
GEMINI_TRANSCRIPT_MODEL=gemini-3.5-flash
GEMINI_TRANSCRIPT_MAX_ATTEMPTS=2
GEMINI_TRANSCRIPT_TIMEOUT_SECONDS=120
GEMINI_TRANSCRIPT_MAX_DURATION_MINUTES=60
GEMINI_TRANSCRIPT_DAILY_VIDEO_MINUTES_BUDGET=120
GEMINI_TRANSCRIPT_COOLDOWN_SECONDS=3600
```

Gemini video processing can incur provider charges and the YouTube-URL feature
may have preview limitations. Start with a small duration and daily budget,
inject the secret, deploy the collector image, then set
`GEMINI_TRANSCRIPT_FALLBACK_ENABLED=true`. Monitor fallback reasons, processed
minutes, cache hits, error codes, and remaining budget before increasing limits.
The model is configurable so a lower-cost compatible model can be selected
without a code change.

## Rollback

Set `GEMINI_TRANSCRIPT_FALLBACK_ENABLED=false` and redeploy or restart the
collector. The primary provider, existing topics, DAGs, tables, lifecycle
states, and legacy fields continue unchanged. All schema and SQLite changes are
additive, so cached/provenance data may remain for audit and does not need to be
deleted during rollback.
