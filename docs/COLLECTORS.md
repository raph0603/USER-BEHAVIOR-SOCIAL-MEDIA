# Collector behavior

## Running collectors

```bash
docker compose run --rm youtube-collector
docker compose run --rm x-collector
docker compose run --rm reddit-collector
```

Collectors publish to their configured raw Kafka topics. Airflow runs the same
entry points with per-run limits.

## Shared rules

- Emit one canonical event for each accepted source item.
- Preserve the native `platform_event_id` and canonical relationship fields.
- Record source publication time separately from collection time.
- Keep unknown counters null; do not invent zeros.
- Persist component outcomes and bounded error details.
- Mark only terminal component outcomes as processed.
- Retry partial, rate-limited, and failed components within configured limits.
- Fail the process for technical source failures instead of reporting a
  successful empty run.

The processed-state database is persistent under `data/collector-state/`.
Replaying an incomplete event is safe because downstream tables merge by
stable source identity.

## YouTube

The scheduled path uses the Data API `search.list` only to discover new video
IDs. Each query carries its own language and persistent watermark. Continuous
runs request one page; set `YOUTUBE_SEARCH_BACKFILL_ENABLED=true` only for an
intentional bounded multi-page backfill.

New IDs are enriched by `youtube_metadata_worker.py` using `yt-dlp` with
`download=False` and `skip_download=True`. The worker never downloads media.
It stores the raw response for Bronze reprocessing, publishes normalized
metadata, and schedules sparse descriptive checks. Concurrency defaults to one
and is capped at two. Retries use exponential backoff and jitter; explicit
blocking opens a global cooldown circuit.

Engagement refresh is separate. `insight_refresh.py` calls `videos.list` with
up to 50 IDs and reads only view, like and comment counts. It does not fetch
transcripts, text comments, collaborators, subscribers, full metadata or
watch pages. The due schedule ranges from 30 minutes for a new video to seven
days for a video older than 30 days, with an optional shorter interval for
high-growth videos.

Transcript, text-comment and channel-statistic workers consume separate
request topics. Successful transcripts are not fetched again. Comments stop
when a persistent known ID is found. Subscriber statistics are deduplicated
and cached by channel, then refreshed daily for active channels and weekly for
inactive channels via `channels.list` batches.

Caption selection is explicit and deterministic:

1. manually created track matching the configured language order, with an
   exact code preferred over a regional match;
2. generated track matching that language order;
3. another manually created track, sorted by language code;
4. another generated track, sorted by language code.

When a fallback track supports translation, it is translated to the explicit
translation language or the first preferred language. The recorded strategy is
`translated_manual_fallback` or `translated_generated_fallback`. A translation
or translated-fetch failure falls back to the original track and returns
`partial`. Empty fetched content is also `partial`.

Disabled captions, no usable track, and unavailable videos are terminal.
IP/request blocks and HTTP 429 are retriable `rate_limited` outcomes.
Transport, dependency, and unclassified errors are retriable `failed`
outcomes. Legitimate unavailability does not consume the systemic-failure
budget.

Comment pagination can produce `partial` when a configured page bound is
reached or a later page fails after earlier pages succeeded. Disabled comments
are `not_available`, not a technical failure.

Important settings include:

- `YOUTUBE_SEARCH_QUERIES_JSON`, `YOUTUBE_SEARCH_OVERLAP_MINUTES` and
  `YOUTUBE_SEARCH_BACKFILL_MAX_PAGES`;
- `YOUTUBE_METADATA_BATCH_SIZE`, `YOUTUBE_METADATA_CONCURRENCY`,
  `YOUTUBE_METADATA_REFRESH_HOURS` and `YOUTUBE_METADATA_BLOCK_COOLDOWN_SECONDS`;
- `YOUTUBE_TRANSCRIPT_LANGUAGES`;
- `YOUTUBE_COMMENT_BATCH_SIZE`, `YOUTUBE_COMMENT_MAX_PAGES` and
  `YOUTUBE_COMMENT_DAILY_REQUEST_BUDGET`;
- `YOUTUBE_CHANNEL_BATCH_SIZE`, `YOUTUBE_CHANNEL_DAILY_REQUEST_BUDGET`,
  `YOUTUBE_CHANNEL_ACTIVE_REFRESH_HOURS` and
  `YOUTUBE_CHANNEL_INACTIVE_REFRESH_HOURS`;
- `YOUTUBE_SEARCH_DAILY_REQUEST_BUDGET` and
  `YOUTUBE_VIDEOS_DAILY_REQUEST_BUDGET`;
- `YOUTUBE_TRANSCRIPT_MAX_FAILURES`;
- `YOUTUBE_COLLECTION_TIMEOUT_SECONDS`.

The legacy `YOUTUBE_SEARCH_QUERIES` and `YOUTUBE_SEARCH_LANGUAGES` variables
remain accepted as positionally paired values; they are never combined as a
Cartesian product.

## X

X uses an authenticated Playwright browser. Root posts and replies must retain
native conversation and parent IDs when the page exposes them. Metrics include
likes, views, replies, reposts, bookmarks, and follower counts when available.

An authentication challenge, blocked page, or exhausted retry loop is a
technical collection outcome. It must be visible to Airflow and must not be
reported as a valid zero-event run.

The browser profile and login diagnostics belong in persistent local state and
are excluded from version control.

## Reddit

Reddit collection scans each configured subreddit independently, attaches that
subreddit's metadata to its own candidates, then sorts accepted comments
globally by publication time. Metadata from one subreddit must never be reused
for another.

Root IDs come from the Reddit post, immediate parents come from comment parent
links, and depth distinguishes top-level comments from nested replies. Score is
kept as `score` rather than mapped to likes.

A zero-event run is valid only after source pages were successfully scanned and
all candidates were already processed or rejected by configured filters.
