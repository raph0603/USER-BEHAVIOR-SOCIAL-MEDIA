"""Incremental YouTube discovery using search.list only."""

from __future__ import annotations

import json
import os
import uuid
from datetime import timedelta

from googleapiclient.discovery import build

from common.youtube_pipeline import (
    SearchQuery,
    YouTubeStateStore,
    isoformat,
    parse_datetime,
    parse_search_queries,
    published_after,
    utc_now,
)
from youtube_pipeline_events import EventProducer, pipeline_event


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(_env(name, str(default))))
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _legacy_queries() -> list[str]:
    return [
        query.strip()
        for query in _env("YOUTUBE_SEARCH_QUERIES").split("||")
        if query.strip()
    ]


def _legacy_languages() -> list[str]:
    raw = _env(
        "YOUTUBE_SEARCH_LANGUAGES",
        _env("YOUTUBE_SEARCH_LANGUAGE", "en"),
    )
    return [language.strip() for language in raw.split(",") if language.strip()]


def search_query(
    youtube,
    spec: SearchQuery,
    *,
    published_after_value: str,
    backfill: bool,
    max_pages: int,
    order: str,
) -> tuple[list[dict], int]:
    """Return bounded discovery items and the number of API calls made."""

    items: list[dict] = []
    page_token = None
    pages = max_pages if backfill else 1
    calls = 0
    for _page in range(pages):
        response = (
            youtube.search()
            .list(
                part="id,snippet",
                q=spec.query,
                type="video",
                relevanceLanguage=spec.language or None,
                order=order,
                publishedAfter=published_after_value,
                maxResults=50,
                pageToken=page_token,
            )
            .execute()
        )
        calls += 1
        items.extend(response.get("items", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return items, calls


def _video_id(item: dict) -> str | None:
    item_id = item.get("id")
    if isinstance(item_id, dict):
        value = item_id.get("videoId")
        return str(value).strip() if value else None
    return str(item_id).strip() if isinstance(item_id, str) and item_id.strip() else None


def main() -> None:
    api_key = _env("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is required for discovery")

    specs = parse_search_queries(
        _env("YOUTUBE_SEARCH_QUERIES_JSON") or None,
        _legacy_queries(),
        _legacy_languages(),
    )
    state_path = _env("YOUTUBE_PIPELINE_STATE_DB", "/app/state/youtube-pipeline.sqlite")
    bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    registry = _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    schema_path = _env("SCHEMA_PATH", "/app/schemas/playwright_event.avsc")
    topic = _env("YOUTUBE_DISCOVERY_TOPIC", "youtube.discovery.events")
    backfill = _env_bool("YOUTUBE_SEARCH_BACKFILL_ENABLED", False)
    max_pages = _env_int("YOUTUBE_SEARCH_BACKFILL_MAX_PAGES", 10)
    run_limit = _env_int("YOUTUBE_DISCOVERY_MAX_EVENTS", 200)
    overlap = timedelta(
        minutes=_env_int("YOUTUBE_SEARCH_OVERLAP_MINUTES", 120)
    )
    lookback = timedelta(
        hours=_env_int("YOUTUBE_SEARCH_INITIAL_LOOKBACK_HOURS", 24)
    )
    order = _env("YOUTUBE_SEARCH_ORDER", "date")
    youtube = build("youtube", "v3", developerKey=api_key)
    producer = EventProducer(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        schema_path=schema_path,
    )
    totals = {
        "event": "youtube_discovery_summary",
        "search_calls": 0,
        "pages": 0,
        "discovered": 0,
        "duplicates": 0,
        "new_videos": 0,
        "queries": {},
        "backfill": backfill,
    }

    with YouTubeStateStore(state_path) as state:
        for spec in specs:
            if totals["new_videos"] >= run_limit:
                break
            started_at = utc_now()
            daily_budget = _env_int("YOUTUBE_SEARCH_DAILY_REQUEST_BUDGET", 100)
            used_budget = state.api_requests_today("search.list", started_at)
            remaining_budget = max(0, daily_budget - used_budget)
            if remaining_budget == 0:
                totals["budget_exhausted"] = True
                break
            watermark = state.watermark(spec.query_id) or {}
            after = published_after(
                watermark.get("last_published_at_seen")
                or watermark.get("last_successful_search_at"),
                overlap=overlap,
                initial_lookback=lookback,
                now=started_at,
            )
            items, calls = search_query(
                youtube,
                spec,
                published_after_value=isoformat(after).replace("+00:00", "Z"),
                backfill=backfill,
                max_pages=min(max_pages, remaining_budget),
                order=order,
            )
            totals["search_calls"] += calls
            totals["pages"] += calls
            totals["discovered"] += len(items)
            state.record_api_usage(
                endpoint="search.list",
                request_count=calls,
                resource_count=len(items),
                success_count=calls,
                error_count=0,
                quota_bucket="discovery",
                observed_at=started_at,
            )
            query_events: list[dict] = []
            last_published = parse_datetime(watermark.get("last_published_at_seen"))
            for item in items:
                video_id = _video_id(item)
                if not video_id:
                    continue
                snippet = item.get("snippet") or {}
                item_published = parse_datetime(snippet.get("publishedAt"))
                if item_published and (last_published is None or item_published > last_published):
                    last_published = item_published
                if state.is_discovered(video_id) or any(
                    event["video_id"] == video_id for event in query_events
                ):
                    totals["duplicates"] += 1
                    continue
                correlation_id = str(uuid.uuid4())
                query_events.append(
                    pipeline_event(
                        "youtube.discovery.discovered",
                        video_id,
                        correlation_id=correlation_id,
                        collected_at=started_at,
                        published_at=snippet.get("publishedAt"),
                        query_id=spec.query_id,
                        language=spec.language,
                        payload_json=json.dumps(
                            {
                                "query": spec.query,
                                "language": spec.language,
                                "search_snippet": snippet,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        collection_status="success",
                    )
                )
                if totals["new_videos"] + len(query_events) >= run_limit:
                    break

            producer.publish(topic, query_events)
            for event in query_events:
                state.record_discovery(
                    video_id=event["video_id"],
                    query_id=spec.query_id,
                    first_seen_at=started_at,
                    published_at=event.get("published_at"),
                    correlation_id=event["correlation_id"],
                )
            state.record_search_success(
                spec,
                searched_at=started_at,
                last_published_at_seen=last_published,
            )
            totals["new_videos"] += len(query_events)
            totals["queries"][spec.query_id] = {
                "query": spec.query,
                "language": spec.language,
                "results": len(items),
                "new": len(query_events),
                "published_after": isoformat(after),
            }

    print(json.dumps(totals, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
