"""Independent incremental worker for YouTube comment text."""

from __future__ import annotations

import json
import os
from datetime import timedelta

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from common.youtube_pipeline import parse_datetime, utc_now
from common.youtube_state import YouTubeStateStore
from youtube_pipeline_events import EventConsumer, EventProducer, pipeline_event


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(_env(name, str(default))))
    except ValueError:
        return default


def comment_refresh_interval(published_at, observed_at) -> timedelta:
    published = parse_datetime(published_at) or observed_at
    age = max(timedelta(0), observed_at - published)
    if age < timedelta(days=1):
        return timedelta(hours=6)
    if age < timedelta(days=7):
        return timedelta(days=1)
    if age < timedelta(days=30):
        return timedelta(days=3)
    return timedelta(days=7)


def fetch_incremental_comments(
    youtube,
    video_id: str,
    *,
    known_comment_ids: set[str],
    max_pages: int,
) -> tuple[list[dict], int, bool]:
    comments: list[dict] = []
    page_token = None
    stopped_on_known = False
    pages = 0
    for _page in range(max(1, max_pages)):
        response = (
            youtube.commentThreads()
            .list(
                part="snippet,replies",
                videoId=video_id,
                order="time",
                maxResults=100,
                textFormat="plainText",
                pageToken=page_token,
            )
            .execute()
        )
        pages += 1
        for thread in response.get("items", []):
            top_level = (thread.get("snippet") or {}).get("topLevelComment") or {}
            thread_comments = [top_level, *((thread.get("replies") or {}).get("comments") or [])]
            for comment in thread_comments:
                comment_id = comment.get("id")
                if not comment_id:
                    continue
                if comment_id in known_comment_ids:
                    stopped_on_known = True
                    break
                comments.append(comment)
            if stopped_on_known:
                break
        if stopped_on_known:
            break
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return comments, pages, stopped_on_known


def _ingest_requests(state, bootstrap, registry, topic, limit):
    with EventConsumer(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        topic=topic,
        group_id=_env("YOUTUBE_COMMENT_GROUP_ID", "youtube-comments-v1"),
    ) as consumer:
        events = consumer.poll_batch(limit=limit * 2, idle_seconds=2)
        for event in events:
            if event.get("event_type") != "youtube.comment.requested":
                continue
            state.enqueue_request(
                "comment",
                video_id=event["video_id"],
                correlation_id=event.get("correlation_id") or event["video_id"],
                first_seen_at=utc_now(),
                published_at=event.get("published_at"),
                request=event,
            )
        consumer.commit()


def main() -> None:
    api_key = _env("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is required for comment collection")
    bootstrap = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    registry = _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    schema_path = _env("SCHEMA_PATH", "/app/schemas/playwright_event.avsc")
    request_topic = _env("YOUTUBE_COMMENT_REQUEST_TOPIC", "youtube.comment.requests")
    result_topic = _env("YOUTUBE_COMMENT_RESULT_TOPIC", "youtube.comment.results")
    state_path = _env("YOUTUBE_PIPELINE_STATE_DB", "/app/state/youtube-pipeline.sqlite")
    batch_size = _env_int("YOUTUBE_COMMENT_BATCH_SIZE", 10)
    max_pages = _env_int("YOUTUBE_COMMENT_MAX_PAGES", 3)
    daily_budget = _env_int("YOUTUBE_COMMENT_DAILY_REQUEST_BUDGET", 100)
    youtube = build("youtube", "v3", developerKey=api_key)
    producer = EventProducer(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        schema_path=schema_path,
    )
    summary = {
        "event": "youtube_comment_summary",
        "due": 0,
        "new_comments": 0,
        "pages": 0,
        "stopped_on_known": 0,
        "errors": 0,
        "budget_exhausted": False,
    }
    with YouTubeStateStore(state_path) as state:
        _ingest_requests(state, bootstrap, registry, request_topic, batch_size)
        now = utc_now()
        due = state.due_requests("comment", now=now, limit=batch_size)
        summary["due"] = len(due)
        remaining_budget = max(
            0,
            daily_budget - state.api_requests_today("commentThreads.list", now),
        )
        for row in due:
            if remaining_budget == 0:
                summary["budget_exhausted"] = True
                break
            attempted_at = utc_now()
            attempt_count = int(row.get("attempt_count") or 0) + 1
            try:
                comments, pages, stopped = fetch_incremental_comments(
                    youtube,
                    row["video_id"],
                    known_comment_ids=state.known_comment_ids(row["video_id"]),
                    max_pages=min(max_pages, remaining_budget),
                )
                remaining_budget = max(0, remaining_budget - pages)
                state.record_api_usage(
                    endpoint="commentThreads.list",
                    request_count=pages,
                    resource_count=len(comments),
                    success_count=pages,
                    error_count=0,
                    quota_bucket="secondary",
                    observed_at=attempted_at,
                )
                state.record_comment_ids(
                    row["video_id"],
                    [comment.get("id") for comment in comments],
                    attempted_at,
                )
                next_attempt = attempted_at + comment_refresh_interval(
                    row.get("published_at"), attempted_at
                )
                result = {
                    "comments": comments,
                    "pages": pages,
                    "stopped_on_known": stopped,
                }
                state.record_request_result(
                    "comment",
                    video_id=row["video_id"],
                    status="available",
                    attempted_at=attempted_at,
                    next_attempt_at=next_attempt,
                    result=result,
                )
                producer.publish(
                    result_topic,
                    [
                        pipeline_event(
                            "youtube.comment.result",
                            row["video_id"],
                            correlation_id=row["correlation_id"],
                            collected_at=attempted_at,
                            attempt_count=attempt_count,
                            comments_status="success",
                            comments_collected_at=attempted_at.isoformat(),
                            next_attempt_at=next_attempt.isoformat(),
                            payload_json=json.dumps(
                                result, ensure_ascii=False, sort_keys=True
                            ),
                        )
                    ],
                )
                summary["new_comments"] += len(comments)
                summary["pages"] += pages
                summary["stopped_on_known"] += int(stopped)
            except HttpError as exc:
                remaining_budget = max(0, remaining_budget - 1)
                status_code = int(getattr(getattr(exc, "resp", None), "status", 0) or 0)
                permanent = status_code in {400, 404}
                status = "permanent_error" if permanent else "retryable_error"
                state.record_api_usage(
                    endpoint="commentThreads.list",
                    request_count=1,
                    resource_count=0,
                    success_count=0,
                    error_count=1,
                    quota_bucket="secondary",
                    observed_at=attempted_at,
                )
                next_attempt = None if permanent else attempted_at + timedelta(hours=6)
                state.record_request_result(
                    "comment",
                    video_id=row["video_id"],
                    status=status,
                    attempted_at=attempted_at,
                    next_attempt_at=next_attempt,
                    error_class=f"youtube_http_{status_code or 'error'}",
                    error_message=str(exc),
                )
                summary["errors"] += 1
            except BaseException as exc:
                remaining_budget = max(0, remaining_budget - 1)
                state.record_api_usage(
                    endpoint="commentThreads.list",
                    request_count=1,
                    resource_count=0,
                    success_count=0,
                    error_count=1,
                    quota_bucket="secondary",
                    observed_at=attempted_at,
                )
                next_attempt = attempted_at + timedelta(hours=6)
                state.record_request_result(
                    "comment",
                    video_id=row["video_id"],
                    status="retryable_error",
                    attempted_at=attempted_at,
                    next_attempt_at=next_attempt,
                    error_class=type(exc).__name__,
                    error_message=str(exc),
                )
                summary["errors"] += 1
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
