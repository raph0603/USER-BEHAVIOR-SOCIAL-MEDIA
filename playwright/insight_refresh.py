import json
import os
import re
import socket
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

import requests
from googleapiclient.discovery import build
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from common.event_envelope import enrich_event_envelope
from common.youtube_pipeline import (
    next_metrics_refresh_at,
    parse_datetime,
)
from common.youtube_state import YouTubeStateStore
from engagement import extract_x_followers, extract_x_metric, parse_count


METRIC_COLUMNS = (
    "like_count",
    "view_count",
    "comment_count",
    "reply_count",
    "retweet_count",
    "bookmark_count",
    "score",
    "follower_count",
    "subscriber_count",
    "subreddit_member_count",
)
SKIPPED_REFRESH_SOURCES: set[str] = set()


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def _load_targets(path: Path, source: str) -> list[dict]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8-sig") as target_file:
        targets = []
        for line in target_file:
            if not line.strip():
                continue
            target = json.loads(line)
            if target.get("source") == source:
                targets.append(target)
        return targets


def _base_update(target: dict, observed_at: datetime | None = None) -> dict:
    observed_at = observed_at or datetime.now(timezone.utc)
    observed_value = observed_at.isoformat()
    try:
        next_refresh = next_metrics_refresh_at(
            target.get("event_ts") or observed_value,
            observed_at,
        ).isoformat()
    except ValueError:
        next_refresh = None
    update = {
        "user_id": target.get("user_id"),
        "url": target.get("url"),
        "event_ts": target.get("event_ts"),
        "source": target.get("source"),
        "platform_event_id": target.get("platform_event_id"),
        "metadata_refreshed_at": observed_value,
        "last_metrics_refresh_at": observed_value,
        "next_metrics_refresh_at": next_refresh,
        "metrics_refresh_count": int(target.get("metrics_refresh_count") or 0) + 1,
        "metrics_refresh_status": "success",
    }
    update.update({column: None for column in METRIC_COLUMNS})
    update["owner_channel_id"] = None
    update["collaborator_channel_ids"] = None
    return update


def _youtube_video_id(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.hostname in {"youtu.be", "www.youtu.be"}:
        return parsed.path.strip("/").split("/", 1)[0] or None
    return parse_qs(parsed.query).get("v", [None])[0]


def _youtube_growth_multiplier(
    target: dict,
    current_view_count: int | None,
    observed_at: datetime,
) -> float:
    previous_view_count = parse_count(target.get("view_count"))
    previous_observed_at = parse_datetime(
        target.get("last_metrics_refresh_at") or target.get("metadata_refreshed_at")
    )
    if (
        current_view_count is None
        or previous_view_count is None
        or previous_observed_at is None
        or observed_at <= previous_observed_at
    ):
        return 1.0
    elapsed_hours = (observed_at - previous_observed_at).total_seconds() / 3600
    views_per_hour = max(0, current_view_count - previous_view_count) / elapsed_hours
    threshold = float(_env("YOUTUBE_HIGH_GROWTH_VIEWS_PER_HOUR", "1000"))
    if views_per_hour < threshold:
        return 1.0
    return max(
        0.25,
        min(1.0, float(_env("YOUTUBE_HIGH_GROWTH_INTERVAL_MULTIPLIER", "0.5"))),
    )


def _refresh_youtube(targets: list[dict]) -> list[dict]:
    api_key = _env("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is required for insight refresh")

    targets_by_id = {
        video_id: target
        for target in targets
        if (video_id := (target.get("platform_event_id") or _youtube_video_id(target["url"])))
    }
    youtube = build("youtube", "v3", developerKey=api_key)
    updates = []
    video_ids = list(targets_by_id)
    state_path = _env("YOUTUBE_PIPELINE_STATE_DB")
    usage_store = YouTubeStateStore(state_path) if state_path else None
    try:
        if usage_store:
            budget = int(_env("YOUTUBE_VIDEOS_DAILY_REQUEST_BUDGET", "500"))
            used = usage_store.api_requests_today("videos.list", datetime.now(timezone.utc))
            remaining_batches = max(0, budget - used)
            video_ids = video_ids[: remaining_batches * 50]
            if not video_ids and targets_by_id:
                print("Skipping YouTube metrics refresh: videos.list budget exhausted")
                SKIPPED_REFRESH_SOURCES.add("youtube")
        for start in range(0, len(video_ids), 50):
            batch_ids = video_ids[start : start + 50]
            observed_at = datetime.now(timezone.utc)
            response = youtube.videos().list(part="statistics", id=",".join(batch_ids)).execute()
            items_by_id = {item["id"]: item for item in response.get("items", []) if item.get("id")}
            if usage_store:
                usage_store.record_api_usage(
                    endpoint="videos.list",
                    request_count=1,
                    resource_count=len(batch_ids),
                    success_count=1,
                    error_count=0,
                    quota_bucket="recent_metrics",
                    observed_at=observed_at,
                )
            for video_id in batch_ids:
                target = targets_by_id[video_id]
                item = items_by_id.get(video_id)
                update = _base_update(target, observed_at)
                if item is None:
                    update["metrics_refresh_status"] = "not_available"
                    updates.append(update)
                    continue
                statistics = item.get("statistics", {})
                view_count = parse_count(statistics.get("viewCount"))
                growth_multiplier = _youtube_growth_multiplier(
                    target,
                    view_count,
                    observed_at,
                )
                update["next_metrics_refresh_at"] = next_metrics_refresh_at(
                    target.get("event_ts") or observed_at,
                    observed_at,
                    growth_multiplier=growth_multiplier,
                ).isoformat()
                update.update(
                    {
                        "like_count": parse_count(statistics.get("likeCount")),
                        "view_count": view_count,
                        "comment_count": parse_count(statistics.get("commentCount")),
                    }
                )
                updates.append(update)
    finally:
        if usage_store:
            usage_store.close()
    return updates


def _x_cdp_access_token() -> str:
    token = _env("X_CDP_TOKEN")
    if token:
        return token

    token_file = _env("X_CDP_TOKEN_FILE")
    if not token_file:
        return ""
    path = Path(token_file)
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _with_x_cdp_token(url: str) -> str:
    token = _x_cdp_access_token()
    if not token:
        return url

    parsed = urlparse(url)
    query = [
        (name, value)
        for name, value in parse_qsl(parsed.query, keep_blank_values=True)
        if name != "token"
    ]
    query.append(("token", token))
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query),
            parsed.fragment,
        )
    )


def _x_cdp_url_with_path(cdp_url: str, path: str) -> str:
    parsed = urlparse(cdp_url)
    return _with_x_cdp_token(
        urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                path,
                "",
                parsed.query,
                parsed.fragment,
            )
        )
    )


def _resolve_cdp_url(cdp_url: str) -> str:
    if not cdp_url.startswith(("http://", "https://")):
        return cdp_url
    parsed = urlparse(cdp_url)
    source_host = parsed.hostname or ""
    resolved_host = socket.gethostbyname(source_host)
    discovery_url = _x_cdp_url_with_path(cdp_url, "/json/version").replace(
        source_host,
        resolved_host,
        1,
    )
    response = requests.get(
        discovery_url,
        timeout=5,
    )
    response.raise_for_status()
    websocket_url = response.json()["webSocketDebuggerUrl"]
    websocket_host = urlparse(websocket_url).hostname
    if websocket_host:
        websocket_url = websocket_url.replace(
            websocket_host,
            resolved_host,
            1,
        )
    return _with_x_cdp_token(websocket_url)


def _x_cdp_url() -> str:
    port_file = _env("X_CDP_PORT_FILE")
    if port_file:
        port = int(Path(port_file).read_text(encoding="utf-8").strip())
        return f"http://{_env('X_CDP_HOST', 'host.docker.internal')}:{port}"
    cdp_url = _env("X_CDP_URL")
    if not cdp_url:
        raise RuntimeError("No X CDP endpoint configured")
    return cdp_url


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _x_auth_cookies() -> list[dict]:
    auth_token = _env("X_AUTH_TOKEN")
    ct0 = _env("X_CT0")
    if not auth_token:
        return []

    cookies = []
    for domain in [".x.com", ".twitter.com"]:
        cookies.append(
            {
                "name": "auth_token",
                "value": auth_token,
                "domain": domain,
                "path": "/",
                "httpOnly": True,
                "secure": True,
                "sameSite": "None",
            }
        )
        if ct0:
            cookies.append(
                {
                    "name": "ct0",
                    "value": ct0,
                    "domain": domain,
                    "path": "/",
                    "httpOnly": False,
                    "secure": True,
                    "sameSite": "Lax",
                }
            )
    return cookies


def _x_browser_context_options() -> dict:
    return {
        "locale": "en-US",
        "viewport": {"width": 1280, "height": 900},
        "user_agent": _env(
            "X_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0 Safari/537.36",
        ),
    }


def _refresh_x(targets: list[dict]) -> list[dict]:
    updates = []
    with sync_playwright() as playwright:
        try:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=_env("X_USER_DATA_DIR", "/app/x-browser-profile"),
                headless=_env_bool("X_HEADLESS", True),
                args=["--no-sandbox"],
                **_x_browser_context_options(),
            )
        except PlaywrightError as exc:
            print(f"Skipping X insight refresh: browser unavailable: {exc}")
            SKIPPED_REFRESH_SOURCES.add("x")
            return []
        auth_cookies = _x_auth_cookies()
        if auth_cookies:
            context.add_cookies(auth_cookies)
        page = context.new_page()
        try:
            for target in targets:
                try:
                    page.goto(
                        target["url"],
                        wait_until="domcontentloaded",
                        timeout=60000,
                    )
                    article = page.locator('article[data-testid="tweet"]').first
                    article.wait_for(state="visible", timeout=20000)
                    update = _base_update(target)
                    update.update(
                        {
                            "like_count": extract_x_metric(article, "like"),
                            "view_count": extract_x_metric(
                                article,
                                "analytics",
                            ),
                            "follower_count": extract_x_followers(article),
                        }
                    )
                    updates.append(update)
                except (PlaywrightTimeoutError, PlaywrightError) as exc:
                    print(f"Unable to refresh X insights for {target['url']}: {exc}")
        finally:
            page.close()
            context.close()
    return updates


def _reddit_comment_id(url: str) -> str | None:
    match = re.search(
        r"/comments/[^/]+/[^/]+/([a-z0-9]+)/?",
        url,
        re.IGNORECASE,
    )
    return match.group(1) if match else None


def _reddit_comment_json_url(url: str) -> str:
    return f"{url.rstrip('/')}.json"


def _reddit_old_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(
        (
            parsed.scheme or "https",
            "old.reddit.com",
            parsed.path,
            "",
            "",
            "",
        )
    )


def _find_reddit_comment(node, comment_id: str) -> dict | None:
    if isinstance(node, list):
        for item in node:
            found = _find_reddit_comment(item, comment_id)
            if found:
                return found
        return None

    if not isinstance(node, dict):
        return None

    data = node.get("data", {})
    if not isinstance(data, dict):
        return None
    if node.get("kind") == "t1" and data.get("id") == comment_id:
        return data

    return _find_reddit_comment(data.get("children", []), comment_id)


def _reddit_post_data(payload) -> dict:
    try:
        if isinstance(payload, list) and payload:
            post = payload[0]["data"]["children"][0]["data"]
            return post if isinstance(post, dict) else {}
    except (IndexError, KeyError, TypeError, AttributeError):
        pass
    return {}


def _count_reddit_reply_children(replies) -> int | None:
    if not isinstance(replies, dict):
        return 0

    children = replies.get("data", {}).get("children", [])
    if not isinstance(children, list):
        return 0

    count = 0
    for child in children:
        if not isinstance(child, dict) or child.get("kind") != "t1":
            continue
        count += 1
        child_data = child.get("data", {})
        if isinstance(child_data, dict):
            nested_count = _count_reddit_reply_children(child_data.get("replies"))
            count += nested_count or 0
    return count


def _extract_old_reddit_comment_block(html: str, comment_id: str) -> str:
    marker = f'data-fullname="t1_{comment_id}"'
    start = html.find(marker)
    if start < 0:
        return ""
    block_start = html.rfind("<div", 0, start)
    if block_start < 0:
        block_start = start
    next_start = html.find('data-fullname="t1_', start + len(marker))
    return html[block_start : next_start if next_start > start else len(html)]


def _extract_old_reddit_comment_score(block: str) -> int | None:
    for pattern in (
        r'<span[^>]+class="score[^"]*"[^>]+title="([^"]+)"',
        r'<span[^>]+class="score[^"]*"[^>]*>([^<]+)</span>',
        r'data-score="([^"]+)"',
    ):
        match = re.search(pattern, block, re.IGNORECASE)
        if match:
            parsed = parse_count(match.group(1))
            if parsed is not None:
                return parsed
    return None


def _extract_old_reddit_member_count(html: str) -> int | None:
    for pattern in (
        r'<span[^>]+class="number"[^>]*>([^<]+)</span>\s*'
        r'<span[^>]+class="word"[^>]*>(?:subscribers|members)</span>',
        r"([0-9][0-9,.kmbKMB]*)\s+(?:subscribers|members)",
    ):
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            parsed = parse_count(match.group(1))
            if parsed is not None:
                return parsed
    return None


def _extract_old_reddit_comment_count(html: str) -> int | None:
    for pattern in (
        r'<a[^>]+class="[^"]*comments[^"]*"[^>]*>([0-9][0-9,.kmbKMB]*)\s+comments?',
        r"([0-9][0-9,.kmbKMB]*)\s+comments?",
    ):
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            parsed = parse_count(match.group(1))
            if parsed is not None:
                return parsed
    return None


def _extract_old_reddit_reply_count(block: str) -> int:
    return max(0, len(re.findall(r'data-fullname="t1_', block)) - 1)


def _refresh_reddit_from_html(target: dict, headers: dict) -> dict | None:
    comment_id = _reddit_comment_id(target["url"])
    if not comment_id:
        return None
    response = requests.get(
        _reddit_old_url(target["url"]),
        headers=headers,
        timeout=int(_env("REDDIT_REFRESH_TIMEOUT_SECONDS", "15")),
    )
    response.raise_for_status()
    html = response.text
    block = _extract_old_reddit_comment_block(html, comment_id)
    if not block:
        return None

    update = _base_update(target)
    update.update(
        {
            "comment_count": _extract_old_reddit_comment_count(html),
            "reply_count": _extract_old_reddit_reply_count(block),
            "score": _extract_old_reddit_comment_score(block),
            "subreddit_member_count": _extract_old_reddit_member_count(html),
        }
    )
    return update


def _finalize_update(update: dict) -> dict:
    source = str(update.get("source") or "unknown").lower()
    collection_methods = {
        "youtube": "youtube_data_api",
        "reddit": "reddit_public_json",
        "x": "x_browser",
    }
    endpoints = {
        "youtube": "videos.list",
        "reddit": "comments.json",
        "x": "post.metrics",
    }
    observed_at = update.get("metadata_refreshed_at")
    event = {
        **update,
        "event_type": f"{source}.engagement.snapshot",
        "event_version": "1.0",
        "collected_at": observed_at,
        "observed_at": observed_at,
        "timestamp": update.get("event_ts") or observed_at,
        "collector_version": _env("COLLECTOR_VERSION", "1"),
        "source_payload_version": "2",
        "metadata_available": update.get("metrics_refresh_status") in {"success", "available"},
    }
    return enrich_event_envelope(
        event,
        producer_name=f"{source}_metrics_worker",
        producer_run_id=_env("PIPELINE_RUN_ID", "standalone"),
        collection_method=collection_methods.get(source),
        api_endpoint=endpoints.get(source),
    )


def _refresh_reddit(targets: list[dict]) -> list[dict]:
    updates = []
    headers = {
        "User-Agent": _env(
            "REDDIT_USER_AGENT",
            "Mozilla/5.0 Chrome/124 Safari/537.36 user-behavior-lakehouse/1.0",
        )
    }
    timeout = int(_env("REDDIT_REFRESH_TIMEOUT_SECONDS", "15"))
    for target in targets:
        comment_id = _reddit_comment_id(target["url"])
        if not comment_id:
            continue
        try:
            response = requests.get(
                _reddit_comment_json_url(target["url"]),
                headers=headers,
                timeout=timeout,
            )
            if response.status_code in {403, 429}:
                html_update = _refresh_reddit_from_html(target, headers)
                if html_update:
                    updates.append(html_update)
                    continue
                print(
                    "Unable to refresh Reddit insights from JSON or HTML for "
                    f"{target['url']}: HTTP {response.status_code}"
                )
                continue
            response.raise_for_status()
            payload = response.json()
            comment = _find_reddit_comment(payload, comment_id)
            if not comment:
                print(
                    "Unable to refresh Reddit insights for "
                    f"{target['url']}: comment not found in JSON response"
                )
                continue
            post = _reddit_post_data(payload)
            update = _base_update(target)
            update.update(
                {
                    "comment_count": parse_count(post.get("num_comments")),
                    "reply_count": _count_reddit_reply_children(comment.get("replies")),
                    "score": parse_count(comment.get("score")),
                    "subreddit_member_count": parse_count(post.get("subreddit_subscribers")),
                }
            )
            updates.append(update)
        except (ValueError, requests.RequestException, AttributeError) as exc:
            print(f"Unable to refresh Reddit insights for {target['url']}: {exc}")
    return updates


def _publish_youtube_engagement(updates: list[dict]) -> None:
    if not updates or not _env("KAFKA_BOOTSTRAP"):
        return
    from youtube_pipeline_events import EventProducer, pipeline_event

    producer = EventProducer(
        bootstrap_servers=_env("KAFKA_BOOTSTRAP", "kafka:9092"),
        schema_registry_url=_env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
        schema_path=_env("SCHEMA_PATH", "/app/schemas/playwright_event.avsc"),
    )
    events = [
        pipeline_event(
            "youtube.engagement.snapshot",
            update["platform_event_id"],
            collected_at=parse_datetime(update["metadata_refreshed_at"]),
            attempt_count=update.get("metrics_refresh_count") or 1,
            published_at=update.get("event_ts"),
            view_count=update.get("view_count"),
            like_count=update.get("like_count"),
            comment_count=update.get("comment_count"),
            last_metrics_refresh_at=update.get("last_metrics_refresh_at"),
            next_metrics_refresh_at=update.get("next_metrics_refresh_at"),
            metrics_refresh_count=update.get("metrics_refresh_count"),
            metrics_refresh_status=update.get("metrics_refresh_status"),
            collection_status=update.get("metrics_refresh_status"),
            event_id=update.get("event_id"),
            observation_id=update.get("observation_id"),
            observed_at=update.get("observed_at"),
            producer_name=update.get("producer_name"),
            producer_run_id=update.get("producer_run_id"),
            collection_method=update.get("collection_method"),
            api_endpoint=update.get("api_endpoint"),
            payload_fingerprint=update.get("payload_fingerprint"),
            provenance_json=update.get("provenance_json"),
            coverage_json=update.get("coverage_json"),
            **{
                f"{metric}_available": update.get(f"{metric}_available")
                for metric in METRIC_COLUMNS
            },
            metadata_available=update.get("metadata_available"),
            transcript_available=update.get("transcript_available"),
            comments_available=update.get("comments_available"),
            payload_json=json.dumps(update, ensure_ascii=False, sort_keys=True),
        )
        for update in updates
        if update.get("platform_event_id")
    ]
    producer.publish(
        _env("YOUTUBE_ENGAGEMENT_TOPIC", "youtube.engagement.snapshots"),
        events,
    )


def main() -> None:
    source = _env("INSIGHT_REFRESH_SOURCE").lower()
    if source not in {"youtube", "x", "reddit"}:
        raise RuntimeError("INSIGHT_REFRESH_SOURCE must be youtube, x or reddit")

    targets_path = Path(
        _env(
            "INSIGHT_REFRESH_TARGETS_PATH",
            "/app/insight-refresh/targets.jsonl",
        )
    )
    output_path = Path(
        _env(
            "INSIGHT_REFRESH_OUTPUT_PATH",
            f"/app/insight-refresh/{source}.jsonl",
        )
    )
    targets = _load_targets(targets_path, source)
    refreshers = {
        "youtube": _refresh_youtube,
        "x": _refresh_x,
        "reddit": _refresh_reddit,
    }
    updates = (
        [_finalize_update(update) for update in refreshers[source](targets)] if targets else []
    )
    if source == "youtube":
        _publish_youtube_engagement(updates)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8") as output:
        for update in updates:
            output.write(json.dumps(update, ensure_ascii=True) + "\n")
    temporary_path.replace(output_path)

    print(f"Refreshed {len(updates)} of {len(targets)} {source} insight targets")
    if targets and not updates and source not in SKIPPED_REFRESH_SOURCES:
        raise RuntimeError(f"No {source} insight target could be refreshed")


if __name__ == "__main__":
    main()
