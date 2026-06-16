import json
import os
import re
import socket
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from googleapiclient.discovery import build
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from engagement import extract_x_metric, parse_count
from youtube_authors import fetch_youtube_collaborators


METRIC_COLUMNS = (
    "like_count",
    "view_count",
)
SKIPPED_REFRESH_SOURCES = set()


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def _load_targets(path: Path, source: str) -> list[dict]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8") as target_file:
        targets = []
        for line in target_file:
            if not line.strip():
                continue
            target = json.loads(line)
            if target.get("source") == source:
                targets.append(target)
        return targets


def _base_update(target: dict) -> dict:
    update = {
        "user_id": target["user_id"],
        "url": target["url"],
        "event_ts": target["event_ts"],
        "source": target["source"],
        "platform_event_id": target.get("platform_event_id"),
        "metadata_refreshed_at": datetime.now(timezone.utc).isoformat(),
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


def _refresh_youtube(targets: list[dict]) -> list[dict]:
    api_key = _env("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is required for insight refresh")

    targets_by_id = {
        video_id: target
        for target in targets
        if (video_id := _youtube_video_id(target["url"]))
    }
    youtube = build("youtube", "v3", developerKey=api_key)
    updates = []
    video_ids = list(targets_by_id)
    for start in range(0, len(video_ids), 50):
        batch_ids = video_ids[start : start + 50]
        response = (
            youtube.videos()
            .list(part="snippet,statistics", id=",".join(batch_ids))
            .execute()
        )
        items = response.get("items", [])
        owner_by_id = {
            item["id"]: owner_channel_id
            for item in items
            if (
                owner_channel_id := (
                    item.get("snippet", {}).get("channelId")
                )
            )
        }
        collaborators_by_id = fetch_youtube_collaborators(
            owner_by_id,
            timeout_seconds=float(
                _env("YOUTUBE_WATCH_PAGE_TIMEOUT_SECONDS", "20")
            ),
            max_workers=int(
                _env("YOUTUBE_AUTHOR_FETCH_WORKERS", "8")
            ),
        )
        for item in items:
            target = targets_by_id.get(item.get("id"))
            if not target:
                continue
            statistics = item.get("statistics", {})
            owner_channel_id = item.get("snippet", {}).get("channelId")
            update = _base_update(target)
            update.update(
                {
                    "owner_channel_id": owner_channel_id,
                    "collaborator_channel_ids": collaborators_by_id.get(
                        item["id"]
                    ),
                    "like_count": parse_count(statistics.get("likeCount")),
                    "view_count": parse_count(statistics.get("viewCount")),
                }
            )
            updates.append(update)
    return updates


def _resolve_cdp_url(cdp_url: str) -> str:
    if not cdp_url.startswith(("http://", "https://")):
        return cdp_url
    parsed = urlparse(cdp_url)
    resolved_host = socket.gethostbyname(parsed.hostname or "")
    discovery_url = cdp_url.replace(parsed.hostname, resolved_host, 1)
    response = requests.get(
        f"{discovery_url.rstrip('/')}/json/version",
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
    return websocket_url


def _x_cdp_url() -> str:
    port_file = _env("X_CDP_PORT_FILE")
    if port_file:
        port = int(Path(port_file).read_text(encoding="utf-8").strip())
        return f"http://{_env('X_CDP_HOST', 'host.docker.internal')}:{port}"
    cdp_url = _env("X_CDP_URL")
    if not cdp_url:
        raise RuntimeError("No X CDP endpoint configured")
    return cdp_url


def _refresh_x(targets: list[dict]) -> list[dict]:
    try:
        cdp_url = _x_cdp_url()
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"Skipping X insight refresh: CDP endpoint unavailable: {exc}")
        SKIPPED_REFRESH_SOURCES.add("x")
        return []

    if cdp_url.startswith(("http://", "https://")):
        try:
            response = requests.get(
                f"{cdp_url.rstrip('/')}/__x_cdp__/ensure",
                params={"headless": _env("X_HEADLESS", "true")},
                timeout=int(_env("X_CDP_WAIT_SECONDS", "90")),
            )
            if response.status_code not in {200, 404}:
                response.raise_for_status()
        except requests.RequestException as exc:
            print(f"Skipping X insight refresh: CDP proxy unavailable: {exc}")
            SKIPPED_REFRESH_SOURCES.add("x")
            return []

    updates = []
    with sync_playwright() as playwright:
        try:
            browser = playwright.chromium.connect_over_cdp(
                _resolve_cdp_url(cdp_url),
                timeout=30000,
            )
        except (OSError, requests.RequestException, PlaywrightError) as exc:
            print(f"Skipping X insight refresh: browser CDP unavailable: {exc}")
            SKIPPED_REFRESH_SOURCES.add("x")
            return []
        context = (
            browser.contexts[0]
            if browser.contexts
            else browser.new_context()
        )
        page = context.new_page()
        try:
            for target in targets:
                try:
                    page.goto(
                        target["url"],
                        wait_until="domcontentloaded",
                        timeout=60000,
                    )
                    article = page.locator(
                        'article[data-testid="tweet"]'
                    ).first
                    article.wait_for(state="visible", timeout=20000)
                    update = _base_update(target)
                    update.update(
                        {
                            "like_count": extract_x_metric(article, "like"),
                            "view_count": extract_x_metric(
                                article,
                                "analytics",
                            ),
                        }
                    )
                    updates.append(update)
                except (PlaywrightTimeoutError, PlaywrightError) as exc:
                    print(f"Unable to refresh X insights for {target['url']}: {exc}")
        finally:
            page.close()
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
    if node.get("kind") == "t1" and data.get("id") == comment_id:
        return data

    return _find_reddit_comment(data.get("children", []), comment_id)


def _refresh_reddit(targets: list[dict]) -> list[dict]:
    updates = []
    headers = {
        "User-Agent": _env(
            "REDDIT_USER_AGENT",
            "Mozilla/5.0 Chrome/124 Safari/537.36 "
            "user-behavior-lakehouse/1.0",
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
                print(
                    "Skipping Reddit insight refresh: Reddit JSON endpoint "
                    f"returned HTTP {response.status_code}"
                )
                SKIPPED_REFRESH_SOURCES.add("reddit")
                return updates
            response.raise_for_status()
            comment = _find_reddit_comment(response.json(), comment_id)
            if not comment:
                print(
                    "Unable to refresh Reddit insights for "
                    f"{target['url']}: comment not found in JSON response"
                )
                continue
            updates.append(_base_update(target))
        except (ValueError, requests.RequestException) as exc:
            print(
                "Unable to refresh Reddit insights for "
                f"{target['url']}: {exc}"
            )
    return updates


def main() -> None:
    source = _env("INSIGHT_REFRESH_SOURCE").lower()
    if source not in {"youtube", "x", "reddit"}:
        raise RuntimeError(
            "INSIGHT_REFRESH_SOURCE must be youtube, x or reddit"
        )

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
    updates = refreshers[source](targets) if targets else []

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8") as output:
        for update in updates:
            output.write(json.dumps(update, ensure_ascii=True) + "\n")
    temporary_path.replace(output_path)

    print(
        f"Refreshed {len(updates)} of {len(targets)} {source} insight targets"
    )
    if targets and not updates and source not in SKIPPED_REFRESH_SOURCES:
        raise RuntimeError(f"No {source} insight target could be refreshed")


if __name__ == "__main__":
    main()
