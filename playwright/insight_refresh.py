import json
import os
import re
import socket
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from googleapiclient.discovery import build
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from engagement import extract_x_metric, parse_count


METRIC_COLUMNS = (
    "like_count",
    "comment_count",
    "reply_count",
    "view_count",
    "retweet_count",
    "bookmark_count",
    "score",
)


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
    }
    update.update({column: None for column in METRIC_COLUMNS})
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
            .list(part="statistics", id=",".join(batch_ids))
            .execute()
        )
        for item in response.get("items", []):
            target = targets_by_id.get(item.get("id"))
            if not target:
                continue
            statistics = item.get("statistics", {})
            update = _base_update(target)
            update.update(
                {
                    "like_count": parse_count(statistics.get("likeCount")),
                    "comment_count": parse_count(
                        statistics.get("commentCount")
                    ),
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
    cdp_url = _x_cdp_url()
    if cdp_url.startswith(("http://", "https://")):
        response = requests.get(
            f"{cdp_url.rstrip('/')}/__x_cdp__/ensure",
            params={"headless": _env("X_HEADLESS", "true")},
            timeout=int(_env("X_CDP_WAIT_SECONDS", "90")),
        )
        if response.status_code not in {200, 404}:
            response.raise_for_status()

    updates = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.connect_over_cdp(
            _resolve_cdp_url(cdp_url),
            timeout=30000,
        )
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
                            "reply_count": extract_x_metric(
                                article,
                                "reply",
                            ),
                            "view_count": extract_x_metric(
                                article,
                                "analytics",
                            ),
                            "retweet_count": extract_x_metric(
                                article,
                                "retweet",
                            ),
                            "bookmark_count": extract_x_metric(
                                article,
                                "bookmark",
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


def _refresh_reddit(targets: list[dict]) -> list[dict]:
    updates = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox"],
        )
        context = browser.new_context(
            user_agent=_env(
                "REDDIT_USER_AGENT",
                "Mozilla/5.0 Chrome/124 Safari/537.36 "
                "user-behavior-lakehouse/1.0",
            )
        )
        page = context.new_page()
        try:
            for target in targets:
                comment_id = _reddit_comment_id(target["url"])
                if not comment_id:
                    continue
                try:
                    reddit_url = target["url"].replace(
                        "https://www.reddit.com",
                        "https://old.reddit.com",
                    )
                    page.goto(
                        reddit_url,
                        wait_until="domcontentloaded",
                        timeout=60000,
                    )
                    comment = page.locator(
                        f'div.thing.comment[data-fullname="t1_{comment_id}"]'
                    ).first
                    comment.wait_for(state="attached", timeout=15000)
                    score_values = [
                        comment.get_attribute("data-score"),
                    ]
                    score = comment.locator(".score")
                    if score.count():
                        score_values.extend(
                            [
                                score.first.get_attribute("title"),
                                score.first.inner_text(),
                            ]
                        )
                    update = _base_update(target)
                    update["score"] = next(
                        (
                            parsed
                            for value in score_values
                            if (parsed := parse_count(value)) is not None
                        ),
                        None,
                    )
                    update["reply_count"] = comment.locator(
                        ":scope > .child > .sitetable > .thing.comment"
                    ).count()
                    updates.append(update)
                except (PlaywrightTimeoutError, PlaywrightError) as exc:
                    print(
                        "Unable to refresh Reddit insights for "
                        f"{target['url']}: {exc}"
                    )
        finally:
            browser.close()
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
    if targets and not updates:
        raise RuntimeError(f"No {source} insight target could be refreshed")


if __name__ == "__main__":
    main()
