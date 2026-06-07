import csv
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.sync_api import Locator, Page, TimeoutError, sync_playwright


X_COLUMNS = [
    "page_url",
    "tweet_url",
    "status_id",
    "article_index",
    "screen_name",
    "display_name",
    "author_hash",
    "tweet_text",
    "lang",
    "tweet_time",
    "tweet_time_iso",
    "reply_count",
    "retweet_count",
    "like_count",
    "bookmark_count",
    "view_count",
    "is_reply",
    "is_pinned",
    "has_media",
    "media_count",
    "hashtags",
    "mentions",
    "external_links",
    "scraped_at_utc",
]


@dataclass
class XRecord:
    page_url: str
    tweet_url: str
    status_id: str
    article_index: int | None
    screen_name: str | None
    display_name: str | None
    author_hash: str
    tweet_text: str
    lang: str | None
    tweet_time: str | None
    tweet_time_iso: str
    reply_count: int | None
    retweet_count: int | None
    like_count: int | None
    bookmark_count: int | None
    view_count: int | None
    is_reply: bool
    is_pinned: bool | None
    has_media: bool | None
    media_count: int | None
    hashtags: str | None
    mentions: str | None
    external_links: str | None
    scraped_at_utc: str


def _env_list(name: str) -> list[str]:
    value = os.getenv(name, "")
    return [item.strip() for item in value.split(",") if item.strip()]


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        return int(value)
    except ValueError:
        return default


def _env_path(name: str, default: str) -> Path:
    return Path(os.getenv(name, default))


def _x_auth_cookies() -> list[dict]:
    auth_token = os.getenv("X_AUTH_TOKEN")
    ct0 = os.getenv("X_CT0")
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


def _status_id_from_url(url: str) -> str | None:
    match = re.search(r"/status/(\d+)", url)
    return match.group(1) if match else None


def _clean_tweet_url(base_url: str, href: str | None) -> str | None:
    if not href:
        return None
    absolute = urljoin(base_url, href)
    parsed = urlparse(absolute)
    status_id = _status_id_from_url(parsed.path)
    if not status_id:
        return None
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 3:
        return None
    return f"https://x.com/{parts[0]}/status/{status_id}"


def _first_attr(locator: Locator, attr: str) -> str | None:
    try:
        if locator.count() == 0:
            return None
        return locator.first.get_attribute(attr, timeout=1500)
    except TimeoutError:
        return None


def _first_text(locator: Locator) -> str | None:
    try:
        if locator.count() == 0:
            return None
        text = locator.first.inner_text(timeout=1500).strip()
        return text or None
    except TimeoutError:
        return None


def _extract_tweet_url(article: Locator, page_url: str) -> str | None:
    links = article.locator('a[href*="/status/"]')
    for idx in range(links.count()):
        href = links.nth(idx).get_attribute("href")
        tweet_url = _clean_tweet_url(page_url, href)
        if tweet_url:
            return tweet_url
    return None


def _extract_text(article: Locator) -> str | None:
    tweet_text = _first_text(article.locator('[data-testid="tweetText"]'))
    if tweet_text:
        return tweet_text

    full_text = _first_text(article)
    if not full_text:
        return None
    lines = [line.strip() for line in full_text.splitlines() if line.strip()]
    return " ".join(lines) if lines else None


def _extract_user(article: Locator) -> tuple[str | None, str | None]:
    user_text = _first_text(article.locator('[data-testid="User-Name"]'))
    if not user_text:
        return None, None

    lines = [line.strip() for line in user_text.splitlines() if line.strip()]
    display_name = lines[0] if lines else None
    screen_name = next((line for line in lines if line.startswith("@")), None)
    return screen_name, display_name


def _extract_entities(article: Locator, text: str | None) -> tuple[str | None, str | None, str | None]:
    hashtags = None
    mentions = None
    if text:
        hashtag_values = sorted(set(re.findall(r"#\w+", text)))
        mention_values = sorted(set(re.findall(r"@\w+", text)))
        hashtags = " | ".join(hashtag_values) or None
        mentions = " | ".join(mention_values) or None

    links = []
    anchors = article.locator("a[href]")
    for idx in range(anchors.count()):
        href = anchors.nth(idx).get_attribute("href")
        if not href:
            continue
        absolute = urljoin("https://x.com", href)
        if "/status/" in absolute or "hashtag" in absolute:
            continue
        if absolute.startswith(("http://", "https://")):
            links.append(absolute)

    external_links = " | ".join(sorted(set(links))) or None
    return hashtags, mentions, external_links


def _extract_record(article: Locator, page_url: str, article_index: int, root_status_id: str | None) -> XRecord | None:
    tweet_url = _extract_tweet_url(article, page_url)
    status_id = _status_id_from_url(tweet_url or "")
    if not tweet_url or not status_id:
        return None

    text = _extract_text(article)
    tweet_time_iso = _first_attr(article.locator("time"), "datetime")
    if not text or not tweet_time_iso:
        return None

    screen_name, display_name = _extract_user(article)
    hashtags, mentions, external_links = _extract_entities(article, text)
    media_count = article.locator('[data-testid="tweetPhoto"], [data-testid="videoPlayer"]').count()
    scraped_at = datetime.now(timezone.utc).isoformat()

    return XRecord(
        page_url=page_url,
        tweet_url=tweet_url,
        status_id=status_id,
        article_index=article_index,
        screen_name=screen_name,
        display_name=display_name,
        author_hash="anonymous",
        tweet_text=text,
        lang=None,
        tweet_time=tweet_time_iso,
        tweet_time_iso=tweet_time_iso,
        reply_count=None,
        retweet_count=None,
        like_count=None,
        bookmark_count=None,
        view_count=None,
        is_reply=bool(root_status_id and status_id != root_status_id),
        is_pinned=False,
        has_media=media_count > 0,
        media_count=media_count,
        hashtags=hashtags,
        mentions=mentions,
        external_links=external_links,
        scraped_at_utc=scraped_at,
    )


def crawl_tweet_thread(page: Page, tweet_url: str, max_replies: int, scroll_limit: int) -> list[XRecord]:
    root_status_id = _status_id_from_url(tweet_url)
    records_by_status: dict[str, XRecord] = {}

    page.goto(tweet_url, wait_until="domcontentloaded", timeout=45_000)
    page.wait_for_selector("article", timeout=20_000)

    for _ in range(scroll_limit + 1):
        articles = page.locator("article")
        for idx in range(articles.count()):
            record = _extract_record(articles.nth(idx), tweet_url, idx, root_status_id)
            if not record or record.status_id in records_by_status:
                continue
            records_by_status[record.status_id] = record

        reply_count = sum(1 for record in records_by_status.values() if record.is_reply)
        if reply_count >= max_replies:
            break

        page.mouse.wheel(0, 2400)
        page.wait_for_timeout(1200)

    root_records = [
        record for record in records_by_status.values()
        if root_status_id and record.status_id == root_status_id
    ]
    reply_records = [
        record for record in records_by_status.values()
        if not root_status_id or record.status_id != root_status_id
    ][:max_replies]
    return root_records + reply_records


def write_csv(records: list[XRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=X_COLUMNS)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def main() -> None:
    target_urls = _env_list("X_TARGET_URLS")
    output_path = _env_path("X_OUTPUT_CSV", "data/samples/x_crawler_output.csv")
    headless = _env_bool("X_HEADLESS", True)
    max_replies = _env_int("X_MAX_REPLIES_PER_TWEET", 20)
    scroll_limit = _env_int("X_SCROLL_LIMIT", 8)
    storage_state = os.getenv("X_STORAGE_STATE")

    if not target_urls:
        raise SystemExit("Set X_TARGET_URLS to one or more tweet URLs.")

    records: list[XRecord] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--no-sandbox"])
        context_options = dict(
            locale="en-US",
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0 Safari/537.36"
            ),
        )
        if storage_state:
            context_options["storage_state"] = storage_state
        context = browser.new_context(**context_options)
        auth_cookies = _x_auth_cookies()
        if auth_cookies:
            context.add_cookies(auth_cookies)
        page = context.new_page()
        for url in target_urls:
            records.extend(crawl_tweet_thread(page, url, max_replies, scroll_limit))
        browser.close()

    write_csv(records, output_path)
    root_count = sum(1 for record in records if not record.is_reply)
    reply_count = sum(1 for record in records if record.is_reply)
    print(f"Wrote {len(records)} X records to {output_path}")
    print(f"Root tweets: {root_count}; replies: {reply_count}")


if __name__ == "__main__":
    main()
