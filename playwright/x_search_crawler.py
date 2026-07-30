"""Collect topical X search results with engagement metrics into a resumable CSV.

This collector is intentionally file-oriented: it supports one-off corpus growth
without publishing partial batches to Kafka. Search queries cover electric
vehicles in English and Vietnamese across several time windows.
"""

from __future__ import annotations

import csv
import hashlib
import os
import random
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from engagement import extract_x_metric
from x_url_finder import env_bool, normalize_status_url, try_switch_to_latest, x_auth_cookies


OUTPUT_COLUMNS = [
    "page_url",
    "search_query",
    "tweet_url",
    "status_id",
    "screen_name",
    "author_hash",
    "tweet_text",
    "lang",
    "tweet_time_iso",
    "reply_count",
    "retweet_count",
    "like_count",
    "bookmark_count",
    "view_count",
    "follower_count",
    "scraped_at_utc",
]

TOPICS = [
    '(electric vehicle OR EV OR "electric car")',
    '("EV charging" OR "charging network" OR Supercharger)',
    '("EV battery" OR "battery degradation" OR "battery range")',
    '(Tesla OR "Model 3" OR "Model Y")',
    '(Rivian OR R1T OR R1S OR "Rivian R2")',
    '("Ioniq 5" OR "Ioniq 6" OR EV6 OR EV9)',
    '(BYD OR "Chinese EV" OR "Xiaomi SU7")',
    '(Lucid OR Polestar OR "Volkswagen ID.4" OR "ID Buzz")',
    '("Mustang Mach-E" OR "F-150 Lightning" OR "Equinox EV")',
    '("EV tax credit" OR "electric vehicle policy" OR "EV incentive")',
    '("EV road trip" OR "range anxiety" OR "home charging")',
]

VIETNAMESE_TOPICS = [
    '("xe điện" OR "ô tô điện" OR "xe hơi điện")',
    '(VinFast OR VF3 OR VF5 OR VF6 OR VF7 OR VF8 OR VF9)',
    '("trạm sạc" OR "sạc xe điện" OR "hạ tầng sạc")',
    '("pin xe điện" OR "quãng đường xe điện" OR "tuổi thọ pin")',
    '("xe điện BYD" OR "Xiaomi SU7" OR "Tesla Việt Nam")',
]

DATE_WINDOWS = [
    ("2024-01-01", "2025-01-01"),
    ("2025-01-01", "2026-01-01"),
    ("2026-01-01", None),
]

VIETNAMESE_CHARACTERS = re.compile(
    r"[ăâđêôơưĂÂĐÊÔƠƯàáảãạèéẻẽẹìíỉĩịòóỏõọùúủũụỳýỷỹỵ]"
)
VIETNAMESE_CONTEXT = (
    " xe điện",
    " ô tô",
    " trạm sạc",
    " pin xe",
    " của ",
    " và ",
    " không ",
    " được ",
    " với ",
    " tại ",
)


def default_queries() -> list[str]:
    queries = []
    # Run Vietnamese EV searches first so a capped collection cannot exhaust its
    # record budget on English posts before reaching Vietnamese queries.
    for topic in VIETNAMESE_TOPICS:
        for start, end in DATE_WINDOWS:
            until = f" until:{end}" if end else ""
            queries.append(
                f"{topic} lang:vi -filter:replies since:{start}{until}"
            )
    for topic in TOPICS:
        for start, end in DATE_WINDOWS:
            until = f" until:{end}" if end else ""
            queries.append(
                f"{topic} lang:en -filter:replies since:{start}{until}"
            )
    return queries


def configured_queries() -> list[str]:
    raw = os.getenv("X_SEARCH_QUERIES", "")
    return [query.strip() for query in raw.split("||") if query.strip()] or default_queries()


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def is_vietnamese_text(text: str) -> bool:
    normalized = f" {text.casefold()} "
    return (
        len(VIETNAMESE_CHARACTERS.findall(text)) >= 2
        and any(marker in normalized for marker in VIETNAMESE_CONTEXT)
    )


def hash_username(username: str) -> str:
    return hashlib.sha256(username.casefold().encode("utf-8")).hexdigest()


def build_search_url(query: str) -> str:
    return f"https://x.com/search?q={quote(query, safe='')}&src=typed_query&f=live"


def load_existing(output_path: Path) -> tuple[set[str], int]:
    if not output_path.exists():
        return set(), 0
    with output_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return {row["status_id"] for row in rows if row.get("status_id")}, len(rows)


def article_record(article, search_url: str, query: str) -> dict | None:
    status_id = ""
    tweet_url = ""
    links = article.locator('a[href*="/status/"]')
    for index in range(links.count()):
        href = links.nth(index).get_attribute("href", timeout=1000) or ""
        match = re.search(r"^(/[^/]+/status/(\d+))", href)
        if match:
            tweet_url = normalize_status_url(f"https://x.com{match.group(1)}")
            status_id = match.group(2)
            break
    if not status_id:
        return None

    text_node = article.locator('[data-testid="tweetText"]')
    if not text_node.count():
        return None
    text = clean_text(text_node.first.inner_text(timeout=1500))
    if not text:
        return None
    if "lang:vi" in query.casefold() and not is_vietnamese_text(text):
        return None

    lang = text_node.first.get_attribute("lang", timeout=1000)
    user_node = article.locator('[data-testid="User-Name"]')
    user_text = user_node.first.inner_text(timeout=1000) if user_node.count() else ""
    screen_name_match = re.search(r"@([A-Za-z0-9_]+)", user_text)
    screen_name = screen_name_match.group(1) if screen_name_match else "anonymous"
    time_node = article.locator("time")
    timestamp = (
        time_node.first.get_attribute("datetime", timeout=1000)
        if time_node.count()
        else None
    )

    return {
        "page_url": search_url,
        "search_query": query,
        "tweet_url": tweet_url,
        "status_id": status_id,
        "screen_name": screen_name,
        "author_hash": hash_username(screen_name),
        "tweet_text": text,
        "lang": lang,
        "tweet_time_iso": timestamp,
        "reply_count": extract_x_metric(article, "reply"),
        "retweet_count": extract_x_metric(article, "retweet"),
        "like_count": extract_x_metric(article, "like"),
        "bookmark_count": extract_x_metric(article, "bookmark"),
        "view_count": extract_x_metric(article, "analytics"),
        # Profile hover-card collection costs up to two seconds per post. Keep
        # audience unknown in this bulk pass and enrich distinct authors later.
        "follower_count": None,
        "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def append_rows(output_path: Path, rows: list[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_path.exists()
    with output_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    output_path = Path(
        os.getenv("X_SEARCH_OUTPUT", "/data/x_search_collection.csv")
    )
    max_records = env_int("X_SEARCH_MAX_RECORDS", 1600)
    scroll_rounds = env_int("X_SEARCH_SCROLL_ROUNDS", 30)
    wait_ms = env_int("X_SEARCH_SCROLL_WAIT_MS", 1200)
    queries = configured_queries()
    seen_ids, existing_count = load_existing(output_path)
    print(
        f"Resuming with {existing_count} rows; target={max_records}; "
        f"queries={len(queries)}",
        flush=True,
    )

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=os.getenv(
                "X_USER_DATA_DIR",
                "/app/state/x-browser-profile",
            ),
            headless=env_bool("X_HEADLESS", True),
            args=["--no-sandbox"],
            locale="en-US",
            viewport={"width": 1280, "height": 900},
        )
        cookies = x_auth_cookies()
        if cookies:
            context.add_cookies(cookies)
        page = context.new_page()

        try:
            for query_index, query in enumerate(queries, start=1):
                if len(seen_ids) >= max_records:
                    break
                search_url = build_search_url(query)
                print(f"[{query_index}/{len(queries)}] {query}", flush=True)
                try:
                    page.goto(
                        search_url,
                        wait_until="domcontentloaded",
                        timeout=60_000,
                    )
                    page.locator('article[data-testid="tweet"]').first.wait_for(
                        state="visible",
                        timeout=25_000,
                    )
                    try_switch_to_latest(page)
                except (PlaywrightTimeoutError, PlaywrightError) as exc:
                    print(f"  query unavailable: {exc}", flush=True)
                    continue

                stagnant_rounds = 0
                for round_index in range(1, scroll_rounds + 1):
                    new_rows = []
                    articles = page.locator('article[data-testid="tweet"]')
                    for article_index in range(articles.count()):
                        try:
                            row = article_record(
                                articles.nth(article_index),
                                search_url,
                                query,
                            )
                        except (PlaywrightTimeoutError, PlaywrightError):
                            continue
                        if not row or row["status_id"] in seen_ids:
                            continue
                        seen_ids.add(row["status_id"])
                        new_rows.append(row)
                        if len(seen_ids) >= max_records:
                            break
                    if new_rows:
                        append_rows(output_path, new_rows)
                        stagnant_rounds = 0
                    else:
                        stagnant_rounds += 1
                    print(
                        f"  round {round_index}: +{len(new_rows)} "
                        f"({len(seen_ids)}/{max_records})",
                        flush=True,
                    )
                    if len(seen_ids) >= max_records or stagnant_rounds >= 5:
                        break
                    page.mouse.wheel(0, random.randint(1800, 2800))
                    page.wait_for_timeout(wait_ms)
        finally:
            context.close()

    print(f"Collected {len(seen_ids)} unique X posts -> {output_path}", flush=True)


if __name__ == "__main__":
    main()
