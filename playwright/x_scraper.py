from __future__ import annotations

import csv
import hashlib
import math
import random
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "x_urls.txt"
OUTPUT_CSV = BASE_DIR / f"x_thread_parallel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

CDP_URL = "http://127.0.0.1:9222"

NUM_WORKERS = 3
MAX_SCROLL_ROUNDS = 80
SCROLL_PIXELS_MIN = 1400
SCROLL_PIXELS_MAX = 2400
SCROLL_WAIT_MIN_MS = 1800
SCROLL_WAIT_MAX_MS = 3200
STOP_NO_NEW_IDS_ROUNDS = 8

FIELDNAMES = [
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


def hash_username(username: str | None) -> str:
    if not username:
        return "anonymous"
    return hashlib.sha256(username.encode("utf-8")).hexdigest()


def load_urls() -> list[str]:
    if not INPUT_FILE.exists():
        print(f"Fichier introuvable : {INPUT_FILE}")
        return []

    urls = [line.strip() for line in INPUT_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]

    deduped = []
    seen = set()
    for url in urls:
        normalized = normalize_url(url)
        if normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)

    return deduped


def chunk_list(items: list[str], n_chunks: int) -> list[list[str]]:
    if not items:
        return []

    n_chunks = max(1, min(n_chunks, len(items)))
    chunk_size = math.ceil(len(items) / n_chunks)

    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def normalize_url(url: str) -> str:
    url = url.strip()
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    url = url.replace("twitter.com/", "x.com/")
    return url.split("?")[0].rstrip("/")


def parse_status_id(tweet_url: str) -> str:
    match = re.search(r"/status/(\d+)", tweet_url or "")
    return match.group(1) if match else ""


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def parse_count(raw: str) -> str:
    raw = clean_text(raw).replace(",", "")
    return raw


def extract_hashtags(text: str) -> str:
    return " | ".join(sorted(set(re.findall(r"#\w+", text))))


def extract_mentions(text: str) -> str:
    return " | ".join(sorted(set(re.findall(r"@\w+", text))))


def safe_inner_text(locator) -> str:
    try:
        return clean_text(locator.inner_text(timeout=1500))
    except Exception:
        return ""


def safe_get_attribute(locator, name: str) -> str:
    try:
        value = locator.get_attribute(name, timeout=1500)
        return value or ""
    except Exception:
        return ""


def extract_metric(article, testid: str) -> str:
    loc = article.locator(f'[data-testid="{testid}"]')
    if loc.count() == 0:
        return ""
    text = safe_inner_text(loc.first)
    return parse_count(text)


def collect_external_links(article) -> str:
    links = []
    try:
        anchors = article.locator('a[href]')
        count = anchors.count()
        for i in range(count):
            href = safe_get_attribute(anchors.nth(i), "href")
            if not href:
                continue
            if href.startswith("/"):
                href = "https://x.com" + href
            if "x.com/" not in href and "/hashtag/" not in href and "/search?" not in href:
                links.append(href)
    except Exception:
        pass
    return " | ".join(sorted(set(links)))


def find_tweet_url(article) -> str:
    try:
        anchors = article.locator('a[href*="/status/"]')
        for i in range(anchors.count()):
            href = safe_get_attribute(anchors.nth(i), "href")
            if "/status/" in href:
                if href.startswith("/"):
                    return "https://x.com" + href
                if href.startswith("http"):
                    return href
    except Exception:
        pass
    return ""


def scrape_article(article, page_url: str, article_index: int) -> dict:
    tweet_url = find_tweet_url(article)
    status_id = parse_status_id(tweet_url)

    text = ""
    lang = ""
    tweet_text_loc = article.locator('[data-testid="tweetText"]')
    if tweet_text_loc.count() > 0:
        text = safe_inner_text(tweet_text_loc.first)
        lang = safe_get_attribute(tweet_text_loc.first, "lang")

    display_name = ""
    screen_name = ""
    try:
        user_text = safe_inner_text(article.locator('div[data-testid="User-Name"]').first)
        lines = [x.strip() for x in user_text.split("\n") if x.strip()]
        if lines:
            display_name = lines[0]
        for item in lines:
            if item.startswith("@"):
                screen_name = item.lstrip("@")
                break
    except Exception:
        pass

    tweet_time = ""
    tweet_time_iso = ""
    try:
        time_loc = article.locator("time").first
        tweet_time = safe_get_attribute(time_loc, "datetime")
        if tweet_time:
            try:
                dt = datetime.fromisoformat(tweet_time.replace("Z", "+00:00"))
                tweet_time_iso = dt.astimezone(timezone.utc).isoformat()
            except Exception:
                tweet_time_iso = tweet_time
    except Exception:
        pass

    media_count = 0
    try:
        media_count = article.locator("img").count()
    except Exception:
        media_count = 0

    article_text = safe_inner_text(article)

    return {
        "page_url": page_url,
        "tweet_url": tweet_url,
        "status_id": status_id,
        "article_index": article_index,
        "screen_name": screen_name,
        "display_name": display_name,
        "author_hash": hash_username(screen_name),
        "tweet_text": text,
        "lang": lang,
        "tweet_time": tweet_time,
        "tweet_time_iso": tweet_time_iso,
        "reply_count": extract_metric(article, "reply"),
        "retweet_count": extract_metric(article, "retweet"),
        "like_count": extract_metric(article, "like"),
        "bookmark_count": extract_metric(article, "bookmark"),
        "view_count": extract_metric(article, "analytics"),
        "is_reply": "Replying to" in article_text,
        "is_pinned": "Pinned" in article_text,
        "has_media": media_count > 0,
        "media_count": media_count,
        "hashtags": extract_hashtags(text),
        "mentions": extract_mentions(text),
        "external_links": collect_external_links(article),
        "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def collect_visible_articles(page, page_url: str, seen_status_ids: set[str], seen_urls: set[str]) -> list[dict]:
    articles = page.locator('article:has([data-testid="tweetText"]), article[data-testid="tweet"]')
    article_count = articles.count()
    new_rows = []

    for i in range(article_count):
        try:
            row = scrape_article(articles.nth(i), page_url, i)

            unique_status = row["status_id"]
            unique_url = row["tweet_url"] or f"{page_url}__{i}"

            if unique_status and unique_status in seen_status_ids:
                continue
            if unique_url in seen_urls:
                continue

            if unique_status:
                seen_status_ids.add(unique_status)
            seen_urls.add(unique_url)

            new_rows.append(row)
        except Exception:
            continue

    return new_rows


def scrape_one_thread(page, page_url: str, worker_name: str) -> list[dict]:
    seen_urls = set()
    seen_status_ids = set()
    collected_rows = []

    try:
        page.goto(page_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)
    except PlaywrightTimeoutError:
        print(f"[{worker_name}] Timeout chargement : {page_url}")
    except Exception as e:
        print(f"[{worker_name}] Erreur chargement {page_url}: {e}")
        return collected_rows

    initial_rows = collect_visible_articles(page, page_url, seen_status_ids, seen_urls)
    collected_rows.extend(initial_rows)
    print(f"[{worker_name}] Initial: {len(initial_rows)} nouveaux tweets | {page_url}")

    no_new_ids_rounds = 0

    for round_idx in range(1, MAX_SCROLL_ROUNDS + 1):
        scroll_pixels = random.randint(SCROLL_PIXELS_MIN, SCROLL_PIXELS_MAX)
        page.mouse.wheel(0, scroll_pixels)
        page.wait_for_timeout(random.randint(SCROLL_WAIT_MIN_MS, SCROLL_WAIT_MAX_MS))

        new_rows = collect_visible_articles(page, page_url, seen_status_ids, seen_urls)
        collected_rows.extend(new_rows)

        print(
            f"[{worker_name}] Round {round_idx}: "
            f"{len(new_rows)} nouveaux, {len(seen_status_ids)} status_id uniques"
        )

        if len(new_rows) == 0:
            no_new_ids_rounds += 1
        else:
            no_new_ids_rounds = 0

        if no_new_ids_rounds >= STOP_NO_NEW_IDS_ROUNDS:
            print(f"[{worker_name}] Arrêt : plus de nouveaux tweets | {page_url}")
            break

    return collected_rows


def worker_process(worker_id: int, urls: list[str]) -> list[dict]:
    worker_name = f"W{worker_id}"
    all_rows = []

    print(f"[{worker_name}] Démarrage avec {len(urls)} URLs")

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP_URL)

        if browser.contexts:
            context = browser.contexts[0]
        else:
            context = browser.new_context()

        page = context.new_page()

        for idx, page_url in enumerate(urls, start=1):
            print(f"\n[{worker_name}] URL {idx}/{len(urls)} : {page_url}")
            rows = scrape_one_thread(page, page_url, worker_name)
            all_rows.extend(rows)
            print(f"[{worker_name}] Terminé : {page_url} -> {len(rows)} lignes")

        try:
            page.close()
        except Exception:
            pass

    print(f"[{worker_name}] Fin worker : {len(all_rows)} lignes collectées")
    return all_rows


def main() -> None:
    urls = load_urls()
    if not urls:
        return

    url_chunks = chunk_list(urls, NUM_WORKERS)

    print(f"{len(urls)} URLs à scraper.")
    print(f"Workers : {len(url_chunks)}")
    print(f"Connexion CDP : {CDP_URL}")

    all_rows = []

    with ProcessPoolExecutor(max_workers=len(url_chunks)) as executor:
        futures = [
            executor.submit(worker_process, worker_id + 1, chunk)
            for worker_id, chunk in enumerate(url_chunks)
        ]

        for future in as_completed(futures):
            try:
                rows = future.result()
                all_rows.extend(rows)
            except Exception as e:
                print(f"Erreur worker : {e}")

    global_seen_ids = set()
    total_written = 0

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()

        for row in all_rows:
            status_id = row.get("status_id", "")
            unique_key = status_id or row.get("tweet_url", "")

            if unique_key and unique_key in global_seen_ids:
                continue

            if unique_key:
                global_seen_ids.add(unique_key)

            writer.writerow(row)
            total_written += 1

    print(f"\n[TERMINÉ] {total_written} lignes écrites dans {OUTPUT_CSV}")


if __name__ == "__main__":
    main()