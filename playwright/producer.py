import hashlib
import json
import logging
import os
import re
import socket
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, quote, urljoin, urlparse

import requests
from confluent_kafka import DeserializingConsumer, SerializingProducer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringSerializer
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from engagement import extract_x_metric, parse_count
from youtube_authors import fetch_youtube_collaborators


LOGGER = logging.getLogger(__name__)


DEFAULT_X_QUERIES = [
    '(electric vehicle OR EV OR "electric car") lang:en -filter:replies',
    '(Tesla OR "EV charging" OR "battery range") lang:en -filter:replies',
]


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value.strip() if value else default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _load_schema(schema_path: str) -> str:
    return Path(schema_path).read_text(encoding="utf-8")


def _wait_for_schema_registry(url: str, timeout_sec: int) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            if requests.get(f"{url}/subjects", timeout=5).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError(f"Schema Registry not reachable at {url}")


def _ensure_schema_registered(
    url: str,
    topic: str,
    schema: str,
    timeout_sec: int,
) -> None:
    deadline = time.time() + timeout_sec
    endpoint = f"{url}/subjects/{topic}-value/versions"
    last_error = "unknown error"

    while time.time() < deadline:
        try:
            response = requests.post(
                endpoint,
                json={"schema": schema},
                timeout=15,
            )
            if response.status_code in {200, 201}:
                return
            last_error = f"HTTP {response.status_code}: {response.text}"
            if response.status_code < 500:
                break
        except requests.RequestException as exc:
            last_error = str(exc)
        time.sleep(2)

    raise RuntimeError(
        f"Schema registration failed for {topic}: {last_error}"
    )


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _env_json_list(name: str, fallback: list[str]) -> list[str]:
    raw_value = os.getenv(name)
    if not raw_value:
        return fallback
    try:
        values = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{name} must contain a JSON array") from exc
    if not isinstance(values, list):
        raise RuntimeError(f"{name} must contain a JSON array")
    return [
        str(value).strip()
        for value in values
        if str(value).strip()
    ]


def _matches_keywords(text: str, keywords: list[str], match_mode: str) -> bool:
    if not keywords:
        return True
    normalized_text = text.casefold()
    matches = []
    for keyword in keywords:
        normalized_keyword = keyword.casefold()
        if re.fullmatch(r"\w+", normalized_keyword):
            matches.append(
                re.search(
                    rf"\b{re.escape(normalized_keyword)}\b",
                    normalized_text,
                )
                is not None
            )
        else:
            matches.append(normalized_keyword in normalized_text)
    return all(matches) if match_mode == "AND" else any(matches)


def _extract_reddit_comment_event(comment, fallback_url: str) -> dict | None:
    fullname = comment.get_attribute("data-fullname") or ""
    comment_id = fullname.removeprefix("t1_")
    if not comment_id:
        return None

    body_locator = comment.locator(".usertext-body .md")
    text = (
        _clean_text(body_locator.first.inner_text(timeout=1500))
        if body_locator.count()
        else ""
    )
    if not text:
        return None

    author_locator = comment.locator("a.author")
    author = (
        author_locator.first.inner_text(timeout=1500)
        if author_locator.count()
        else "anonymous"
    )
    time_locator = comment.locator("time")
    timestamp = (
        time_locator.first.get_attribute("datetime")
        if time_locator.count()
        else None
    )
    permalink_locator = comment.locator("a.bylink")
    href = (
        permalink_locator.first.get_attribute("href")
        if permalink_locator.count()
        else ""
    )
    comment_url = urljoin(fallback_url, href) if href else fallback_url
    comment_url = comment_url.replace(
        "https://old.reddit.com",
        "https://www.reddit.com",
    )

    return {
        "event_id": comment_id,
        "platform_event_id": comment_id,
        "user_id": f"reddit-{_hash_identity(author)}",
        "url": comment_url,
        "title": text,
        "timestamp": timestamp,
        "source": "reddit",
        "like_count": None,
        "view_count": None,
    }


def _hash_identity(value: str | None) -> str:
    return hashlib.sha256((value or "anonymous").encode("utf-8")).hexdigest()


class ProcessedState:
    def __init__(self, path: str) -> None:
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(db_path, timeout=30)
        self.connection.execute("PRAGMA busy_timeout = 30000")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
              source TEXT NOT NULL,
              event_id TEXT NOT NULL,
              processed_at TEXT NOT NULL,
              PRIMARY KEY (source, event_id)
            )
            """
        )
        self.connection.commit()

    def contains(self, source: str, event_id: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM processed_events WHERE source = ? AND event_id = ?",
            (source, event_id),
        ).fetchone()
        return row is not None

    def mark_many(self, source: str, event_ids: list[str]) -> None:
        if not event_ids:
            return
        processed_at = datetime.now(timezone.utc).isoformat()
        self.connection.executemany(
            """
            INSERT OR IGNORE INTO processed_events (source, event_id, processed_at)
            VALUES (?, ?, ?)
            """,
            [(source, event_id, processed_at) for event_id in event_ids],
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def _event_id_from_url(source: str, url: str) -> str | None:
    if source == "x":
        match = re.search(r"/status/(\d+)", url)
        return match.group(1) if match else None
    if source == "reddit":
        match = re.search(r"/comments/[^/]+/[^/]+/([a-z0-9]+)/?", url, re.IGNORECASE)
        return match.group(1) if match else None
    if source == "youtube":
        return parse_qs(urlparse(url).query).get("v", [None])[0]
    return None


def _bootstrap_state_from_kafka(
    state: ProcessedState,
    bootstrap: str,
    schema_registry_url: str,
    topic: str,
    source: str,
) -> None:
    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"collector-state-bootstrap-{source}-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            "value.deserializer": AvroDeserializer(
                SchemaRegistryClient({"url": schema_registry_url})
            ),
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            return

        assignments = []
        expected_messages = 0
        for partition in topic_metadata.partitions:
            tp = TopicPartition(topic, partition)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            assignments.append(TopicPartition(topic, partition, low))
            expected_messages += max(0, high - low)

        if expected_messages == 0:
            return

        consumer.assign(assignments)
        processed_ids = []
        consumed = 0
        deadline = time.time() + 60
        while consumed < expected_messages and time.time() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                continue
            consumed += 1
            value = message.value() or {}
            if value.get("source") != source:
                continue
            event_id = _event_id_from_url(source, value.get("url", ""))
            if event_id:
                processed_ids.append(event_id)

        state.mark_many(source, processed_ids)
        print(f"Bootstrapped {len(set(processed_ids))} {source} IDs from Kafka")
    finally:
        consumer.close()


def _get_youtube_service(api_key: str):
    return build("youtube", "v3", developerKey=api_key)


def _search_youtube_video_ids(
    youtube,
    search_query: str,
    max_results: int,
    relevance_language: str,
    order: str,
) -> list[str]:
    video_ids = []
    page_token = None

    while len(video_ids) < max_results:
        response = youtube.search().list(
            part="id",
            q=search_query,
            type="video",
            relevanceLanguage=relevance_language or None,
            order=order,
            maxResults=min(50, max_results - len(video_ids)),
            pageToken=page_token,
        ).execute()
        video_ids.extend(
            item["id"]["videoId"]
            for item in response.get("items", [])
            if item.get("id", {}).get("videoId")
        )
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return video_ids[:max_results]


def _fetch_video_metadata(youtube, video_id: str):
    try:
        response = youtube.videos().list(
            part="snippet,statistics,contentDetails,status,topicDetails",
            id=video_id,
        ).execute()
        items = response.get("items", [])
        return items[0] if items else None
    except HttpError as exc:
        print(f"[YouTube] Metadata error for {video_id}: {exc}")
        return None


def _fetch_youtube_comments(youtube, video_id: str, sleep_seconds: float):
    pages = []
    page_token = None
    while True:
        try:
            response = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=100,
                pageToken=page_token,
                textFormat="plainText",
            ).execute()
            pages.append(response)
            page_token = response.get("nextPageToken")
            if not page_token:
                return pages
            time.sleep(sleep_seconds)
        except HttpError as exc:
            print(f"[YouTube] Comment fetch stopped for {video_id}: {exc}")
            return pages


def _x_is_authenticated(page) -> bool:
    try:
        if _x_has_auth_cookies(page.context):
            return True

        page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2500)
        return (
            page.locator('[data-testid="SideNav_AccountSwitcher_Button"]').count() > 0
            or page.locator('[data-testid="primaryColumn"]').count() > 0
        )
    except Exception:
        return False


def _x_has_auth_cookies(context) -> bool:
    cookie_names = {
        cookie["name"]
        for cookie in context.cookies("https://x.com")
    }
    return {"auth_token", "ct0"}.issubset(cookie_names)


def _login_to_x_with_google(page) -> None:
    context = page.context
    google_email = _env_str("X_GOOGLE_EMAIL", "")
    page.goto(
        "https://x.com/i/flow/login",
        wait_until="domcontentloaded",
        timeout=60000,
    )
    page.wait_for_timeout(2500)

    google_button_pattern = re.compile(
        r"continue with google|sign in with google|log in with google|"
        r"continuer avec google|se connecter avec google",
        re.IGNORECASE,
    )
    google_button = page.get_by_text(google_button_pattern).first
    clicked = False
    if google_button.count() > 0:
        google_button.click(timeout=15000)
        clicked = True
    else:
        for frame in page.frames:
            if "accounts.google.com/gsi/button" not in frame.url:
                continue
            frame_button = frame.locator('[role="button"]').first
            if frame_button.count() > 0:
                frame_button.click(timeout=15000)
                clicked = True
                break

    if not clicked:
        raise RuntimeError("The Google login button was not found on X")

    deadline = time.time() + _env_int("X_GOOGLE_LOGIN_WAIT_SECONDS", 90)
    while time.time() < deadline:
        if _x_has_auth_cookies(context):
            page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=60000)
            return

        for candidate in list(context.pages):
            if "accounts.google.com" not in candidate.url:
                continue

            if candidate.locator('input[type="password"]').count() > 0:
                raise RuntimeError(
                    "Google requested a password, CAPTCHA or MFA. Complete the "
                    "challenge in the Edge window and rerun the task."
                )

            account = None
            if google_email:
                account = candidate.locator(
                    f'[data-identifier="{google_email}"], [data-email="{google_email}"]'
                ).first
                if account.count() == 0:
                    account = candidate.get_by_text(google_email, exact=False).first
            else:
                account = candidate.locator(
                    '[data-identifier], [data-email]'
                ).first

            if account is not None and account.count() > 0:
                try:
                    account.click(timeout=3000)
                except Exception:
                    pass

            continue_button = candidate.get_by_role(
                "button",
                name=re.compile(
                    r"continue|allow|accept|continuer|autoriser|accepter",
                    re.IGNORECASE,
                ),
            ).first
            if continue_button.count() > 0:
                try:
                    continue_button.click(timeout=3000)
                except Exception:
                    pass

        page.wait_for_timeout(1000)

    raise RuntimeError(
        "Google login did not complete before the timeout. Complete any visible "
        "Google or X challenge in the Edge window."
    )


def _resolve_x_cdp_url(cdp_url: str) -> str:
    if not cdp_url.startswith(("http://", "https://")):
        return cdp_url

    parsed_cdp_url = urlparse(cdp_url)
    cdp_host = parsed_cdp_url.hostname
    resolved_host = socket.gethostbyname(cdp_host) if cdp_host else ""
    discovery_url = (
        cdp_url.replace(cdp_host, resolved_host, 1)
        if cdp_host and resolved_host
        else cdp_url
    )
    response = requests.get(
        f"{discovery_url.rstrip('/')}/json/version",
        timeout=5,
    )
    response.raise_for_status()
    connect_url = response.json()["webSocketDebuggerUrl"]
    source_host = urlparse(connect_url).hostname
    target_host = resolved_host or cdp_host
    if source_host and target_host and source_host != target_host:
        connect_url = connect_url.replace(source_host, target_host, 1)
    return connect_url


def _x_cdp_url() -> str:
    port_file_value = os.getenv("X_CDP_PORT_FILE")
    if port_file_value:
        port_file = Path(port_file_value)
        if not port_file.is_file():
            raise FileNotFoundError(
                f"X CDP runtime port file not found: {port_file}. "
                "Run scripts/start_x_browser.ps1 first."
            )
        try:
            port = int(port_file.read_text(encoding="utf-8").strip())
        except ValueError as exc:
            raise ValueError(f"Invalid X CDP port in {port_file}") from exc
        if not 1 <= port <= 65535:
            raise ValueError(f"X CDP port out of range in {port_file}: {port}")
        host = _env_str("X_CDP_HOST", "host.docker.internal")
        return f"http://{host}:{port}"

    cdp_url = os.getenv("X_CDP_URL", "").strip()
    if not cdp_url:
        raise RuntimeError(
            "No X CDP endpoint configured. Run scripts/start_x_browser.ps1 "
            "or set X_CDP_URL explicitly."
        )
    return cdp_url


def _load_x_full_tweet_text(context, tweet_url: str, fallback_text: str) -> str:
    detail_page = None
    try:
        detail_page = context.new_page()
        detail_page.goto(
            tweet_url,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        article = detail_page.locator('article[data-testid="tweet"]').first
        article.wait_for(state="visible", timeout=15000)

        show_more = article.locator(
            '[data-testid="tweet-text-show-more-link"]'
        )
        if show_more.count():
            show_more.first.click(timeout=3000)
            detail_page.wait_for_timeout(500)

        text_locator = article.locator('[data-testid="tweetText"]').first
        text_locator.wait_for(state="visible", timeout=10000)
        full_text = _clean_text(text_locator.inner_text(timeout=5000))
        return full_text or fallback_text
    except (PlaywrightTimeoutError, PlaywrightError) as exc:
        print(f"Unable to load full X post text from {tweet_url}: {exc}")
        return fallback_text
    finally:
        if detail_page is not None:
            try:
                detail_page.close()
            except PlaywrightError:
                pass


def _collect_x_events(state: ProcessedState, max_events: int) -> list[dict]:
    cdp_url = _x_cdp_url()
    cdp_wait_seconds = _env_int("X_CDP_WAIT_SECONDS", 60)
    headless = _env_bool("X_HEADLESS", True)
    queries = _env_json_list(
        "X_SEARCH_QUERIES_JSON",
        [
            query.strip()
            for query in _env_str(
                "X_SEARCH_QUERIES",
                "||".join(DEFAULT_X_QUERIES),
            ).split("||")
            if query.strip()
        ],
    )
    scroll_rounds = _env_int("X_SCROLL_ROUNDS", 5)
    wait_ms = _env_int("X_SCROLL_WAIT_MS", 1500)
    search_wait_ms = _env_int("X_SEARCH_WAIT_MS", 20000)
    search_retries = _env_int("X_SEARCH_RETRIES", 3)
    search_retry_seconds = _env_int("X_SEARCH_RETRY_SECONDS", 10)
    events = []
    seen_ids = set()
    discovered_statuses = 0

    try:
        if cdp_url.startswith(("http://", "https://")):
            control_response = requests.get(
                f"{cdp_url.rstrip('/')}/__x_cdp__/ensure",
                params={"headless": str(headless).lower()},
                timeout=cdp_wait_seconds,
            )
            if control_response.status_code not in {200, 404}:
                control_response.raise_for_status()

        deadline = time.time() + cdp_wait_seconds
        with sync_playwright() as playwright:
            browser = None
            last_error = None
            while time.time() < deadline:
                try:
                    connect_url = _resolve_x_cdp_url(cdp_url)
                    browser = playwright.chromium.connect_over_cdp(
                        connect_url,
                        timeout=20000,
                    )
                    break
                except Exception as exc:
                    last_error = exc
                    print(f"Waiting for responsive X CDP at {cdp_url}...")
                    time.sleep(3)

            if browser is None:
                raise RuntimeError(
                    f"X CDP unavailable after {cdp_wait_seconds} seconds"
                ) from last_error

            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = context.new_page()
            if not _x_is_authenticated(page):
                _login_to_x_with_google(page)

            for query in queries:
                search_url = (
                    f"https://x.com/search?q={quote(query, safe='')}"
                    "&src=typed_query&f=live"
                )
                query_ready = False
                for attempt in range(1, search_retries + 1):
                    page.goto(
                        search_url,
                        wait_until="domcontentloaded",
                        timeout=60000,
                    )
                    try:
                        page.locator('article[data-testid="tweet"]').first.wait_for(
                            state="visible",
                            timeout=search_wait_ms,
                        )
                        query_ready = True
                        break
                    except PlaywrightTimeoutError:
                        print(
                            "No visible X posts for query "
                            f"(attempt {attempt}/{search_retries})"
                        )
                        if attempt < search_retries:
                            page.wait_for_timeout(search_retry_seconds * 1000)

                if not query_ready:
                    continue

                for _ in range(scroll_rounds):
                    articles = page.locator('article[data-testid="tweet"]')
                    for index in range(articles.count()):
                        try:
                            article = articles.nth(index)
                            status_links = article.locator('a[href*="/status/"]')
                            tweet_url = ""
                            status_id = ""
                            for link_index in range(status_links.count()):
                                href = status_links.nth(link_index).get_attribute(
                                    "href",
                                    timeout=1000,
                                )
                                match = re.search(
                                    r"^(/[^/]+/status/(\d+))",
                                    href or "",
                                )
                                if match:
                                    tweet_url = f"https://x.com{match.group(1)}"
                                    status_id = match.group(2)
                                    break
                            if not status_id:
                                continue

                            discovered_statuses += 1
                            if status_id in seen_ids or state.contains("x", status_id):
                                continue

                            text_locator = article.locator('[data-testid="tweetText"]')
                            text = (
                                _clean_text(
                                    text_locator.first.inner_text(timeout=1000)
                                )
                                if text_locator.count()
                                else ""
                            )
                            if not text:
                                continue

                            text = _load_x_full_tweet_text(
                                context,
                                tweet_url,
                                text,
                            )
                            user_locator = article.locator(
                                'div[data-testid="User-Name"]'
                            )
                            user_text = (
                                user_locator.first.inner_text(timeout=1000)
                                if user_locator.count()
                                else ""
                            )
                            screen_name_match = re.search(
                                r"@([A-Za-z0-9_]+)",
                                user_text,
                            )
                            screen_name = (
                                screen_name_match.group(1)
                                if screen_name_match
                                else "anonymous"
                            )
                            time_locator = article.locator("time")
                            timestamp = (
                                time_locator.first.get_attribute(
                                    "datetime",
                                    timeout=1000,
                                )
                                if time_locator.count()
                                else None
                            )
                            events.append(
                                {
                                    "event_id": status_id,
                                    "platform_event_id": status_id,
                                    "user_id": f"x-{_hash_identity(screen_name)}",
                                    "url": tweet_url,
                                    "title": text,
                                    "timestamp": timestamp,
                                    "source": "x",
                                    "like_count": extract_x_metric(
                                        article,
                                        "like",
                                    ),
                                    "view_count": extract_x_metric(
                                        article,
                                        "analytics",
                                    ),
                                }
                            )
                            seen_ids.add(status_id)
                            if max_events > 0 and len(events) >= max_events:
                                return events
                        except (PlaywrightTimeoutError, PlaywrightError):
                            continue

                    try:
                        page.mouse.wheel(0, 2200)
                        page.wait_for_timeout(wait_ms)
                    except PlaywrightError:
                        if discovered_statuses > 0:
                            print(
                                "X page closed after posts were discovered; "
                                "keeping the collected batch"
                            )
                            return events
                        raise

            try:
                page.close()
            except PlaywrightError:
                pass
            if discovered_statuses == 0:
                raise RuntimeError(
                    "No X posts were visible after all search retries. The X "
                    "session may be rate-limited or temporarily unavailable."
                )
    except Exception as exc:
        raise RuntimeError(
            f"X online collection failed via {cdp_url}: {exc}"
        ) from exc

    return events


def _collect_reddit_events(state: ProcessedState, max_events: int) -> list[dict]:
    subreddits = _env_json_list(
        "REDDIT_SUBREDDITS_JSON",
        [
            value.strip()
            for value in _env_str(
                "REDDIT_SUBREDDITS",
                "electricvehicles,teslamotors",
            ).split(",")
            if value.strip()
        ],
    )
    configured_scan_limit = _env_int("REDDIT_COMMENT_SCAN_LIMIT", 100)
    keywords = _env_json_list("REDDIT_KEYWORDS_JSON", [])
    keyword_match_mode = _env_str(
        "REDDIT_KEYWORD_MATCH_MODE",
        "OR",
    ).upper()
    scan_limit = min(
        100,
        max(1, configured_scan_limit, max_events if max_events > 0 else 0),
    )
    wait_ms = _env_int("REDDIT_WAIT_MS", 750)
    events = []
    candidates = {}
    discovered_comments = 0

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(
            user_agent=_env_str(
                "REDDIT_USER_AGENT",
                "Mozilla/5.0 Chrome/124 Safari/537.36 user-behavior-lakehouse/1.0",
            )
        )
        page = context.new_page()

        for subreddit in subreddits:
            listing_url = (
                f"https://old.reddit.com/r/{subreddit}/comments/"
                f"?limit={scan_limit}"
            )
            page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
            comments = page.locator("div.thing.comment")
            discovered_comments += comments.count()
            for index in range(comments.count()):
                event = _extract_reddit_comment_event(
                    comments.nth(index),
                    listing_url,
                )
                if event is None or state.contains("reddit", event["event_id"]):
                    continue
                if not _matches_keywords(
                    event["title"],
                    keywords,
                    keyword_match_mode,
                ):
                    continue
                candidates[event["event_id"]] = event

            page.wait_for_timeout(wait_ms)

        if discovered_comments == 0:
            browser.close()
            raise RuntimeError("Reddit online collection found no public comments")

        ordered_candidates = sorted(
            candidates.values(),
            key=lambda event: event.get("timestamp") or "",
            reverse=True,
        )
        if max_events > 0:
            ordered_candidates = ordered_candidates[:max_events]

        for candidate in ordered_candidates:
            event = candidate
            detail_url = candidate["url"].replace(
                "https://www.reddit.com",
                "https://old.reddit.com",
            )
            try:
                page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                exact_comment = page.locator(
                    "div.thing.comment"
                    f'[data-fullname="t1_{candidate["event_id"]}"]'
                ).first
                if exact_comment.count():
                    detailed_event = _extract_reddit_comment_event(
                        exact_comment,
                        detail_url,
                    )
                    if detailed_event is not None:
                        event = detailed_event
            except (PlaywrightTimeoutError, PlaywrightError) as exc:
                LOGGER.warning(
                    "Using Reddit listing metadata for %s: %s",
                    candidate["url"],
                    exc,
                )

            events.append(event)
            page.wait_for_timeout(wait_ms)

        browser.close()
    return events


def main() -> None:
    bootstrap = _env_str("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = _env_str("KAFKA_TOPIC", "")
    mode = _env_str("PRODUCER_MODE", "").lower()
    schema_registry_url = _env_str(
        "SCHEMA_REGISTRY_URL",
        "http://schema-registry:8081",
    )
    schema_path = _env_str("SCHEMA_PATH", "/app/schemas/playwright_event.avsc")
    schema = _load_schema(schema_path)
    max_events = _env_int("PRODUCER_MAX_EVENTS", 5)
    state = ProcessedState(
        _env_str("COLLECTOR_STATE_DB", "/app/state/processed.sqlite")
    )

    if not topic or mode not in {"youtube", "x", "reddit"}:
        raise RuntimeError("KAFKA_TOPIC and a supported PRODUCER_MODE are required")

    print("Waiting for Schema Registry...")
    _wait_for_schema_registry(
        schema_registry_url,
        _env_int("SCHEMA_REGISTRY_WAIT_SEC", 60),
    )
    print("Schema Registry ready")
    _ensure_schema_registered(
        schema_registry_url,
        topic,
        schema,
        _env_int("SCHEMA_REGISTRY_WAIT_SEC", 60),
    )
    print(f"Schema ready for {topic}")

    if _env_str("BOOTSTRAP_STATE_FROM_KAFKA", "true").lower() == "true":
        _bootstrap_state_from_kafka(
            state,
            bootstrap,
            schema_registry_url,
            topic,
            mode,
        )

    delivery_errors = []

    def delivery_report(error, message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    producer = SerializingProducer(
        {
            "bootstrap.servers": bootstrap,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": AvroSerializer(
                SchemaRegistryClient({"url": schema_registry_url}),
                schema,
            ),
        }
    )

    def publish(events: list[dict]) -> None:
        for event in events:
            producer.produce(
                topic=topic,
                key=event["user_id"],
                value={
                    "user_id": event["user_id"],
                    "url": event["url"],
                    "title": event["title"],
                    "timestamp": event.get("timestamp")
                    or datetime.now(timezone.utc).isoformat(),
                    "source": event["source"],
                    "error": None,
                    "platform_event_id": event.get("platform_event_id")
                    or event.get("event_id"),
                    "owner_channel_id": event.get("owner_channel_id"),
                    "collaborator_channel_ids": event.get(
                        "collaborator_channel_ids"
                    ),
                    "like_count": event.get("like_count"),
                    "view_count": event.get("view_count"),
                },
                on_delivery=delivery_report,
            )
            producer.poll(0)

        undelivered = producer.flush(60)
        if undelivered or delivery_errors:
            raise RuntimeError(
                f"Kafka delivery failed: {undelivered} undelivered, {delivery_errors}"
            )
        state.mark_many(mode, [event["event_id"] for event in events])

    try:
        print(f"Producer mode: {mode} (online)")
        if mode == "youtube":
            api_key = _env_str("YOUTUBE_API_KEY", "")
            if not api_key:
                raise RuntimeError("YOUTUBE_API_KEY is required")
            youtube = _get_youtube_service(api_key)
            search_limit = _env_int("YOUTUBE_SEARCH_MAX_RESULTS", 10)
            search_language = _env_str("YOUTUBE_SEARCH_LANGUAGE", "en")
            search_order = _env_str("YOUTUBE_SEARCH_ORDER", "date")
            youtube_queries = _env_json_list(
                "YOUTUBE_SEARCH_QUERIES_JSON",
                [
                    _env_str(
                        "YOUTUBE_SEARCH_QUERY",
                        "electric vehicle review",
                    )
                ],
            )
            video_ids = []
            for search_query in youtube_queries:
                discovered_ids = _search_youtube_video_ids(
                    youtube,
                    search_query,
                    search_limit,
                    search_language,
                    search_order,
                )
                for video_id in discovered_ids:
                    if video_id not in video_ids:
                        video_ids.append(video_id)
                    if len(video_ids) >= search_limit:
                        break
                if len(video_ids) >= search_limit:
                    break
            events = []
            output_dir = Path(
                _env_str("YOUTUBE_OUTPUT_DIR", "/app/api/yt_raw_json")
            )
            candidate_ids = [
                video_id
                for video_id in video_ids
                if not state.contains("youtube", video_id)
            ]
            if max_events > 0:
                candidate_ids = candidate_ids[:max_events]
            metadata_by_id = {
                video_id: _fetch_video_metadata(youtube, video_id)
                for video_id in candidate_ids
            }
            owner_by_id = {
                video_id: owner_channel_id
                for video_id, metadata in metadata_by_id.items()
                if (
                    owner_channel_id := (
                        (metadata or {})
                        .get("snippet", {})
                        .get("channelId")
                    )
                )
            }
            collaborators_by_id = fetch_youtube_collaborators(
                owner_by_id,
                timeout_seconds=_env_float(
                    "YOUTUBE_WATCH_PAGE_TIMEOUT_SECONDS",
                    20,
                ),
                max_workers=_env_int(
                    "YOUTUBE_AUTHOR_FETCH_WORKERS",
                    8,
                ),
            )
            for video_id in candidate_ids:
                metadata = metadata_by_id[video_id]
                owner_channel_id = owner_by_id.get(video_id)
                collaborator_channel_ids = collaborators_by_id.get(video_id)
                comments = _fetch_youtube_comments(
                    youtube,
                    video_id,
                    _env_float("YOUTUBE_SLEEP_SECONDS", 0.5),
                )
                output_dir.mkdir(parents=True, exist_ok=True)
                (output_dir / f"{video_id}.json").write_text(
                    json.dumps(
                        {
                            "video_id": video_id,
                            "video_metadata": metadata,
                            "owner_channel_id": owner_channel_id,
                            "collaborator_channel_ids": (
                                collaborator_channel_ids
                            ),
                            "comment_threads_pages": comments,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                events.append(
                    {
                        "event_id": video_id,
                        "platform_event_id": video_id,
                        "user_id": f"youtube-{video_id}",
                        "url": f"https://www.youtube.com/watch?v={video_id}",
                        "title": (metadata or {}).get("snippet", {}).get("title"),
                        "timestamp": None,
                        "source": "youtube",
                        "owner_channel_id": owner_channel_id,
                        "collaborator_channel_ids": (
                            collaborator_channel_ids
                        ),
                        "like_count": parse_count(
                            (metadata or {}).get("statistics", {}).get("likeCount")
                        ),
                        "view_count": parse_count(
                            (metadata or {}).get("statistics", {}).get("viewCount")
                        ),
                    }
                )
        elif mode == "x":
            events = _collect_x_events(state, max_events)
        else:
            events = _collect_reddit_events(state, max_events)

        publish(events)
        print(f"Produced {len(events)} new {mode} events")
    finally:
        state.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Collector failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
