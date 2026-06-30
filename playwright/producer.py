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
from xml.etree import ElementTree
from urllib.parse import (
    parse_qs,
    parse_qsl,
    quote,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
)

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
from youtube_transcript_api import YouTubeTranscriptApi

from engagement import extract_x_metric, parse_count
from youtube_authors import fetch_youtube_collaborators


LOGGER = logging.getLogger(__name__)


DEFAULT_X_QUERIES = [
    '(electric vehicle OR EV OR "electric car") lang:en -filter:replies',
    '(Tesla OR "EV charging" OR "battery range") lang:en -filter:replies',
    '("xe điện" OR "ô tô điện" OR "pin xe điện") lang:vi -filter:replies',
    '(Tesla OR VinFast OR "trạm sạc") lang:vi -filter:replies',
]

DEFAULT_YOUTUBE_QUERIES = [
    "electric vehicle review",
    "EV charging battery range",
    "đánh giá xe điện",
    "xe điện VinFast trạm sạc",
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


def _env_list(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return default
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or default


def _env_pipe_list(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return default
    items = [item.strip() for item in value.split("||") if item.strip()]
    return items or default


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


def _extract_reddit_feed_event(entry) -> dict | None:
    namespace = {"atom": "http://www.w3.org/2005/Atom"}
    entry_id = (entry.findtext("atom:id", default="", namespaces=namespace) or "")
    link = entry.find("atom:link", namespace)
    url = link.get("href", "") if link is not None else entry_id
    match = re.search(r"/comments/[^/]+/[^/]+/([A-Za-z0-9_]+)/?", url)
    comment_id = match.group(1) if match else hashlib.sha256(entry_id.encode()).hexdigest()
    title = _clean_text(entry.findtext("atom:title", default="", namespaces=namespace))
    content = _clean_text(entry.findtext("atom:content", default="", namespaces=namespace))
    text = title or content
    if not text:
        return None
    author = entry.find("atom:author/atom:name", namespace)
    author_name = author.text if author is not None and author.text else "anonymous"
    timestamp = entry.findtext("atom:updated", default=None, namespaces=namespace)
    return {
        "event_id": comment_id,
        "platform_event_id": comment_id,
        "user_id": f"reddit-{_hash_identity(author_name)}",
        "url": url.replace("https://old.reddit.com", "https://www.reddit.com"),
        "title": text,
        "timestamp": timestamp,
        "source": "reddit",
        "like_count": None,
        "view_count": None,
    }


def _collect_reddit_feed_events(
    subreddit: str,
    state: "ProcessedState",
    keywords: list[str],
    keyword_match_mode: str,
    scan_limit: int,
) -> tuple[list[dict], int]:
    url = f"https://www.reddit.com/r/{subreddit}/comments/.rss?limit={scan_limit}"
    headers = {
        "User-Agent": _env_str(
            "REDDIT_USER_AGENT",
            "Mozilla/5.0 Chrome/124 Safari/537.36 user-behavior-lakehouse/1.0",
        )
    }
    attempts = _env_int("REDDIT_FALLBACK_RETRIES", 3)
    wait_seconds = _env_float("REDDIT_FALLBACK_RETRY_SECONDS", 5)
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code}")
            root = ElementTree.fromstring(response.text)
            events = []
            discovered = 0
            for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
                discovered += 1
                event = _extract_reddit_feed_event(entry)
                if event is None or state.contains("reddit", event["event_id"]):
                    continue
                if not _matches_keywords(
                    event["title"],
                    keywords,
                    keyword_match_mode,
                ):
                    continue
                events.append(event)
            return events, discovered
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                print(
                    f"Reddit RSS fallback retry {attempt}/{attempts} "
                    f"for {subreddit}: {exc}",
                    flush=True,
                )
                time.sleep(wait_seconds)
    raise RuntimeError(
        f"Reddit RSS fallback failed for {subreddit}: {last_error}"
    )


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


def _fetch_youtube_comments(
    youtube,
    video_id: str,
    sleep_seconds: float,
    max_pages: int,
):
    pages = []
    page_token = None
    while max_pages <= 0 or len(pages) < max_pages:
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
    return pages


def _fetch_youtube_transcript(
    video_id: str,
    languages: list[str],
) -> tuple[list[dict] | None, str | None]:
    try:
        transcript = YouTubeTranscriptApi().fetch(video_id, languages=languages)
        clean_transcript = []
        for item in transcript:
            if isinstance(item, dict):
                clean_transcript.append(item)
            else:
                clean_transcript.append(
                    {
                        "text": getattr(item, "text", str(item)),
                        "start": getattr(item, "start", 0.0),
                        "duration": getattr(item, "duration", 0.0),
                    }
                )
        return clean_transcript or None, None
    except Exception as exc:
        reason = type(exc).__name__
        print(f"[YouTube] Transcript unavailable for {video_id}: {reason}")
        return None, reason


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


def _x_login_debug_dir() -> Path:
    return Path(_env_str("X_LOGIN_DEBUG_DIR", "/app/state/x-login-debug"))


def _write_x_login_debug_artifacts(page, reason: str) -> str:
    debug_dir = _x_login_debug_dir()
    debug_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    artifacts = []

    for index, candidate_page in enumerate(page.context.pages):
        prefix = debug_dir / f"x-login-{timestamp}-page-{index + 1}"
        html_path = prefix.with_suffix(".html")
        screenshot_path = prefix.with_suffix(".png")

        try:
            html_path.write_text(candidate_page.content(), encoding="utf-8")
        except PlaywrightError as exc:
            html_path = prefix.with_suffix(".html-error.txt")
            html_path.write_text(str(exc), encoding="utf-8")

        try:
            candidate_page.screenshot(path=str(screenshot_path), full_page=True)
        except PlaywrightError as exc:
            screenshot_path = prefix.with_suffix(".screenshot-error.txt")
            screenshot_path.write_text(str(exc), encoding="utf-8")

        artifacts.extend([html_path, screenshot_path])

    return (
        f"{reason}. Saved X login debug artifacts to "
        f"{', '.join(str(artifact) for artifact in artifacts)}."
    )


def _google_login_started(context, page) -> bool:
    if any("accounts.google.com" in candidate.url for candidate in context.pages):
        return True
    return _x_has_auth_cookies(context)


def _click_first_visible(locator, timeout_ms: int = 15000) -> bool:
    try:
        count = locator.count()
    except PlaywrightError:
        return False

    for index in range(count):
        candidate = locator.nth(index)
        try:
            if candidate.is_visible(timeout=1000):
                candidate.scroll_into_view_if_needed(timeout=3000)
                candidate.click(timeout=timeout_ms)
                return True
        except PlaywrightError:
            try:
                candidate.click(timeout=timeout_ms, force=True)
                return True
            except PlaywrightError:
                try:
                    candidate.evaluate("(element) => element.click()")
                    return True
                except PlaywrightError:
                    continue
    return False


def _click_x_form_submit(page) -> bool:
    scope = _x_active_login_scope(page)
    buttons = scope.locator("button, [role='button']")
    try:
        count = buttons.count()
    except PlaywrightError:
        return False

    for index in range(count):
        button = buttons.nth(index)
        try:
            label = button.inner_text(timeout=1000).strip()
            normalized_label = re.sub(r"\s+", " ", label).lower()
            if normalized_label not in {"next", "continue", "log in"}:
                continue
            if any(
                blocked in normalized_label
                for blocked in ("phone", "google", "apple")
            ):
                continue
            if button.is_visible(timeout=1000):
                button.scroll_into_view_if_needed(timeout=3000)
                button.click(timeout=5000)
                return True
        except PlaywrightError:
            continue
    return False


def _x_active_login_scope(page):
    modal = page.locator('[aria-modal="true"]').last
    try:
        if modal.count() > 0 and modal.is_visible(timeout=1000):
            return modal
    except PlaywrightError:
        pass

    dialog = page.locator('[role="dialog"]').last
    try:
        if dialog.count() > 0 and dialog.is_visible(timeout=1000):
            return dialog
    except PlaywrightError:
        pass

    return page


def _click_x_google_login_button(page, pattern: re.Pattern) -> bool:
    context = page.context
    candidates = [
        page.locator('[aria-modal="true"] button').filter(has_text=pattern),
        page.locator('[role="dialog"] button').filter(has_text=pattern),
        page.locator('[aria-modal="true"] [role="button"]').filter(has_text=pattern),
        page.locator('[role="dialog"] [role="button"]').filter(has_text=pattern),
        page.locator("button").filter(has_text=pattern),
        page.locator('[role="button"]').filter(has_text=pattern),
        page.get_by_role("button", name=pattern),
        page.locator('div[data-testid="google_sign_in_container"] [role="button"]'),
    ]

    for candidate in candidates:
        if _click_first_visible(candidate):
            page.wait_for_timeout(3000)
            if _google_login_started(context, page):
                return True

    for frame in page.frames:
        if "accounts.google.com" not in frame.url:
            continue
        frame_candidates = [
            frame.get_by_role("button", name=pattern),
            frame.get_by_text(pattern),
            frame.locator('[role="button"]').filter(has_text=pattern),
            frame.locator('[role="button"], button').first,
        ]
        for candidate in frame_candidates:
            if _click_first_visible(candidate):
                page.wait_for_timeout(3000)
                if _google_login_started(context, page):
                    return True

    return False


def _click_google_next(candidate) -> bool:
    next_button = candidate.get_by_role(
        "button",
        name=re.compile(r"next|suivant", re.IGNORECASE),
    ).first
    if next_button.count() > 0:
        try:
            next_button.click(timeout=5000)
            return True
        except PlaywrightError:
            pass
    return False


def _submit_google_email(candidate, google_email: str) -> bool:
    if not google_email:
        return False

    email_input = candidate.locator(
        'input[type="email"], input[name="identifier"], input#identifierId'
    ).first
    if email_input.count() == 0:
        return False

    try:
        email_input.fill(google_email, timeout=5000)
        return _click_google_next(candidate)
    except PlaywrightError:
        return False


def _x_verification_prompt_text(page) -> str:
    try:
        return page.locator('[role="dialog"], main').first.inner_text(timeout=2000)
    except PlaywrightError:
        return ""


def _raise_if_x_direct_login_requires_phone(page) -> None:
    prompt_text = _x_verification_prompt_text(page)
    if re.search(
        r"temporarily limited your login|try again later|"
        r"temporairement limit[eé]",
        prompt_text,
        re.IGNORECASE,
    ):
        raise RuntimeError(
            _write_x_login_debug_artifacts(
                page,
                "X temporarily limited direct email login. Try again later",
            )
        )

    if not re.search(
        r"enter your phone number|phone number|num[eé]ro de t[eé]l[eé]phone",
        prompt_text,
        re.IGNORECASE,
    ):
        return

    raise RuntimeError(
        _write_x_login_debug_artifacts(
            page,
            "X direct login used the email identifier, but X requested a phone "
            "number as an additional account verification step",
        )
    )


def _x_password_input(page):
    return _x_active_login_scope(page).locator(
        'input[autocomplete="current-password"], input[name="password"], '
        'input[type="password"]'
    ).first


def _wait_for_x_login_step_after_identifier(page, timeout_seconds: int = 20) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if _x_has_auth_cookies(page.context):
            return
        if _x_password_input(page).count() > 0:
            return
        _raise_if_x_direct_login_requires_phone(page)
        page.wait_for_timeout(1000)

    raise RuntimeError(
        _write_x_login_debug_artifacts(
            page,
            "X direct login submitted the email but did not advance before "
            "the timeout",
        )
    )


def _click_x_login_next(page) -> bool:
    scope = _x_active_login_scope(page)
    login_button = scope.locator('[data-testid="LoginForm_Login_Button"]').first
    if login_button.count() > 0 and _click_first_visible(login_button):
        return True
    return _click_x_form_submit(page)


def _login_to_x_directly(page) -> bool:
    identifier = _env_str("X_LOGIN_IDENTIFIER", "")
    password = _env_str("X_PASSWORD", "")
    if not identifier or not password:
        return False

    context = page.context
    page.goto(
        "https://x.com/i/flow/login",
        wait_until="domcontentloaded",
        timeout=60000,
    )
    page.wait_for_timeout(2500)

    scope = _x_active_login_scope(page)
    identifier_input = scope.locator(
        'input[autocomplete="username"], input[name="text"], '
        'input[type="text"]'
    ).first
    if identifier_input.count() == 0:
        raise RuntimeError(
            _write_x_login_debug_artifacts(
                page,
                "X direct login could not find the identifier input",
            )
        )

    identifier_input.fill(identifier, timeout=10000)
    try:
        identifier_input.press("Enter", timeout=3000)
    except PlaywrightError:
        pass
    page.wait_for_timeout(2500)
    if _x_password_input(page).count() == 0:
        _click_x_login_next(page)

    _wait_for_x_login_step_after_identifier(
        page,
        _env_int("X_LOGIN_STEP_WAIT_SECONDS", 20),
    )

    password_input = _x_password_input(page)
    if password_input.count() == 0:
        raise RuntimeError(
            _write_x_login_debug_artifacts(
                page,
                "X direct login needs an additional verification step before "
                "the password field",
            )
        )

    password_input.fill(password, timeout=10000)
    if not _click_x_login_next(page):
        raise RuntimeError(
            _write_x_login_debug_artifacts(
                page,
                "X direct login could not submit the password",
            )
        )

    deadline = time.time() + _env_int("X_LOGIN_WAIT_SECONDS", 90)
    while time.time() < deadline:
        if _x_has_auth_cookies(context):
            page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=60000)
            return True

        _raise_if_x_direct_login_requires_phone(page)

        page.wait_for_timeout(1000)

    raise RuntimeError(
        _write_x_login_debug_artifacts(
            page,
            "X direct login did not complete before the timeout",
        )
    )


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
        r"continue as|google|"
        r"continuer avec google|se connecter avec google|continuer en tant que",
        re.IGNORECASE,
    )
    if not _click_x_google_login_button(page, google_button_pattern):
        raise RuntimeError(
            _write_x_login_debug_artifacts(
                page,
                "The Google login button was not found on X or did not open "
                "the Google login flow",
            )
        )

    deadline = time.time() + _env_int("X_GOOGLE_LOGIN_WAIT_SECONDS", 90)
    while time.time() < deadline:
        if _x_has_auth_cookies(context):
            page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=60000)
            return

        for candidate in list(context.pages):
            if "accounts.google.com" not in candidate.url:
                continue

            if _submit_google_email(candidate, google_email):
                candidate.wait_for_timeout(1500)

            if candidate.locator('input[type="password"]').count() > 0:
                raise RuntimeError(
                    _write_x_login_debug_artifacts(
                        candidate,
                        "Google requested a password, CAPTCHA or MFA. Complete "
                        "the challenge in the Edge window and rerun the task",
                    )
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
        _write_x_login_debug_artifacts(
            page,
            "Google login did not complete before the timeout. Complete any "
            "visible Google or X challenge in the Edge window",
        )
    )


def _x_cdp_access_token() -> str:
    token = os.getenv("X_CDP_TOKEN", "").strip()
    if token:
        return token

    token_file_value = os.getenv("X_CDP_TOKEN_FILE", "").strip()
    if not token_file_value:
        return ""
    token_file = Path(token_file_value)
    if not token_file.is_file():
        return ""
    return token_file.read_text(encoding="utf-8-sig").strip()


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


def _resolve_x_cdp_url(cdp_url: str) -> str:
    if not cdp_url.startswith(("http://", "https://")):
        return cdp_url

    parsed_cdp_url = urlparse(cdp_url)
    cdp_host = parsed_cdp_url.hostname
    resolved_host = socket.gethostbyname(cdp_host) if cdp_host else ""
    discovery_url = _x_cdp_url_with_path(cdp_url, "/json/version")
    discovery_url = (
        discovery_url.replace(cdp_host, resolved_host, 1)
        if cdp_host and resolved_host
        else discovery_url
    )
    response = requests.get(
        discovery_url,
        timeout=5,
    )
    response.raise_for_status()
    connect_url = response.json()["webSocketDebuggerUrl"]
    source_host = urlparse(connect_url).hostname
    target_host = resolved_host or cdp_host
    if source_host and target_host and source_host != target_host:
        connect_url = connect_url.replace(source_host, target_host, 1)
    return _with_x_cdp_token(connect_url)


def _ensure_x_cdp_endpoint(
    cdp_url: str,
    headless: bool,
    deadline: float,
) -> None:
    if not cdp_url.startswith(("http://", "https://")):
        return

    ensure_url = _x_cdp_url_with_path(cdp_url, "/__x_cdp__/ensure")
    last_error = None
    while time.time() < deadline:
        try:
            control_response = requests.get(
                ensure_url,
                params={"headless": str(headless).lower()},
                timeout=min(10, max(1, int(deadline - time.time()))),
            )
            if control_response.status_code in {200, 404}:
                return
            control_response.raise_for_status()
            return
        except Exception as exc:
            last_error = exc
            print(f"Waiting for X CDP control endpoint at {cdp_url}: {exc}")
            time.sleep(3)

    raise RuntimeError(
        f"X CDP control endpoint did not become ready at {cdp_url}: "
        f"{last_error}"
    )


def _x_cdp_url() -> str:
    port_file_value = os.getenv("X_CDP_PORT_FILE")
    if port_file_value:
        port_file = Path(port_file_value)
        if not port_file.is_file():
            raise FileNotFoundError(
                f"X CDP runtime port file not found: {port_file}."
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


def _x_auth_cookies() -> list[dict]:
    auth_token = os.getenv("X_AUTH_TOKEN", "").strip()
    ct0 = os.getenv("X_CT0", "").strip()
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
        "user_agent": _env_str(
            "X_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0 Safari/537.36",
        ),
    }


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
    headless = _env_bool("X_HEADLESS", True)
    user_data_dir = _env_str("X_USER_DATA_DIR", "/app/x-browser-profile")
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
    search_navigation_timeout_ms = _env_int(
        "X_SEARCH_NAVIGATION_TIMEOUT_MS",
        30000,
    )
    search_retries = _env_int("X_SEARCH_RETRIES", 3)
    search_retry_seconds = _env_int("X_SEARCH_RETRY_SECONDS", 10)
    events = []
    seen_ids = set()
    discovered_statuses = 0
    limit_reached = False
    page_closed_after_posts = False

    try:
        with sync_playwright() as playwright:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=headless,
                args=["--no-sandbox"],
                **_x_browser_context_options(),
            )
            auth_cookies = _x_auth_cookies()
            if auth_cookies:
                context.add_cookies(auth_cookies)
            page = context.new_page()
            if not _x_is_authenticated(page):
                if not _login_to_x_directly(page):
                    raise RuntimeError(
                        "X direct login requires X_LOGIN_IDENTIFIER and "
                        "X_PASSWORD, or X_AUTH_TOKEN/X_CT0 cookies."
                    )

            for query in queries:
                search_url = (
                    f"https://x.com/search?q={quote(query, safe='')}"
                    "&src=typed_query&f=live"
                )
                query_ready = False
                for attempt in range(1, search_retries + 1):
                    try:
                        page.goto(
                            search_url,
                            wait_until="domcontentloaded",
                            timeout=search_navigation_timeout_ms,
                        )
                        page.locator('article[data-testid="tweet"]').first.wait_for(
                            state="visible",
                            timeout=search_wait_ms,
                        )
                        query_ready = True
                        break
                    except (PlaywrightTimeoutError, PlaywrightError) as exc:
                        print(
                            "X query did not become ready "
                            f"(attempt {attempt}/{search_retries}): {exc}",
                            flush=True,
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
                                limit_reached = True
                                break
                        except (PlaywrightTimeoutError, PlaywrightError):
                            continue

                    if limit_reached:
                        break

                    try:
                        page.mouse.wheel(0, 2200)
                        page.wait_for_timeout(wait_ms)
                    except PlaywrightError:
                        if discovered_statuses > 0:
                            print(
                                "X page closed after posts were discovered; "
                                "keeping the collected batch"
                            )
                            page_closed_after_posts = True
                            break
                        raise

                if limit_reached or page_closed_after_posts:
                    break

            try:
                page.close()
            except PlaywrightError:
                pass
            context.close()
            if discovered_statuses == 0:
                print(
                    "No X posts were visible after all search retries. The X "
                    "session may be rate-limited or temporarily unavailable.",
                    flush=True,
                )
                return []
    except Exception as exc:
        if _env_bool("X_FAIL_ON_ERROR", False):
            raise RuntimeError(
                f"X online collection failed: {exc}"
            ) from exc
        print(f"X online collection skipped: {exc}", flush=True)
        return events

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
    configured_scan_limit = _env_int("REDDIT_COMMENT_SCAN_LIMIT", 1000)
    keywords = _env_json_list("REDDIT_KEYWORDS_JSON", [])
    keyword_match_mode = _env_str(
        "REDDIT_KEYWORD_MATCH_MODE",
        "OR",
    ).upper()
    scan_limit = max(
        1,
        configured_scan_limit,
        max_events if max_events > 0 else 0,
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
            listing_url = f"https://old.reddit.com/r/{subreddit}/comments/?limit=100"
            visited_pages = set()
            scanned_comments = 0

            while listing_url and scanned_comments < scan_limit:
                if listing_url in visited_pages:
                    break
                visited_pages.add(listing_url)
                response = page.goto(
                    listing_url,
                    wait_until="domcontentloaded",
                    timeout=60000,
                )
                if response is None:
                    raise RuntimeError(f"Reddit did not return a response for {listing_url}")
                if response.status in {403, 429}:
                    print(
                        "Reddit HTML listing is unavailable "
                        f"for {subreddit}: HTTP {response.status}; "
                        "trying RSS fallback",
                        flush=True,
                    )
                    try:
                        fallback_events, fallback_discovered = (
                            _collect_reddit_feed_events(
                                subreddit,
                                state,
                                keywords,
                                keyword_match_mode,
                                scan_limit,
                            )
                        )
                    except RuntimeError:
                        if candidates or discovered_comments > 0:
                            LOGGER.warning(
                                "Skipping unavailable subreddit %s after "
                                "discovering Reddit comments",
                                subreddit,
                            )
                            break
                        raise
                    for event in fallback_events:
                        candidates[event["event_id"]] = event
                    discovered_comments += fallback_discovered
                    break
                if response.status >= 400:
                    raise RuntimeError(
                        "Reddit listing did not load "
                        f"for {subreddit}: HTTP {response.status}"
                    )
                comments = page.locator("div.thing.comment")
                page_comment_count = min(
                    comments.count(),
                    scan_limit - scanned_comments,
                )
                discovered_comments += page_comment_count
                scanned_comments += page_comment_count

                for index in range(page_comment_count):
                    event = _extract_reddit_comment_event(
                        comments.nth(index),
                        listing_url,
                    )
                    if event is None or state.contains(
                        "reddit",
                        event["event_id"],
                    ):
                        continue
                    if not _matches_keywords(
                        event["title"],
                        keywords,
                        keyword_match_mode,
                    ):
                        continue
                    candidates[event["event_id"]] = event

                next_link = page.locator("span.next-button a")
                next_href = (
                    next_link.first.get_attribute("href")
                    if next_link.count()
                    else None
                )
                listing_url = urljoin(listing_url, next_href) if next_href else None
                page.wait_for_timeout(wait_ms)

            if max_events > 0 and len(candidates) >= max_events:
                break

        if discovered_comments == 0:
            browser.close()
            raise RuntimeError(
                "Reddit listing loaded but no comments were found. "
                "Treating this as a load/parsing failure, not an empty collection."
            )

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
    max_events = _env_int("PRODUCER_MAX_EVENTS", 1000)
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
            search_languages = _env_list(
                "YOUTUBE_SEARCH_LANGUAGES",
                [_env_str("YOUTUBE_SEARCH_LANGUAGE", "en")],
            )
            transcript_languages = _env_list(
                "YOUTUBE_TRANSCRIPT_LANGUAGES",
                ["en", "vi"],
            )
            comment_max_pages = _env_int("YOUTUBE_COMMENT_MAX_PAGES", 3)
            transcript_max_failures = _env_int(
                "YOUTUBE_TRANSCRIPT_MAX_FAILURES",
                5,
            )
            search_order = _env_str("YOUTUBE_SEARCH_ORDER", "date")
            youtube_queries = _env_json_list(
                "YOUTUBE_SEARCH_QUERIES_JSON",
                _env_pipe_list("YOUTUBE_SEARCH_QUERIES", DEFAULT_YOUTUBE_QUERIES),
            )
            video_ids = []
            for search_query in youtube_queries:
                for search_language in search_languages:
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
            transcript_failures = 0
            transcripts_disabled = transcript_max_failures == 0
            for video_id in candidate_ids:
                metadata = metadata_by_id[video_id]
                owner_channel_id = owner_by_id.get(video_id)
                collaborator_channel_ids = collaborators_by_id.get(video_id)
                comments = _fetch_youtube_comments(
                    youtube,
                    video_id,
                    _env_float("YOUTUBE_SLEEP_SECONDS", 0.5),
                    comment_max_pages,
                )
                transcript = None
                if not transcripts_disabled:
                    transcript, transcript_error = _fetch_youtube_transcript(
                        video_id,
                        transcript_languages,
                    )
                    if transcript_error:
                        transcript_failures += 1
                        if (
                            transcript_error == "IpBlocked"
                            or transcript_failures >= transcript_max_failures
                        ):
                            transcripts_disabled = True
                            print(
                                "[YouTube] Transcript collection disabled for "
                                "the remaining videos after "
                                f"{transcript_failures} failures"
                            )
                output_dir.mkdir(parents=True, exist_ok=True)
                (output_dir / f"{video_id}.json").write_text(
                    json.dumps(
                        {
                            "video_id": video_id,
                            "video_metadata": metadata,
                            "video_transcript": transcript,
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
