import hashlib
import html as html_lib
import json
import logging
import os
import re
import socket
import sqlite3
import sys
import time
import traceback
import unicodedata
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
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

from common.collection import (
    ContentRelationship,
    OperationResult,
    STATUS_DISABLED,
    STATUS_FAILED,
    STATUS_NOT_AVAILABLE,
    STATUS_PARTIAL,
    STATUS_RATE_LIMITED,
    STATUS_SUCCESS,
    canonical_content_id,
    isoformat_utc,
    is_terminal_status,
    overall_status,
    safe_json_dumps,
    utc_now,
)
from common.event_envelope import enrich_event_envelope
from common.transcripts import (
    fetch_transcript,
    legacy_transcript_status,
    transcript_lifecycle_status,
)
from engagement import extract_x_followers, extract_x_metric, parse_count
import youtube_authors
from youtube_authors import fetch_youtube_collaborators


LOGGER = logging.getLogger(__name__)


STATIC_REDDIT_COMMUNITY_FALLBACKS = {
    "electricvehicles": {
        "subreddit_title": "Electric Vehicle News and Discussion",
        "subreddit_description": (
            "The future of sustainable transportation is here! This is the Reddit "
            "community for EV owners and enthusiasts."
        ),
        "subreddit_created_at": "Apr 20, 2009",
        "subreddit_visibility": "public",
        "subreddit_member_count": 509000,
    },
    "teslamotors": {
        "subreddit_title": "TeslaMotors - The original and largest Tesla community!",
        "subreddit_description": "The original and largest Tesla community!",
        "subreddit_created_at": "Sep 4, 2010",
        "subreddit_visibility": "public",
        "subreddit_member_count": 124000,
    },
}


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


class CollectorSoftBlock(RuntimeError):
    """Source auth, quota, or rate-limit issue requiring an explicit failure."""


def _http_error_status(exc: Exception) -> int | None:
    response = getattr(exc, "resp", None)
    status = getattr(response, "status", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _is_auth_or_quota_block(exc: Exception) -> bool:
    status = _http_error_status(exc)
    if status in {401, 403, 429}:
        return True

    message = str(exc).lower()
    markers = (
        "api key",
        "auth",
        "captcha",
        "credential",
        "forbidden",
        "login",
        "mfa",
        "permission",
        "quota",
        "rate limit",
        "rate-limit",
        "too many requests",
        "temporarily limited",
        "unauthorized",
    )
    return any(marker in message for marker in markers)


def _soft_block(source: str, reason: str) -> CollectorSoftBlock:
    return CollectorSoftBlock(f"{source} collection blocked: {reason}")


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

    raise RuntimeError(f"Schema registration failed for {topic}: {last_error}")


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _env_json_list(name: str, fallback: list[str]) -> list[str]:
    def normalize_item(value) -> str:
        if isinstance(value, dict):
            value = (
                value.get("name")
                or value.get("subreddit")
                or value.get("keyword")
                or value.get("value")
                or ""
            )
        return str(value).strip()

    raw_value = os.getenv(name)
    if not raw_value:
        return fallback
    try:
        values = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{name} must contain a JSON array") from exc
    if isinstance(values, str):
        try:
            values = json.loads(values)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{name} must contain a JSON array") from exc
    if not isinstance(values, list):
        raise RuntimeError(f"{name} must contain a JSON array")
    return [item for item in (normalize_item(value) for value in values) if item]


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


def _extract_reddit_score(comment) -> int | None:
    selectors = (
        "span.score.unvoted",
        "span.score.likes",
        "span.score.dislikes",
        "span.score",
    )
    for selector in selectors:
        score_locator = comment.locator(selector)
        if not score_locator.count():
            continue
        score_node = score_locator.first
        for raw_value in (
            score_node.get_attribute("title"),
            score_node.inner_text(timeout=1000),
        ):
            parsed = parse_count(raw_value)
            if parsed is not None:
                return parsed
    return None


def _extract_reddit_subreddit_members(page) -> int | None:
    selectors = (
        ".side .subscribers .number",
        ".side span.subscribers .number",
        ".side .subscribers",
    )
    for selector in selectors:
        locator = page.locator(selector)
        if not locator.count():
            continue
        node = locator.first
        for raw_value in (
            node.get_attribute("title"),
            node.inner_text(timeout=1000),
        ):
            parsed = parse_count(raw_value)
            if parsed is not None:
                return parsed
    return None


def _normalize_reddit_sidebar_label(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return normalized.lower()


def _parse_reddit_sidebar_count(lines: list[str], labels: tuple[str, ...]) -> int | None:
    normalized_labels = tuple(_normalize_reddit_sidebar_label(label) for label in labels)
    for index, line in enumerate(lines):
        normalized_line = _normalize_reddit_sidebar_label(line)
        if not any(label in normalized_line for label in normalized_labels):
            continue
        for candidate in (line, lines[index - 1] if index > 0 else ""):
            match = re.search(r"[\d][\d\s,.]*\s*[KMBkmb]?", candidate)
            if match:
                parsed = parse_count(match.group(0))
                if parsed is not None:
                    return parsed
    return None


def _extract_reddit_subreddit_about_json(subreddit: str) -> dict:
    info: dict[str, Any] = {
        "subreddit_title": None,
        "subreddit_description": None,
        "subreddit_created_at": None,
        "subreddit_visibility": None,
        "subreddit_weekly_visitors": None,
        "subreddit_weekly_contributions": None,
        "subreddit_member_count": None,
    }
    try:
        response = requests.get(
            f"https://www.reddit.com/r/{subreddit}/about.json",
            headers={
                "User-Agent": _env_str(
                    "REDDIT_USER_AGENT",
                    "Mozilla/5.0 Chrome/124 Safari/537.36 user-behavior-lakehouse/1.0",
                )
            },
            timeout=_env_int("REDDIT_COMMUNITY_ABOUT_TIMEOUT_SECONDS", 15),
        )
        response.raise_for_status()
        data = response.json().get("data") or {}
    except (ValueError, requests.RequestException, AttributeError) as exc:
        LOGGER.warning("Could not collect Reddit about.json for %s: %s", subreddit, exc)
        return info

    title = _clean_text(data.get("title") or "")
    public_description = _clean_text(data.get("public_description") or "")
    created_utc = data.get("created_utc")
    if created_utc is not None:
        try:
            info["subreddit_created_at"] = (
                datetime.fromtimestamp(
                    float(created_utc),
                    timezone.utc,
                )
                .date()
                .isoformat()
            )
        except (TypeError, ValueError, OSError):
            info["subreddit_created_at"] = None

    info.update(
        {
            "subreddit_title": title or None,
            "subreddit_description": public_description or None,
            "subreddit_visibility": data.get("subreddit_type"),
            "subreddit_member_count": parse_count(data.get("subscribers")),
        }
    )
    return info


def _extract_reddit_subreddit_old_html(subreddit: str) -> dict:
    info: dict[str, Any] = {
        "subreddit_title": None,
        "subreddit_description": None,
        "subreddit_created_at": None,
        "subreddit_visibility": None,
        "subreddit_weekly_visitors": None,
        "subreddit_weekly_contributions": None,
        "subreddit_member_count": None,
    }
    fallback = STATIC_REDDIT_COMMUNITY_FALLBACKS.get(subreddit.lower(), {})
    try:
        response = requests.get(
            f"https://old.reddit.com/r/{subreddit}/",
            headers={
                "User-Agent": _env_str(
                    "REDDIT_USER_AGENT",
                    "Mozilla/5.0 Chrome/124 Safari/537.36 user-behavior-lakehouse/1.0",
                )
            },
            timeout=_env_int("REDDIT_COMMUNITY_OLD_HTML_TIMEOUT_SECONDS", 15),
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("Could not collect old Reddit HTML for %s: %s", subreddit, exc)
        info.update(fallback)
        return info

    html = response.text

    title_match = re.search(
        r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    )
    if not title_match:
        title_match = re.search(
            r"<title>(.*?)</title>",
            html,
            re.IGNORECASE | re.DOTALL,
        )
    if title_match:
        title = html_lib.unescape(title_match.group(1))
        title = re.sub(r"\s+[•:|-]\s+r/[A-Za-z0-9_]+.*$", "", title).strip()
        info["subreddit_title"] = _clean_text(title) or None

    description_match = re.search(
        r'<meta\s+(?:name|property)=["\'](?:description|og:description)["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    )
    if description_match:
        description = html_lib.unescape(description_match.group(1))
        info["subreddit_description"] = _clean_text(description) or None

    info["subreddit_visibility"] = "public"
    member_match = re.search(
        r'<span[^>]+class=["\'][^"\']*number[^"\']*["\'][^>]*>'
        r"\s*([^<]+?)\s*</span>\s*"
        r'<span[^>]+class=["\'][^"\']*word[^"\']*["\'][^>]*>'
        r"\s*(?:readers|subscribers|members|abonnes|membres)\s*</span>",
        html,
        re.IGNORECASE,
    )
    if not member_match:
        member_match = re.search(
            r"([\d][\d\s,.]*\s*[KMBkmb]?)\s+"
            r"(?:readers|subscribers|members|abonnes|membres)\b",
            html_lib.unescape(re.sub(r"<[^>]+>", " ", html)),
            re.IGNORECASE,
        )
    if member_match:
        info["subreddit_member_count"] = parse_count(member_match.group(1))
    for column, value in fallback.items():
        if info.get(column) is None:
            info[column] = value
    return info


def _extract_reddit_subreddit_info(context, subreddit: str) -> dict:
    info = _extract_reddit_subreddit_about_json(subreddit)
    if not any(value is not None for value in info.values()):
        info = _extract_reddit_subreddit_old_html(subreddit)
    page = context.new_page()
    try:
        response = page.goto(
            f"https://www.reddit.com/r/{subreddit}/",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        if response is None or response.status >= 400:
            return info
        page.wait_for_timeout(_env_int("REDDIT_COMMUNITY_WAIT_MS", 2500))
        h1 = page.locator("h1")
        if not info["subreddit_title"] and h1.count():
            info["subreddit_title"] = _clean_text(h1.first.inner_text(timeout=1500))

        meta_description = page.locator('meta[name="description"]')
        if not info["subreddit_description"] and meta_description.count():
            info["subreddit_description"] = _clean_text(
                meta_description.first.get_attribute("content") or ""
            )

        body_text = page.locator("body").inner_text(timeout=3000)
        lines = [_clean_text(line) for line in body_text.splitlines()]
        lines = [line for line in lines if line]
        normalized_lines = [_normalize_reddit_sidebar_label(line) for line in lines]

        for index, normalized_line in enumerate(normalized_lines):
            line = lines[index]
            if not info["subreddit_visibility"] and normalized_line in {
                "public",
                "private",
                "restricted",
            }:
                info["subreddit_visibility"] = line
                break

        for index, normalized_line in enumerate(normalized_lines):
            if info["subreddit_created_at"]:
                break
            if normalized_line.startswith("created") or normalized_line.startswith("cree"):
                created_value = re.sub(
                    r"^(?:created|creee?)\s+(?:on\s+|le\s+)?",
                    "",
                    normalized_line,
                    flags=re.IGNORECASE,
                )
                info["subreddit_created_at"] = created_value or lines[index]
                break

        for index, normalized_line in enumerate(normalized_lines):
            if (
                "weekly visitors" in normalized_line or "visiteur" in normalized_line
            ) and index > 0:
                info["subreddit_weekly_visitors"] = parse_count(lines[index - 1])
            if (
                "weekly contributions" in normalized_line
                or "contributions hebdomadaires" in normalized_line
            ) and index > 0:
                info["subreddit_weekly_contributions"] = parse_count(lines[index - 1])

        info["subreddit_weekly_visitors"] = info[
            "subreddit_weekly_visitors"
        ] or _parse_reddit_sidebar_count(
            lines,
            ("weekly visitors", "weekly active", "visiteur"),
        )
        info["subreddit_weekly_contributions"] = info[
            "subreddit_weekly_contributions"
        ] or _parse_reddit_sidebar_count(
            lines,
            ("weekly contributions", "contributions hebdomadaires"),
        )
        info["subreddit_member_count"] = info[
            "subreddit_member_count"
        ] or _parse_reddit_sidebar_count(
            lines,
            ("subscribers", "abonnes", "membres"),
        )
    except (PlaywrightTimeoutError, PlaywrightError) as exc:
        LOGGER.warning("Could not collect Reddit community info for %s: %s", subreddit, exc)
    finally:
        page.close()
    return info


def _extract_reddit_comment_event(
    comment,
    fallback_url: str,
    subreddit_member_count: int | None = None,
    subreddit_info: dict | None = None,
) -> dict | None:
    fullname = comment.get_attribute("data-fullname") or ""
    comment_id = fullname.removeprefix("t1_")
    if not comment_id:
        return None

    body_locator = comment.locator(".usertext-body .md")
    try:
        text = (
            _clean_text(body_locator.first.inner_text(timeout=1500)) if body_locator.count() else ""
        )
    except (PlaywrightTimeoutError, PlaywrightError) as exc:
        LOGGER.debug("Skipping Reddit comment %s without readable text: %s", comment_id, exc)
        return None
    if not text:
        return None

    author_locator = comment.locator("a.author")
    try:
        author = (
            author_locator.first.inner_text(timeout=1500) if author_locator.count() else "anonymous"
        )
    except (PlaywrightTimeoutError, PlaywrightError) as exc:
        LOGGER.debug("Using anonymous author for Reddit comment %s: %s", comment_id, exc)
        author = "anonymous"
    time_locator = comment.locator("time")
    timestamp = time_locator.first.get_attribute("datetime") if time_locator.count() else None
    permalink_locator = comment.locator("a.bylink")
    href = permalink_locator.first.get_attribute("href") if permalink_locator.count() else ""
    comment_url = urljoin(fallback_url, href) if href else fallback_url
    comment_url = comment_url.replace(
        "https://old.reddit.com",
        "https://www.reddit.com",
    )
    post_match = re.search(r"/r/([^/]+)/comments/([^/]+)", comment_url, re.IGNORECASE)
    subreddit = post_match.group(1) if post_match else None
    conversation_id = post_match.group(2) if post_match else None
    parent_id = comment.get_attribute("data-parent") or ""
    parent_interaction_id = parent_id.removeprefix("t1_") if parent_id.startswith("t1_") else None
    try:
        depth = int(comment.get_attribute("data-depth") or 0) + 1
    except (TypeError, ValueError):
        depth = 2 if parent_interaction_id else 1

    event = {
        "event_id": comment_id,
        "platform_event_id": comment_id,
        "user_id": f"reddit-{_hash_identity(author, comment_id)}",
        "url": comment_url,
        "title": text,
        "timestamp": timestamp,
        "source": "reddit",
        "subreddit": subreddit,
        "conversation_id": conversation_id,
        "parent_interaction_id": parent_interaction_id,
        "depth": depth,
        "like_count": None,
        "view_count": None,
        "comment_count": None,
        "reply_count": None,
        "retweet_count": None,
        "bookmark_count": None,
        "score": _extract_reddit_score(comment),
        "subreddit_member_count": subreddit_member_count,
    }
    if subreddit_info:
        for key, value in subreddit_info.items():
            if value is not None:
                event[key] = value
    event["subreddit_member_count"] = (
        subreddit_member_count
        or event.get("subreddit_member_count")
        or (subreddit_info or {}).get("subreddit_member_count")
    )
    return event


def _extract_reddit_feed_event(entry, subreddit_info: dict | None = None) -> dict | None:
    namespace = {"atom": "http://www.w3.org/2005/Atom"}
    entry_id = entry.findtext("atom:id", default="", namespaces=namespace) or ""
    link = entry.find("atom:link", namespace)
    url = link.get("href", "") if link is not None else entry_id
    match = re.search(r"/comments/[^/]+/[^/]+/([A-Za-z0-9_]+)/?", url)
    comment_id = match.group(1) if match else hashlib.sha256(entry_id.encode()).hexdigest()
    post_match = re.search(r"/r/([^/]+)/comments/([^/]+)", url, re.IGNORECASE)
    subreddit = post_match.group(1) if post_match else None
    conversation_id = post_match.group(2) if post_match else None
    title = _clean_text(entry.findtext("atom:title", default="", namespaces=namespace))
    content = _clean_text(entry.findtext("atom:content", default="", namespaces=namespace))
    text = title or content
    if not text:
        return None
    author = entry.find("atom:author/atom:name", namespace)
    author_name = author.text if author is not None and author.text else "anonymous"
    timestamp = entry.findtext("atom:updated", default=None, namespaces=namespace)
    event = {
        "event_id": comment_id,
        "platform_event_id": comment_id,
        "user_id": f"reddit-{_hash_identity(author_name, comment_id)}",
        "url": url.replace("https://old.reddit.com", "https://www.reddit.com"),
        "title": text,
        "timestamp": timestamp,
        "source": "reddit",
        "subreddit": subreddit,
        "conversation_id": conversation_id,
        "parent_interaction_id": None,
        "depth": 1,
        "like_count": None,
        "view_count": None,
        "comment_count": None,
        "reply_count": None,
        "retweet_count": None,
        "bookmark_count": None,
        "score": None,
        "subreddit_member_count": None,
    }
    if subreddit_info:
        for key, value in subreddit_info.items():
            if value is not None:
                event[key] = value
    return event


def _collect_reddit_feed_events(
    subreddit: str,
    state: "ProcessedState",
    keywords: list[str],
    keyword_match_mode: str,
    scan_limit: int,
    subreddit_info: dict | None = None,
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
                event = _extract_reddit_feed_event(entry, subreddit_info)
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
                    f"Reddit RSS fallback retry {attempt}/{attempts} for {subreddit}: {exc}",
                    flush=True,
                )
                time.sleep(wait_seconds)
    raise RuntimeError(f"Reddit RSS fallback failed for {subreddit}: {last_error}")


def _hash_identity(value: str | None, fallback: str | None = None) -> str:
    normalized = str(value or "").strip()
    if normalized.lower() in {"", "anonymous", "deleted", "unknown", "[deleted]"}:
        normalized = f"event:{fallback}" if fallback else "anonymous"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


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
              collection_status TEXT,
              metadata_status TEXT,
              transcript_status TEXT,
              comments_status TEXT,
              attempt_count INTEGER NOT NULL DEFAULT 0,
              last_attempt_at TEXT,
              terminal INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (source, event_id)
            )
            """
        )
        current_columns = {
            row[1] for row in self.connection.execute("PRAGMA table_info(processed_events)")
        }
        migrations = {
            "collection_status": "TEXT",
            "metadata_status": "TEXT",
            "transcript_status": "TEXT",
            "comments_status": "TEXT",
            "attempt_count": "INTEGER NOT NULL DEFAULT 0",
            "last_attempt_at": "TEXT",
            "terminal": "INTEGER NOT NULL DEFAULT 0",
        }
        for column, data_type in migrations.items():
            if column not in current_columns:
                self.connection.execute(
                    f"ALTER TABLE processed_events ADD COLUMN {column} {data_type}"
                )
        self.connection.commit()

    def contains(self, source: str, event_id: str) -> bool:
        row = self.connection.execute(
            """
            SELECT terminal
            FROM processed_events
            WHERE source = ? AND event_id = ?
            """,
            (source, event_id),
        ).fetchone()
        return bool(row and row[0])

    def next_attempt_count(self, source: str, event_id: str) -> int:
        row = self.connection.execute(
            """
            SELECT attempt_count
            FROM processed_events
            WHERE source = ? AND event_id = ?
            """,
            (source, event_id),
        ).fetchone()
        return int(row[0] or 0) + 1 if row else 1

    @staticmethod
    def _is_terminal_event(event: dict) -> bool:
        statuses = {
            "collection": event.get("collection_status"),
            "metadata": event.get("metadata_status"),
            "transcript": event.get("transcript_status"),
            "comments": event.get("comments_status"),
        }
        for component, status in statuses.items():
            bounded_comment_page = (
                component == "comments"
                and status == STATUS_PARTIAL
                and event.get("comments_error_code") == "comment_page_limit_reached"
            )
            if not bounded_comment_page and not (status and is_terminal_status(status)):
                return False
        return True

    def mark_many(self, source: str, events: list[dict]) -> None:
        if not events:
            return
        processed_at = datetime.now(timezone.utc).isoformat()
        self.connection.executemany(
            """
            INSERT INTO processed_events (
              source,
              event_id,
              processed_at,
              collection_status,
              metadata_status,
              transcript_status,
              comments_status,
              attempt_count,
              last_attempt_at,
              terminal
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, event_id) DO UPDATE SET
              processed_at = excluded.processed_at,
              collection_status = excluded.collection_status,
              metadata_status = excluded.metadata_status,
              transcript_status = excluded.transcript_status,
              comments_status = excluded.comments_status,
              attempt_count = MAX(
                processed_events.attempt_count,
                excluded.attempt_count
              ),
              last_attempt_at = excluded.last_attempt_at,
              terminal = excluded.terminal
            """,
            [
                (
                    source,
                    event.get("platform_event_id") or event.get("event_id"),
                    processed_at,
                    event.get("collection_status"),
                    event.get("metadata_status"),
                    event.get("transcript_status"),
                    event.get("comments_status"),
                    int(event.get("attempt_count") or 1),
                    event.get("last_attempt_at") or processed_at,
                    int(self._is_terminal_event(event)),
                )
                for event in events
                if event.get("event_id") or event.get("platform_event_id")
            ],
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
        processed_events = []
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
            event_id = value.get("platform_event_id") or _event_id_from_url(
                source,
                value.get("url", ""),
            )
            if event_id:
                processed_events.append({**value, "event_id": event_id})

        state.mark_many(source, processed_events)
        terminal_ids = {
            event["event_id"] for event in processed_events if state._is_terminal_event(event)
        }
        print(f"Bootstrapped {len(terminal_ids)} terminal {source} IDs from Kafka")
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
    video_ids: list[str] = []
    page_token: str | None = None

    def video_id_from_item(item) -> str | None:
        if not isinstance(item, dict):
            return None
        item_id = item.get("id")
        if isinstance(item_id, dict):
            video_id = item_id.get("videoId")
            return str(video_id).strip() if video_id else None
        if isinstance(item_id, str):
            return item_id.strip() or None
        return None

    while len(video_ids) < max_results:
        try:
            response = (
                youtube.search()
                .list(
                    part="id",
                    q=search_query,
                    type="video",
                    relevanceLanguage=relevance_language or None,
                    order=order,
                    maxResults=min(50, max_results - len(video_ids)),
                    pageToken=page_token,
                )
                .execute()
            )
        except HttpError as exc:
            if _is_auth_or_quota_block(exc):
                raise _soft_block("youtube", str(exc)) from exc
            raise
        video_ids.extend(
            video_id
            for video_id in (video_id_from_item(item) for item in response.get("items", []))
            if video_id
        )
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return video_ids[:max_results]


def _fetch_video_metadata(youtube, video_id: str) -> OperationResult[dict]:
    started_at = utc_now()
    try:
        response = (
            youtube.videos()
            .list(
                part="snippet,statistics,contentDetails,status,topicDetails",
                id=video_id,
            )
            .execute()
        )
        items = response.get("items", [])
        if not items:
            return OperationResult.unavailable(
                error_code="video_not_available",
                error_message=f"No metadata was returned for {video_id}",
                started_at=started_at,
                completed_at=utc_now(),
            )
        return OperationResult.success(
            items[0],
            started_at=started_at,
            completed_at=utc_now(),
        )
    except HttpError as exc:
        status = _http_error_status(exc)
        if status == 404:
            return OperationResult.unavailable(
                error_code="video_not_available",
                error_message=str(exc),
                started_at=started_at,
                completed_at=utc_now(),
            )
        if _is_auth_or_quota_block(exc):
            return OperationResult.rate_limited(
                error_code=f"youtube_http_{status or 'blocked'}",
                error_message=str(exc),
                started_at=started_at,
                completed_at=utc_now(),
            )
        return OperationResult.failed(
            error_code=f"youtube_http_{status or 'error'}",
            error_message=str(exc),
            started_at=started_at,
            completed_at=utc_now(),
        )


def _fetch_youtube_comments(
    youtube,
    video_id: str,
    sleep_seconds: float,
    max_pages: int,
) -> OperationResult[list[dict]]:
    started_at = utc_now()
    pages: list[dict] = []
    page_token: str | None = None
    while max_pages <= 0 or len(pages) < max_pages:
        try:
            response = (
                youtube.commentThreads()
                .list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=page_token,
                    textFormat="plainText",
                )
                .execute()
            )
            pages.append(response)
            page_token = response.get("nextPageToken")
            if not page_token:
                return OperationResult.success(
                    pages,
                    started_at=started_at,
                    completed_at=utc_now(),
                )
            time.sleep(sleep_seconds)
        except HttpError as exc:
            status = _http_error_status(exc)
            message = str(exc)
            if "commentsdisabled" in message.replace("_", "").lower():
                return OperationResult.unavailable(
                    error_code="comments_disabled",
                    error_message=message,
                    started_at=started_at,
                    completed_at=utc_now(),
                )
            if pages:
                return OperationResult.partial(
                    pages,
                    error_code=f"youtube_http_{status or 'error'}",
                    error_message=message,
                    started_at=started_at,
                    completed_at=utc_now(),
                )
            if _is_auth_or_quota_block(exc):
                return OperationResult.rate_limited(
                    error_code=f"youtube_http_{status or 'blocked'}",
                    error_message=message,
                    started_at=started_at,
                    completed_at=utc_now(),
                )
            return OperationResult.failed(
                error_code=f"youtube_http_{status or 'error'}",
                error_message=message,
                started_at=started_at,
                completed_at=utc_now(),
            )
    return OperationResult.partial(
        pages,
        error_code="comment_page_limit_reached",
        error_message=f"Stopped after the configured limit of {max_pages} pages",
        started_at=started_at,
        completed_at=utc_now(),
    )


def _fetch_youtube_transcript(
    video_id: str,
    languages: list[str],
    attempt_count: int = 1,
) -> OperationResult:
    return fetch_transcript(
        video_id,
        preferred_languages=languages,
        require_preferred_language=True,
        attempt_count=attempt_count,
    )


def _preferred_youtube_transcript_languages(metadata: dict) -> list[str]:
    """Use Vietnamese captions for Vietnamese videos and English otherwise."""

    snippet = metadata.get("snippet") or {}
    language = (
        str(snippet.get("defaultLanguage") or snippet.get("defaultAudioLanguage") or "")
        .strip()
        .casefold()
        .replace("_", "-")
    )
    if language == "vi" or language.startswith("vi-") or "vietnam" in language:
        return ["vi"]
    return ["en"]


def _youtube_transcript_text(transcript: list[dict] | None) -> str | None:
    if not transcript:
        return None
    text = " ".join(
        str(item.get("text", "")).strip()
        for item in transcript
        if isinstance(item, dict) and str(item.get("text", "")).strip()
    )
    return text or None


def _completed_at(result: OperationResult) -> str | None:
    return isoformat_utc(result.completed_at or result.started_at)


def _terminal_collection_status(*results: OperationResult) -> str:
    if all(
        result.is_terminal
        or (result.status == STATUS_PARTIAL and result.error_code == "comment_page_limit_reached")
        for result in results
    ):
        return STATUS_SUCCESS
    return overall_status(result.status for result in results)


def _first_operation_error(*results: OperationResult) -> tuple[str | None, str | None]:
    for result in results:
        if result.error_code or result.error_message:
            return result.error_code, result.error_message
    return None, None


def _youtube_video_event(
    video_id: str,
    metadata_result: OperationResult[dict],
    comments_result: OperationResult[list[dict]],
    transcript_result: OperationResult,
    owner_channel_id: str | None,
    collaborator_channel_ids: list[str] | None,
    attempt_count: int,
) -> dict:
    metadata = metadata_result.payload or {}
    snippet = metadata.get("snippet") or {}
    statistics = metadata.get("statistics") or {}
    content_details = metadata.get("contentDetails") or {}
    status = metadata.get("status") or {}
    topic_details = metadata.get("topicDetails") or {}
    transcript = transcript_result.payload
    requested_language_code = _preferred_youtube_transcript_languages(metadata)[0]
    lifecycle_status = transcript_lifecycle_status(
        transcript_result.status,
        error_code=transcript_result.error_code,
        has_text=bool(transcript and transcript.text.strip()),
        attempt_count=attempt_count,
    )
    thumbnails = snippet.get("thumbnails") or {}
    low_thumbnail = thumbnails.get("default") or {}
    thumbnail_url = low_thumbnail.get("url")
    relationship = ContentRelationship.root(
        source="youtube",
        platform_content_id=video_id,
        content_type="youtube_video",
    )
    collected_at = isoformat_utc(utc_now())
    error_code, error_message = _first_operation_error(
        metadata_result,
        comments_result,
        transcript_result,
    )
    canonical_metadata = {
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "tags": snippet.get("tags"),
        "category_id": snippet.get("categoryId"),
        "default_language": snippet.get("defaultLanguage"),
        "default_audio_language": snippet.get("defaultAudioLanguage"),
        "thumbnail_url": thumbnail_url,
        "channel_id": snippet.get("channelId"),
        "channel_title": snippet.get("channelTitle"),
        "privacy_status": status.get("privacyStatus"),
        "license": status.get("license"),
        "embeddable": status.get("embeddable"),
        "made_for_kids": status.get("madeForKids"),
        "caption": content_details.get("caption"),
        "definition": content_details.get("definition"),
        "dimension": content_details.get("dimension"),
        "projection": content_details.get("projection"),
        "topic_ids": topic_details.get("topicIds"),
        "topic_categories": topic_details.get("topicCategories"),
        "statistics": statistics,
    }
    available_languages = None
    if transcript is not None:
        available_languages = [
            str(item.get("language_code") or item.get("language"))
            for item in transcript.available_languages
            if item.get("language_code") or item.get("language")
        ]
    return {
        "event_id": video_id,
        "platform_event_id": video_id,
        "user_id": (
            f"youtube-channel-{owner_channel_id}"
            if owner_channel_id
            else f"youtube-video-{video_id}"
        ),
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "title": snippet.get("title") or f"YouTube video {video_id}",
        "raw_text": snippet.get("description") or snippet.get("title"),
        "thumbnail_url": thumbnail_url,
        "timestamp": snippet.get("publishedAt") or collected_at,
        "published_at": snippet.get("publishedAt"),
        "collected_at": collected_at,
        "updated_at": collected_at,
        "last_attempt_at": collected_at,
        "source": "youtube",
        "owner_channel_id": owner_channel_id,
        "youtube_channel_name": snippet.get("channelTitle"),
        "language": (
            snippet.get("defaultLanguage")
            or snippet.get("defaultAudioLanguage")
            or (transcript.language_code if transcript else None)
        ),
        **relationship.to_dict(),
        "parent_interaction_id": None,
        "transcript_text": transcript.text if transcript else None,
        "transcript_segments_json": transcript.segments_json if transcript else None,
        "duration_seconds": _parse_youtube_duration_seconds(content_details.get("duration")),
        "has_auto_captions": transcript.is_generated if transcript else None,
        "collaborator_channel_ids": collaborator_channel_ids,
        "like_count": parse_count(statistics.get("likeCount")),
        "view_count": parse_count(statistics.get("viewCount")),
        "comment_count": parse_count(statistics.get("commentCount")),
        "reply_count": sum(
            int((thread.get("snippet") or {}).get("totalReplyCount") or 0)
            for page in comments_result.payload or []
            for thread in page.get("items") or []
        ),
        "subscriber_count": youtube_authors.SUBSCRIBER_COUNTS.get(video_id),
        "collection_status": _terminal_collection_status(
            metadata_result,
            comments_result,
            transcript_result,
        ),
        "metadata_status": metadata_result.status,
        "transcript_lifecycle_status": lifecycle_status,
        "transcript_status": legacy_transcript_status(lifecycle_status),
        "comments_status": comments_result.status,
        "storage_status": "pending",
        "error_code": error_code,
        "error_message": error_message,
        "attempt_count": attempt_count,
        "collector_version": _env_str("COLLECTOR_VERSION", "1"),
        "source_payload_version": "2",
        "metadata_collected_at": _completed_at(metadata_result),
        "metadata_error_code": metadata_result.error_code,
        "metadata_error_message": metadata_result.error_message,
        "comments_collected_at": _completed_at(comments_result),
        "comments_error_code": comments_result.error_code,
        "comments_error_message": comments_result.error_message,
        "transcript_language": transcript.language if transcript else None,
        "transcript_language_code": transcript.language_code if transcript else None,
        "transcript_requested_language": requested_language_code,
        "transcript_requested_language_code": requested_language_code,
        "transcript_obtained_language": transcript.language if transcript else None,
        "transcript_obtained_language_code": (transcript.language_code if transcript else None),
        "transcript_source_language": transcript.source_language if transcript else None,
        "transcript_source_language_code": (
            transcript.source_language_code if transcript else None
        ),
        "transcript_is_generated": transcript.is_generated if transcript else None,
        "transcript_is_translated": transcript.is_translated if transcript else None,
        "transcript_generation_type": (transcript.generation_type if transcript else None),
        "transcript_provider": transcript.source if transcript else "youtube_transcript_api",
        "transcript_source": transcript.source if transcript else None,
        "transcript_selection_strategy": (transcript.selection_strategy if transcript else None),
        "transcript_segment_count": transcript.segment_count if transcript else None,
        "transcript_available_languages": available_languages,
        "transcript_covered_duration_seconds": (
            transcript.covered_duration_seconds if transcript else None
        ),
        "transcript_collected_at": _completed_at(transcript_result),
        "transcript_attempt_count": transcript_result.attempt_count,
        "transcript_last_attempt_at": _completed_at(transcript_result),
        "transcript_next_attempt_at": None,
        "transcript_recovered_at": None,
        "transcript_content_version": (transcript.content_version if transcript else None),
        "transcript_error_code": transcript_result.error_code,
        "transcript_error_message": transcript_result.error_message,
        "canonical_metadata": safe_json_dumps(canonical_metadata),
        "source_specific_metadata": safe_json_dumps(metadata),
        "raw_source_payload": safe_json_dumps(
            {
                "video_metadata": metadata,
                "comment_thread_page_count": len(comments_result.payload or []),
            }
        ),
    }


def _youtube_comment_events(
    video_id: str,
    comments_result: OperationResult[list[dict]],
    attempt_count: int,
) -> list[dict]:
    events = []
    root_content_id = canonical_content_id("youtube", video_id)
    collected_at = _completed_at(comments_result) or isoformat_utc(utc_now())
    position = 0

    def build(comment: dict, parent_platform_id: str, depth: int, kind: str) -> dict | None:
        nonlocal position
        comment_id = comment.get("id")
        snippet = comment.get("snippet") or {}
        text = snippet.get("textOriginal") or snippet.get("textDisplay")
        if not comment_id or not text:
            return None
        author_channel = (snippet.get("authorChannelId") or {}).get("value")
        relationship = ContentRelationship.child(
            source="youtube",
            platform_content_id=comment_id,
            parent_content_id=canonical_content_id("youtube", parent_platform_id),
            root_content_id=root_content_id,
            conversation_id=video_id,
            content_type=kind,
            relation_type="reply" if depth > 1 else "comment",
            depth=depth,
            position_in_thread=position,
        )
        position += 1
        return {
            "event_id": comment_id,
            "platform_event_id": comment_id,
            "user_id": (
                f"youtube-channel-{author_channel}"
                if author_channel
                else f"youtube-comment-{comment_id}"
            ),
            "url": f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}",
            "title": text,
            "raw_text": text,
            "timestamp": snippet.get("publishedAt") or collected_at,
            "published_at": snippet.get("publishedAt"),
            "collected_at": collected_at,
            "updated_at": snippet.get("updatedAt") or collected_at,
            "last_attempt_at": collected_at,
            "source": "youtube",
            "owner_channel_id": author_channel,
            "youtube_channel_name": snippet.get("authorDisplayName"),
            **relationship.to_dict(),
            "parent_interaction_id": parent_platform_id,
            "like_count": parse_count(snippet.get("likeCount")),
            "collection_status": STATUS_SUCCESS,
            "metadata_status": STATUS_SUCCESS,
            "transcript_status": STATUS_DISABLED,
            "transcript_lifecycle_status": "disabled",
            "comments_status": STATUS_DISABLED,
            "storage_status": "pending",
            "attempt_count": attempt_count,
            "collector_version": _env_str("COLLECTOR_VERSION", "1"),
            "source_payload_version": "2",
            "metadata_collected_at": collected_at,
            "canonical_metadata": safe_json_dumps(
                {
                    "author_channel_id": author_channel,
                    "author_display_name": snippet.get("authorDisplayName"),
                    "published_at": snippet.get("publishedAt"),
                    "updated_at": snippet.get("updatedAt"),
                }
            ),
            "source_specific_metadata": safe_json_dumps(
                {
                    "published_at": snippet.get("publishedAt"),
                    "updated_at": snippet.get("updatedAt"),
                    "like_count": snippet.get("likeCount"),
                    "viewer_rating": snippet.get("viewerRating"),
                }
            ),
            "raw_source_payload": safe_json_dumps(
                {
                    "id": comment_id,
                    "snippet": {
                        "publishedAt": snippet.get("publishedAt"),
                        "updatedAt": snippet.get("updatedAt"),
                        "likeCount": snippet.get("likeCount"),
                    },
                }
            ),
        }

    for page in comments_result.payload or []:
        for thread in page.get("items") or []:
            top_level = (thread.get("snippet") or {}).get("topLevelComment") or {}
            top_event = build(top_level, video_id, 1, "youtube_comment")
            if top_event is not None:
                top_event["reply_count"] = parse_count(
                    (thread.get("snippet") or {}).get("totalReplyCount")
                )
                events.append(top_event)
                for reply in (thread.get("replies") or {}).get("comments") or []:
                    reply_event = build(
                        reply,
                        top_event["platform_event_id"],
                        2,
                        "youtube_reply",
                    )
                    if reply_event is not None:
                        events.append(reply_event)
    return events


def _prepare_event(event: dict) -> dict:
    """Fill the canonical contract for collectors that emit legacy-shaped events."""

    prepared = dict(event)
    source = str(prepared["source"]).lower()
    platform_event_id = str(prepared.get("platform_event_id") or prepared.get("event_id"))
    now = isoformat_utc(utc_now())
    prepared["event_id"] = platform_event_id
    prepared["platform_event_id"] = platform_event_id
    prepared.setdefault("raw_text", prepared.get("title"))
    prepared.setdefault("thumbnail_url", None)
    prepared.setdefault("published_at", prepared.get("timestamp"))
    prepared["timestamp"] = prepared.get("timestamp") or now
    prepared.setdefault("collected_at", now)
    prepared.setdefault("updated_at", prepared.get("published_at") or now)
    prepared.setdefault("last_attempt_at", now)

    if not prepared.get("content_id"):
        if source == "reddit":
            conversation_id = str(prepared.get("conversation_id") or platform_event_id)
            root_content_id = canonical_content_id(source, conversation_id)
            parent_platform_id = str(prepared.get("parent_interaction_id") or conversation_id)
            depth = max(1, int(prepared.get("depth") or 1))
            relationship = ContentRelationship.child(
                source=source,
                platform_content_id=platform_event_id,
                parent_content_id=canonical_content_id(source, parent_platform_id),
                root_content_id=root_content_id,
                conversation_id=conversation_id,
                content_type="reddit_comment",
                relation_type="reply" if prepared.get("parent_interaction_id") else "comment",
                depth=depth,
                position_in_thread=prepared.get("position_in_thread"),
            )
        else:
            relationship = ContentRelationship.root(
                source=source,
                platform_content_id=platform_event_id,
                content_type=f"{source}_post",
                conversation_id=prepared.get("conversation_id") or platform_event_id,
            )
        for key, value in relationship.to_dict().items():
            prepared.setdefault(key, value)

    prepared.setdefault("collection_status", STATUS_SUCCESS)
    prepared.setdefault("metadata_status", STATUS_SUCCESS)
    prepared.setdefault("transcript_status", STATUS_DISABLED)
    prepared.setdefault(
        "transcript_lifecycle_status",
        transcript_lifecycle_status(prepared.get("transcript_status")),
    )
    prepared.setdefault("comments_status", STATUS_DISABLED)
    prepared.setdefault("storage_status", "pending")
    prepared.setdefault("attempt_count", 1)
    prepared.setdefault("collector_version", _env_str("COLLECTOR_VERSION", "1"))
    prepared.setdefault("source_payload_version", "2")
    prepared.setdefault("event_type", f"{source}.content.observed")
    prepared.setdefault("event_version", "1.0")
    prepared.setdefault("metadata_collected_at", prepared.get("collected_at"))
    prepared.setdefault(
        "canonical_metadata",
        safe_json_dumps(
            {
                "like_count": prepared.get("like_count"),
                "view_count": prepared.get("view_count"),
                "comment_count": prepared.get("comment_count"),
                "reply_count": prepared.get("reply_count"),
                "retweet_count": prepared.get("retweet_count"),
                "bookmark_count": prepared.get("bookmark_count"),
                "score": prepared.get("score"),
                "follower_count": prepared.get("follower_count"),
                "subreddit_member_count": prepared.get("subreddit_member_count"),
            }
        ),
    )
    prepared.setdefault(
        "source_specific_metadata",
        safe_json_dumps(
            {
                "x_account": prepared.get("x_account"),
                "subreddit": prepared.get("subreddit"),
                "subreddit_title": prepared.get("subreddit_title"),
                "subreddit_visibility": prepared.get("subreddit_visibility"),
            }
        ),
    )
    prepared.setdefault(
        "raw_source_payload",
        safe_json_dumps(
            {
                "platform_event_id": platform_event_id,
                "url": prepared.get("url"),
                "raw_text": prepared.get("raw_text"),
            }
        ),
    )
    collection_methods = {
        "youtube": "youtube_data_api",
        "reddit": "reddit_public_web",
        "x": "x_browser",
    }
    return enrich_event_envelope(
        prepared,
        producer_name=_env_str("COLLECTOR_PRODUCER_NAME", "playwright_collector"),
        producer_run_id=_env_str("PIPELINE_RUN_ID", "standalone"),
        collection_method=collection_methods.get(source, "public_web"),
        api_endpoint=prepared.get("api_endpoint"),
    )


def _parse_youtube_duration_seconds(duration: str | None) -> float | None:
    if not duration:
        return None
    match = re.fullmatch(
        r"P(?:(?P<days>\d+)D)?"
        r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?"
        r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?",
        duration,
    )
    if not match:
        return None
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = float(match.group("seconds") or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


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
    cookie_names = {cookie["name"] for cookie in context.cookies("https://x.com")}
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
            if any(blocked in normalized_label for blocked in ("phone", "google", "apple")):
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
    return (
        _x_active_login_scope(page)
        .locator(
            'input[autocomplete="current-password"], input[name="password"], input[type="password"]'
        )
        .first
    )


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
            "X direct login submitted the email but did not advance before the timeout",
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
        'input[autocomplete="username"], input[name="text"], input[type="text"]'
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
                "X direct login needs an additional verification step before the password field",
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
                "The Google login button was not found on X or did not open the Google login flow",
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
                account = candidate.locator("[data-identifier], [data-email]").first

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

    raise RuntimeError(f"X CDP control endpoint did not become ready at {cdp_url}: {last_error}")


def _x_cdp_url() -> str:
    port_file_value = os.getenv("X_CDP_PORT_FILE")
    if port_file_value:
        port_file = Path(port_file_value)
        if not port_file.is_file():
            raise FileNotFoundError(f"X CDP runtime port file not found: {port_file}.")
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

        show_more = article.locator('[data-testid="tweet-text-show-more-link"]')
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
                    f"https://x.com/search?q={quote(query, safe='')}&src=typed_query&f=live"
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
                                _clean_text(text_locator.first.inner_text(timeout=1000))
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
                            user_locator = article.locator('div[data-testid="User-Name"]')
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
                                screen_name_match.group(1) if screen_name_match else "anonymous"
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
                                    "user_id": f"x-{_hash_identity(screen_name, status_id)}",
                                    "url": tweet_url,
                                    "title": text,
                                    "raw_text": text,
                                    "timestamp": timestamp,
                                    "source": "x",
                                    "x_account": screen_name,
                                    "conversation_id": status_id,
                                    "parent_interaction_id": None,
                                    "like_count": extract_x_metric(
                                        article,
                                        "like",
                                    ),
                                    "view_count": extract_x_metric(
                                        article,
                                        "analytics",
                                    ),
                                    "reply_count": extract_x_metric(
                                        article,
                                        "reply",
                                    ),
                                    "retweet_count": extract_x_metric(
                                        article,
                                        "retweet",
                                    ),
                                    "bookmark_count": extract_x_metric(
                                        article,
                                        "bookmark",
                                    ),
                                    "follower_count": extract_x_followers(article),
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
                raise RuntimeError(
                    "No X posts were visible after all search retries. The X "
                    "session may be rate-limited or temporarily unavailable."
                )
                return []
    except Exception as exc:
        if _is_auth_or_quota_block(exc):
            raise _soft_block("x", str(exc)) from exc
        if _env_bool("X_FAIL_ON_ERROR", True):
            raise RuntimeError(f"X online collection failed: {exc}") from exc
        print(
            f"X online collection explicitly ignored by configuration: {exc}",
            flush=True,
        )
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
    events: list[dict] = []
    candidates: dict[str, dict] = {}
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
            subreddit_info = _extract_reddit_subreddit_info(context, subreddit)
            listing_url: str | None = f"https://old.reddit.com/r/{subreddit}/comments/?limit=100"
            visited_pages: set[str] = set()
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
                        fallback_events, fallback_discovered = _collect_reddit_feed_events(
                            subreddit,
                            state,
                            keywords,
                            keyword_match_mode,
                            scan_limit,
                            subreddit_info,
                        )
                    except RuntimeError:
                        if candidates or discovered_comments > 0:
                            LOGGER.warning(
                                "Skipping unavailable subreddit %s after "
                                "discovering Reddit comments",
                                subreddit,
                            )
                            break
                        raise _soft_block(
                            "reddit",
                            "HTML listing and RSS fallback are unavailable "
                            f"for {subreddit}: HTTP {response.status}",
                        )
                    for event in fallback_events:
                        candidates[event["event_id"]] = event
                    discovered_comments += fallback_discovered
                    break
                if response.status >= 400:
                    if response.status in {401, 403, 429}:
                        raise _soft_block(
                            "reddit",
                            f"listing is unavailable for {subreddit}: HTTP {response.status}",
                        )
                    raise RuntimeError(
                        f"Reddit listing did not load for {subreddit}: HTTP {response.status}"
                    )
                subreddit_member_count = _extract_reddit_subreddit_members(
                    page
                ) or subreddit_info.get("subreddit_member_count")
                comments = page.locator("div.thing.comment")
                page_comment_count = min(
                    comments.count(),
                    scan_limit - scanned_comments,
                )
                discovered_comments += page_comment_count
                scanned_comments += page_comment_count

                for index in range(page_comment_count):
                    comment_event = _extract_reddit_comment_event(
                        comments.nth(index),
                        listing_url,
                        subreddit_member_count,
                        subreddit_info,
                    )
                    if comment_event is None or state.contains(
                        "reddit",
                        comment_event["event_id"],
                    ):
                        continue
                    if not _matches_keywords(
                        comment_event["title"],
                        keywords,
                        keyword_match_mode,
                    ):
                        continue
                    candidates[comment_event["event_id"]] = comment_event

                next_link = page.locator("span.next-button a")
                next_href = next_link.first.get_attribute("href") if next_link.count() else None
                listing_url = urljoin(listing_url, str(next_href)) if next_href else None
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
                    f'div.thing.comment[data-fullname="t1_{candidate["event_id"]}"]'
                ).first
                if exact_comment.count():
                    detail_member_count = _extract_reddit_subreddit_members(page) or candidate.get(
                        "subreddit_member_count"
                    )
                    candidate_subreddit_info = {
                        key: candidate.get(key)
                        for key in (
                            "subreddit_title",
                            "subreddit_description",
                            "subreddit_created_at",
                            "subreddit_visibility",
                            "subreddit_weekly_visitors",
                            "subreddit_weekly_contributions",
                            "subreddit_member_count",
                        )
                    }
                    detailed_event = _extract_reddit_comment_event(
                        exact_comment,
                        detail_url,
                        detail_member_count,
                        candidate_subreddit_info,
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
    try:
        schema_definition = json.loads(schema)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid Avro schema JSON: {schema_path}") from exc
    if not isinstance(schema_definition, dict) or not isinstance(
        schema_definition.get("fields"), list
    ):
        raise RuntimeError(f"Invalid Avro schema fields: {schema_path}")
    max_events = _env_int("PRODUCER_MAX_EVENTS", 50)
    state = ProcessedState(_env_str("COLLECTOR_STATE_DB", "/app/state/processed.sqlite"))

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

    schema_fields = tuple(
        field["name"]
        for field in schema_definition["fields"]
        if isinstance(field, dict) and isinstance(field.get("name"), str)
    )
    if not schema_fields:
        raise RuntimeError(f"Avro schema has no named fields: {schema_path}")

    def publish(events: list[dict]) -> None:
        events[:] = [_prepare_event(event) for event in events]
        for event in events:
            value = {field: event.get(field) for field in schema_fields}
            value.update(
                {
                    "user_id": event["user_id"],
                    "url": event["url"],
                    "title": event["title"],
                    "raw_text": event.get("raw_text") or event["title"],
                    "clean_text": None,
                    "text_for_model": None,
                    "timestamp": event.get("timestamp") or datetime.now(timezone.utc).isoformat(),
                    "source": event["source"],
                    "error": None,
                }
            )
            producer.produce(
                topic=topic,
                key=event["user_id"],
                value=value,
                on_delivery=delivery_report,
            )
            producer.poll(0)

        undelivered = producer.flush(60)
        if undelivered or delivery_errors:
            raise RuntimeError(
                f"Kafka delivery failed: {undelivered} undelivered, {delivery_errors}"
            )
        state.mark_many(mode, events)

    produced_event_count = 0
    try:
        print(f"Producer mode: {mode} (online)")
        if mode == "youtube":
            api_key = _env_str("YOUTUBE_API_KEY", "")
            if not api_key:
                raise _soft_block("youtube", "YOUTUBE_API_KEY is required")
            youtube = _get_youtube_service(api_key)
            search_limit = _env_int("YOUTUBE_SEARCH_MAX_RESULTS", 50)
            search_languages = _env_list(
                "YOUTUBE_SEARCH_LANGUAGES",
                [_env_str("YOUTUBE_SEARCH_LANGUAGE", "en")],
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
            if not video_ids:
                raise RuntimeError(
                    "YouTube search completed without any video IDs; refusing a green empty run"
                )
            events = []
            youtube_processing_deadline = time.monotonic() + max(
                1,
                _env_int("YOUTUBE_COLLECTION_TIMEOUT_SECONDS", 900) - 120,
            )
            output_dir = Path(_env_str("YOUTUBE_OUTPUT_DIR", "/app/api/yt_raw_json"))
            candidate_ids = [
                video_id for video_id in video_ids if not state.contains("youtube", video_id)
            ]
            if max_events > 0:
                candidate_ids = candidate_ids[:max_events]
            metadata_by_id = {
                video_id: _fetch_video_metadata(youtube, video_id) for video_id in candidate_ids
            }
            owner_by_id = {
                video_id: owner_channel_id
                for video_id, metadata_result in metadata_by_id.items()
                if (
                    owner_channel_id := (
                        (metadata_result.payload or {}).get("snippet", {}).get("channelId")
                    )
                )
            }
            if _env_bool("YOUTUBE_COLLABORATOR_COLLECTION_ENABLED", False):
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
            else:
                print("YouTube collaborator page collection is disabled")
                collaborators_by_id = {}
            transcript_failures = 0
            transcripts_disabled = transcript_max_failures == 0
            transcript_circuit_retryable = False
            for video_id in candidate_ids:
                if time.monotonic() >= youtube_processing_deadline:
                    print(
                        "YouTube processing budget reached; ending the batch "
                        "before the Airflow timeout"
                    )
                    break
                attempt_count = state.next_attempt_count("youtube", video_id)
                metadata_result = metadata_by_id[video_id]
                owner_channel_id = owner_by_id.get(video_id)
                collaborator_channel_ids = collaborators_by_id.get(video_id)
                comments_result = _fetch_youtube_comments(
                    youtube,
                    video_id,
                    _env_float("YOUTUBE_SLEEP_SECONDS", 0.5),
                    comment_max_pages,
                )
                if transcripts_disabled:
                    result_factory = (
                        OperationResult.rate_limited
                        if transcript_circuit_retryable
                        else OperationResult.disabled
                    )
                    transcript_result = result_factory(
                        error_code="transcript_circuit_open",
                        error_message="Transcript collection is disabled for this batch",
                        attempt_count=attempt_count,
                        completed_at=utc_now(),
                    )
                else:
                    transcript_result = _fetch_youtube_transcript(
                        video_id,
                        _preferred_youtube_transcript_languages(metadata_result.payload or {}),
                        attempt_count,
                    )
                    if transcript_result.status in {
                        STATUS_FAILED,
                        STATUS_RATE_LIMITED,
                    }:
                        transcript_failures += 1
                        if (
                            transcript_result.error_code
                            in {"ip_blocked", "request_blocked", "rate_limited"}
                            or transcript_failures >= transcript_max_failures
                        ):
                            transcripts_disabled = True
                            transcript_circuit_retryable = True
                            print(
                                "[YouTube] Transcript collection disabled for "
                                "the remaining videos after "
                                f"{transcript_failures} failures"
                            )
                output_dir.mkdir(parents=True, exist_ok=True)
                (output_dir / f"{video_id}.json").write_text(
                    safe_json_dumps(
                        {
                            "video_id": video_id,
                            "video_metadata": metadata_result.to_dict(),
                            "video_transcript": transcript_result.to_dict(),
                            "owner_channel_id": owner_channel_id,
                            "collaborator_channel_ids": (collaborator_channel_ids),
                            "comment_threads": comments_result.to_dict(),
                        },
                        pretty=True,
                    ),
                    encoding="utf-8",
                )
                video_events = [
                    _youtube_video_event(
                        video_id,
                        metadata_result,
                        comments_result,
                        transcript_result,
                        owner_channel_id,
                        collaborator_channel_ids,
                        attempt_count,
                    )
                ]
                video_events.extend(
                    _youtube_comment_events(
                        video_id,
                        comments_result,
                        attempt_count,
                    )
                )
                publish(video_events)
                produced_event_count += len(video_events)
        elif mode == "x":
            events = _collect_x_events(state, max_events)
        else:
            events = _collect_reddit_events(state, max_events)

        if mode != "youtube":
            publish(events)
            produced_event_count = len(events)
        print(f"Produced {produced_event_count} new {mode} events")
    except CollectorSoftBlock as exc:
        print(f"Collector soft-blocked: {exc}", file=sys.stderr, flush=True)
        raise RuntimeError(str(exc)) from exc
    finally:
        state.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Collector failed: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        raise SystemExit(1) from None
