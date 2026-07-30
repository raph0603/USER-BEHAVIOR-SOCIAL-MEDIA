"""Resumably collect current X follower counts for a CSV of distinct authors."""

from __future__ import annotations

import csv
import os
import random
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from engagement import parse_count
from x_url_finder import env_bool, x_auth_cookies

OUTPUT_COLUMNS = [
    "screen_name",
    "author_hash",
    "profile_url",
    "follower_count",
    "metric_collected_at_utc",
    "status",
    "error",
]
TERMINAL_STATUSES = {"ok", "not_found", "suspended"}


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def load_authors(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    authors = []
    seen = set()
    for row in rows:
        screen_name = (row.get("screen_name") or "").strip().lstrip("@")
        key = screen_name.casefold()
        if not screen_name or key in seen:
            continue
        seen.add(key)
        authors.append(
            {
                "screen_name": screen_name,
                "author_hash": (row.get("author_hash") or "").strip(),
            }
        )
    return authors


def load_completed(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return {
            (row.get("screen_name") or "").strip().casefold()
            for row in csv.DictReader(handle)
            if (row.get("status") or "").strip().lower() in TERMINAL_STATUSES
        }


def append_result(path: Path, result: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(result)


def follower_count_from_profile(page, screen_name: str) -> int | None:
    escaped = screen_name.replace('"', '\\"')
    selectors = [
        f'a[href="/{escaped}/verified_followers"]',
        f'a[href="/{escaped}/followers"]',
        'a[href$="/verified_followers"]',
        'a[href$="/followers"]',
    ]
    for selector in selectors:
        links = page.locator(selector)
        for index in range(links.count()):
            link = links.nth(index)
            values = []
            for reader in (
                lambda: link.get_attribute("aria-label", timeout=1000),
                lambda: link.inner_text(timeout=1000),
            ):
                try:
                    values.append(reader())
                except (PlaywrightTimeoutError, PlaywrightError):
                    continue
            for value in values:
                parsed = parse_count(value)
                if parsed is not None:
                    return parsed
    return None


def profile_status(page) -> tuple[str, str]:
    body = page.locator("body").inner_text(timeout=3000).casefold()
    if "account suspended" in body or "compte suspendu" in body:
        return "suspended", "account suspended"
    if (
        "this account doesn\u2019t exist" in body
        or "this account doesn't exist" in body
        or "ce compte n\u2019existe pas" in body
    ):
        return "not_found", "account not found"
    if "rate limit exceeded" in body or "taux limite dépassé" in body:
        return "rate_limited", "rate limit exceeded"
    return "missing_metric", "follower link not found"


def collect_profile(page, author: dict[str, str], timeout_ms: int) -> dict:
    screen_name = author["screen_name"]
    profile_url = f"https://x.com/{screen_name}"
    collected_at = datetime.now(timezone.utc).isoformat()
    try:
        page.goto(
            profile_url,
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        page.locator("body").wait_for(state="visible", timeout=timeout_ms)
        follower_links = page.locator(
            'a[href$="/verified_followers"], a[href$="/followers"]'
        )
        try:
            follower_links.first.wait_for(
                state="visible",
                timeout=min(timeout_ms, 15_000),
            )
        except (PlaywrightTimeoutError, PlaywrightError):
            pass
        count = follower_count_from_profile(page, screen_name)
        if count is not None:
            return {
                **author,
                "profile_url": profile_url,
                "follower_count": count,
                "metric_collected_at_utc": collected_at,
                "status": "ok",
                "error": "",
            }
        status, error = profile_status(page)
    except (PlaywrightTimeoutError, PlaywrightError) as exc:
        status, error = "navigation_error", str(exc).splitlines()[0]
    return {
        **author,
        "profile_url": profile_url,
        "follower_count": "",
        "metric_collected_at_utc": collected_at,
        "status": status,
        "error": error,
    }


def main() -> None:
    input_path = Path(
        os.getenv("X_AUDIENCE_INPUT", "/data/x_audience_authors.csv")
    )
    output_path = Path(
        os.getenv("X_AUDIENCE_OUTPUT", "/data/x_audience_enrichment.csv")
    )
    max_authors = env_int("X_AUDIENCE_MAX_AUTHORS", 0)
    wait_ms = env_int("X_AUDIENCE_WAIT_MS", 900)
    timeout_ms = env_int("X_AUDIENCE_TIMEOUT_MS", 20_000)
    max_failures = env_int("X_AUDIENCE_MAX_CONSECUTIVE_FAILURES", 12)

    completed = load_completed(output_path)
    pending = [
        author
        for author in load_authors(input_path)
        if author["screen_name"].casefold() not in completed
    ]
    if max_authors > 0:
        pending = pending[:max_authors]
    print(
        f"Audience enrichment: {len(completed)} completed, "
        f"{len(pending)} pending in this run",
        flush=True,
    )

    success = 0
    consecutive_failures = 0
    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=os.getenv(
                "X_USER_DATA_DIR",
                "/app/state/x-audience-profile",
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
            for index, author in enumerate(pending, start=1):
                result = collect_profile(page, author, timeout_ms)
                append_result(output_path, result)
                if result["status"] == "ok":
                    success += 1
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                print(
                    f"[{index}/{len(pending)}] @{author['screen_name']}: "
                    f"{result['status']} {result['follower_count']}",
                    flush=True,
                )
                if (
                    result["status"] == "rate_limited"
                    or consecutive_failures >= max_failures
                ):
                    print(
                        "Stopping after repeated failures; output is resumable.",
                        flush=True,
                    )
                    break
                page.wait_for_timeout(wait_ms + random.randint(0, 400))
        finally:
            context.close()
    print(f"Collected {success} follower counts -> {output_path}", flush=True)


if __name__ == "__main__":
    main()
