"""Collect current X profile audience metrics in resumable public batches."""

from __future__ import annotations

import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

OUTPUT_COLUMNS = [
    "screen_name",
    "author_hash",
    "profile_url",
    "follower_count",
    "metric_collected_at_utc",
    "status",
    "error",
    "metric_source",
]
TERMINAL_STATUSES = {"ok", "not_found", "suspended", "content_unavailable"}


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


def completed_names(paths: list[Path]) -> set[str]:
    completed = set()
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                if (row.get("status") or "").strip().lower() not in TERMINAL_STATUSES:
                    continue
                completed.add(
                    (row.get("screen_name") or "").strip().casefold()
                )
    return completed


def append_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def chunks(items: list[dict[str, str]], size: int):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def normalize_result(
    author: dict[str, str],
    result: dict | None,
) -> dict:
    screen_name = author["screen_name"]
    profile_url = f"https://x.com/{screen_name}"
    now = datetime.now(timezone.utc).isoformat()
    if not result:
        return {
            **author,
            "profile_url": profile_url,
            "follower_count": "",
            "metric_collected_at_utc": now,
            "status": "missing_result",
            "error": "batch response omitted this profile",
            "metric_source": "pulse_public_profile_batch",
        }
    followers = result.get("followers")
    error = str(result.get("error") or "")
    if followers is not None:
        status = "ok"
    elif error:
        status = error.strip().lower().replace(" ", "_")
    else:
        status = "missing_metric"
        error = "followers field missing"
    return {
        **author,
        "profile_url": str(result.get("url") or profile_url),
        "follower_count": followers if followers is not None else "",
        "metric_collected_at_utc": str(result.get("fetchedAt") or now),
        "status": status,
        "error": error,
        "metric_source": "pulse_public_profile_batch",
    }


def main() -> None:
    input_path = Path(
        os.getenv("X_AUDIENCE_INPUT", "/data/x_audience_authors.csv")
    )
    output_path = Path(
        os.getenv(
            "X_AUDIENCE_BATCH_OUTPUT",
            "/data/x_audience_enrichment_batch.csv",
        )
    )
    skip_paths = [
        Path(value)
        for value in os.getenv(
            "X_AUDIENCE_SKIP_OUTPUTS",
            "/data/x_audience_enrichment.csv",
        ).split("||")
        if value.strip()
    ]
    skip_paths.append(output_path)
    endpoint = os.getenv(
        "X_AUDIENCE_BATCH_ENDPOINT",
        "https://pulse.walls.sh/profile/batch",
    )
    batch_size = min(25, max(1, env_int("X_AUDIENCE_BATCH_SIZE", 25)))
    max_batches = env_int("X_AUDIENCE_MAX_BATCHES", 0)
    wait_ms = env_int("X_AUDIENCE_BATCH_WAIT_MS", 250)

    completed = completed_names(skip_paths)
    pending = [
        author
        for author in load_authors(input_path)
        if author["screen_name"].casefold() not in completed
    ]
    batches = list(chunks(pending, batch_size))
    if max_batches > 0:
        batches = batches[:max_batches]
    print(
        f"Batch audience enrichment: {len(completed)} completed, "
        f"{len(pending)} pending, {len(batches)} batches this run",
        flush=True,
    )

    session = requests.Session()
    session.headers["User-Agent"] = (
        "USER-BEHAVIOR-SOCIAL-MEDIA research audience enrichment"
    )
    successes = 0
    for batch_number, batch in enumerate(batches, start=1):
        urls = ",".join(
            f"https://x.com/{author['screen_name']}" for author in batch
        )
        response = session.get(
            endpoint,
            params={"urls": urls},
            timeout=90,
        )
        if response.status_code == 429:
            print("Rate limit reached; output remains resumable.", flush=True)
            break
        response.raise_for_status()
        payload = response.json()
        raw_results = payload.get("results", payload)
        if not isinstance(raw_results, list):
            raise ValueError("Unexpected batch response shape")
        by_handle = {
            str(result.get("handle") or "")
            .strip()
            .casefold(): result
            for result in raw_results
            if isinstance(result, dict) and result.get("handle")
        }
        by_url = {
            str(result.get("url") or "").rstrip("/").casefold(): result
            for result in raw_results
            if isinstance(result, dict) and result.get("url")
        }
        normalized = []
        for author in batch:
            key = author["screen_name"].casefold()
            result = by_handle.get(key) or by_url.get(
                f"https://x.com/{key}"
            )
            row = normalize_result(author, result)
            successes += int(row["status"] == "ok")
            normalized.append(row)
        append_rows(output_path, normalized)
        print(
            f"[{batch_number}/{len(batches)}] "
            f"{sum(row['status'] == 'ok' for row in normalized)}/"
            f"{len(normalized)} follower counts",
            flush=True,
        )
        time.sleep(wait_ms / 1000)
    print(
        f"Collected {successes} follower counts -> {output_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
