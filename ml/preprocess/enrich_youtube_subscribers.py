"""Enrich the exported events with REAL YouTube subscriber counts (channel audience).

The lakehouse export has no follower/subscriber column yet, so this resolves the subscriber
count per YouTube channel and writes a `subscriber_count` column that
`preprocess/build_dataset.py` turns into the channel features.

Two backends:
- `api` (default when YOUTUBE_API_KEY is set) — `channels.list?part=statistics`, 50 channel
  IDs per request and 1 quota unit per request, so a few thousand channels cost almost
  nothing. Far more reliable than scraping.
- `scrape` — the public watch page via playwright/youtube_authors. No key needed, but slow
  and it silently loses channels whose page layout does not match.

Design:
- Resolve ONCE per unique channel (not per video) -> far fewer requests.
- Cache channel_id -> subscriber_count on disk so re-runs are incremental.
- `--limit N` resolves at most N NEW (uncached) channels this run, so you can do a small
  subset first, check it, then scale up; `--limit 0` applies the cache and makes no request.
- Writes to a NEW file by default (never clobbers the source CSV).

A channel that hides its subscriber count is cached as `None`, never `0`: unknown and "an
author with no audience" are different states, and collapsing them is what previously let
the feature stand in for the platform instead of the signal.

Usage:
  set YOUTUBE_API_KEY=...
  python ml/preprocess/enrich_youtube_subscribers.py --limit 50      # small test
  python ml/preprocess/enrich_youtube_subscribers.py                 # all channels
  python ml/preprocess/enrich_youtube_subscribers.py --backend scrape
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

ML_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ML_ROOT.parent
# Only the scrape backend needs playwright/, and it imports lazily, so the api backend runs
# without that tree on sys.path at all.
if str(REPO_ROOT / "playwright") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "playwright"))

DEFAULT_INPUT = REPO_ROOT / "data" / "samples" / "filtered_events.csv"
DEFAULT_OUTPUT = REPO_ROOT / "data" / "samples" / "filtered_events_enriched.csv"
CACHE_PATH = ML_ROOT / "data" / "youtube_subscribers.json"

_VID = [
    re.compile(r"[?&]v=([\w-]{11})"),
    re.compile(r"youtu\.be/([\w-]{11})"),
    re.compile(r"/watch/([\w-]{11})"),
]


def video_id(url: object) -> str | None:
    for pat in _VID:
        m = pat.search(str(url))
        if m:
            return m.group(1)
    return None


API_URL = "https://www.googleapis.com/youtube/v3/channels"
API_BATCH = 50  # channels.list accepts 50 ids per request, still 1 quota unit


def subscriber_from_statistics(statistics: dict) -> int | None:
    """A hidden or unparsable count is unknown -- never 0, which means "no audience"."""
    if statistics.get("hiddenSubscriberCount"):
        return None
    value = statistics.get("subscriberCount")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def fetch_via_api(channel_ids: list[str], api_key: str, fetch_json=None) -> dict[str, int | None]:
    """Resolve channels through channels.list. `fetch_json` is injected by the tests."""
    import urllib.parse
    import urllib.request

    def _default_fetch(url: str) -> dict:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.loads(resp.read())

    fetch_json = fetch_json or _default_fetch
    resolved: dict[str, int | None] = {}
    for start in range(0, len(channel_ids), API_BATCH):
        batch = channel_ids[start:start + API_BATCH]
        query = urllib.parse.urlencode(
            {"part": "statistics", "id": ",".join(batch), "maxResults": len(batch), "key": api_key}
        )
        payload = fetch_json(f"{API_URL}?{query}")
        for item in payload.get("items", []):
            resolved[item["id"]] = subscriber_from_statistics(item.get("statistics") or {})
        # Channels the API did not return (deleted, private) stay unknown rather than absent,
        # so a later run does not keep retrying them.
        for channel in batch:
            resolved.setdefault(channel, None)
        print(f"  channels.list: {min(start + API_BATCH, len(channel_ids))}/{len(channel_ids)}", flush=True)
    return resolved


def load_cache() -> dict:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, indent=0), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Add real YouTube subscriber_count to the events CSV.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Fetch at most N NEW channels this run; 0 applies the cache without any request.",
    )
    parser.add_argument("--workers", type=int, default=8, help="Scrape backend only.")
    parser.add_argument(
        "--backend",
        choices=["api", "scrape"],
        default=None,
        help="Default: 'api' when YOUTUBE_API_KEY is set, otherwise 'scrape'.",
    )
    args = parser.parse_args()

    api_key = os.environ.get("YOUTUBE_API_KEY", "").strip()
    backend = args.backend or ("api" if api_key else "scrape")
    if backend == "api" and not api_key:
        raise SystemExit("--backend api needs YOUTUBE_API_KEY in the environment.")

    df = pd.read_csv(args.input)
    is_yt = df["source"].astype("string").str.lower() == "youtube"
    yt = df[is_yt].copy()
    yt["vid"] = yt["url"].map(video_id)
    yt = yt[yt["vid"].notna() & yt["owner_channel_id"].notna()]

    # one representative video per channel
    rep = yt.drop_duplicates(subset=["owner_channel_id"])[["owner_channel_id", "vid"]]
    cache = load_cache()
    todo = rep[~rep["owner_channel_id"].isin(cache.keys())]
    if args.limit is not None:
        todo = todo.head(args.limit)

    print(f"YouTube rows: {len(yt)} | unique channels: {rep['owner_channel_id'].nunique()} "
          f"| cached: {len(cache)} | resolving now: {len(todo)} via {backend}")

    if len(todo):
        if backend == "api":
            cache.update(fetch_via_api(todo["owner_channel_id"].tolist(), api_key))
        else:
            from youtube_authors import fetch_youtube_collaborators, SUBSCRIBER_COUNTS  # noqa: PLC0415

            video_owners = dict(zip(todo["vid"], todo["owner_channel_id"]))
            fetch_youtube_collaborators(video_owners, max_workers=args.workers)
            for vid, channel in video_owners.items():
                cache[channel] = SUBSCRIBER_COUNTS.get(vid)  # None when the page hid it
        save_cache(cache)

    got = sum(1 for c in rep["owner_channel_id"] if cache.get(c))
    print(f"Channels with a subscriber value: {got}/{rep['owner_channel_id'].nunique()}")

    # map channel -> subscriber back onto every YouTube row; a count the export already
    # carries came straight from the API, so it wins over the older scraped cache and
    # only the gaps are filled.
    cached = pd.to_numeric(
        df["owner_channel_id"].map(lambda c: cache.get(c) if pd.notna(c) else None),
        errors="coerce",
    )
    if "subscriber_count" in df.columns:
        cached = pd.to_numeric(df["subscriber_count"], errors="coerce").combine_first(cached)
    df["subscriber_count"] = cached
    df.loc[~is_yt, "subscriber_count"] = pd.NA

    filled = int(df["subscriber_count"].notna().sum())
    df.to_csv(args.output, index=False)
    print(f"Rows with subscriber_count: {filled}/{len(df)}")
    print(f"Saved -> {args.output}")


if __name__ == "__main__":
    main()
