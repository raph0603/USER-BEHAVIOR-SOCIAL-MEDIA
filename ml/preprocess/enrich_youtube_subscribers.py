"""Enrich the exported events with REAL YouTube subscriber counts (channel audience).

The lakehouse export has no follower/subscriber column yet, so this fetches the
subscriber count per YouTube channel directly (via playwright/youtube_authors,
which scrapes the public watch page) and writes a `subscriber_count` column that
`preprocess/build_dataset.py` turns into the channel features.

Design:
- Fetch ONCE per unique channel (not per video) -> far fewer requests.
- Cache channel_id -> subscriber_count on disk so re-runs are incremental.
- `--limit N` fetches at most N NEW (uncached) channels this run, so you can do a
  small subset first, check it, then scale up. The full cache is always applied.
- Writes to a NEW file by default (never clobbers the source CSV).

Usage:
  python ml/preprocess/enrich_youtube_subscribers.py --limit 30      # small test
  python ml/preprocess/enrich_youtube_subscribers.py                 # all channels
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd

ML_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ML_ROOT.parent
if str(REPO_ROOT / "playwright") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "playwright"))

from youtube_authors import fetch_youtube_collaborators, SUBSCRIBER_COUNTS

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
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

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
          f"| cached: {len(cache)} | fetching now: {len(todo)}")

    if len(todo):
        video_owners = dict(zip(todo["vid"], todo["owner_channel_id"]))
        fetch_youtube_collaborators(video_owners, max_workers=args.workers)  # fills SUBSCRIBER_COUNTS
        for vid, channel in video_owners.items():
            cache[channel] = SUBSCRIBER_COUNTS.get(vid)  # may be None if the page hid it
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
