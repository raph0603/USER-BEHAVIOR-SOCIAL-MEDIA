"""Merge current lakehouse export with additional real Reddit and X records."""

from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path

import pandas as pd

ALLOWED_EV_SUBREDDITS = {
    "electricvehicles",
    "evcharging",
    "evs",
    "ioniq5",
    "kiaev6",
    "rivian",
    "teslamodel3",
    "teslamodely",
    "teslamotors",
    "vinfastcomm",
}
STRONG_EV_RELEVANCE = re.compile(
    r"\b(?:"
    r"electric vehicles?|electric cars?|electric mobility|e-?mobility|"
    r"battery electric vehicles?|plug-?in hybrids?|zero-emission vehicles?|"
    r"ev charging|ev chargers?|ev batteries?|ev tax credits?|"
    r"ev policies|ev incentives?|charging (?:stations?|networks?|"
    r"infrastructure)|range anxiety|home charging|fast charging|"
    r"electric range|model 3|model y|cybertruck|rivian|r1t|r1s|"
    r"ioniq(?: 5| 6)?|ev6|ev9|byd|nio|xpeng|zeekr|xiaomi su7|"
    r"id\.?4|id buzz|mach-?e|f-?150 lightning|equinox ev|blazer ev|"
    r"nissan leaf|ariya|vinfast|vf[3-9]"
    r")\b|xe điện|ô tô điện|trạm sạc|pin xe",
    re.IGNORECASE,
)
EV_TOKEN = re.compile(r"\bevs?\b", re.IGNORECASE)
EV_CONTEXT = re.compile(
    r"\b(?:"
    r"automotive|automakers?|batter(?:y|ies)|cars?|charg(?:e|er|ers|ing)|"
    r"crossovers?|dealerships?|deliveries|drivers?|electric|emissions?|"
    r"factories|fleet|incentives?|infrastructure|mobility|models?|"
    r"plug-?in|policies|range|road trip|sales|sedans?|stations?|suvs?|"
    r"tax credits?|trucks?|vehicles?|zero-emission"
    r")\b",
    re.IGNORECASE,
)
CONTEXTUAL_EV_BRAND = re.compile(
    r"\b(?:tesla|lucid|polestar|supercharger)\b",
    re.IGNORECASE,
)


def is_ev_relevant(text: object) -> bool:
    """Reject generic search recommendations that only contain ambiguous terms."""

    value = str(text or "")
    if STRONG_EV_RELEVANCE.search(value):
        return True
    has_context = EV_CONTEXT.search(value) is not None
    return has_context and (
        EV_TOKEN.search(value) is not None
        or CONTEXTUAL_EV_BRAND.search(value) is not None
    )


def normalized_ids(frame: pd.DataFrame, source: str) -> set[str]:
    source_rows = frame.loc[
        frame["source"].astype("string").str.strip().str.lower().eq(source)
    ]
    values = []
    for column in ("platform_event_id", "event_id"):
        if column in source_rows.columns:
            values.extend(source_rows[column].dropna().astype(str))
    return {value for value in values if value and value.lower() != "nan"}


def enrich_and_filter_current_reddit(current: pd.DataFrame) -> pd.DataFrame:
    """Recover subreddit names from URLs and exclude unverifiable communities."""

    current = current.copy()
    reddit = (
        current["source"].astype("string").str.strip().str.lower().eq("reddit")
    )
    if "subreddit" not in current.columns:
        current["subreddit"] = pd.NA
    extracted = current["url"].astype("string").str.extract(
        r"/r/([^/]+)",
        flags=re.IGNORECASE,
    )[0]
    current.loc[reddit, "subreddit"] = current.loc[
        reddit,
        "subreddit",
    ].fillna(extracted.loc[reddit])
    subreddit_key = current["subreddit"].astype("string").str.lower()
    current = current.loc[
        ~reddit | subreddit_key.isin(ALLOWED_EV_SUBREDDITS)
    ].copy()
    reddit = (
        current["source"].astype("string").str.strip().str.lower().eq("reddit")
    )
    subreddit_key = current["subreddit"].astype("string").str.lower()

    if "subreddit_member_count" in current.columns:
        sizes = pd.to_numeric(
            current["subreddit_member_count"],
            errors="coerce",
        )
        size_by_subreddit = (
            current.assign(_size=sizes, _subreddit_key=subreddit_key)
            .dropna(subset=["_subreddit_key", "_size"])
            .groupby("_subreddit_key")["_size"]
            .median()
        )
        missing_size = reddit & sizes.isna()
        current.loc[missing_size, "subreddit_member_count"] = (
            subreddit_key.loc[missing_size].map(size_by_subreddit)
        )
        available = pd.to_numeric(
            current["subreddit_member_count"],
            errors="coerce",
        ).notna()
        current["subreddit_member_count_available"] = available
    return current


def reddit_archive_rows(
    archive_files: list[Path],
    current: pd.DataFrame,
    *,
    limit: int,
    seed: int,
) -> pd.DataFrame:
    frames = [pd.read_csv(path, low_memory=False) for path in archive_files]
    archive = pd.concat(frames, ignore_index=True)
    archive = archive.dropna(subset=["comment_id", "comment_text"])
    archive["comment_id"] = archive["comment_id"].astype(str)
    archive = archive.drop_duplicates("comment_id")
    archive = archive.loc[
        ~archive["comment_id"].isin(normalized_ids(current, "reddit"))
    ].copy()
    archive["comment_text"] = archive["comment_text"].astype(str).str.strip()
    archive = archive.loc[archive["comment_text"].str.len().ge(3)]

    reply_counts = (
        archive["parent_id"]
        .astype("string")
        .str.removeprefix("t1_")
        .value_counts()
    )
    archive["comment_count"] = (
        archive["comment_id"].map(reply_counts).fillna(0).astype(int)
    )
    archive["subreddit"] = (
        archive["post_url"]
        .astype(str)
        .str.extract(r"/r/([^/]+)", flags=re.IGNORECASE)[0]
    )
    archive = archive.loc[
        archive["subreddit"].astype("string").str.lower().isin(
            ALLOWED_EV_SUBREDDITS
        )
    ].copy()

    if limit > 0 and len(archive) > limit:
        # Preserve every subreddit rather than allowing the largest archive to
        # monopolize the augmentation.
        eligible = archive
        sampled = []
        per_subreddit = max(1, limit // archive["subreddit"].nunique())
        for offset, (_, rows) in enumerate(
            archive.groupby("subreddit", dropna=False, sort=True)
        ):
            sampled.append(
                rows.sample(
                    n=min(per_subreddit, len(rows)),
                    random_state=seed + offset,
                )
            )
        archive = pd.concat(sampled)
        if len(archive) < limit:
            candidates = eligible.loc[~eligible.index.isin(archive.index)]
            extra = candidates.sample(
                n=min(limit - len(archive), len(candidates)),
                random_state=seed + 100,
            )
            archive = pd.concat([archive, extra])
        archive = archive.head(limit)

    community_sizes = {}
    if {"subreddit", "subreddit_member_count"}.issubset(current.columns):
        current_reddit = current.loc[
            current["source"].astype("string").str.lower().eq("reddit")
        ].copy()
        size = pd.to_numeric(
            current_reddit["subreddit_member_count"],
            errors="coerce",
        )
        community_sizes = (
            current_reddit.assign(_size=size)
            .dropna(subset=["subreddit", "_size"])
            .groupby(current_reddit["subreddit"].astype("string").str.lower())[
                "_size"
            ]
            .median()
            .to_dict()
        )
    subreddit_key = archive["subreddit"].astype("string").str.lower()
    archive["subreddit_member_count"] = subreddit_key.map(community_sizes)

    result = pd.DataFrame(
        {
            "user_id": "reddit-" + archive["author_hash"].astype(str),
            "url": archive["comment_permalink"].fillna(archive["post_url"]),
            "title": archive["comment_text"],
            "event_ts": archive["created_iso"],
            "source": "reddit",
            "platform_event_id": archive["comment_id"],
            "score": pd.to_numeric(archive["score"], errors="coerce"),
            "comment_count": archive["comment_count"],
            "subreddit": archive["subreddit"],
            "subreddit_member_count": archive["subreddit_member_count"],
            "subreddit_member_count_available": archive[
                "subreddit_member_count"
            ].notna(),
            "collection_method": "existing_reddit_json_archive",
        }
    )
    return result.drop_duplicates("platform_event_id")


def x_collection_rows(path: Path, current: pd.DataFrame) -> pd.DataFrame:
    collected = pd.read_csv(path, low_memory=False)
    collected["status_id"] = collected["status_id"].astype(str)
    collected = collected.dropna(subset=["status_id", "tweet_text"])
    collected = collected.drop_duplicates("status_id")
    collected = collected.loc[
        ~collected["status_id"].isin(normalized_ids(current, "x"))
    ].copy()
    collected["tweet_text"] = collected["tweet_text"].astype(str).str.strip()
    collected = collected.loc[collected["tweet_text"].str.len().ge(3)]
    collected = collected.loc[collected["tweet_text"].map(is_ev_relevant)]

    result = pd.DataFrame(
        {
            "user_id": "x-" + collected["author_hash"].astype(str),
            "url": collected["tweet_url"],
            "title": collected["tweet_text"],
            "event_ts": collected["tweet_time_iso"],
            "source": "x",
            "platform_event_id": collected["status_id"],
            "like_count": pd.to_numeric(collected["like_count"], errors="coerce"),
            "view_count": pd.to_numeric(collected["view_count"], errors="coerce"),
            "retweet_count": pd.to_numeric(
                collected["retweet_count"],
                errors="coerce",
            ),
            "reply_count": pd.to_numeric(
                collected["reply_count"],
                errors="coerce",
            ),
            "bookmark_count": pd.to_numeric(
                collected["bookmark_count"],
                errors="coerce",
            ),
            "follower_count": pd.to_numeric(
                collected["follower_count"],
                errors="coerce",
            ),
            "language": collected["lang"],
            "x_account": collected["screen_name"],
            "collection_method": "authenticated_x_search",
        }
    )
    return result.drop_duplicates("platform_event_id")


def stable_author_hash(value: object) -> str:
    """Hash a public account identifier using the collector's SHA-256 convention."""

    return hashlib.sha256(str(value).casefold().encode("utf-8")).hexdigest()


def x_public_rows(
    paths: list[Path],
    current: pd.DataFrame,
    *,
    limit: int,
    seed: int,
) -> pd.DataFrame:
    """Normalize EV-specific rows from the CC0 public Tesla tweet dataset."""

    frames = [pd.read_csv(path, low_memory=False) for path in paths if path.exists()]
    if not frames:
        return pd.DataFrame()
    public = pd.concat(frames, ignore_index=True)
    required = {
        "id",
        "tweet",
        "date",
        "username",
        "language",
        "link",
        "nlikes",
        "nreplies",
        "nretweets",
    }
    missing = sorted(required - set(public.columns))
    if missing:
        raise ValueError(f"Public X dataset is missing columns: {missing}")

    public = public.dropna(subset=["id", "tweet", "username"]).copy()
    public["id"] = public["id"].astype("int64").astype(str)
    public["tweet"] = public["tweet"].astype(str).str.strip()
    public = public.loc[public["language"].astype("string").eq("en")]
    public = public.loc[public["tweet"].map(is_ev_relevant)]
    public = public.loc[public["tweet"].str.len().ge(3)]
    public = public.drop_duplicates("id")
    public = public.loc[
        ~public["id"].isin(normalized_ids(current, "x"))
    ].copy()

    # Maximize author diversity so grouped K-fold validation does not let a
    # prolific account dominate this compact augmentation.
    public["_author_key"] = public["username"].astype(str).str.casefold()
    public = public.sample(frac=1.0, random_state=seed)
    public = public.drop_duplicates("_author_key")
    if limit > 0 and len(public) > limit:
        public = public.sample(n=limit, random_state=seed + 1)

    result = pd.DataFrame(
        {
            "user_id": "x-"
            + public["username"].map(stable_author_hash).astype(str),
            "url": public["link"],
            "title": public["tweet"],
            "event_ts": public["date"],
            "source": "x",
            "platform_event_id": public["id"],
            "like_count": pd.to_numeric(public["nlikes"], errors="coerce"),
            "view_count": pd.NA,
            "retweet_count": pd.to_numeric(
                public["nretweets"],
                errors="coerce",
            ),
            "reply_count": pd.to_numeric(
                public["nreplies"],
                errors="coerce",
            ),
            "bookmark_count": pd.NA,
            "follower_count": pd.NA,
            "language": public["language"],
            "x_account": public["username"],
            "collection_method": "cc0_huggingface_tesla_dataset",
            "dataset_source_url": (
                "https://huggingface.co/datasets/"
                "hugginglearners/twitter-dataset-tesla"
            ),
        }
    )
    return result.drop_duplicates("platform_event_id")


def reddit_live_rows(paths: list[Path], current: pd.DataFrame) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    frames = [pd.read_csv(path, low_memory=False) for path in paths if path.exists()]
    if not frames:
        return pd.DataFrame()
    live = pd.concat(frames, ignore_index=True)
    if "platform_event_id" not in live.columns:
        live["platform_event_id"] = live["event_id"]
    live["subreddit"] = live["subreddit"].astype("string")
    live = live.loc[
        live["subreddit"].str.lower().isin(ALLOWED_EV_SUBREDDITS)
    ].copy()
    live = live.loc[
        ~live["platform_event_id"]
        .astype(str)
        .isin(normalized_ids(current, "reddit"))
    ]
    live = live.rename(columns={"timestamp": "event_ts"})
    live["collection_method"] = "live_ev_subreddit_collection"
    selected = [
        "user_id",
        "url",
        "title",
        "event_ts",
        "source",
        "platform_event_id",
        "score",
        "comment_count",
        "subreddit",
        "subreddit_member_count",
        "subreddit_member_count_available",
        "collection_method",
    ]
    return live.reindex(columns=selected).drop_duplicates("platform_event_id")


def align_and_merge(current: pd.DataFrame, additions: list[pd.DataFrame]) -> pd.DataFrame:
    columns = list(current.columns)
    for addition in additions:
        columns.extend(column for column in addition.columns if column not in columns)
    aligned = [current.reindex(columns=columns)]
    aligned.extend(addition.reindex(columns=columns) for addition in additions)
    merged = pd.concat(aligned, ignore_index=True)

    event_key = (
        merged["source"].astype("string").str.lower()
        + "|"
        + merged["platform_event_id"].astype("string")
    )
    has_event_id = merged["platform_event_id"].notna()
    keep = ~event_key.duplicated() | ~has_event_id
    merged = merged.loc[keep].copy()

    text_key = (
        merged["source"].astype("string").str.lower()
        + "|"
        + merged["title"].astype("string").str.strip().str.casefold()
    )
    merged = merged.loc[~text_key.duplicated()].reset_index(drop=True)
    return merged


def apply_x_audience_enrichment(
    merged: pd.DataFrame,
    paths: list[Path],
) -> tuple[pd.DataFrame, int]:
    """Backfill missing current follower counts from timestamped profile reads."""

    frames = []
    for path in paths:
        if not path.exists():
            continue
        frame = pd.read_csv(path, low_memory=False)
        if "metric_source" not in frame.columns:
            frame["metric_source"] = "authenticated_x_profile_browser"
        frames.append(frame)
    if not frames:
        return merged, 0

    audience = pd.concat(frames, ignore_index=True)
    required = {
        "screen_name",
        "follower_count",
        "metric_collected_at_utc",
        "status",
        "metric_source",
    }
    missing = sorted(required - set(audience.columns))
    if missing:
        raise ValueError(f"X audience enrichment is missing columns: {missing}")
    audience["follower_count"] = pd.to_numeric(
        audience["follower_count"],
        errors="coerce",
    )
    audience = audience.loc[
        audience["status"].astype("string").str.lower().eq("ok")
        & audience["follower_count"].notna()
    ].copy()
    if audience.empty:
        return merged, 0
    audience["_account_key"] = (
        audience["screen_name"]
        .astype("string")
        .str.strip()
        .str.lstrip("@")
        .str.casefold()
    )
    audience["_collected_at"] = pd.to_datetime(
        audience["metric_collected_at_utc"],
        errors="coerce",
        utc=True,
    )
    audience = audience.sort_values("_collected_at").drop_duplicates(
        "_account_key",
        keep="last",
    )
    count_by_account = audience.set_index("_account_key")["follower_count"]
    time_by_account = audience.set_index("_account_key")[
        "metric_collected_at_utc"
    ]
    source_by_account = audience.set_index("_account_key")["metric_source"]

    merged = merged.copy()
    account = merged["x_account"].astype("string").str.strip().str.lstrip("@")
    from_url = (
        merged["url"]
        .astype("string")
        .str.extract(
            r"(?:x\.com|twitter\.com)/([^/]+)/status/",
            flags=re.IGNORECASE,
        )[0]
    )
    account = account.mask(account.isna() | account.eq(""), from_url)
    account_key = account.str.casefold()
    current_count = pd.to_numeric(
        merged["follower_count"],
        errors="coerce",
    )
    enriched_count = account_key.map(count_by_account)
    is_x = (
        merged["source"].astype("string").str.strip().str.lower().eq("x")
    )
    fill = is_x & current_count.isna() & enriched_count.notna()
    merged.loc[fill, "follower_count"] = enriched_count.loc[fill]
    merged.loc[fill, "follower_count_available"] = True
    merged.loc[fill, "follower_count_collected_at"] = account_key.loc[
        fill
    ].map(time_by_account)
    merged.loc[fill, "follower_count_metric_source"] = account_key.loc[
        fill
    ].map(source_by_account)
    return merged, int(fill.sum())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--x-collection", type=Path, required=True)
    parser.add_argument("--x-public", type=Path, nargs="*", default=[])
    parser.add_argument("--x-public-limit", type=int, default=700)
    parser.add_argument("--x-audience", type=Path, nargs="*", default=[])
    parser.add_argument("--reddit-glob", required=True)
    parser.add_argument("--reddit-live", type=Path, nargs="*", default=[])
    parser.add_argument("--reddit-limit", type=int, default=2500)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    current = pd.read_csv(args.current, low_memory=False)
    current = enrich_and_filter_current_reddit(current)
    reddit_files = sorted(Path().glob(args.reddit_glob))
    if not reddit_files:
        raise FileNotFoundError(
            f"No Reddit archive files matched {args.reddit_glob}"
        )
    reddit = reddit_archive_rows(
        reddit_files,
        current,
        limit=args.reddit_limit,
        seed=args.seed,
    )
    reddit_live = reddit_live_rows(args.reddit_live, current)
    x_rows = x_collection_rows(args.x_collection, current)
    x_public = x_public_rows(
        args.x_public,
        current,
        limit=args.x_public_limit,
        seed=args.seed,
    )
    merged = align_and_merge(
        current,
        [reddit, reddit_live, x_rows, x_public],
    )
    merged, x_audience_backfilled = apply_x_audience_enrichment(
        merged,
        args.x_audience,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output, index=False)

    counts = (
        merged["source"].astype("string").str.lower().value_counts().sort_index()
    )
    print(f"Reddit archive additions: {len(reddit)}")
    print(f"Live EV-subreddit additions: {len(reddit_live)}")
    print(f"New X search additions: {len(x_rows)}")
    print(f"Public CC0 X additions: {len(x_public)}")
    print(f"X rows with newly backfilled audience: {x_audience_backfilled}")
    print("Merged source counts:")
    print(counts.to_string())
    print(f"Saved -> {args.output}")


if __name__ == "__main__":
    main()
