"""Turn lakehouse examples or an explicit manual export into model features.

Pipeline: load -> clean text -> drop empty/duplicate -> static content features
-> consume frozen official label -> one-hot source -> save parquet.

Design choices:
- Content features are UNIFIED across sources (pure functions of the text).
- Official labels are produced upstream from a versioned, platform-specific
  virality engagement threshold contract. They are never recomputed here.
- Engagement columns are the LABEL source, never features (avoids leakage).
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from features.cognitive_friction import cognitive_friction
from features.rhetorical_roles import add_role_features
from features.topics import add_topic_features
from experiment_config import DEFAULT_RANDOM_SEED

DEFAULT_INPUT = ML_ROOT.parent / "data" / "samples" / "filtered_events.csv"
DEFAULT_OUTPUT = ML_ROOT / "data" / "train_dataset.parquet"

MIN_TEXT_CHARS = 3
# Retained only for the explicitly marked manual/legacy preprocessing path.
VIRAL_QUANTILE = 0.75

# Engagement metrics available per platform -> used only to build the label.
ENGAGEMENT_METRICS = {
    "youtube": ["view_count", "like_count", "comment_count"],
    "x": ["like_count", "view_count", "retweet_count", "reply_count", "bookmark_count"],
    "reddit": ["score", "comment_count"],
}

# Channel/author audience size per platform. Unlike engagement, this is a
# PRE-LAUNCH property of the author (known before the post spreads), so it is a
# legitimate feature, not leakage. Each platform names it differently but they
# mean the same thing (how big the author's audience is); we unify them into one.
CHANNEL_AUDIENCE_COLS = [
    "audience_count",
    "subscriber_count",
    "follower_count",
    "subreddit_member_count",
]

_URL = re.compile(r"https?://\S+|www\.\S+", re.I)
_SPACED_SCHEME = re.compile(r"(https?://)\s+", re.I)  # crawler sometimes inserts a space after //
_REDACTION = re.compile(r"<[A-Z_]+>")  # <PHONE>, <EMAIL>, ...
_MENTION = re.compile(r"@\w+")
_WS = re.compile(r"\s+")


def clean_text(text: object) -> str:
    text = unicodedata.normalize("NFKC", str(text or ""))
    text = _SPACED_SCHEME.sub(r"\1", text)
    text = _URL.sub(" ", text)
    text = _REDACTION.sub(" ", text)
    text = _MENTION.sub(" ", text)
    text = text.replace("#", " ")  # keep the hashtag word, drop the symbol
    return _WS.sub(" ", text).strip()


# Derived columns the exporter already added; we recompute them on the cleaned text.
_STALE_DERIVED = ["text_len_chars", "text_len_words", "has_question"]


def load_events(path: Path) -> pd.DataFrame:
    df = (
        pd.read_parquet(path)
        if path.is_dir() or path.suffix == ".parquet"
        else pd.read_csv(path, low_memory=False)
    )
    df = df.drop(columns=[c for c in _STALE_DERIVED if c in df.columns])
    # The lossless Silver export keeps canonical lakehouse names, whereas the
    # older dashboard export already renamed these fields for ML consumers.
    # Accept both so retraining can use the current database directly.
    compatibility_names = {
        "user_id": "author_hash",
        "title": "text",
        "event_ts": "created_at",
    }
    df = df.rename(
        columns={
            source: target
            for source, target in compatibility_names.items()
            if source in df.columns and target not in df.columns
        }
    )
    for identifier in (
        "author_hash",
        "platform_event_id",
        "event_id",
        "conversation_id",
        "content_id",
        "parent_content_id",
        "root_content_id",
        "video_id",
        "channel_id",
        "correlation_id",
        "observation_id",
    ):
        if identifier in df.columns:
            df[identifier] = df[identifier].astype("string")
    df["source"] = df["source"].astype("string").str.strip().str.lower()
    official = "label_value" in df.columns
    if official and "dataset_version" not in df.columns:
        raise ValueError("Official lakehouse input with label_value must include dataset_version")
    if official:
        required_lineage = {
            "split_name",
            "virality_policy",
            "virality_contract_fingerprint",
        }
        missing = sorted(required_lineage - set(df.columns))
        if missing:
            raise ValueError(
                "Official lakehouse labels are missing virality contract lineage: "
                + ", ".join(missing)
            )
    text_column = "text_for_model" if official else "text"
    if text_column not in df.columns:
        raise ValueError(f"Training input is missing required text column: {text_column}")
    df["clean_text"] = df[text_column].map(clean_text)
    return df


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    long_enough = df["clean_text"].str.len() >= MIN_TEXT_CHARS
    df = df[long_enough].drop_duplicates(subset=["clean_text"])
    return df.reset_index(drop=True)


def add_text_features(df: pd.DataFrame) -> pd.DataFrame:
    friction = df["clean_text"].map(cognitive_friction).apply(pd.Series)
    friction["is_vietnamese"] = (friction.pop("lang") == "vi").astype(int)

    base = pd.DataFrame(
        {
            "char_count": df["clean_text"].str.len(),
            "word_count": df["clean_text"].str.split().str.len(),
            "has_question": df["clean_text"].str.contains(r"\?", regex=True).astype(int),
        }
    )
    return pd.concat([df, base, friction], axis=1)


def as_bool(series: pd.Series) -> pd.Series:
    """Coerce an export coverage flag (bool, "true"/"1"/"yes", or null) to bool."""
    return series.map(
        lambda value: value
        if isinstance(value, (bool, np.bool_))
        else str(value).strip().lower() in {"1", "true", "yes"}
        if pd.notna(value)
        else False
    ).astype(bool)


def add_channel_features(df: pd.DataFrame) -> pd.DataFrame:
    """Unified channel/author audience-size features (best-effort).

    Coalesces the per-platform audience columns into one nullable number, then exposes:
    - chan_log_audience: log1p of the audience size (null when unknown)
    - chan_has_audience: 1 if the audience size is known, including a real zero
    - chan_audience_is_zero: 1 only for an observed zero
    If none of the source columns are present (older exports), skip silently-ish
    so the rest of the pipeline is unchanged.
    """
    present = [c for c in CHANNEL_AUDIENCE_COLS if c in df.columns]
    if not present:
        print(
            "[channel] no follower/subscriber/member columns in data "
            "-> skipping channel features (re-export from the lakehouse to enable)"
        )
        return df

    df = df.copy()
    numeric = df[present].apply(pd.to_numeric, errors="coerce").clip(lower=0)
    availability = pd.DataFrame(index=df.index)
    for column in present:
        flag = f"{column}_available"
        if column == "audience_count" and "audience_available" in df.columns:
            flag = "audience_available"
        has_value = numeric[column].notna()
        if flag in df.columns:
            # The flag records what ONE observation covered, but the export merges
            # values and flags coming from different collectors (YouTube videos.list
            # cannot see subscribers, yet the row carries a count fetched by
            # channels.list). A positive number can only exist because some collector
            # observed it, so the value wins; the flag still decides whether a 0 is a
            # real audience of zero or an un-observed placeholder.
            availability[column] = has_value & (as_bool(df[flag]) | numeric[column].gt(0))
        else:
            availability[column] = has_value
    known = availability.any(axis=1)
    audience = numeric.where(availability).max(axis=1, skipna=True).where(known)
    # A subreddit member count describes a community, not the audience of the
    # individual Reddit author. Keep Reddit audience missing until a genuine
    # author-level, pre-outcome metric (for example timestamped author karma)
    # is collected. YouTube subscribers and X followers remain eligible.
    if "source" in df.columns:
        reddit = df["source"].astype("string").str.strip().str.lower().eq("reddit")
        known = known & ~reddit
        audience = audience.mask(reddit)
    df["chan_log_audience"] = np.log1p(audience)
    df["chan_has_audience"] = known.astype(int)
    df["chan_audience_available"] = known.astype(int)
    df["chan_audience_is_zero"] = (known & audience.eq(0)).astype(int)
    have = int(known.sum())
    known_zero = int((known & audience.eq(0)).sum())
    print(
        f"[channel] audience from {present}: {have}/{len(df)} rows are known "
        f"({known_zero} observed zeros)"
    )
    return df


def add_viral_label(df: pd.DataFrame, quantile: float = VIRAL_QUANTILE) -> pd.DataFrame:
    df = df.copy()
    df["engagement_score"] = np.nan
    df["engagement_observed_metrics"] = 0
    df["engagement_coverage"] = 0.0
    df["viral"] = pd.Series(pd.NA, index=df.index, dtype="Int64")

    for source, metrics in ENGAGEMENT_METRICS.items():
        mask = df["source"] == source
        if not mask.any():
            continue
        source_rows = df.loc[mask]
        raw = pd.DataFrame(index=source_rows.index)
        for metric in metrics:
            raw[metric] = (
                pd.to_numeric(source_rows[metric], errors="coerce")
                if metric in source_rows.columns
                else np.nan
            )
        raw = raw.clip(lower=0)
        observed = raw.notna().sum(axis=1)
        coverage = observed / float(len(metrics))
        valid = observed > 0

        df.loc[mask, "engagement_observed_metrics"] = observed
        df.loc[mask, "engagement_coverage"] = coverage
        if not valid.any():
            continue

        logged = np.log1p(raw)
        std = logged.loc[valid].std(ddof=0).replace(0, 1.0).fillna(1.0)
        standardized = (logged - logged.loc[valid].mean()) / std
        score = standardized.sum(axis=1, min_count=1) / np.sqrt(observed.clip(lower=1))
        threshold = score.loc[valid].quantile(quantile)

        df.loc[score.loc[valid].index, "engagement_score"] = score.loc[valid]
        df.loc[score.loc[valid].index, "viral"] = (score.loc[valid] >= threshold).astype(int)

    return df


def build(
    input_path: Path,
    output_path: Path,
    quantile: float = VIRAL_QUANTILE,
    *,
    expected_dataset_version: str | None = None,
    seed: int = DEFAULT_RANDOM_SEED,
) -> pd.DataFrame:
    df = load_events(input_path)
    df = filter_rows(df)
    df = add_text_features(df)
    df = add_role_features(df)
    df = add_topic_features(df, seed=seed)
    df = add_channel_features(df)
    if "label_value" in df.columns:
        versions = sorted(df["dataset_version"].dropna().astype(str).unique())
        if len(versions) != 1:
            raise ValueError("Official lakehouse input must contain exactly one dataset_version")
        if expected_dataset_version and versions[0] != expected_dataset_version:
            raise ValueError(f"Expected dataset {expected_dataset_version}, received {versions[0]}")
        labels = df["label_value"].map({"viral": 1, "not_viral": 0})
        if labels.isna().any():
            invalid = sorted(df.loc[labels.isna(), "label_value"].astype(str).unique())
            raise ValueError(f"Unsupported official label values: {invalid}")
        df["viral"] = labels.astype("Int64")
    else:
        print(
            "WARNING: manual compatibility input uses "
            "legacy_dataset_relative_top_quartile; it is not an official label contract."
        )
        df = add_viral_label(df, quantile)
        df["virality_policy"] = "legacy_dataset_relative_top_quartile"
        df["virality_contract_fingerprint"] = pd.NA
    unlabeled = int(df["viral"].isna().sum())
    if unlabeled:
        print(
            f"[label] dropping {unlabeled} rows without observed engagement; "
            "missing counters remain unknown"
        )
        df = df[df["viral"].notna()].copy()
    df["viral"] = df["viral"].astype(int)
    df = pd.concat([df, pd.get_dummies(df["source"], prefix="src")], axis=1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return df


def _report(df: pd.DataFrame) -> None:
    print(f"Rows: {len(df)}")
    balance = df.groupby("source")["viral"].agg(["mean", "sum", "count"])
    balance.columns = ["viral_rate", "viral_n", "total"]
    print(balance.round(3))
    print(f"Overall viral rate: {df['viral'].mean():.3f}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build model features from an official lakehouse Parquet dataset or manual export."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--quantile", type=float, default=VIRAL_QUANTILE)
    parser.add_argument("--dataset-version")
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    args = parser.parse_args()

    df = build(
        args.input,
        args.output,
        args.quantile,
        expected_dataset_version=args.dataset_version,
        seed=args.seed,
    )
    _report(df)
    print(f"Saved -> {args.output}")


if __name__ == "__main__":
    main()
