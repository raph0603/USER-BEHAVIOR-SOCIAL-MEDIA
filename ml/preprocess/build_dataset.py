"""Turn raw exported events into a training-ready dataset.

Pipeline: load -> clean text -> drop empty/duplicate -> static content features
-> per-source viral label -> one-hot source -> save parquet.

Design choices:
- Content features are UNIFIED across sources (pure functions of the text).
- The viral label is computed PER SOURCE, because engagement scales differ
  (YouTube views vs X retweets vs Reddit upvotes are not comparable). Within a
  source we z-score log1p(engagement metrics) so each metric contributes evenly,
  then label the top `VIRAL_QUANTILE` as viral.
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

DEFAULT_INPUT = ML_ROOT.parent / "data" / "samples" / "filtered_events.csv"
DEFAULT_OUTPUT = ML_ROOT / "data" / "train_dataset.parquet"

MIN_TEXT_CHARS = 3
VIRAL_QUANTILE = 0.75  # top 25% per source = viral; raise toward 0.90 as data grows

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
CHANNEL_AUDIENCE_COLS = ["subscriber_count", "follower_count", "subreddit_member_count"]

_URL = re.compile(r"https?://\S+|www\.\S+", re.I)
_SPACED_SCHEME = re.compile(r"(https?://)\s+", re.I)  # crawler sometimes inserts a space after //
_REDACTION = re.compile(r"<[A-Z_]+>")                 # <PHONE>, <EMAIL>, ...
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
    df = pd.read_csv(path)
    df = df.drop(columns=[c for c in _STALE_DERIVED if c in df.columns])
    df["source"] = df["source"].astype("string").str.strip().str.lower()
    df["clean_text"] = df["text"].map(clean_text)
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


def add_channel_features(df: pd.DataFrame) -> pd.DataFrame:
    """Unified channel/author audience-size features (best-effort).

    Coalesces the per-platform audience columns into one number, then exposes:
    - chan_log_audience: log1p of the audience size (0 when unknown)
    - chan_has_audience: 1 if the audience size is known (>0), else 0
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
    # Columns are mutually exclusive per platform; max() picks whichever applies.
    audience = (
        df[present].apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0).max(axis=1)
    )
    df["chan_log_audience"] = np.log1p(audience)
    df["chan_has_audience"] = (audience > 0).astype(int)
    have = int((audience > 0).sum())
    print(f"[channel] audience from {present}: {have}/{len(df)} rows have a known (>0) audience")
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
        df.loc[score.loc[valid].index, "viral"] = (
            score.loc[valid] >= threshold
        ).astype(int)

    return df


def build(input_path: Path, output_path: Path, quantile: float = VIRAL_QUANTILE) -> pd.DataFrame:
    df = load_events(input_path)
    df = filter_rows(df)
    df = add_text_features(df)
    df = add_role_features(df)
    df = add_topic_features(df)
    df = add_channel_features(df)
    df = add_viral_label(df, quantile)
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
    parser = argparse.ArgumentParser(description="Build the training dataset from exported events.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--quantile", type=float, default=VIRAL_QUANTILE)
    args = parser.parse_args()

    df = build(args.input, args.output, args.quantile)
    _report(df)
    print(f"Saved -> {args.output}")


if __name__ == "__main__":
    main()
