"""Turn engagement snapshots into a Stage-2 training set (post-launch prediction).

Stage 1 asks "will this post do well?" from the text alone, before publishing. Stage 2
asks "is it actually taking off?" from how engagement grows in the first hours after
publishing, and its answer refines the Stage-1 prior.

Input is ``lakehouse.silver.engagement_snapshots`` — an append-only table with one row per
observation of a post, carrying cumulative counters plus the per-observation velocity the
Spark job already derives (``views_per_hour``, ``views_acceleration``, …).

    post A:  age=0.5h views=120   age=2h views=900   age=6h views=4100  age=24h views=9000
             └────────── early window (features) ──────────┘           └── label horizon ──┘

The split in time is the whole point and the only real leakage risk: **features come only
from observations at or before ``--horizon-hours``, the label only from an observation at
or after ``--label-hours``**. A post is dropped when it has no observation on either side,
because a guessed label is worse than a missing row — the same rule Stage 1 applies to
posts with no observed engagement.

The label reuses ``build_dataset.add_viral_label`` verbatim, so "viral" means the same
thing in both stages and the two scores can be fused later.

    python ml/preprocess/build_stage2_dataset.py --input <snapshots.parquet>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from preprocess.build_dataset import add_viral_label

DEFAULT_OUTPUT = ML_ROOT / "data" / "stage2_dataset.parquet"

HORIZON_HOURS = 6.0  # observations after this are invisible to the features
LABEL_HOURS = 24.0  # the outcome we are trying to predict
MIN_OBSERVATIONS = 2  # one point has no velocity, so it cannot describe a trajectory

POST_KEYS = ["source", "platform_event_id", "url"]
COUNTERS = ["view_count", "like_count", "comment_count", "score", "reply_count",
            "retweet_count", "bookmark_count"]
RATES = ["views_per_hour", "likes_per_hour", "comments_per_hour"]
RATIOS = ["like_rate", "comment_rate", "engagement_rate"]


def post_id(df: pd.DataFrame) -> pd.Series:
    """Identify a post by its platform id, falling back to the URL when it is absent."""
    platform = df["platform_event_id"].astype("string") if "platform_event_id" in df else pd.NA
    url = df["url"].astype("string")
    key = url if platform is pd.NA else platform.fillna(url)
    return df["source"].astype("string").str.lower() + "|" + key


def prepare_snapshots(df: pd.DataFrame) -> pd.DataFrame:
    """Attach the post identity and age in hours; drop observations with no usable age."""
    missing = {"source", "url", "age_minutes"} - set(df.columns)
    if missing:
        raise ValueError(f"Snapshots are missing required columns: {sorted(missing)}")
    df = df.copy()
    df["post_id"] = post_id(df)
    df["age_hours"] = pd.to_numeric(df["age_minutes"], errors="coerce") / 60.0
    return df[df["age_hours"].notna() & (df["age_hours"] >= 0)]


def load_snapshots(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path) if path.is_dir() or path.suffix == ".parquet" else pd.read_csv(path)
    return prepare_snapshots(df)


def _agg(window: pd.DataFrame) -> dict:
    """Summarise one post's early trajectory. Only columns the export carries are used."""
    last = window.iloc[-1]
    feats = {
        "seq_n_observations": len(window),
        "seq_first_age_hours": float(window["age_hours"].iloc[0]),
        "seq_last_age_hours": float(last["age_hours"]),
        "seq_span_hours": float(last["age_hours"] - window["age_hours"].iloc[0]),
    }
    for column in COUNTERS:
        if column in window:
            value = pd.to_numeric(last[column], errors="coerce")
            feats[f"seq_log_{column}"] = float(np.log1p(value)) if pd.notna(value) else np.nan
    for column in RATES:
        if column in window:
            series = pd.to_numeric(window[column], errors="coerce")
            feats[f"seq_{column}_last"] = float(series.iloc[-1]) if pd.notna(series.iloc[-1]) else np.nan
            feats[f"seq_{column}_mean"] = float(series.mean()) if series.notna().any() else np.nan
            feats[f"seq_{column}_max"] = float(series.max()) if series.notna().any() else np.nan
    if "views_acceleration" in window:
        accel = pd.to_numeric(window["views_acceleration"], errors="coerce")
        feats["seq_views_acceleration_last"] = float(accel.iloc[-1]) if pd.notna(accel.iloc[-1]) else np.nan
        feats["seq_views_acceleration_mean"] = float(accel.mean()) if accel.notna().any() else np.nan
    for column in RATIOS:
        if column in window:
            value = pd.to_numeric(last[column], errors="coerce")
            feats[f"seq_{column}"] = float(value) if pd.notna(value) else np.nan
    return feats


def build_sequences(
    df: pd.DataFrame,
    horizon_hours: float = HORIZON_HOURS,
    label_hours: float = LABEL_HOURS,
    min_observations: int = MIN_OBSERVATIONS,
) -> pd.DataFrame:
    """One row per post: early-window features + the counters seen at the label horizon."""
    if horizon_hours >= label_hours:
        raise ValueError("horizon_hours must be strictly before label_hours")

    rows = []
    dropped = {"no_window": 0, "too_few": 0, "no_outcome": 0}
    for _, post in df.sort_values("age_hours").groupby("post_id", sort=False):
        window = post[post["age_hours"] <= horizon_hours]
        outcome = post[post["age_hours"] >= label_hours]
        if window.empty:
            dropped["no_window"] += 1
            continue
        if len(window) < min_observations:
            dropped["too_few"] += 1
            continue
        if outcome.empty:
            dropped["no_outcome"] += 1
            continue
        # `url` travels with the row so the trainer can join back to the Stage-1 dataset,
        # which is the only place carrying the author identity and the post text.
        row = {
            "post_id": post["post_id"].iloc[0],
            "source": post["source"].iloc[0],
            "url": post["url"].iloc[0],
        }
        row.update(_agg(window))
        # Counters at the label horizon feed add_viral_label; they never become features.
        final = outcome.iloc[0]
        for column in COUNTERS:
            if column in post:
                row[column] = pd.to_numeric(final[column], errors="coerce")
        rows.append(row)

    print(
        f"[stage2] posts kept: {len(rows)} | dropped: "
        f"{dropped['no_window']} with no early observation, "
        f"{dropped['too_few']} with fewer than {min_observations} in the window, "
        f"{dropped['no_outcome']} with no observation at or after {label_hours:g}h"
    )
    return pd.DataFrame(rows)


def feature_columns(df: pd.DataFrame) -> list[str]:
    return sorted(c for c in df.columns if c.startswith("seq_"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Stage-2 dataset from engagement snapshots.")
    parser.add_argument("--input", type=Path, required=True, help="engagement_snapshots export")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--horizon-hours", type=float, default=HORIZON_HOURS)
    parser.add_argument("--label-hours", type=float, default=LABEL_HOURS)
    parser.add_argument("--min-observations", type=int, default=MIN_OBSERVATIONS)
    parser.add_argument("--quantile", type=float, default=0.75)
    args = parser.parse_args()

    snapshots = load_snapshots(args.input)
    print(f"Observations: {len(snapshots)} | posts: {snapshots['post_id'].nunique()}")
    sequences = build_sequences(
        snapshots, args.horizon_hours, args.label_hours, args.min_observations
    )
    if sequences.empty:
        raise SystemExit(
            "No post has both an early observation and one at the label horizon. "
            "The snapshot table needs to accumulate more readings per post first."
        )
    labelled = add_viral_label(sequences, args.quantile)
    print(f"Rows: {len(labelled)} | features: {len(feature_columns(labelled))}")
    print(labelled.groupby("source")["viral"].agg(["mean", "sum", "size"]).round(3))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    labelled.to_parquet(args.output, index=False)
    print(f"Saved -> {args.output}")


if __name__ == "__main__":
    main()
