"""Train the Stage-2 model: does early engagement say the post is taking off?

Stage 1 answers "will this post do well?" from the text, before publishing. Stage 2 runs
after publishing and reads the shape of the first hours of engagement. The two are fused
by feeding the Stage-1 probability in as one more feature, so Stage 2 *corrects* the prior
rather than replacing it -- a post with a weak text but a steep early curve should end up
above a post with a strong text and a flat one.

Why gradient boosting and not the LSTM/GNN the project brief names: a trajectory here is
3-4 observations, and `build_stage2_dataset.py` has already reduced it to velocity,
acceleration and ratios. A recurrent model needs long sequences and far more posts than we
will have for months; on tabular summaries of this size boosted trees are the stronger and
far cheaper choice. Revisit once posts routinely carry dozens of observations.

Joining the Stage-1 dataset does double duty: it supplies the fusion feature *and* the
`author_hash` that the snapshot table lacks, without which train and test could share an
author. Training without the join is supported for a quick look, and says so loudly.

Read the baselines before believing the headline number. Engagement counters are cumulative,
so whatever a post has already accumulated by the horizon is a lower bound on what it will
show at the label horizon -- "big now" predicts "big later" nearly for free, and on synthetic
trajectories the 6h view count alone scored ROC-AUC 0.89. The interesting question is whether
the *shape* of the curve adds anything on top of that level, which is exactly the gap this
script prints.

    python ml/train/train_stage2.py --data ml/data/stage2_dataset.parquet \
        --stage1-data ml/data/train_dataset.parquet \
        --stage1-model ml/models/stage1_multisource.joblib
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
from sklearn.model_selection import GroupShuffleSplit

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from train.train_viral import apply_calibrator, fit_calibrator  # noqa: E402

DEFAULT_DATA = ML_ROOT / "data" / "stage2_dataset.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage2_engagement.joblib"
TARGET = "viral"
STAGE1_FEATURE = "stage1_score"
LEVEL_BASELINE = "seq_log_view_count"  # "it is already big", the line Stage 2 must clear


def feature_columns(df: pd.DataFrame) -> list[str]:
    sequence = sorted(c for c in df.columns if c.startswith("seq_"))
    return sequence + ([STAGE1_FEATURE] if STAGE1_FEATURE in df.columns else [])


def attach_stage1(df: pd.DataFrame, stage1_data: Path, stage1_model: Path) -> pd.DataFrame:
    """Join the Stage-1 score and author identity onto the sequences, keyed by URL."""
    bundle = joblib.load(stage1_model)
    model, features = bundle["model"], bundle["features"]
    events = pd.read_parquet(stage1_data)
    if "url" not in events.columns:
        raise ValueError("The Stage-1 dataset has no url column to join on")

    X = events.reindex(columns=[c for c in features if c != "content_score"], fill_value=0.0)
    X = X.astype(float)
    if "content_score" in features:
        X["content_score"] = bundle["content_model"].predict_proba(
            events["clean_text"].astype(str)
        )[:, 1]
    proba = model.predict_proba(X.reindex(columns=features, fill_value=0.0))[:, 1]
    if bundle.get("calibrator") is not None:
        proba = apply_calibrator(bundle["calibrator"], proba)

    side = pd.DataFrame({"url": events["url"], STAGE1_FEATURE: proba})
    if "author_hash" in events.columns:
        side["author_hash"] = events["author_hash"]
    side = side.drop_duplicates(subset=["url"])

    merged = df.merge(side, on="url", how="left")
    matched = int(merged[STAGE1_FEATURE].notna().sum())
    print(f"[fusion] Stage-1 score joined for {matched}/{len(merged)} posts")
    if not matched:
        raise SystemExit(
            "No sequence matched a Stage-1 row by URL; the two datasets describe "
            "different posts, so the fusion feature would be entirely missing."
        )
    return merged


def split_indices(df: pd.DataFrame, test_size: float, seed: int):
    """Group on the author when we have one; fall back to one group per post."""
    if "author_hash" in df.columns and df["author_hash"].notna().any():
        groups = df["author_hash"].fillna(pd.Series(df.index.astype(str), index=df.index))
    else:
        print(
            "[split] no author_hash available -- splitting per post. The snapshot table "
            "carries no author identity, so pass --stage1-data to group properly."
        )
        groups = pd.Series(df.index.astype(str), index=df.index)
    splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=seed)
    return next(splitter.split(df, df[TARGET], groups))


def train_model(X_train: pd.DataFrame, y_train: pd.Series, seed: int) -> xgb.XGBClassifier:
    pos = int(y_train.sum())
    neg = int(len(y_train) - pos)
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=3,
        learning_rate=0.03,
        subsample=0.9,
        colsample_bytree=0.9,
        min_child_weight=2,
        reg_lambda=1.0,
        scale_pos_weight=(neg / pos) if pos else 1.0,
        objective="binary:logistic",
        eval_metric="aucpr",
        random_state=seed,
    )
    model.fit(X_train, y_train)
    return model


def report(y_true, score, threshold: float, label: str) -> None:
    """Rank quality always; Brier and the catch rate only when `score` is a probability.

    The level baseline is a raw log-count, so it ranks posts but cannot be read as a
    probability or cut at the serving threshold.
    """
    if len(np.unique(y_true)) < 2:
        print(f"{label:9} n={len(y_true):4}  single class in this slice")
        return
    line = (
        f"{label:9} n={len(y_true):4}  PR-AUC={average_precision_score(y_true, score):.3f}  "
        f"ROC-AUC={roc_auc_score(y_true, score):.3f}"
    )
    if float(np.nanmin(score)) >= 0.0 and float(np.nanmax(score)) <= 1.0:
        hits = (score >= threshold).astype(int)
        tp = int(((hits == 1) & (y_true == 1)).sum())
        line += (
            f"  Brier={brier_score_loss(y_true, score):.3f}  caught={tp}/{int(y_true.sum())}"
        )
    else:
        line += "  (ranking score, not a probability)"
    print(line)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the Stage-2 engagement model.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--stage1-data", type=Path, help="Stage-1 dataset, for fusion + grouping.")
    parser.add_argument("--stage1-model", type=Path, help="Stage-1 bundle used to score it.")
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if bool(args.stage1_data) != bool(args.stage1_model):
        raise SystemExit("--stage1-data and --stage1-model must be given together")

    df = pd.read_parquet(args.data)
    if args.stage1_data:
        df = attach_stage1(df, args.stage1_data, args.stage1_model)
    else:
        print("[fusion] no Stage-1 model given -- training on the sequence features alone.")

    features = feature_columns(df)
    X = df[features].astype(float)
    y = df[TARGET].astype(int)
    train_idx, test_idx = split_indices(df, args.test_size, args.seed)
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
    print(f"Train: {len(X_train)} | Test: {len(X_test)} | Features: {len(features)}")

    model = train_model(X_train, y_train, args.seed)
    groups = (
        df["author_hash"].fillna(pd.Series(df.index.astype(str), index=df.index)).iloc[train_idx]
        if "author_hash" in df.columns
        else pd.Series(df.index.astype(str), index=df.index).iloc[train_idx]
    )
    calibrator, threshold = fit_calibrator(X_train, y_train, groups, args.seed)
    proba = apply_calibrator(calibrator, model.predict_proba(X_test)[:, 1])

    print(f"\nDecision threshold (picked out-of-fold): {threshold:.2f}")
    report(y_test.to_numpy(), proba, threshold, "overall")
    sources = df["source"].iloc[test_idx].to_numpy()
    for source in sorted(pd.unique(sources)):
        mask = sources == source
        report(y_test.to_numpy()[mask], proba[mask], threshold, source)

    # Baselines. Engagement counters are cumulative, so the level already reached at the
    # horizon is a lower bound on the level at the label horizon: "big now" predicts "big
    # later" almost for free. A Stage-2 model that does not clear this line has learnt
    # nothing about the shape of the curve, whatever its absolute ROC-AUC looks like.
    print("\nBaselines on the same rows -- Stage 2 has to beat these to mean anything:")
    if LEVEL_BASELINE in df.columns:
        level = df[LEVEL_BASELINE].iloc[test_idx].to_numpy()
        report(y_test.to_numpy(), level, threshold, "level")
    if STAGE1_FEATURE in features:
        stage1 = df[STAGE1_FEATURE].iloc[test_idx].to_numpy()
        keep = ~np.isnan(stage1)
        if keep.any() and len(np.unique(y_test.to_numpy()[keep])) > 1:
            report(y_test.to_numpy()[keep], stage1[keep], threshold, "stage1")

    importance = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
    print("\nTop features:")
    print(importance.head(8).round(4).to_string())

    args.model.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {"model": model, "calibrator": calibrator, "threshold": threshold, "features": features},
        args.model,
    )
    print(f"\nSaved -> {args.model}")


if __name__ == "__main__":
    main()
