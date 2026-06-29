"""Train the unified multi-source viral classifier (Stage 1).

One XGBoost model on the unified content features + source one-hot. The viral
label was already defined per-source in preprocess/build_dataset.py.

Split is grouped by author_hash so the same author never appears in both train
and test (avoids identity leakage). Metrics: PR-AUC (primary, data is imbalanced)
+ ROC-AUC. Global SHAP importance (via XGBoost pred_contribs) shows which
features drive virality — the basis for the explanation layer.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import sys

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score, classification_report, roc_auc_score
from sklearn.model_selection import GroupShuffleSplit, StratifiedKFold, cross_val_predict

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from features.text_content import build_content_model

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
TEXT = "clean_text"

CONTENT_FEATURES = [
    "char_count",
    "word_count",
    "has_question",
    "is_vietnamese",
    "f_word",
    "f_sent",
    "f_clause",
    "f_info",
    "f_visual",
    "cognitive_friction_score",
]
TARGET = "viral"
GROUP = "author_hash"


def feature_columns(df: pd.DataFrame) -> list[str]:
    extra = sorted(c for c in df.columns if c.startswith(("src_", "role_")))
    return CONTENT_FEATURES + extra


def split_indices(df: pd.DataFrame, test_size: float, seed: int):
    groups = df[GROUP].fillna(df.index.to_series().astype(str))
    splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=seed)
    return next(splitter.split(df, df[TARGET], groups))


def content_scores(text: pd.Series, y: pd.Series, train_idx, test_idx, seed: int):
    """Out-of-fold P(viral) for train rows; full-train model applied to test (no leakage)."""
    model = build_content_model()
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=seed)
    train_score = cross_val_predict(
        model, text.iloc[train_idx], y.iloc[train_idx], cv=cv, method="predict_proba"
    )[:, 1]
    model.fit(text.iloc[train_idx], y.iloc[train_idx])
    test_score = model.predict_proba(text.iloc[test_idx])[:, 1]
    return train_score, test_score, model


def train_model(X_train: pd.DataFrame, y_train: pd.Series, seed: int) -> xgb.XGBClassifier:
    pos = int(y_train.sum())
    neg = int(len(y_train) - pos)
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
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


def evaluate(model: xgb.XGBClassifier, X_test: pd.DataFrame, y_test: pd.Series) -> None:
    proba = model.predict_proba(X_test)[:, 1]
    print(f"PR-AUC : {average_precision_score(y_test, proba):.3f}")
    print(f"ROC-AUC: {roc_auc_score(y_test, proba):.3f}")
    print(f"Baseline viral rate (test): {y_test.mean():.3f}")
    print(classification_report(y_test, (proba >= 0.5).astype(int), digits=3))


def shap_importance(model: xgb.XGBClassifier, X: pd.DataFrame) -> pd.Series:
    booster = model.get_booster()
    contribs = booster.predict(xgb.DMatrix(X), pred_contribs=True)  # last col = bias
    mean_abs = np.abs(contribs[:, :-1]).mean(axis=0)
    return pd.Series(mean_abs, index=X.columns).sort_values(ascending=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the unified viral classifier.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    df = pd.read_parquet(args.data)
    features = feature_columns(df)
    train_idx, test_idx = split_indices(df, args.test_size, args.seed)

    text = df[TEXT].astype(str)
    y = df[TARGET].astype(int)
    train_score, test_score, content_model = content_scores(text, y, train_idx, test_idx, args.seed)

    X = df[features].astype(float)
    features = features + ["content_score"]
    X_train = X.iloc[train_idx].assign(content_score=train_score)
    X_test = X.iloc[test_idx].assign(content_score=test_score)
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

    print(f"Train: {len(X_train)} | Test: {len(X_test)} | Features: {len(features)}")
    model = train_model(X_train, y_train, args.seed)
    evaluate(model, X_test, y_test)

    print("\nTop SHAP feature importance (mean |contribution|):")
    print(shap_importance(model, X_test).head(10).round(4))

    args.model.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "content_model": content_model, "features": features}, args.model)
    print(f"\nSaved -> {args.model}")


if __name__ == "__main__":
    main()
