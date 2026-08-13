"""Paired ablation of exploratory rhetorical-role features.

Both variants use the same versioned dataset, author-grouped split, content scores,
training parameters, calibration procedure, and seed. The only difference is whether
the downstream XGBoost model receives ``role_*`` columns.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, brier_score_loss, f1_score, roc_auc_score

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from dataset_lineage import load_dataset_lineage
from role_contract import role_feature_contract
from train.train_viral import (
    GROUP,
    TARGET,
    TEXT,
    apply_calibrator,
    content_scores,
    fit_calibrator,
    split_indices,
    train_model,
    validate_dataset_version,
)

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
DEFAULT_OUTPUT = ML_ROOT / "results" / "stage1_role_ablation.json"


def _metrics(y_true: pd.Series, proba: np.ndarray, threshold: float) -> dict:
    return {
        "n": int(len(y_true)),
        "pr_auc": round(float(average_precision_score(y_true, proba)), 6),
        "roc_auc": round(float(roc_auc_score(y_true, proba)), 6),
        "brier": round(float(brier_score_loss(y_true, proba)), 6),
        "f1": round(
            float(f1_score(y_true, proba >= threshold, zero_division=0)),
            6,
        ),
        "decision_threshold": round(float(threshold), 6),
    }


def _fit_variant(
    df: pd.DataFrame,
    y: pd.Series,
    train_idx,
    test_idx,
    train_score: np.ndarray,
    test_score: np.ndarray,
    feature_names: list[str],
    seed: int,
) -> tuple[dict, np.ndarray]:
    X = df[feature_names].astype(float)
    X_train = X.iloc[train_idx].assign(content_score=train_score)
    X_test = X.iloc[test_idx].assign(content_score=test_score)
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
    groups = df[GROUP].fillna(df.index.to_series().astype(str)).iloc[train_idx]

    model = train_model(X_train, y_train, seed)
    calibrator, threshold = fit_calibrator(X_train, y_train, groups, seed)
    proba = apply_calibrator(calibrator, model.predict_proba(X_test)[:, 1])
    result = _metrics(y_test, proba, threshold)
    result["feature_count"] = int(X_train.shape[1])
    result["role_feature_count"] = sum(name.startswith("role_") for name in X_train.columns)
    return result, proba


def _paired_delta_ci(
    y_true: pd.Series,
    with_roles: np.ndarray,
    without_roles: np.ndarray,
    metric,
    *,
    n_boot: int,
    seed: int,
    require_two_classes: bool = True,
) -> list[float] | None:
    y = np.asarray(y_true)
    rng = np.random.default_rng(seed)
    deltas = []
    for _ in range(n_boot):
        idx = rng.integers(0, len(y), len(y))
        sampled_y = y[idx]
        if require_two_classes and len(np.unique(sampled_y)) < 2:
            continue
        deltas.append(metric(sampled_y, without_roles[idx]) - metric(sampled_y, with_roles[idx]))
    if not deltas:
        return None
    low, high = np.percentile(deltas, [2.5, 97.5])
    return [round(float(low), 6), round(float(high), 6)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Ablate exploratory role features.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--dataset-manifest", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-boot", type=int, default=2000)
    args = parser.parse_args()

    bundle = joblib.load(args.model)
    lineage = bundle.get("dataset_lineage")
    if not lineage:
        raise ValueError("Role ablation requires an official model with dataset lineage")
    if args.dataset_manifest:
        _, expected_lineage = load_dataset_lineage(args.dataset_manifest)
        if lineage != expected_lineage:
            raise ValueError("Model artifact lineage does not match --dataset-manifest")

    df = pd.read_parquet(args.data)
    validate_dataset_version(df, bundle.get("dataset_version"))
    y = df[TARGET].astype(int)
    train_idx, test_idx = split_indices(df, args.test_size, args.seed)
    train_score, test_score, _ = content_scores(
        df[TEXT].astype(str), y, train_idx, test_idx, args.seed
    )

    model_features = [name for name in bundle["features"] if name != "content_score"]
    role_features = [name for name in model_features if name.startswith("role_")]
    if not role_features:
        raise ValueError("Reference model contains no role_* features to ablate")
    without_role_features = [name for name in model_features if not name.startswith("role_")]

    with_metrics, with_proba = _fit_variant(
        df,
        y,
        train_idx,
        test_idx,
        train_score,
        test_score,
        model_features,
        args.seed,
    )
    without_metrics, without_proba = _fit_variant(
        df,
        y,
        train_idx,
        test_idx,
        train_score,
        test_score,
        without_role_features,
        args.seed,
    )
    y_test = y.iloc[test_idx]

    delta = {
        "definition": "without_roles_minus_with_roles",
        "pr_auc": round(without_metrics["pr_auc"] - with_metrics["pr_auc"], 6),
        "roc_auc": round(without_metrics["roc_auc"] - with_metrics["roc_auc"], 6),
        "brier": round(without_metrics["brier"] - with_metrics["brier"], 6),
        "pr_auc_ci95": _paired_delta_ci(
            y_test,
            with_proba,
            without_proba,
            average_precision_score,
            n_boot=args.n_boot,
            seed=args.seed,
        ),
        "roc_auc_ci95": _paired_delta_ci(
            y_test,
            with_proba,
            without_proba,
            roc_auc_score,
            n_boot=args.n_boot,
            seed=args.seed + 1,
        ),
        "brier_ci95": _paired_delta_ci(
            y_test,
            with_proba,
            without_proba,
            brier_score_loss,
            n_boot=args.n_boot,
            seed=args.seed + 2,
            require_two_classes=False,
        ),
    }
    result = {
        "dataset_lineage": lineage,
        "reference_model": {
            "file": args.model.name,
            "sha256": hashlib.sha256(args.model.read_bytes()).hexdigest(),
        },
        "role_feature_contract": role_feature_contract(),
        "methodology": {
            "split": "GroupShuffleSplit by author_hash",
            "test_size": args.test_size,
            "seed": args.seed,
            "paired_bootstrap_iterations": args.n_boot,
            "controlled_difference": "presence of role_* features only",
        },
        "with_roles": with_metrics,
        "without_roles": without_metrics,
        "delta": delta,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print("=== Exploratory role-feature ablation ===")
    print(f"with roles   : {with_metrics}")
    print(f"without roles: {without_metrics}")
    print(f"delta        : {delta}")
    print(f"Saved -> {args.output}")


if __name__ == "__main__":
    main()
