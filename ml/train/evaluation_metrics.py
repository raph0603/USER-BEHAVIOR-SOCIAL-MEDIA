"""Shared metrics, bootstrap, and CV logic."""

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

def metric_summary(
    y_true: pd.Series,
    probabilities: np.ndarray,
    thresholds: float | np.ndarray,
) -> dict:
    threshold_values = np.asarray(thresholds, dtype=float)
    predicted = (probabilities >= threshold_values).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, predicted, labels=[0, 1]).ravel()
    n = int(len(y_true))
    metrics = {
        "n": n,
        "viral_rate": float(y_true.mean()),
        "pr_auc": float(average_precision_score(y_true, probabilities)) if y_true.nunique() > 1 else None,
        "roc_auc": float(roc_auc_score(y_true, probabilities)) if y_true.nunique() > 1 else None,
        "brier": float(brier_score_loss(y_true, probabilities)),
        "accuracy": float(accuracy_score(y_true, predicted)),
        "precision": float(precision_score(y_true, predicted, zero_division=0)),
        "recall": float(recall_score(y_true, predicted, zero_division=0)),
        "f1": float(f1_score(y_true, predicted, zero_division=0)),
        "threshold": float(threshold_values.mean()),
        "true_positive": int(tp),
        "false_positive": int(fp),
        "false_negative": int(fn),
        "true_negative": int(tn),
        "ece": float(expected_calibration_error(y_true, probabilities))
    }
    # remove None
    return {k: v for k, v in metrics.items() if v is not None}

def expected_calibration_error(y_true, proba, n_bins: int = 10) -> float:
    y_true = np.asarray(y_true, dtype=float)
    proba = np.asarray(proba, dtype=float)
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.clip(np.digitize(proba, edges[1:-1]), 0, n_bins - 1)
    ece = 0.0
    total = len(y_true)
    for b in range(n_bins):
        m = idx == b
        if m.any():
            acc = float(y_true[m].mean())
            conf = float(proba[m].mean())
            ece += (m.sum() / total) * abs(acc - conf)
    return ece

def author_bootstrap_metrics(y_true, proba, group_series, n_iterations=1000, seed=42):
    rng = np.random.default_rng(seed)
    unique_groups = group_series.unique()
    bootstrapped = {"roc_auc": [], "pr_auc": [], "brier": [], "ece": []}
    
    group_to_idx = group_series.groupby(group_series).groups
    
    for _ in range(n_iterations):
        sampled_groups = rng.choice(unique_groups, size=len(unique_groups), replace=True)
        sampled_idx = np.concatenate([group_to_idx[g] for g in sampled_groups])
        
        y_boot = y_true.iloc[sampled_idx]
        proba_boot = proba[sampled_idx]
        
        if y_boot.nunique() > 1:
            bootstrapped["roc_auc"].append(roc_auc_score(y_boot, proba_boot))
            bootstrapped["pr_auc"].append(average_precision_score(y_boot, proba_boot))
        bootstrapped["brier"].append(brier_score_loss(y_boot, proba_boot))
        bootstrapped["ece"].append(expected_calibration_error(y_boot, proba_boot))
        
    def ci(arr):
        if not arr: return None
        return [float(np.percentile(arr, 2.5)), float(np.percentile(arr, 97.5))]

    return {
        "roc_auc_ci95": ci(bootstrapped["roc_auc"]),
        "pr_auc_ci95": ci(bootstrapped["pr_auc"]),
        "brier_ci95": ci(bootstrapped["brier"]),
        "ece_ci95": ci(bootstrapped["ece"]),
        "bootstrap_repetitions": len(bootstrapped["brier"])
    }
