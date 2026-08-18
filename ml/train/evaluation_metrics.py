"""Shared metrics and author-level bootstrap utilities for Stage-1 evaluation."""

from __future__ import annotations

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

ECE_N_BINS = 10
ECE_BINNING = "equal_width"
BOOTSTRAP_UNIT = "author_hash"
BOOTSTRAP_ITERATIONS = 1_000
BOOTSTRAP_CONFIDENCE_LEVEL = 0.95


def metric_summary(
    y_true: pd.Series,
    probabilities: np.ndarray,
    thresholds: float | np.ndarray,
) -> dict[str, float | int]:
    """Compute ranking, calibration, and thresholded metrics."""

    labels = pd.Series(y_true).astype(int).reset_index(drop=True)
    probabilities = np.asarray(probabilities, dtype=float)
    threshold_values = np.asarray(thresholds, dtype=float)
    predicted = (probabilities >= threshold_values).astype(int)
    tn, fp, fn, tp = confusion_matrix(labels, predicted, labels=[0, 1]).ravel()

    metrics: dict[str, float | int | None] = {
        "n": int(len(labels)),
        "viral_rate": float(labels.mean()),
        "pr_auc": (
            float(average_precision_score(labels, probabilities))
            if labels.nunique() > 1
            else None
        ),
        "roc_auc": (
            float(roc_auc_score(labels, probabilities)) if labels.nunique() > 1 else None
        ),
        "brier": float(brier_score_loss(labels, probabilities)),
        "accuracy": float(accuracy_score(labels, predicted)),
        "precision": float(precision_score(labels, predicted, zero_division=0)),
        "recall": float(recall_score(labels, predicted, zero_division=0)),
        "f1": float(f1_score(labels, predicted, zero_division=0)),
        "threshold": float(threshold_values.mean()),
        "true_positive": int(tp),
        "false_positive": int(fp),
        "false_negative": int(fn),
        "true_negative": int(tn),
        "ece": float(expected_calibration_error(labels, probabilities)),
    }
    return {key: value for key, value in metrics.items() if value is not None}


def expected_calibration_error(
    y_true,
    proba,
    n_bins: int = ECE_N_BINS,
) -> float:
    """Return equal-width Expected Calibration Error on [0, 1]."""

    if n_bins < 1:
        raise ValueError("ECE requires at least one bin")

    y_true = np.asarray(y_true, dtype=float)
    proba = np.asarray(proba, dtype=float)
    if len(y_true) != len(proba):
        raise ValueError("ECE labels and probabilities must have the same length")
    if len(y_true) == 0:
        raise ValueError("ECE requires at least one observation")

    edges = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.clip(np.digitize(proba, edges[1:-1]), 0, n_bins - 1)
    ece = 0.0
    total = len(y_true)
    for bin_index in range(n_bins):
        mask = idx == bin_index
        if mask.any():
            accuracy = float(y_true[mask].mean())
            confidence = float(proba[mask].mean())
            ece += (mask.sum() / total) * abs(accuracy - confidence)
    return float(ece)


def author_bootstrap_metrics(
    y_true,
    proba,
    group_series,
    n_iterations: int = BOOTSTRAP_ITERATIONS,
    seed: int = 42,
) -> dict[str, object]:
    """Compute percentile 95% CIs by resampling authors with replacement."""

    if n_iterations < 1:
        raise ValueError("Author bootstrap requires at least one iteration")

    labels = pd.Series(y_true).astype(int).reset_index(drop=True)
    probabilities = np.asarray(proba, dtype=float)
    groups = pd.Series(group_series).reset_index(drop=True)
    if not (len(labels) == len(probabilities) == len(groups)):
        raise ValueError("Bootstrap labels, probabilities, and groups must align")
    if groups.isna().any():
        raise ValueError("Author bootstrap requires a group for every observation")

    rng = np.random.default_rng(seed)
    unique_groups = groups.unique()
    if len(unique_groups) == 0:
        raise ValueError("Author bootstrap requires at least one author")

    bootstrapped = {"roc_auc": [], "pr_auc": [], "brier": [], "ece": []}
    group_to_idx = groups.groupby(groups).groups

    for _ in range(n_iterations):
        sampled_groups = rng.choice(unique_groups, size=len(unique_groups), replace=True)
        sampled_idx = np.concatenate([np.asarray(group_to_idx[group]) for group in sampled_groups])

        y_boot = labels.iloc[sampled_idx]
        proba_boot = probabilities[sampled_idx]

        if y_boot.nunique() > 1:
            bootstrapped["roc_auc"].append(float(roc_auc_score(y_boot, proba_boot)))
            bootstrapped["pr_auc"].append(float(average_precision_score(y_boot, proba_boot)))
        bootstrapped["brier"].append(float(brier_score_loss(y_boot, proba_boot)))
        bootstrapped["ece"].append(float(expected_calibration_error(y_boot, proba_boot)))

    def ci(values: list[float]) -> list[float] | None:
        if not values:
            return None
        alpha = (1.0 - BOOTSTRAP_CONFIDENCE_LEVEL) / 2.0
        return [
            float(np.percentile(values, 100.0 * alpha)),
            float(np.percentile(values, 100.0 * (1.0 - alpha))),
        ]

    valid_classification = len(bootstrapped["roc_auc"])
    return {
        "bootstrap_unit": BOOTSTRAP_UNIT,
        "bootstrap_seed": int(seed),
        "requested_repetitions": int(n_iterations),
        "valid_classification_repetitions": int(valid_classification),
        "invalid_classification_repetitions": int(n_iterations - valid_classification),
        "valid_calibration_repetitions": int(len(bootstrapped["brier"])),
        "confidence_level": BOOTSTRAP_CONFIDENCE_LEVEL,
        "roc_auc_ci95": ci(bootstrapped["roc_auc"]),
        "pr_auc_ci95": ci(bootstrapped["pr_auc"]),
        "brier_ci95": ci(bootstrapped["brier"]),
        "ece_ci95": ci(bootstrapped["ece"]),
    }
