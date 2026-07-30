from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = ROOT / "ml"
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from train.train_reddit_audience_ablation import (
    ablation_feature_sets,
    author_groups,
    bootstrap_intervals,
    fold_balance,
    metric_summary,
    reddit_rows,
    stratified_group_folds,
)


def sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source": ["reddit", "reddit", "x"],
            "viral": [0, 1, 1],
            "author_hash": ["a", "b", "c"],
            "clean_text": ["one", "two", "three"],
            "char_count": [3, 3, 5],
            "word_count": [1, 1, 1],
            "has_question": [0, 0, 0],
            "is_vietnamese": [0, 0, 0],
            "f_word": [0.0, 0.0, 0.0],
            "f_sent": [0.0, 0.0, 0.0],
            "f_clause": [0.0, 0.0, 0.0],
            "f_info": [0.0, 0.0, 0.0],
            "f_visual": [0.0, 0.0, 0.0],
            "cognitive_friction_score": [0.0, 0.0, 0.0],
            "src_reddit": [1, 1, 0],
            "src_x": [0, 0, 1],
            "chan_log_audience": [10.0, 11.0, 5.0],
            "chan_has_audience": [1, 1, 1],
        }
    )


def test_reddit_rows_excludes_other_sources() -> None:
    result = reddit_rows(sample_frame())

    assert len(result) == 2
    assert set(result["source"]) == {"reddit"}


def test_ablation_changes_only_audience_features() -> None:
    reddit = reddit_rows(sample_frame())

    features = ablation_feature_sets(reddit)

    assert "chan_log_audience" in features["with_audience"]
    assert not any(name.startswith("chan_") for name in features["without_audience"])
    assert set(features["with_audience"]) - set(features["without_audience"]) == {
        "chan_log_audience",
        "chan_has_audience",
    }


def test_ablation_rejects_entirely_unknown_audience() -> None:
    reddit = reddit_rows(sample_frame())
    reddit["chan_log_audience"] = np.nan

    with pytest.raises(ValueError, match="unknown for every row"):
        ablation_feature_sets(reddit)


def test_ablation_rejects_one_injected_audience_value() -> None:
    reddit = reddit_rows(sample_frame())
    reddit["chan_log_audience"] = 10.0

    with pytest.raises(ValueError, match="fewer than two observed values"):
        ablation_feature_sets(reddit)


def test_metric_summary_uses_the_supplied_threshold() -> None:
    y = pd.Series([0, 0, 1, 1])
    proba = np.array([0.1, 0.4, 0.45, 0.9])

    metrics = metric_summary(y, proba, threshold=0.4)

    assert metrics["true_positive"] == 2
    assert metrics["false_positive"] == 1
    assert metrics["f1"] == pytest.approx(0.8)


def test_metric_summary_accepts_fold_specific_thresholds() -> None:
    y = pd.Series([0, 0, 1, 1])
    proba = np.array([0.1, 0.4, 0.45, 0.9])
    thresholds = np.array([0.3, 0.5, 0.4, 0.8])

    metrics = metric_summary(y, proba, threshold=thresholds)

    assert metrics["true_positive"] == 2
    assert metrics["false_positive"] == 0
    assert metrics["threshold"] == pytest.approx(0.5)


def test_stratified_group_folds_keep_authors_out_of_validation() -> None:
    rows = 40
    frame = pd.DataFrame(
        {
            "source": ["reddit"] * rows,
            "viral": [1 if index % 4 == 0 else 0 for index in range(rows)],
            "author_hash": [f"author-{index}" for index in range(rows)],
            "clean_text": [f"text {index}" for index in range(rows)],
        }
    )

    folds = stratified_group_folds(frame, n_splits=5, seed=11)
    groups = author_groups(frame)
    validation_rows = []

    for train_idx, validation_idx in folds:
        validation_rows.extend(validation_idx.tolist())
        assert set(groups.iloc[train_idx]).isdisjoint(groups.iloc[validation_idx])
        assert frame.iloc[train_idx]["viral"].nunique() == 2
        assert frame.iloc[validation_idx]["viral"].nunique() == 2

    assert sorted(validation_rows) == list(range(rows))


def test_fold_balance_weights_only_the_training_partition() -> None:
    summary = fold_balance(
        pd.Series([0, 0, 0, 1]),
        pd.Series([0, 0, 1]),
    )

    assert summary["scale_pos_weight"] == pytest.approx(3.0)
    assert summary["validation_viral_rate"] == pytest.approx(1 / 3)


def test_bootstrap_reports_paired_deltas() -> None:
    y = pd.Series([0, 0, 0, 1, 1, 1])
    probabilities = {
        "with_audience": np.array([0.05, 0.1, 0.2, 0.7, 0.8, 0.95]),
        "without_audience": np.array([0.4, 0.1, 0.3, 0.55, 0.6, 0.7]),
    }

    intervals, deltas = bootstrap_intervals(y, probabilities, n_boot=25, seed=7)

    assert set(intervals) == {"with_audience", "without_audience"}
    assert set(deltas) == {"roc_auc", "pr_auc", "brier"}
    assert deltas["brier"][1] < 0
