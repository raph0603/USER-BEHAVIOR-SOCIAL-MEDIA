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

from train.train_multisource_cv import (
    author_groups,
    balance_sources_for_cv,
    metric_summary,
    source_balance_weights,
    source_weight_totals,
    stratified_source_folds,
)


def multisource_frame() -> pd.DataFrame:
    rows = []
    for source in ("youtube", "reddit", "x"):
        for index in range(40):
            rows.append(
                {
                    "source": source,
                    "viral": 1 if index % 4 == 0 else 0,
                    "author_hash": f"{source}-author-{index}",
                    "clean_text": f"{source} text {index}",
                }
            )
    return pd.DataFrame(rows)


def test_source_weights_give_every_source_equal_total_weight() -> None:
    sources = pd.Series(["youtube"] * 8 + ["reddit"] * 4 + ["x"] * 2)

    weights = source_balance_weights(sources)
    totals = source_weight_totals(sources, weights)

    assert totals["youtube"] == pytest.approx(totals["reddit"])
    assert totals["reddit"] == pytest.approx(totals["x"])
    assert weights.iloc[-1] > weights.iloc[0]


def test_source_balancing_uses_equal_counts_and_preserves_viral_rates() -> None:
    frame = pd.concat(
        [
            multisource_frame(),
            pd.DataFrame(
                [
                    {
                        "source": "youtube",
                        "viral": 1 if index % 4 == 0 else 0,
                        "author_hash": f"extra-youtube-author-{index}",
                        "clean_text": f"extra youtube text {index}",
                    }
                    for index in range(40)
                ]
            ),
        ],
        ignore_index=True,
    )

    balanced = balance_sources_for_cv(frame, seed=13)

    assert balanced["source"].value_counts().to_dict() == {
        "youtube": 40,
        "reddit": 40,
        "x": 40,
    }
    assert balanced.groupby("source")["viral"].mean().to_dict() == {
        "reddit": pytest.approx(0.25),
        "x": pytest.approx(0.25),
        "youtube": pytest.approx(0.25),
    }


def test_folds_preserve_sources_classes_and_author_isolation() -> None:
    frame = multisource_frame()
    groups = author_groups(frame)
    folds = stratified_source_folds(frame, n_splits=5, seed=13)
    validation_rows = []

    for train_idx, validation_idx in folds:
        validation = frame.iloc[validation_idx]
        validation_rows.extend(validation_idx.tolist())
        assert set(groups.iloc[train_idx]).isdisjoint(groups.iloc[validation_idx])
        assert set(validation["source"]) == {"youtube", "reddit", "x"}
        for _, source_rows in validation.groupby("source"):
            assert source_rows["viral"].nunique() == 2

    assert sorted(validation_rows) == list(range(len(frame)))


def test_metric_summary_supports_fold_specific_thresholds() -> None:
    labels = pd.Series([0, 0, 1, 1])
    probabilities = np.array([0.1, 0.4, 0.45, 0.9])
    thresholds = np.array([0.3, 0.5, 0.4, 0.8])

    metrics = metric_summary(labels, probabilities, thresholds)

    assert metrics["true_positive"] == 2
    assert metrics["false_positive"] == 0
    assert metrics["threshold"] == pytest.approx(0.5)
