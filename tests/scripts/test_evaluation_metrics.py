from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = ROOT / "ml"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from train.evaluation_metrics import (
    BOOTSTRAP_CONFIDENCE_LEVEL,
    BOOTSTRAP_UNIT,
    ECE_BINNING,
    ECE_N_BINS,
    author_bootstrap_metrics,
    expected_calibration_error,
)


def test_ece_contract_is_explicit_and_deterministic() -> None:
    labels = pd.Series([0, 0, 1, 1])
    probabilities = np.array([0.1, 0.2, 0.8, 0.9])

    assert ECE_N_BINS == 10
    assert ECE_BINNING == "equal_width"
    assert expected_calibration_error(labels, probabilities) == pytest.approx(0.15)


def test_author_bootstrap_reports_requested_and_valid_repetitions() -> None:
    labels = pd.Series([0, 1, 0, 1, 0, 1, 0, 1])
    probabilities = np.array([0.1, 0.9, 0.2, 0.8, 0.3, 0.7, 0.4, 0.6])
    groups = pd.Series(["a", "a", "b", "b", "c", "c", "d", "d"])

    first = author_bootstrap_metrics(labels, probabilities, groups, n_iterations=50, seed=13)
    second = author_bootstrap_metrics(labels, probabilities, groups, n_iterations=50, seed=13)

    assert first == second
    assert first["bootstrap_unit"] == BOOTSTRAP_UNIT == "author_hash"
    assert first["confidence_level"] == BOOTSTRAP_CONFIDENCE_LEVEL == 0.95
    assert first["requested_repetitions"] == 50
    assert first["valid_calibration_repetitions"] == 50
    assert first["valid_classification_repetitions"] + first["invalid_classification_repetitions"] == 50
    assert first["roc_auc_ci95"] is not None
    assert first["pr_auc_ci95"] is not None
    assert first["brier_ci95"] is not None
    assert first["ece_ci95"] is not None


def test_author_bootstrap_marks_single_class_ranking_replicates_invalid() -> None:
    labels = pd.Series([0, 0, 0, 0])
    probabilities = np.array([0.1, 0.2, 0.3, 0.4])
    groups = pd.Series(["a", "b", "c", "d"])

    result = author_bootstrap_metrics(labels, probabilities, groups, n_iterations=20, seed=7)

    assert result["requested_repetitions"] == 20
    assert result["valid_classification_repetitions"] == 0
    assert result["invalid_classification_repetitions"] == 20
    assert result["valid_calibration_repetitions"] == 20
    assert result["roc_auc_ci95"] is None
    assert result["pr_auc_ci95"] is None
    assert result["brier_ci95"] is not None
    assert result["ece_ci95"] is not None
