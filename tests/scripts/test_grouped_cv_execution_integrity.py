from __future__ import annotations

import inspect
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = ROOT / "ml"
for import_root in (ROOT, ML_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from common.reproducibility import fingerprint
from train import grouped_cv_stage1 as grouped


def _frame(n_per_stratum: int = 10) -> pd.DataFrame:
    rows = []
    index = 0
    for source in ("x", "youtube"):
        for viral in (0, 1):
            for _ in range(n_per_stratum):
                rows.append(
                    {
                        "example_id": f"example-{index}",
                        "source": source,
                        "viral": viral,
                        "author_hash": f"author-{index}",
                    }
                )
                index += 1
    return pd.DataFrame(rows)


def test_grouped_experiment_id_has_no_legacy_split_dependency() -> None:
    lineage = {
        field: f"value-{index}"
        for index, field in enumerate(grouped.GROUPED_EXPERIMENT_ID_FIELDS)
    }

    experiment_id = grouped.grouped_experiment_id(lineage)

    assert experiment_id.startswith("experiment-v2-")
    assert "split_fingerprint" not in grouped.GROUPED_EXPERIMENT_ID_FIELDS


def test_outer_folds_are_author_disjoint_complete_and_deterministic() -> None:
    frame = _frame()
    strata = grouped.source_class_strata(frame)
    labels = frame["viral"]
    groups = frame["author_hash"]

    first = grouped._validated_grouped_splits(
        strata=strata,
        labels=labels,
        groups=groups,
        n_splits=5,
        seed=42,
        context="test",
    )
    second = grouped._validated_grouped_splits(
        strata=strata,
        labels=labels,
        groups=groups,
        n_splits=5,
        seed=42,
        context="test",
    )

    coverage = np.zeros(len(frame), dtype=int)
    for (train_idx, held_idx), (train_idx_2, held_idx_2) in zip(first, second, strict=True):
        assert np.array_equal(train_idx, train_idx_2)
        assert np.array_equal(held_idx, held_idx_2)
        assert set(groups.iloc[train_idx]).isdisjoint(groups.iloc[held_idx])
        coverage[held_idx] += 1
    assert np.all(coverage == 1)


def test_fold_fingerprint_contains_exact_example_assignments() -> None:
    frame = _frame()
    folds = grouped._validated_grouped_splits(
        strata=grouped.source_class_strata(frame),
        labels=frame["viral"],
        groups=frame["author_hash"],
        n_splits=5,
        seed=42,
        context="test",
    )
    manifest, assignments = grouped._fold_manifest(
        stable_ids=frame["example_id"],
        folds=folds,
        dataset_version="dataset-v3-test",
        virality_fingerprint="a" * 64,
        evaluation_protocol_fingerprint="b" * 64,
        seed=42,
    )

    assert np.all(assignments > 0)
    persisted_ids = {
        example_id
        for fold in manifest["folds"].values()
        for example_id in fold["test_example_ids"]
    }
    assert persisted_ids == set(frame["example_id"])
    assert manifest["evaluation_folds_fingerprint"] == fingerprint(
        {
            key: value
            for key, value in manifest.items()
            if key != "evaluation_folds_fingerprint"
        }
    )

    tampered = {
        **manifest,
        "folds": {key: dict(value) for key, value in manifest["folds"].items()},
    }
    tampered["folds"]["1"]["test_example_ids"] = list(
        tampered["folds"]["1"]["test_example_ids"]
    )
    tampered["folds"]["1"]["test_example_ids"][0] = "different-example"
    assert fingerprint(
        {
            key: value
            for key, value in tampered.items()
            if key != "evaluation_folds_fingerprint"
        }
    ) != manifest["evaluation_folds_fingerprint"]


def test_impossible_grouped_split_fails_explicitly() -> None:
    frame = _frame(n_per_stratum=4)
    with pytest.raises(ValueError, match="at least 5 rows in every stratum"):
        grouped._validated_grouped_splits(
            strata=grouped.source_class_strata(frame),
            labels=frame["viral"],
            groups=frame["author_hash"],
            n_splits=5,
            seed=42,
            context="test",
        )


def test_calibration_uses_grouped_stratified_oof(monkeypatch) -> None:
    frame = _frame(n_per_stratum=10)
    X = pd.DataFrame({"value": np.linspace(-1.0, 1.0, len(frame))})
    labels = frame["viral"].astype(int)
    groups = frame["author_hash"]
    strata = grouped.source_class_strata(frame)

    class FakeModel:
        def predict_proba(self, values):
            x = np.asarray(values["value"], dtype=float)
            probability = 1.0 / (1.0 + np.exp(-x))
            return np.column_stack([1.0 - probability, probability])

    calls = []

    def fake_train_model(X_train, y_train, seed, **kwargs):
        calls.append((set(X_train.index), int(seed), kwargs))
        return FakeModel()

    monkeypatch.setattr(grouped, "train_model", fake_train_model)
    _, threshold, raw_oof, calibrated_oof, assignments = grouped.fit_calibrator(
        X,
        labels,
        groups,
        42,
        strata=strata,
    )

    assert len(calls) == grouped.CALIBRATION_FOLDS
    assert np.all(assignments > 0)
    assert np.isfinite(raw_oof).all()
    assert np.isfinite(calibrated_oof).all()
    assert 0.0 < threshold < 1.0


def test_main_persists_fold_specific_thresholds() -> None:
    source = inspect.getsource(grouped.main)
    assert "row_thresholds[validation_idx] = threshold" in source
    assert '"classification_threshold": row_thresholds' in source
    assert "avg_threshold" not in source
