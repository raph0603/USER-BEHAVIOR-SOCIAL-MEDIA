from __future__ import annotations

import copy
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


CONTRACT = _load(
    "virality_contract",
    ROOT / "spark" / "jobs" / "pipeline" / "virality_contract.py",
)
LINEAGE = _load("virality_lineage", ROOT / "ml" / "virality_lineage.py")


def _contract(scores=None, **overrides):
    arguments = {
        "reference_scores": scores or {"x": [1.0, 2.0, 3.0, 4.0], "youtube": [10, 20, 30, 40]},
        "policy": CONTRACT.TRAINING_REFERENCE_POLICY,
        "quantile": 0.75,
        "reference": {
            "construction_fingerprint": "a" * 64,
            "source_snapshots": {"features": 123, "engagement": 456},
            "holdout_excluded": True,
        },
        "horizon_hours": 24,
        "tolerance_hours": 24,
        "eligibility_filters": {"min_text_chars": 3, "required_observed_metrics": 1},
        "min_reference_examples_per_platform": 2,
        "generated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    arguments.update(overrides)
    return CONTRACT.build_contract(**arguments)


def test_q75_is_exact_linear_per_platform_and_records_provenance():
    contract = _contract()

    assert contract.thresholds["x"]["value"] == pytest.approx(3.25)
    assert contract.thresholds["youtube"]["value"] == pytest.approx(32.5)
    assert contract.thresholds["x"]["reference_count"] == 4
    assert contract.thresholds["x"]["quantile_method"] == "linear"
    assert contract.thresholds["x"]["reference"]["source_snapshots"] == {
        "features": 123,
        "engagement": 456,
    }


def test_invalid_values_are_ignored_and_minimum_is_enforced():
    contract = _contract(scores={"x": [1.0, None, "invalid", float("nan"), 3.0]})
    assert contract.thresholds["x"]["reference_count"] == 2
    with pytest.raises(ValueError, match="minimum"):
        _contract(
            scores={"x": [1.0, float("nan")]},
            min_reference_examples_per_platform=2,
        )


def test_labels_are_stable_when_evaluation_composition_changes_and_include_boundary():
    contract = _contract(scores={"x": [1.0, 2.0, 3.0, 4.0]})
    threshold = contract.thresholds["x"]["value"]

    before = contract.label("x", threshold)
    unrelated_evaluation_scores = [0.0] * 10_000
    after = contract.label("x", threshold)

    assert unrelated_evaluation_scores
    assert before == after == 1
    assert contract.label("x", threshold - 0.001) == 0
    assert contract.label("x", threshold + 0.001) == 1


def test_holdout_scores_never_change_training_reference_threshold():
    training_scores = {"x": [1.0, 2.0, 3.0, 4.0]}
    baseline = _contract(scores=training_scores)
    changed_holdout = [100_000.0, -100_000.0]
    replay = _contract(scores=training_scores)

    assert changed_holdout
    assert replay.thresholds == baseline.thresholds
    assert replay.fingerprint == baseline.fingerprint


def test_platform_reference_changes_are_isolated():
    baseline = _contract()
    changed = _contract(scores={"x": [1, 2, 3, 4], "youtube": [100, 200, 300, 400]})
    assert changed.thresholds["x"] == baseline.thresholds["x"]
    assert changed.thresholds["youtube"] != baseline.thresholds["youtube"]


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("quantile",), 0.8),
        (("thresholds", "x", "value"), 99.0),
        (("reference", "source_snapshots", "engagement"), 999),
        (("engagement", "engagement_score_version"), "future-score-v2"),
        (("engagement", "horizon_hours"), 72),
        (("engagement", "tolerance_hours"), 6),
        (("quantile_method",), "nearest"),
    ],
)
def test_fingerprint_changes_for_every_semantic_label_input(path, value):
    payload = _contract().to_dict()
    payload.pop("virality_contract_fingerprint")
    changed = copy.deepcopy(payload)
    target = changed
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    assert CONTRACT.contract_fingerprint(changed) != CONTRACT.contract_fingerprint(payload)


def test_fingerprint_ignores_generation_timestamp_and_json_key_order():
    payload = _contract().to_dict()
    payload.pop("virality_contract_fingerprint")
    reordered = json.loads(json.dumps(payload, sort_keys=True))
    reordered["generated_at"] = "2099-01-01T00:00:00+00:00"
    reordered["diagnostics"] = {"descriptive": "changed"}
    assert CONTRACT.contract_fingerprint(reordered) == CONTRACT.contract_fingerprint(payload)


def test_lineage_compatibility_rejects_mismatch_and_legacy_artifacts():
    fingerprint = "b" * 64
    frame = pd.DataFrame(
        {
            "virality_policy": [CONTRACT.TRAINING_REFERENCE_POLICY] * 2,
            "virality_contract_fingerprint": [fingerprint] * 2,
        }
    )
    observed = LINEAGE.dataset_virality_lineage(frame)
    LINEAGE.validate_virality_compatibility(
        observed,
        expected_fingerprint=fingerprint,
        expected_policy=CONTRACT.TRAINING_REFERENCE_POLICY,
    )
    with pytest.raises(ValueError, match="mismatch"):
        LINEAGE.validate_virality_compatibility(
            observed,
            expected_fingerprint="c" * 64,
            expected_policy=CONTRACT.TRAINING_REFERENCE_POLICY,
        )
    with pytest.raises(ValueError, match="legacy artifact"):
        LINEAGE.dataset_virality_lineage(pd.DataFrame({"viral": [1]}))
