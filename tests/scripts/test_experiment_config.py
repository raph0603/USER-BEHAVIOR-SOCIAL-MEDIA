from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = ROOT / "ml"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from experiment_config import resolved_training_config, validate_training_config


def _resolved_config(dataset_manifest: dict) -> dict:
    return resolved_training_config(
        seed=42,
        test_size=0.2,
        feature_columns=["char_count", "src_x"],
        feature_versions=["feature-v1"],
        dataset_schema_version="dataset-v3",
        dataset_manifest=dataset_manifest,
        content_backend="tfidf_logistic_regression",
        scale_pos_weight=1.5,
    )


def test_training_config_reads_official_nested_virality_fingerprint() -> None:
    fingerprint = "a" * 64
    config = _resolved_config(
        {
            "dataset_version": "dataset-v3-example",
            "labeling": {"virality_contract_fingerprint": fingerprint},
        }
    )

    assert config["virality_contract_fingerprint"] == fingerprint
    validate_training_config(config)


def test_training_config_keeps_legacy_root_fingerprint_compatibility() -> None:
    fingerprint = "b" * 64
    config = _resolved_config(
        {
            "dataset_version": "dataset-v3-example",
            "virality_contract_fingerprint": fingerprint,
        }
    )

    assert config["virality_contract_fingerprint"] == fingerprint
    validate_training_config(config)


def test_training_config_rejects_conflicting_virality_fingerprints() -> None:
    with pytest.raises(ValueError, match="conflicting virality contract fingerprints"):
        _resolved_config(
            {
                "dataset_version": "dataset-v3-example",
                "virality_contract_fingerprint": "a" * 64,
                "labeling": {"virality_contract_fingerprint": "b" * 64},
            }
        )
