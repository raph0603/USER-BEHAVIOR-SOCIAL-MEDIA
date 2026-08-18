"""Single source of truth for resolved Stage-1 experiment parameters."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any, Mapping, Sequence

DEFAULT_RANDOM_SEED = 42
DEFAULT_TEST_SIZE = 0.2
CONTENT_MODEL_FOLDS = 5
CALIBRATION_FOLDS = 5

CONTENT_MODEL = {
    "tfidf": {
        "ngram_range": [1, 2],
        "min_df": 2,
        "max_features": 20_000,
        "sublinear_tf": True,
        "strip_accents": None,
    },
    "logistic_regression": {
        "max_iter": 1_000,
        "class_weight": "balanced",
        "solver": "lbfgs",
    },
}

ROLE_MODEL = {
    "minimum_class_size": 10,
    "tfidf": {
        "ngram_range": [1, 2],
        "min_df": 2,
        "sublinear_tf": True,
        "max_features": 20_000,
    },
    "logistic_regression": {
        "max_iter": 1_000,
        "class_weight": "balanced",
        "solver": "lbfgs",
    },
}

TOPIC_MODEL = {
    "n_topics": 8,
    "fit_scope": "outer_training_inductive",
    "tfidf": {"min_df": 3, "max_features": 20_000, "sublinear_tf": True},
    "nmf": {"init": "nndsvda", "max_iter": 400},
}

XGBOOST_MODEL = {
    "n_estimators": 300,
    "max_depth": 3,
    "learning_rate": 0.03,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "min_child_weight": 2,
    "reg_lambda": 1.0,
    "objective": "binary:logistic",
    "eval_metric": "aucpr",
}

PLATT_CALIBRATION = {
    "method": "logistic_regression_on_oof_logit",
    "fold_strategy": "stratified_group_k_fold",
    "folds": CALIBRATION_FOLDS,
    "logit_clip": [1e-6, 1 - 1e-6],
    "logistic_regression": {"solver": "lbfgs", "max_iter": 100},
}

DECISION_THRESHOLD = {
    "strategy": "maximize_f1_on_calibration_oof",
    "grid_start": 0.05,
    "grid_stop_exclusive": 0.95,
    "grid_step": 0.01,
}

AUDIENCE_POLICY = {
    "version": "pre_outcome_author_audience_v1",
    "reddit_community_size_as_author_audience": False,
    "unknown_is_distinct_from_zero": True,
}


def _manifest_filters(dataset_manifest: Mapping[str, Any] | None) -> dict[str, Any]:
    if not dataset_manifest:
        return {}
    raw = dataset_manifest.get("filters_json", dataset_manifest.get("filters", {}))
    if isinstance(raw, str):
        value = json.loads(raw)
    else:
        value = raw
    return dict(value) if isinstance(value, Mapping) else {}


def _virality_contract_fingerprint(
    dataset_manifest: Mapping[str, Any] | None,
) -> str | None:
    """Resolve the frozen virality fingerprint from the official manifest layout."""

    if not dataset_manifest:
        return None

    labeling = dataset_manifest.get("labeling")
    nested = labeling.get("virality_contract_fingerprint") if isinstance(labeling, Mapping) else None
    legacy_root = dataset_manifest.get("virality_contract_fingerprint")

    if nested and legacy_root and str(nested) != str(legacy_root):
        raise ValueError(
            "Dataset manifest has conflicting virality contract fingerprints "
            "between labeling and the legacy root field"
        )
    value = nested or legacy_root
    return str(value) if value else None


def resolved_training_config(
    *,
    seed: int,
    test_size: float,
    feature_columns: Sequence[str],
    feature_versions: Sequence[str],
    dataset_schema_version: str | None,
    dataset_manifest: Mapping[str, Any] | None,
    content_backend: str,
    auxiliary_artifacts: Mapping[str, str] | None = None,
    scale_pos_weight: float,
) -> dict[str, Any]:
    """Record values actually selected for this run, including dataset contracts."""

    from common.reproducibility import fingerprint

    filters = _manifest_filters(dataset_manifest)
    config: dict[str, Any] = {
        "schema_version": "training-config-v1",
        "random_seed": int(seed),
        "outer_split": {
            "strategy": "stratified_group_k_fold",
            "group_column": "author_hash",
            "stratification": "source x viral",
            "n_splits": 5,
        },
        "content_model": {
            "backend": content_backend,
            "folds": CONTENT_MODEL_FOLDS,
            **deepcopy(CONTENT_MODEL),
        },
        "role_model": {**deepcopy(ROLE_MODEL), "random_seed": int(seed)},
        "topic_model": {**deepcopy(TOPIC_MODEL), "random_seed": int(seed)},
        "xgboost": {
            **deepcopy(XGBOOST_MODEL),
            "random_seed": int(seed),
            "scale_pos_weight": float(scale_pos_weight),
        },
        "platt_calibration": deepcopy(PLATT_CALIBRATION),
        "decision_threshold": deepcopy(DECISION_THRESHOLD),
        "feature_schema": {
            "dataset_schema_version": dataset_schema_version,
            "feature_versions": sorted(str(value) for value in feature_versions),
            "model_columns": list(feature_columns),
        },
        "labeling_contract": {
            key: filters.get(key)
            for key in (
                "label_strategy",
                "label_horizon_hours",
                "label_tolerance_hours",
                "viral_quantile",
                "required_observed_metrics",
                "min_text_chars",
            )
        },
        "virality_contract_fingerprint": _virality_contract_fingerprint(dataset_manifest),
        "audience_policy": deepcopy(AUDIENCE_POLICY),
        "auxiliary_artifact_sha256": dict(sorted((auxiliary_artifacts or {}).items())),
    }
    config["training_config_fingerprint"] = fingerprint(config)
    return config


def validate_training_config(config: Mapping[str, Any]) -> None:
    from common.reproducibility import fingerprint

    expected = str(config.get("training_config_fingerprint") or "")
    identity = {key: value for key, value in config.items() if key != "training_config_fingerprint"}
    if expected != fingerprint(identity):
        raise ValueError("Training configuration fingerprint does not match resolved values")
