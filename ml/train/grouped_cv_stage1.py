"""Official Stage-1 grouped-CV training and evaluation.

The scientific evaluation is produced exclusively from outer out-of-fold predictions.
A separate model is then fitted on all eligible rows for deployment.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from sklearn.model_selection import StratifiedGroupKFold, cross_val_predict

ML_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ML_ROOT.parent
for import_root in (ML_ROOT, PROJECT_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from common.reproducibility import file_sha256, fingerprint, load_json, write_json
from dataset_lineage import load_dataset_lineage, model_lineage_path
from experiment_config import (
    CALIBRATION_FOLDS,
    CONTENT_MODEL_FOLDS,
    DECISION_THRESHOLD,
    DEFAULT_RANDOM_SEED,
    PLATT_CALIBRATION,
    XGBOOST_MODEL,
    resolved_training_config,
    validate_training_config,
)
from features.text_content import build_content_model
from features.topics import N_TOPICS, TOPIC_MODEL_PATH, TopicFeaturizer, fit_topic_features
from role_contract import role_feature_contract
from train.evaluation_metrics import (
    BOOTSTRAP_CONFIDENCE_LEVEL,
    BOOTSTRAP_ITERATIONS,
    BOOTSTRAP_UNIT,
    ECE_BINNING,
    ECE_N_BINS,
    author_bootstrap_metrics,
    metric_summary,
)
from virality_lineage import dataset_virality_lineage, validate_virality_compatibility

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
DEFAULT_ENVIRONMENT_MANIFEST = ML_ROOT / "results" / "environment_manifest.json"
DEFAULT_TRAINING_CONFIG = ML_ROOT / "results" / "training_config.json"
DEFAULT_LINEAGE = ML_ROOT / "results" / "experiment_lineage.json"
DEFAULT_OOF_PREDICTIONS = ML_ROOT / "results" / "oof_predictions.parquet"
DEFAULT_EVALUATION_PROTOCOL = ML_ROOT / "results" / "evaluation_protocol.json"
DEFAULT_FOLDS_MANIFEST = ML_ROOT / "results" / "cv_folds_manifest.json"
DEFAULT_METRICS_OUTPUT = ML_ROOT / "results" / "evaluation.json"

TEXT = "clean_text"
TARGET = "viral"
GROUP = "author_hash"
SOURCE = "source"
OUTER_FOLDS = 5

CONTENT_FEATURES = [
    "char_count",
    "word_count",
    "has_question",
    "is_vietnamese",
    "f_word",
    "f_sent",
    "f_clause",
    "f_info",
    "f_visual",
    "cognitive_friction_score",
]

GROUPED_EXPERIMENT_ID_FIELDS = (
    "dataset_version",
    "dataset_fingerprint",
    "manifest_sha256",
    "git_commit",
    "environment_fingerprint",
    "training_config_fingerprint",
    "virality_contract_fingerprint",
    "evaluation_protocol_fingerprint",
    "evaluation_folds_fingerprint",
)


def grouped_experiment_id(lineage: Mapping[str, object]) -> str:
    identity = {field: lineage[field] for field in GROUPED_EXPERIMENT_ID_FIELDS}
    return f"experiment-v2-{fingerprint(identity)[:24]}"


def compact_grouped_lineage(lineage: Mapping[str, object]) -> dict[str, object]:
    fields = ("experiment_id", *GROUPED_EXPERIMENT_ID_FIELDS, "virality_policy")
    return {field: lineage[field] for field in fields if field in lineage}


def validate_expected_grouped_lineage(
    expected: Mapping[str, object], actual: Mapping[str, object]
) -> None:
    mismatches = [
        field
        for field in GROUPED_EXPERIMENT_ID_FIELDS
        if expected.get(field) != actual.get(field)
    ]
    if mismatches:
        raise ValueError("Replay preflight lineage mismatch: " + ", ".join(mismatches))


def validate_dataset_version(df: pd.DataFrame, expected: str | None) -> None:
    if expected is None:
        return
    if "dataset_version" not in df.columns:
        raise ValueError("Versioned training data must include dataset_version")
    versions = sorted(df["dataset_version"].dropna().astype(str).unique())
    if versions != [expected]:
        raise ValueError(f"Expected exactly dataset version {expected}, received {versions}")


def feature_columns(
    df: pd.DataFrame,
    *,
    include_audience: bool = True,
    include_roles: bool = True,
) -> list[str]:
    prefixes = ["src_"]
    if include_roles:
        prefixes.append("role_")
    if include_audience:
        prefixes.append("chan_")
    extra = sorted(column for column in df.columns if column.startswith(tuple(prefixes)))
    return CONTENT_FEATURES + extra


def observation_ids(df: pd.DataFrame, *, official: bool) -> tuple[str, pd.Series]:
    for column in ("example_id", "content_id", "platform_event_id", "observation_id"):
        if column not in df.columns or df[column].isna().any():
            continue
        values = df[column].astype(str)
        if values.is_unique:
            return column, values
    if official:
        raise ValueError("Official training data requires one complete unique stable identifier column")
    return "row_index_nonofficial", pd.Series(
        [f"row-{index}" for index in range(len(df))], index=df.index, dtype="string"
    )


def source_class_strata(df: pd.DataFrame) -> pd.Series:
    sources = df[SOURCE].astype("string").str.strip().str.lower()
    labels = df[TARGET].astype(int).astype(str)
    return sources + "|" + labels


def train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    seed: int,
    *,
    colsample_bytree: float = 0.9,
    sample_weight=None,
) -> xgb.XGBClassifier:
    if sample_weight is None:
        positive = float(y_train.sum())
        negative = float(len(y_train) - y_train.sum())
    else:
        weights = np.asarray(sample_weight, dtype=float)
        labels = np.asarray(y_train, dtype=int)
        positive = float(weights[labels == 1].sum())
        negative = float(weights[labels == 0].sum())
    parameters = {**XGBOOST_MODEL, "colsample_bytree": colsample_bytree}
    model = xgb.XGBClassifier(
        **parameters,
        scale_pos_weight=(negative / positive) if positive else 1.0,
        random_state=seed,
    )
    model.fit(X_train, y_train, sample_weight=sample_weight)
    return model


def logit(probabilities) -> np.ndarray:
    lower, upper = PLATT_CALIBRATION["logit_clip"]
    values = np.clip(np.asarray(probabilities, dtype=float), lower, upper)
    return np.log(values / (1 - values))


def best_f1_threshold(y_true, probabilities) -> float:
    grid = np.round(
        np.arange(
            DECISION_THRESHOLD["grid_start"],
            DECISION_THRESHOLD["grid_stop_exclusive"],
            DECISION_THRESHOLD["grid_step"],
        ),
        2,
    )
    scores = [
        f1_score(y_true, (probabilities >= threshold).astype(int), zero_division=0)
        for threshold in grid
    ]
    return float(grid[int(np.argmax(scores))])


def _validated_grouped_splits(
    *,
    strata: pd.Series,
    labels: pd.Series,
    groups: pd.Series,
    n_splits: int,
    seed: int,
    context: str,
) -> list[tuple[np.ndarray, np.ndarray]]:
    strata = pd.Series(strata).reset_index(drop=True)
    labels = pd.Series(labels).astype(int).reset_index(drop=True)
    groups = pd.Series(groups).astype("string").reset_index(drop=True)
    if not (len(strata) == len(labels) == len(groups)):
        raise ValueError(f"{context} split inputs do not align")
    if groups.isna().any():
        raise ValueError(f"{context} requires a group for every row")
    if groups.nunique() < n_splits:
        raise ValueError(
            f"{context} requires at least {n_splits} distinct authors; received {groups.nunique()}"
        )
    support = strata.value_counts()
    if support.empty or int(support.min()) < n_splits:
        minimum = int(support.min()) if not support.empty else 0
        raise ValueError(
            f"{context} needs at least {n_splits} rows in every stratum; minimum support is {minimum}"
        )

    splitter = StratifiedGroupKFold(n_splits=n_splits, shuffle=True, random_state=seed)
    splits = list(splitter.split(np.zeros(len(labels)), strata, groups))
    coverage = np.zeros(len(labels), dtype=int)
    for fold_number, (fit_idx, held_idx) in enumerate(splits, start=1):
        if set(groups.iloc[fit_idx]) & set(groups.iloc[held_idx]):
            raise ValueError(f"{context} author leakage in fold {fold_number}")
        if labels.iloc[fit_idx].nunique() < 2:
            raise ValueError(f"{context} fit fold {fold_number} does not contain both classes")
        if labels.iloc[held_idx].nunique() < 2:
            raise ValueError(f"{context} held-out fold {fold_number} does not contain both classes")
        coverage[held_idx] += 1
    if not np.all(coverage == 1):
        raise ValueError(f"{context} folds must cover every row exactly once")
    return splits


def apply_calibrator(calibrator: LogisticRegression, probabilities) -> np.ndarray:
    return calibrator.predict_proba(logit(probabilities).reshape(-1, 1))[:, 1]


def fit_calibrator(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    groups: pd.Series,
    seed: int,
    *,
    strata: pd.Series | None = None,
    model_kwargs: dict | None = None,
    sample_weight=None,
) -> tuple[LogisticRegression, float, np.ndarray, np.ndarray, np.ndarray]:
    model_kwargs = model_kwargs or {}
    labels = pd.Series(y_train).astype(int).reset_index(drop=True)
    group_values = pd.Series(groups).astype("string").reset_index(drop=True)
    stratification = labels.astype(str) if strata is None else pd.Series(strata).reset_index(drop=True)
    weights = None if sample_weight is None else np.asarray(sample_weight, dtype=float)

    splits = _validated_grouped_splits(
        strata=stratification,
        labels=labels,
        groups=group_values,
        n_splits=CALIBRATION_FOLDS,
        seed=seed,
        context="Calibration",
    )
    raw_oof = np.full(len(X_train), np.nan, dtype=float)
    assignments = np.zeros(len(X_train), dtype=int)
    for fold_number, (fit_idx, held_idx) in enumerate(splits, start=1):
        fold_model = train_model(
            X_train.iloc[fit_idx],
            labels.iloc[fit_idx],
            seed + fold_number,
            sample_weight=None if weights is None else weights[fit_idx],
            **model_kwargs,
        )
        raw_oof[held_idx] = fold_model.predict_proba(X_train.iloc[held_idx])[:, 1]
        assignments[held_idx] = fold_number

    if np.isnan(raw_oof).any() or np.any(assignments == 0):
        raise ValueError("Calibration did not produce exactly one OOF prediction per row")

    calibrator = LogisticRegression(
        **PLATT_CALIBRATION["logistic_regression"], random_state=seed
    )
    calibrator.fit(logit(raw_oof).reshape(-1, 1), labels)
    calibrated_oof = apply_calibrator(calibrator, raw_oof)
    threshold = best_f1_threshold(labels, calibrated_oof)
    return calibrator, threshold, raw_oof, calibrated_oof, assignments


def shap_importance(model: xgb.XGBClassifier, X: pd.DataFrame) -> pd.Series:
    booster = model.get_booster()
    contributions = booster.predict(xgb.DMatrix(X), pred_contribs=True)
    mean_absolute = np.abs(contributions[:, :-1]).mean(axis=0)
    return pd.Series(mean_absolute, index=X.columns).sort_values(ascending=False)


def _dataset_manifest(path: Path | None, expected_version: str | None) -> dict:
    if path is None:
        return {}
    manifest = load_json(path)
    if expected_version and manifest.get("dataset_version") != expected_version:
        raise ValueError("Dataset manifest version does not match --dataset-version")
    return manifest


def _source_snapshots(manifest: Mapping[str, object]) -> dict[str, int]:
    raw = manifest.get("iceberg_snapshots_json", manifest.get("source_snapshots", {}))
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, Mapping):
        raise ValueError("Dataset manifest source snapshots must be an object")
    return {str(table): int(snapshot) for table, snapshot in sorted(raw.items())}


def build_experiment_lineage(
    *,
    dataset_manifest: Mapping[str, object],
    environment_manifest: Mapping[str, object],
    training_config: Mapping[str, object],
    evaluation_protocol_fingerprint: str,
    evaluation_folds_fingerprint: str,
    official_run: bool,
) -> dict[str, object]:
    code = environment_manifest.get("code", {})
    labeling = dataset_manifest.get("labeling", {})
    lineage: dict[str, object] = {
        "schema_version": "experiment-lineage-v2",
        "official_run": bool(official_run),
        "status": "training",
        "dataset_version": dataset_manifest.get("dataset_version"),
        "dataset_fingerprint": dataset_manifest.get("dataset_fingerprint"),
        "silver_snapshot_ids": _source_snapshots(dataset_manifest),
        "gold_table": dataset_manifest.get("gold_table"),
        "gold_snapshot_id": dataset_manifest.get("gold_snapshot_id"),
        "manifest_sha256": dataset_manifest.get("manifest_sha256"),
        "git_commit": code.get("git_commit") if isinstance(code, Mapping) else None,
        "git_dirty": code.get("git_dirty") if isinstance(code, Mapping) else None,
        "environment_fingerprint": environment_manifest.get("environment_fingerprint"),
        "training_config_fingerprint": training_config.get("training_config_fingerprint"),
        "virality_contract_fingerprint": training_config.get("virality_contract_fingerprint"),
        "virality_policy": labeling.get("policy") if isinstance(labeling, Mapping) else None,
        "evaluation_protocol_fingerprint": evaluation_protocol_fingerprint,
        "evaluation_folds_fingerprint": evaluation_folds_fingerprint,
        "evaluation_fingerprint": None,
        "model_sha256": None,
        "oof_predictions_sha256": None,
        "metrics_sha256": None,
    }
    required = GROUPED_EXPERIMENT_ID_FIELDS + ("virality_policy",)
    if official_run:
        missing = [field for field in required if not lineage.get(field)]
        if missing:
            raise ValueError("Official experiment lineage is incomplete: " + ", ".join(missing))
    lineage["experiment_id"] = grouped_experiment_id(lineage)
    return lineage


def _evaluation_protocol(
    *, dataset_version: str | None, virality_fingerprint: str, seed: int
) -> dict[str, object]:
    protocol: dict[str, object] = {
        "schema_version": "evaluation-protocol-v2",
        "dataset_version": dataset_version,
        "virality_contract_fingerprint": virality_fingerprint,
        "outer_evaluation": {
            "method": "stratified_group_k_fold",
            "n_splits": OUTER_FOLDS,
            "group_column": GROUP,
            "stratification": "source_x_viral",
            "shuffle": True,
            "random_seed": seed,
        },
        "preprocessing": {
            "content_tfidf_fit_scope": "outer_training_only",
            "topic_tfidf_nmf_fit_scope": "outer_training_only",
            "content_score_training": "inner_grouped_oof",
            "content_score_validation": "outer_training_refit",
        },
        "content_model_inner_cv": {
            "method": "stratified_group_k_fold",
            "n_splits": CONTENT_MODEL_FOLDS,
            "group_column": GROUP,
            "stratification": "source_x_viral",
            "shuffle": True,
            "seed_policy": "outer_seed_plus_fold_number",
        },
        "calibration": {
            "method": "platt_logistic_regression_on_oof_logit",
            "splitter": "stratified_group_k_fold",
            "n_splits": CALIBRATION_FOLDS,
            "group_column": GROUP,
            "stratification": "source_x_viral",
            "shuffle": True,
            "seed_policy": "outer_seed_plus_fold_number",
            "logit_clip": list(PLATT_CALIBRATION["logit_clip"]),
        },
        "classification_threshold": {
            "strategy": DECISION_THRESHOLD["strategy"],
            "fit_scope": "outer_training_calibration_oof_only",
            "grid_start": DECISION_THRESHOLD["grid_start"],
            "grid_stop_exclusive": DECISION_THRESHOLD["grid_stop_exclusive"],
            "grid_step": DECISION_THRESHOLD["grid_step"],
            "application": "fold_specific",
        },
        "expected_calibration_error": {
            "n_bins": ECE_N_BINS,
            "binning": ECE_BINNING,
            "range": [0.0, 1.0],
        },
        "bootstrap": {
            "unit": BOOTSTRAP_UNIT,
            "iterations": BOOTSTRAP_ITERATIONS,
            "confidence_level": BOOTSTRAP_CONFIDENCE_LEVEL,
            "interval_method": "percentile",
            "seed": seed,
        },
        "metrics": ["roc_auc", "pr_auc", "brier", "ece", "f1", "precision", "recall"],
    }
    protocol["evaluation_protocol_fingerprint"] = fingerprint(protocol)
    return protocol


def _fold_manifest(
    *,
    stable_ids: pd.Series,
    folds: Iterable[tuple[np.ndarray, np.ndarray]],
    dataset_version: str | None,
    virality_fingerprint: str,
    evaluation_protocol_fingerprint: str,
    seed: int,
) -> tuple[dict[str, object], np.ndarray]:
    assignments = np.zeros(len(stable_ids), dtype=int)
    fold_details: dict[str, object] = {}
    for fold_number, (train_idx, validation_idx) in enumerate(folds, start=1):
        assignments[validation_idx] = fold_number
        validation_ids = sorted(str(stable_ids.iloc[index]) for index in validation_idx)
        fold_details[str(fold_number)] = {
            "train_size": int(len(train_idx)),
            "test_size": int(len(validation_idx)),
            "test_example_ids": validation_ids,
        }
    if np.any(assignments == 0):
        raise RuntimeError("Outer folds did not assign every example")
    manifest: dict[str, object] = {
        "schema_version": "cv-folds-manifest-v3",
        "strategy": "stratified_group_k_fold",
        "group_column": GROUP,
        "stratification": "source_x_viral",
        "n_splits": len(fold_details),
        "seed": seed,
        "dataset_version": dataset_version,
        "virality_contract_fingerprint": virality_fingerprint,
        "evaluation_protocol_fingerprint": evaluation_protocol_fingerprint,
        "folds": fold_details,
    }
    manifest["evaluation_folds_fingerprint"] = fingerprint(manifest)
    return manifest, assignments


def _evaluate_subset(subset: pd.DataFrame, *, seed: int) -> dict[str, object]:
    return {
        "raw": {
            "metrics": metric_summary(
                subset[TARGET], subset["raw_probability"].to_numpy(), subset["classification_threshold"].to_numpy()
            ),
            "bootstrap": author_bootstrap_metrics(
                subset[TARGET], subset["raw_probability"].to_numpy(), subset[GROUP], seed=seed
            ),
        },
        "calibrated": {
            "metrics": metric_summary(
                subset[TARGET],
                subset["calibrated_probability"].to_numpy(),
                subset["classification_threshold"].to_numpy(),
            ),
            "bootstrap": author_bootstrap_metrics(
                subset[TARGET], subset["calibrated_probability"].to_numpy(), subset[GROUP], seed=seed
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Train and evaluate Stage 1 with grouped CV.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--dataset-version")
    parser.add_argument("--dataset-manifest", type=Path)
    parser.add_argument("--environment-manifest", type=Path)
    parser.add_argument("--training-config-output", type=Path, default=DEFAULT_TRAINING_CONFIG)
    parser.add_argument("--lineage-output", type=Path, default=DEFAULT_LINEAGE)
    parser.add_argument("--oof-predictions", type=Path, default=DEFAULT_OOF_PREDICTIONS)
    parser.add_argument("--evaluation-protocol", type=Path, default=DEFAULT_EVALUATION_PROTOCOL)
    parser.add_argument("--cv-folds-manifest", type=Path, default=DEFAULT_FOLDS_MANIFEST)
    parser.add_argument("--metrics-output", type=Path, default=DEFAULT_METRICS_OUTPUT)
    parser.add_argument("--expected-lineage", type=Path)
    parser.add_argument("--official-run", action="store_true")
    parser.add_argument("--virality-contract-fingerprint")
    parser.add_argument("--virality-policy")
    args = parser.parse_args()

    df = pd.read_parquet(args.data).reset_index(drop=True)
    dataset_lineage = None
    if args.dataset_version and not args.dataset_manifest:
        raise ValueError("Official model training requires --dataset-manifest")
    if args.dataset_manifest:
        _, dataset_lineage = load_dataset_lineage(
            args.dataset_manifest, expected_dataset_version=args.dataset_version
        )
        args.dataset_version = str(dataset_lineage["dataset_version"])
    validate_dataset_version(df, args.dataset_version)

    virality_lineage = dataset_virality_lineage(df)
    validate_virality_compatibility(
        virality_lineage,
        expected_fingerprint=args.virality_contract_fingerprint,
        expected_policy=args.virality_policy,
    )
    virality_fingerprint = virality_lineage["virality_contract_fingerprint"]
    virality_policy = virality_lineage["virality_policy"]

    features = feature_columns(
        df,
        include_audience=dataset_lineage is None,
        include_roles=not args.official_run,
    )
    missing_features = [column for column in features if column not in df.columns]
    if missing_features:
        raise ValueError("Training data is missing features: " + ", ".join(missing_features))

    labels = df[TARGET].astype(int).reset_index(drop=True)
    text = df[TEXT].fillna("").astype(str).reset_index(drop=True)
    fallback = pd.Series([f"missing-author-{index}" for index in range(len(df))])
    groups = df[GROUP].astype("string").fillna(fallback).reset_index(drop=True)
    strata = source_class_strata(df).reset_index(drop=True)
    _, stable_ids = observation_ids(df, official=args.official_run)
    stable_ids = stable_ids.reset_index(drop=True)

    outer_folds = _validated_grouped_splits(
        strata=strata,
        labels=labels,
        groups=groups,
        n_splits=OUTER_FOLDS,
        seed=args.seed,
        context="Outer evaluation",
    )

    dataset_manifest = _dataset_manifest(args.dataset_manifest, args.dataset_version)
    if args.environment_manifest is None:
        if args.official_run:
            raise ValueError("Official training requires --environment-manifest")
        environment_manifest: dict[str, object] = {"code": {}, "environment_fingerprint": None}
    else:
        environment_manifest = load_json(args.environment_manifest)
        from common.reproducibility import validate_environment_manifest

        validate_environment_manifest(environment_manifest)

    base_features = list(features) + [f"topic_{index}" for index in range(N_TOPICS)]
    numeric_base = df[features].astype(float).reset_index(drop=True)
    auxiliary_artifacts = {}
    role_model = ML_ROOT / "models" / "rhetorical_role.joblib"
    if not args.official_run and role_model.is_file():
        auxiliary_artifacts[role_model.name] = file_sha256(role_model)

    training_config = resolved_training_config(
        seed=args.seed,
        test_size=0.2,
        feature_columns=base_features + ["content_score"],
        feature_versions=(
            sorted(df["feature_version"].dropna().astype(str).unique())
            if "feature_version" in df.columns
            else []
        ),
        dataset_schema_version=str(dataset_manifest.get("schema_version") or "") or None,
        dataset_manifest=dataset_manifest,
        content_backend="tfidf_logistic_regression",
        auxiliary_artifacts=auxiliary_artifacts,
        scale_pos_weight=(
            float(len(labels) - labels.sum()) / float(labels.sum()) if float(labels.sum()) else 1.0
        ),
    )
    validate_training_config(training_config)
    write_json(args.training_config_output, training_config)

    evaluation_protocol = _evaluation_protocol(
        dataset_version=args.dataset_version,
        virality_fingerprint=virality_fingerprint,
        seed=args.seed,
    )
    write_json(args.evaluation_protocol, evaluation_protocol)

    folds_manifest, outer_assignments = _fold_manifest(
        stable_ids=stable_ids,
        folds=outer_folds,
        dataset_version=args.dataset_version,
        virality_fingerprint=virality_fingerprint,
        evaluation_protocol_fingerprint=str(
            evaluation_protocol["evaluation_protocol_fingerprint"]
        ),
        seed=args.seed,
    )
    write_json(args.cv_folds_manifest, folds_manifest)

    lineage = build_experiment_lineage(
        dataset_manifest=dataset_manifest,
        environment_manifest=environment_manifest,
        training_config=training_config,
        evaluation_protocol_fingerprint=str(
            evaluation_protocol["evaluation_protocol_fingerprint"]
        ),
        evaluation_folds_fingerprint=str(folds_manifest["evaluation_folds_fingerprint"]),
        official_run=args.official_run,
    )
    if args.expected_lineage:
        validate_expected_grouped_lineage(load_json(args.expected_lineage), lineage)
    write_json(args.lineage_output, lineage)

    raw_probabilities = np.full(len(df), np.nan, dtype=float)
    calibrated_probabilities = np.full(len(df), np.nan, dtype=float)
    row_thresholds = np.full(len(df), np.nan, dtype=float)

    print(f"Executing {OUTER_FOLDS}-fold StratifiedGroupKFold evaluation...")
    for fold_number, (train_idx, validation_idx) in enumerate(outer_folds, start=1):
        fold_seed = args.seed + fold_number
        train_labels = labels.iloc[train_idx].reset_index(drop=True)
        train_groups = groups.iloc[train_idx].reset_index(drop=True)
        train_strata = strata.iloc[train_idx].reset_index(drop=True)

        content_splits = _validated_grouped_splits(
            strata=train_strata,
            labels=train_labels,
            groups=train_groups,
            n_splits=CONTENT_MODEL_FOLDS,
            seed=fold_seed,
            context=f"Content model outer fold {fold_number}",
        )
        content_model = build_content_model(fold_seed)
        train_content_score = cross_val_predict(
            content_model,
            text.iloc[train_idx].reset_index(drop=True),
            train_labels,
            cv=content_splits,
            method="predict_proba",
        )[:, 1]
        content_model.fit(text.iloc[train_idx], labels.iloc[train_idx])
        validation_content_score = content_model.predict_proba(text.iloc[validation_idx])[:, 1]

        temporary_topic_model = TOPIC_MODEL_PATH.with_name(
            f"topic_model_outer_fold_{fold_number}.joblib"
        )
        train_topics = fit_topic_features(
            text.iloc[train_idx], N_TOPICS, temporary_topic_model, fold_seed
        )
        validation_topics = TopicFeaturizer(temporary_topic_model).transform(
            text.iloc[validation_idx]
        )
        temporary_topic_model.unlink(missing_ok=True)

        X_train = pd.concat(
            [
                numeric_base.iloc[train_idx].reset_index(drop=True),
                train_topics.reset_index(drop=True),
            ],
            axis=1,
        ).assign(content_score=train_content_score)
        X_validation = pd.concat(
            [
                numeric_base.iloc[validation_idx].reset_index(drop=True),
                validation_topics.reset_index(drop=True),
            ],
            axis=1,
        ).assign(content_score=validation_content_score)

        model = train_model(X_train, train_labels, fold_seed)
        calibrator, threshold, _, _, _ = fit_calibrator(
            X_train,
            train_labels,
            train_groups,
            fold_seed,
            strata=train_strata,
        )
        raw = model.predict_proba(X_validation)[:, 1]
        calibrated = apply_calibrator(calibrator, raw)

        raw_probabilities[validation_idx] = raw
        calibrated_probabilities[validation_idx] = calibrated
        row_thresholds[validation_idx] = threshold

    if any(
        np.isnan(values).any()
        for values in (raw_probabilities, calibrated_probabilities, row_thresholds)
    ):
        raise RuntimeError("Outer CV did not score every row exactly once")

    oof_df = pd.DataFrame(
        {
            "example_id": stable_ids,
            SOURCE: df[SOURCE].astype(str),
            GROUP: groups,
            TARGET: labels,
            "outer_fold": outer_assignments,
            "raw_probability": raw_probabilities,
            "calibrated_probability": calibrated_probabilities,
            "classification_threshold": row_thresholds,
            "predicted_label": (calibrated_probabilities >= row_thresholds).astype(int),
            "dataset_version": args.dataset_version,
            "virality_contract_fingerprint": virality_fingerprint,
            "evaluation_protocol_fingerprint": evaluation_protocol[
                "evaluation_protocol_fingerprint"
            ],
            "evaluation_folds_fingerprint": folds_manifest[
                "evaluation_folds_fingerprint"
            ],
        }
    )
    if not oof_df["example_id"].is_unique or len(oof_df) != len(df):
        raise ValueError("OOF predictions must contain every example exactly once")
    args.oof_predictions.parent.mkdir(parents=True, exist_ok=True)
    oof_df.to_parquet(args.oof_predictions, index=False)
    lineage["oof_predictions_sha256"] = file_sha256(args.oof_predictions)

    overall = _evaluate_subset(oof_df, seed=args.seed)
    per_source = {
        str(source): _evaluate_subset(
            oof_df.loc[oof_df[SOURCE].astype(str).str.lower().eq(str(source).lower())],
            seed=args.seed,
        )
        for source in sorted(oof_df[SOURCE].astype(str).str.lower().unique())
    }
    evaluation_payload: dict[str, object] = {
        "schema_version": "evaluation-v2",
        "dataset_version": args.dataset_version,
        "virality_contract_fingerprint": virality_fingerprint,
        "evaluation_protocol_fingerprint": evaluation_protocol[
            "evaluation_protocol_fingerprint"
        ],
        "evaluation_folds_fingerprint": folds_manifest["evaluation_folds_fingerprint"],
        "oof_predictions_sha256": lineage["oof_predictions_sha256"],
        "overall": overall,
        "per_source": per_source,
        # Compatibility aliases used by the current report generator.
        "overall_raw": overall["raw"]["metrics"],
        "bootstrap_raw": overall["raw"]["bootstrap"],
        "overall_calibrated": overall["calibrated"]["metrics"],
        "bootstrap_calibrated": overall["calibrated"]["bootstrap"],
        "per_source_raw": {
            source: values["raw"]["metrics"] for source, values in per_source.items()
        },
        "per_source_calibrated": {
            source: values["calibrated"]["metrics"]
            for source, values in per_source.items()
        },
        "per_source_bootstrap_raw": {
            source: values["raw"]["bootstrap"] for source, values in per_source.items()
        },
        "per_source_bootstrap_calibrated": {
            source: values["calibrated"]["bootstrap"]
            for source, values in per_source.items()
        },
    }
    evaluation_payload["evaluation_fingerprint"] = fingerprint(evaluation_payload)
    write_json(args.metrics_output, evaluation_payload)
    lineage["evaluation_fingerprint"] = evaluation_payload["evaluation_fingerprint"]
    lineage["metrics_sha256"] = file_sha256(args.metrics_output)

    print("OOF calibrated PR-AUC:", overall["calibrated"]["metrics"].get("pr_auc"))
    print("OOF calibrated ROC-AUC:", overall["calibrated"]["metrics"].get("roc_auc"))
    print("OOF calibrated ECE:", overall["calibrated"]["metrics"].get("ece"))

    print("Training final deployment model on all eligible rows...")
    final_content_splits = _validated_grouped_splits(
        strata=strata,
        labels=labels,
        groups=groups,
        n_splits=CONTENT_MODEL_FOLDS,
        seed=args.seed,
        context="Final content model",
    )
    final_content_model = build_content_model(args.seed)
    final_content_score = cross_val_predict(
        final_content_model,
        text,
        labels,
        cv=final_content_splits,
        method="predict_proba",
    )[:, 1]
    final_content_model.fit(text, labels)
    final_topics = fit_topic_features(text, N_TOPICS, TOPIC_MODEL_PATH, args.seed)
    X_all = pd.concat([numeric_base, final_topics.reset_index(drop=True)], axis=1).assign(
        content_score=final_content_score
    )
    final_model = train_model(X_all, labels, args.seed)
    final_calibrator, final_threshold, _, _, _ = fit_calibrator(
        X_all,
        labels,
        groups,
        args.seed,
        strata=strata,
    )

    args.model.parent.mkdir(parents=True, exist_ok=True)
    bundle = {
        "schema_version": "model-bundle-v2",
        "model": final_model,
        "content_model": final_content_model,
        "calibrator": final_calibrator,
        "classification_probability_threshold": final_threshold,
        "features": base_features + ["content_score"],
        "dataset_version": args.dataset_version,
        "dataset_lineage": dataset_lineage,
        "audience_features_included": dataset_lineage is None,
        "role_feature_contract": role_feature_contract(),
        "lineage": compact_grouped_lineage(lineage),
        **virality_lineage,
    }
    joblib.dump(bundle, args.model)
    lineage["model_sha256"] = file_sha256(args.model)
    lineage["status"] = "trained"
    write_json(args.lineage_output, lineage)

    if dataset_lineage:
        sidecar = {
            "artifact_type": "stage1_viral_model",
            "artifact_file": args.model.name,
            "artifact_sha256": lineage["model_sha256"],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "dataset_lineage": dataset_lineage,
            **virality_lineage,
            "classification_probability_threshold": final_threshold,
            "audience_features_included": False,
            "role_feature_contract": role_feature_contract(),
            "experiment_id": lineage["experiment_id"],
            "evaluation_protocol_fingerprint": lineage[
                "evaluation_protocol_fingerprint"
            ],
            "evaluation_folds_fingerprint": lineage["evaluation_folds_fingerprint"],
        }
        lineage_path = model_lineage_path(args.model)
        lineage_path.write_text(
            json.dumps(sidecar, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    print(f"Saved final deployment model -> {args.model}")
    print("Evaluation complete.")


if __name__ == "__main__":
    main()
