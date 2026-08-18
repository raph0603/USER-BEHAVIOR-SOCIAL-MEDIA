"""Train the unified multi-source viral classifier (Stage 1) with Official Grouped CV.

One XGBoost model on the unified content features + source one-hot. The viral
label was already defined per-source in preprocess/build_dataset.py.

Split is a Stratified Group K-Fold grouped by author_hash so the same author never
appears in both train and test, preserving the natural source distribution.
Paper-grade metrics are produced using the strictly out-of-fold predictions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from sklearn.model_selection import GroupKFold, StratifiedGroupKFold, cross_val_predict

ML_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ML_ROOT.parent
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.reproducibility import (
    compact_lineage,
    experiment_id,
    file_sha256,
    load_json,
    validate_environment_manifest,
    validate_lineage_match,
    write_json,
    fingerprint
)
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
from features.topics import fit_topic_features, TopicFeaturizer, N_TOPICS, TOPIC_MODEL_PATH
from dataset_lineage import load_dataset_lineage, model_lineage_path
from role_contract import role_feature_contract
from virality_lineage import dataset_virality_lineage, validate_virality_compatibility
from train.evaluation_metrics import metric_summary, author_bootstrap_metrics

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
    prefixes = ["src_", "topic_"]
    if include_roles:
        prefixes.append("role_")
    if include_audience:
        prefixes.append("chan_")
    prefixes_tuple = tuple(prefixes)
    extra = sorted(c for c in df.columns if c.startswith(prefixes_tuple))
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
        pos = float(y_train.sum())
        neg = float(len(y_train) - y_train.sum())
    else:
        weights = np.asarray(sample_weight, dtype=float)
        labels = np.asarray(y_train, dtype=int)
        pos = float(weights[labels == 1].sum())
        neg = float(weights[labels == 0].sum())
    parameters = {**XGBOOST_MODEL, "colsample_bytree": colsample_bytree}
    model = xgb.XGBClassifier(
        **parameters,
        scale_pos_weight=(neg / pos) if pos else 1.0,
        random_state=seed,
    )
    model.fit(X_train, y_train, sample_weight=sample_weight)
    return model

def logit(proba) -> np.ndarray:
    p = np.clip(np.asarray(proba, dtype=float), 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))

def best_f1_threshold(y_true, proba) -> float:
    grid = np.round(
        np.arange(
            DECISION_THRESHOLD["grid_start"],
            DECISION_THRESHOLD["grid_stop_exclusive"],
            DECISION_THRESHOLD["grid_step"],
        ),
        2,
    )
    scores = [f1_score(y_true, (proba >= t).astype(int), zero_division=0) for t in grid]
    return float(grid[int(np.argmax(scores))])

def fit_calibrator(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    groups,
    seed: int,
    *,
    model_kwargs: dict | None = None,
    sample_weight=None,
):
    model_kwargs = model_kwargs or {}
    weights = None if sample_weight is None else np.asarray(sample_weight, dtype=float)
    oof = np.zeros(len(X_train))
    splitter = GroupKFold(n_splits=CALIBRATION_FOLDS)
    for fit_idx, held_idx in splitter.split(X_train, y_train, groups):
        fold_model = train_model(
            X_train.iloc[fit_idx],
            y_train.iloc[fit_idx],
            seed,
            sample_weight=None if weights is None else weights[fit_idx],
            **model_kwargs,
        )
        oof[held_idx] = fold_model.predict_proba(X_train.iloc[held_idx])[:, 1]
    calibrator = LogisticRegression(**PLATT_CALIBRATION["logistic_regression"], random_state=seed)
    calibrator.fit(logit(oof).reshape(-1, 1), y_train)
    threshold = best_f1_threshold(y_train, apply_calibrator(calibrator, oof))
    return calibrator, threshold

def apply_calibrator(calibrator, proba) -> np.ndarray:
    return calibrator.predict_proba(logit(proba).reshape(-1, 1))[:, 1]

def shap_importance(model: xgb.XGBClassifier, X: pd.DataFrame) -> pd.Series:
    booster = model.get_booster()
    contribs = booster.predict(xgb.DMatrix(X), pred_contribs=True)
    mean_abs = np.abs(contribs[:, :-1]).mean(axis=0)
    return pd.Series(mean_abs, index=X.columns).sort_values(ascending=False)

def _dataset_manifest(path: Path | None, expected_version: str | None) -> dict:
    if path is None:
        return {}
    manifest = load_json(path)
    if expected_version and manifest.get("dataset_version") != expected_version:
        raise ValueError("Dataset manifest version does not match --dataset-version")
    return manifest

def _source_snapshots(manifest: dict) -> dict[str, int]:
    raw = manifest.get("iceberg_snapshots_json", manifest.get("source_snapshots", {}))
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, dict):
        raise ValueError("Dataset manifest source snapshots must be an object")
    return {str(table): int(snapshot) for table, snapshot in sorted(raw.items())}

def build_experiment_lineage(
    *,
    dataset_manifest: dict,
    environment_manifest: dict,
    training_config: dict,
    evaluation_protocol_fingerprint: str,
    evaluation_folds_fingerprint: str,
    official_run: bool,
) -> dict:
    code = environment_manifest.get("code", {})
    labeling = dataset_manifest.get("labeling", {})
    lineage = {
        "schema_version": "experiment-lineage-v2",
        "official_run": bool(official_run),
        "status": "training",
        "dataset_version": dataset_manifest.get("dataset_version"),
        "dataset_fingerprint": dataset_manifest.get("dataset_fingerprint"),
        "silver_snapshot_ids": _source_snapshots(dataset_manifest),
        "gold_table": dataset_manifest.get("gold_table"),
        "gold_snapshot_id": dataset_manifest.get("gold_snapshot_id"),
        "manifest_sha256": dataset_manifest.get("manifest_sha256"),
        "git_commit": code.get("git_commit"),
        "git_dirty": code.get("git_dirty"),
        "environment_fingerprint": environment_manifest.get("environment_fingerprint"),
        "training_config_fingerprint": training_config.get("training_config_fingerprint"),
        "virality_contract_fingerprint": training_config.get("virality_contract_fingerprint"),
        "virality_policy": labeling.get("policy") if isinstance(labeling, dict) else None,
        "evaluation_protocol_fingerprint": evaluation_protocol_fingerprint,
        "evaluation_folds_fingerprint": evaluation_folds_fingerprint,
        "model_sha256": None,
        "oof_predictions_sha256": None,
        "metrics_sha256": None,
    }
    required = (
        "dataset_version",
        "dataset_fingerprint",
        "manifest_sha256",
        "git_commit",
        "environment_fingerprint",
        "training_config_fingerprint",
        "virality_contract_fingerprint",
        "virality_policy",
        "evaluation_protocol_fingerprint",
        "evaluation_folds_fingerprint",
    )
    if official_run and any(not lineage.get(field) for field in required):
        missing = [field for field in required if not lineage.get(field)]
        raise ValueError(f"Official experiment lineage is incomplete: {', '.join(missing)}")
    lineage["experiment_id"] = experiment_id(lineage)
    return lineage

def main() -> None:
    parser = argparse.ArgumentParser(description="Train and evaluate Stage-1 with official grouped CV.")
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

    df = pd.read_parquet(args.data)
    dataset_lineage = None
    if args.dataset_version and not args.dataset_manifest:
        raise ValueError("Official model training requires --dataset-manifest")
    if args.dataset_manifest:
        _, dataset_lineage = load_dataset_lineage(
            args.dataset_manifest,
            expected_dataset_version=args.dataset_version,
        )
        args.dataset_version = str(dataset_lineage["dataset_version"])
    validate_dataset_version(df, args.dataset_version)
    virality_lineage = dataset_virality_lineage(df)
    validate_virality_compatibility(
        virality_lineage,
        expected_fingerprint=args.virality_contract_fingerprint,
        expected_policy=args.virality_policy,
    )

    features = feature_columns(
        df,
        include_audience=dataset_lineage is None,
        include_roles=not args.official_run,
    )
    y = df[TARGET].astype(int)
    text = df[TEXT].astype(str)
    
    fallback = df.index.to_series().map(lambda idx: f"missing-author-{idx}")
    groups = df[GROUP].astype("string").fillna(fallback)
    strata = source_class_strata(df)
    id_column, stable_ids = observation_ids(df, official=args.official_run)

    n_splits = 5
    support = strata.value_counts()
    if int(support.min()) < n_splits:
        raise ValueError(f"Each source/class stratum needs at least {n_splits} rows; minimum support is {int(support.min())}")
    if groups.nunique() < n_splits:
        raise ValueError(f"K-fold validation needs at least {n_splits} authors; received {groups.nunique()}")

    splitter = StratifiedGroupKFold(n_splits=n_splits, shuffle=True, random_state=args.seed)
    folds = list(splitter.split(df, strata, groups))
    
    # Verify strict evaluation requirements
    validation_rows = []
    for fold_number, (train_idx, validation_idx) in enumerate(folds, start=1):
        if set(groups.iloc[train_idx]) & set(groups.iloc[validation_idx]):
            raise RuntimeError(f"Author leakage detected in fold {fold_number}")
        if df.iloc[validation_idx][TARGET].nunique() < 2:
            raise ValueError(f"Fold {fold_number} does not contain both viral classes")
        validation_rows.extend(validation_idx.tolist())
    if sorted(validation_rows) != list(range(len(df))):
        raise RuntimeError("Validation folds do not cover every eligible row exactly once")

    # Versioned Folds Manifest
    fold_assignments = {}
    for fold_number, (_, validation_idx) in enumerate(folds, start=1):
        for idx in validation_idx:
            fold_assignments[str(stable_ids.iloc[idx])] = fold_number
    
    folds_manifest = {
        "schema_version": "cv-folds-manifest-v2",
        "strategy": "stratified_group_k_fold",
        "group_column": GROUP,
        "stratification": "source x viral",
        "n_splits": n_splits,
        "seed": args.seed,
        "dataset_version": args.dataset_version,
        "virality_contract_fingerprint": args.virality_contract_fingerprint,
        "folds": fold_assignments,
    }
    folds_manifest["evaluation_folds_fingerprint"] = fingerprint(folds_manifest)
    args.cv_folds_manifest.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.cv_folds_manifest, folds_manifest)

    # Pre-build features
    base_features = features.copy()
    base_features.extend([f"topic_{i}" for i in range(N_TOPICS)])
    numeric_base = df[[f for f in features if not f.startswith("topic_")]].astype(float)

    raw_probabilities = np.full(len(df), np.nan, dtype=float)
    calibrated_probabilities = np.full(len(df), np.nan, dtype=float)
    fold_thresholds = np.full(len(df), np.nan, dtype=float)

    print(f"Executing {n_splits}-fold StratifiedGroupKFold CV...")
    for fold_number, (train_idx, validation_idx) in enumerate(folds, start=1):
        fold_seed = args.seed + fold_number
        y_train = y.iloc[train_idx]
        train_groups = groups.iloc[train_idx]
        
        content_model = build_content_model(fold_seed)
        inner_cv = StratifiedGroupKFold(n_splits=CONTENT_MODEL_FOLDS, shuffle=True, random_state=fold_seed)
        train_content_score = cross_val_predict(
            content_model,
            text.iloc[train_idx],
            y_train,
            groups=train_groups,
            cv=inner_cv,
            method="predict_proba",
        )[:, 1]
        content_model.fit(text.iloc[train_idx], y_train)
        validation_content_score = content_model.predict_proba(text.iloc[validation_idx])[:, 1]

        # NMF strictly Outer-Train Only
        train_topics = fit_topic_features(
            text.iloc[train_idx],
            N_TOPICS,
            TOPIC_MODEL_PATH.with_name(f"temp_nmf_{fold_number}.joblib"),
            fold_seed,
        )
        topic_featurizer = TopicFeaturizer(TOPIC_MODEL_PATH.with_name(f"temp_nmf_{fold_number}.joblib"))
        validation_topics = topic_featurizer.transform(text.iloc[validation_idx])
        TOPIC_MODEL_PATH.with_name(f"temp_nmf_{fold_number}.joblib").unlink(missing_ok=True)

        X_train = pd.concat([
            numeric_base.iloc[train_idx].reset_index(drop=True),
            train_topics.reset_index(drop=True)
        ], axis=1).assign(content_score=train_content_score)
        
        X_validation = pd.concat([
            numeric_base.iloc[validation_idx].reset_index(drop=True),
            validation_topics.reset_index(drop=True)
        ], axis=1).assign(content_score=validation_content_score)

        model = train_model(X_train, y_train, fold_seed)
        calibrator, threshold = fit_calibrator(X_train, y_train, train_groups, fold_seed)
        
        raw_proba = model.predict_proba(X_validation)[:, 1]
        calibrated_proba = apply_calibrator(calibrator, raw_proba)
        
        raw_probabilities[validation_idx] = raw_proba
        calibrated_probabilities[validation_idx] = calibrated_proba
        fold_thresholds[validation_idx] = threshold

    if np.isnan(raw_probabilities).any() or np.isnan(calibrated_probabilities).any():
        raise RuntimeError("Cross-validation did not score every row")

    # Persist OOF predictions
    oof_df = pd.DataFrame({
        "example_id": stable_ids,
        "source": df[SOURCE],
        GROUP: groups,
        "viral": y,
        "outer_fold": df.index.map(lambda i: folds_manifest["folds"].get(str(stable_ids.iloc[i]))),
        "raw_probability": raw_probabilities,
        "calibrated_probability": calibrated_probabilities,
        "classification_threshold": fold_thresholds,
        "predicted_label": (calibrated_probabilities >= fold_thresholds).astype(int),
        "dataset_version": args.dataset_version,
    })
    if not oof_df["example_id"].is_unique:
        raise ValueError("OOF example_id is not unique")
    if len(oof_df) != len(df):
        raise ValueError("OOF predictions length does not match dataset length")
        
    args.oof_predictions.parent.mkdir(parents=True, exist_ok=True)
    oof_df.to_parquet(args.oof_predictions, index=False)
    oof_sha256 = file_sha256(args.oof_predictions)

    # Evaluation Protocol
    dataset_manifest = _dataset_manifest(args.dataset_manifest, args.dataset_version)
    if args.environment_manifest is None:
        if args.official_run:
            raise ValueError("Official training requires --environment-manifest")
        environment_manifest = {"code": {}, "environment_fingerprint": None}
    else:
        environment_manifest = load_json(args.environment_manifest)
        validate_environment_manifest(environment_manifest)
        
    training_config = resolved_training_config(
        seed=args.seed,
        test_size=0.2, # Unused but required by config signature currently
        feature_columns=base_features + ["content_score"],
        feature_versions=(
            sorted(df["feature_version"].dropna().astype(str).unique())
            if "feature_version" in df.columns
            else []
        ),
        dataset_schema_version=str(dataset_manifest.get("schema_version") or "") or None,
        dataset_manifest=dataset_manifest,
        content_backend="tfidf_logistic_regression",
        auxiliary_artifacts={
            path.name: file_sha256(path)
            for path in (
                *((ML_ROOT / "models" / "rhetorical_role.joblib",) if not args.official_run else ()),
                ML_ROOT / "models" / "topic_model.joblib",
            )
            if path.is_file()
        },
        scale_pos_weight=(float(len(y) - y.sum()) / float(y.sum()) if float(y.sum()) else 1.0),
    )
    write_json(args.training_config_output, training_config)
    
    evaluation_protocol = {
        "schema_version": "evaluation-protocol-v1",
        "strategy": "stratified_group_k_fold",
        "group_column": GROUP,
        "stratification": "source x viral",
        "n_splits": n_splits,
        "shuffle": True,
        "random_seed": args.seed,
        "preprocessing_fit_scopes": {
            "tfidf": "outer_training_inductive",
            "nmf": "outer_training_inductive"
        },
        "content_model_inner_cv_strategy": "stratified_group_k_fold",
        "calibration_strategy": "platt_on_outer_training_oof",
        "threshold_strategy": "maximize_f1_on_outer_training_oof",
        "bootstrap_strategy": {
            "unit": GROUP,
            "iterations": 1000,
            "confidence_interval": 0.95
        },
        "metrics": ["roc_auc", "pr_auc", "brier", "ece", "f1", "precision", "recall"],
        "dataset_version": args.dataset_version,
        "virality_contract_fingerprint": args.virality_contract_fingerprint,
    }
    evaluation_protocol_fingerprint = fingerprint(evaluation_protocol)
    evaluation_protocol["evaluation_protocol_fingerprint"] = evaluation_protocol_fingerprint
    write_json(args.evaluation_protocol, evaluation_protocol)

    lineage = build_experiment_lineage(
        dataset_manifest=dataset_manifest,
        environment_manifest=environment_manifest,
        training_config=training_config,
        evaluation_protocol_fingerprint=evaluation_protocol_fingerprint,
        evaluation_folds_fingerprint=folds_manifest["evaluation_folds_fingerprint"],
        official_run=args.official_run,
    )
    if args.expected_lineage:
        validate_lineage_match(load_json(args.expected_lineage), lineage, context="Replay preflight")
    
    # Metrics
    overall = metric_summary(y, calibrated_probabilities, fold_thresholds)
    bootstrap = author_bootstrap_metrics(y, calibrated_probabilities, groups, seed=args.seed)
    overall_raw = metric_summary(y, raw_probabilities, fold_thresholds)
    bootstrap_raw = author_bootstrap_metrics(y, raw_probabilities, groups, seed=args.seed)

    per_source = {}
    normalized_sources = df[SOURCE].astype("string").str.lower()
    for source in sorted(normalized_sources.unique()):
        mask = normalized_sources.eq(source).to_numpy()
        per_source[str(source)] = metric_summary(y[mask], calibrated_probabilities[mask], fold_thresholds[mask])

    metrics_report = {
        "schema_version": "paper-metrics-v1",
        "dataset_version": args.dataset_version,
        "overall_calibrated": overall,
        "bootstrap_calibrated": bootstrap,
        "overall_raw": overall_raw,
        "bootstrap_raw": bootstrap_raw,
        "per_source_calibrated": per_source,
    }
    args.metrics_output.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.metrics_output, metrics_report)
    metrics_sha256 = file_sha256(args.metrics_output)

    print("OOF Calibrated PR-AUC:", overall.get("pr_auc"))
    print("OOF Calibrated ROC-AUC:", overall.get("roc_auc"))
    print("OOF Calibrated ECE:", overall.get("ece"))
    print("OOF Calibrated PR-AUC 95% CI:", bootstrap.get("pr_auc_ci95"))

    # Train Final Deployable Model
    print("\nTraining final deployment model on full dataset...")
    final_content_model = build_content_model(args.seed)
    final_inner_cv = StratifiedGroupKFold(n_splits=CONTENT_MODEL_FOLDS, shuffle=True, random_state=args.seed)
    final_content_score = cross_val_predict(
        final_content_model, text, y, groups=groups, cv=final_inner_cv, method="predict_proba"
    )[:, 1]
    final_content_model.fit(text, y)
    
    final_topics = fit_topic_features(text, N_TOPICS, TOPIC_MODEL_PATH, args.seed)
    
    X_all = pd.concat([
        numeric_base.reset_index(drop=True),
        final_topics.reset_index(drop=True)
    ], axis=1).assign(content_score=final_content_score)
    
    final_model = train_model(X_all, y, args.seed)
    final_calibrator, final_threshold = fit_calibrator(X_all, y, groups, args.seed)

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
        "lineage": compact_lineage(lineage),
        **virality_lineage,
    }
    joblib.dump(bundle, args.model)
    
    lineage["model_sha256"] = file_sha256(args.model)
    lineage["oof_predictions_sha256"] = oof_sha256
    lineage["metrics_sha256"] = metrics_sha256
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
        }
        lineage_path = model_lineage_path(args.model)
        lineage_path.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"Saved final deployment model -> {args.model}")
    print("Evaluation complete.")

if __name__ == "__main__":
    main()
