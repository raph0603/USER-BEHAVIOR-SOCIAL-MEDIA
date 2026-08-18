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
    groups: pd.Series,
    seed: int,
    model_kwargs: dict | None = None,
    sample_weight: np.ndarray | None = None,
) -> tuple[LogisticRegression, float, np.ndarray, np.ndarray, np.ndarray]:
    model_kwargs = model_kwargs or {}
    weights = None if sample_weight is None else np.asarray(sample_weight, dtype=float)
    oof = np.zeros(len(X_train))
    fold_assignments = np.zeros(len(X_train), dtype=int)
    
    if groups.nunique() < CALIBRATION_FOLDS:
        raise ValueError(f"Cannot form {CALIBRATION_FOLDS} folds with {groups.nunique()} groups")
    
    # StratifiedGroupKFold for calibration
    splitter = StratifiedGroupKFold(n_splits=CALIBRATION_FOLDS, shuffle=True, random_state=seed)
    
    try:
        splits = list(splitter.split(X_train, y_train, groups))
    except Exception as e:
        raise ValueError(f"Failed to form calibration folds: {e}")
        
    for i, (fit_idx, held_idx) in enumerate(splits, start=1):
        if len(set(groups.iloc[fit_idx]).intersection(set(groups.iloc[held_idx]))) > 0:
            raise ValueError("Author leakage detected between calibration train and holdout")
            
        fold_model = train_model(
            X_train.iloc[fit_idx],
            y_train.iloc[fit_idx],
            seed,
            model_kwargs,
            None if weights is None else weights[fit_idx],
        )
        oof[held_idx] = fold_model.predict_proba(xgb.DMatrix(X_train.iloc[held_idx]))[:, 1]
        fold_assignments[held_idx] = i
        
    if (fold_assignments == 0).any():
        raise ValueError("Not all rows received a calibration OOF prediction")
        
    calibrator = LogisticRegression(**PLATT_CALIBRATION["logistic_regression"], random_state=seed)
    calibrator.fit(logit(oof).reshape(-1, 1), y_train)
    
    cal_oof = apply_calibrator(calibrator, oof)
    threshold = best_f1_threshold(y_train, cal_oof)
    
    return calibrator, threshold, oof, cal_oof, fold_assignments

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-version", type=str)
    parser.add_argument("--dataset-manifest", type=Path)
    parser.add_argument("--environment-manifest", type=Path, default=DEFAULT_ENVIRONMENT_MANIFEST)
    parser.add_argument("--training-config-output", type=Path, default=DEFAULT_TRAINING_CONFIG)
    parser.add_argument("--lineage-output", type=Path, default=DEFAULT_LINEAGE)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--oof-predictions", type=Path, default=DEFAULT_OOF_PREDICTIONS)
    parser.add_argument("--evaluation-protocol", type=Path, default=DEFAULT_EVALUATION_PROTOCOL)
    parser.add_argument("--cv-folds-manifest", type=Path, default=DEFAULT_FOLDS_MANIFEST)
    parser.add_argument("--metrics-output", type=Path, default=DEFAULT_METRICS_OUTPUT)
    parser.add_argument("--virality-contract-fingerprint", type=str)
    parser.add_argument("--virality-policy", type=str)
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--expected-lineage", type=Path)
    parser.add_argument("--official-run", action="store_true")
    args = parser.parse_args()

    print("=== Training Unified Source-Agnostic Content Model (Stage 1) ===")

    dataset_manifest = _dataset_manifest(args.dataset_manifest, args.dataset_version)
    environment_manifest = load_json(args.environment_manifest)
    validate_environment_manifest(environment_manifest, args.official_run)

    training_config = resolved_training_config(
        feature_schema=CONTENT_FEATURES,
        seed=args.seed,
        virality_contract_fingerprint=args.virality_contract_fingerprint,
        virality_policy=args.virality_policy,
        # Force outer inductive CV configuration
        outer_split={"strategy": "stratified_group_k_fold", "n_splits": 5, "group": GROUP, "stratify": [SOURCE, TARGET]},
        nmf_scope="outer_training_inductive",
    )
    validate_training_config(training_config)
    
    # ---------------------------------------------------------
    # Generate Folds and Evaluation Protocol BEFORE fitting
    # ---------------------------------------------------------
    from train.evaluation_metrics import ECE_N_BINS, ECE_BINNING, BOOTSTRAP_ITERATIONS, BOOTSTRAP_CONFIDENCE_LEVEL, BOOTSTRAP_UNIT
    
    evaluation_protocol = {
        "schema_version": "evaluation-protocol-v1",
        "outer_evaluation": {
            "method": "StratifiedGroupKFold",
            "n_splits": 5,
            "grouping_column": GROUP,
            "stratification_targets": [SOURCE, TARGET],
            "shuffle": True,
            "seed": args.seed,
        },
        "preprocessing": {
            "nmf": "outer_training_inductive",
            "tfidf": "outer_training_inductive"
        },
        "calibration": {
            "method": "StratifiedGroupKFold",
            "n_splits": CALIBRATION_FOLDS,
            "grouping_column": GROUP,
            "stratification_target": TARGET,
            "shuffle": True,
            "seed": args.seed,
        },
        "threshold": {
            "strategy": "best_f1_score"
        },
        "expected_calibration_error": {
            "n_bins": ECE_N_BINS,
            "binning": ECE_BINNING,
            "range": [0.0, 1.0]
        },
        "bootstrap": {
            "unit": BOOTSTRAP_UNIT,
            "iterations": BOOTSTRAP_ITERATIONS,
            "confidence_level": BOOTSTRAP_CONFIDENCE_LEVEL,
            "seed": args.seed,
            "interval_method": "percentile"
        }
    }
    evaluation_protocol_fingerprint = fingerprint(evaluation_protocol)
    evaluation_protocol["evaluation_protocol_fingerprint"] = evaluation_protocol_fingerprint
    write_json(args.evaluation_protocol, evaluation_protocol)
    
    cv_folds_manifest = {
        "schema_version": "cv-folds-manifest-v1",
        "dataset_version": args.dataset_version,
        "virality_contract_fingerprint": args.virality_contract_fingerprint,
        "evaluation_protocol_fingerprint": evaluation_protocol_fingerprint,
        "method": "StratifiedGroupKFold",
        "n_splits": 5,
        "grouping_column": GROUP,
        "seed": args.seed,
        "folds": {}  # populated during fit
    }

    df = pd.read_parquet(args.data)
    validate_dataset_version(df, args.dataset_version)
    validate_virality_compatibility(df, args.virality_contract_fingerprint, args.virality_policy)

    features = feature_columns(df)
    for col in features:
        if col not in df.columns:
            raise ValueError(f"Feature {col} is missing from training data")

    y = df[TARGET]
    groups = df[GROUP].fillna(df.index.to_series().astype(str))
    
    # Pre-calculate the 5 folds
    outer_splitter = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=args.seed)
    
    # We must construct a composite stratify target: source + viral
    strata = df[SOURCE].astype(str) + "_" + df[TARGET].astype(str)
    
    outer_folds = list(outer_splitter.split(df, strata, groups))
    
    fold_assignments = np.zeros(len(df), dtype=int)
    for fold_idx, (train_idx, test_idx) in enumerate(outer_folds, start=1):
        fold_assignments[test_idx] = fold_idx
        # Populate cv_folds_manifest
        cv_folds_manifest["folds"][str(fold_idx)] = {
            "train_size": len(train_idx),
            "test_size": len(test_idx),
        }
        
    cv_folds_fingerprint = fingerprint(cv_folds_manifest)
    cv_folds_manifest["evaluation_folds_fingerprint"] = cv_folds_fingerprint
    write_json(args.cv_folds_manifest, cv_folds_manifest)

    # Lineage must be recorded BEFORE fitting
    lineage = build_experiment_lineage(
        dataset_manifest=dataset_manifest,
        environment_manifest=environment_manifest,
        training_config=training_config,
        evaluation_protocol_fingerprint=evaluation_protocol_fingerprint,
        evaluation_folds_fingerprint=cv_folds_fingerprint,
        official_run=args.official_run,
    )
    validate_lineage_match(lineage, args.expected_lineage)
    write_json(args.training_config_output, training_config)
    write_json(args.lineage_output, lineage)
    
    # ---------------------------------------------------------
    # OUTER 5-FOLD CV
    # ---------------------------------------------------------
    print(f"Executing {len(outer_folds)}-fold StratifiedGroupKFold out-of-fold evaluation...")
    
    raw_oof = np.zeros(len(df))
    cal_oof = np.zeros(len(df))
    fold_thresholds = np.zeros(len(outer_folds))
    
    for fold_idx, (train_idx, test_idx) in enumerate(outer_folds, start=1):
        print(f"  Fold {fold_idx}: fitting...")
        
        train_df = df.iloc[train_idx].copy()
        test_df = df.iloc[test_idx].copy()
        
        # 1. Inductive NMF
        print("    Fitting inductive NMF...")
        topic_features = fit_topic_features(train_df[TEXT], N_TOPICS, args.seed)
        for i in range(N_TOPICS):
            col = f"topic_{i}"
            train_df[col] = topic_features.iloc[:, i].values
        
        test_topics = TopicFeaturizer(TOPIC_MODEL_PATH).transform(test_df[TEXT])
        for i in range(N_TOPICS):
            col = f"topic_{i}"
            test_df[col] = test_topics.iloc[:, i].values
            
        # 2. Inductive TF-IDF & XGBoost Dataset
        X_train_fold, y_train_fold, _ = build_content_model(train_df, features)
        X_test_fold, y_test_fold, _ = build_content_model(test_df, features)
        
        # 3. Inner Calibration OOF
        print("    Fitting calibrator via inner grouped CV...")
        fold_groups = groups.iloc[train_idx]
        calibrator, threshold, _, _, _ = fit_calibrator(X_train_fold, y_train_fold, fold_groups, args.seed)
        
        # 4. Fit Full Outer-Train Model
        fold_model = train_model(X_train_fold, y_train_fold, args.seed)
        
        # 5. Predict Outer-Validation
        raw_preds = fold_model.predict_proba(xgb.DMatrix(X_test_fold))[:, 1]
        cal_preds = apply_calibrator(calibrator, raw_preds)
        
        raw_oof[test_idx] = raw_preds
        cal_oof[test_idx] = cal_preds
        fold_thresholds[fold_idx-1] = threshold

    avg_threshold = float(fold_thresholds.mean())

    # ---------------------------------------------------------
    # GENERATE OOF ARTIFACT
    # ---------------------------------------------------------
    oof_df = pd.DataFrame({
        "example_id": df["example_id"].values if "example_id" in df.columns else df.index.values.astype(str),
        "source": df[SOURCE].values,
        "author_hash": df[GROUP].values,
        "viral": df[TARGET].values,
        "outer_fold": fold_assignments,
        "raw_probability": raw_oof,
        "calibrated_probability": cal_oof,
        "classification_threshold": avg_threshold,
        "predicted_label": (cal_oof >= avg_threshold).astype(bool),
        "dataset_version": args.dataset_version,
        "virality_contract_fingerprint": args.virality_contract_fingerprint,
        "evaluation_protocol_fingerprint": evaluation_protocol_fingerprint,
        "evaluation_folds_fingerprint": cv_folds_fingerprint,
    })
    oof_df.to_parquet(args.oof_predictions, index=False)
    lineage["oof_predictions_sha256"] = file_sha256(args.oof_predictions)
    
    # ---------------------------------------------------------
    # GENERATE EVALUATION JSON
    # ---------------------------------------------------------
    from train.evaluation_metrics import metric_summary, author_bootstrap_metrics
    
    def _evaluate_subset(mask):
        subset = oof_df[mask]
        return {
            "raw": {
                "metrics": metric_summary(subset["viral"], subset["raw_probability"], subset["classification_threshold"]),
                "bootstrap": author_bootstrap_metrics(subset["viral"], subset["raw_probability"], subset["author_hash"], seed=args.seed)
            },
            "calibrated": {
                "metrics": metric_summary(subset["viral"], subset["calibrated_probability"], subset["classification_threshold"]),
                "bootstrap": author_bootstrap_metrics(subset["viral"], subset["calibrated_probability"], subset["author_hash"], seed=args.seed)
            }
        }
        
    evaluation_payload = {
        "schema_version": "evaluation-v1",
        "dataset_version": args.dataset_version,
        "virality_contract_fingerprint": args.virality_contract_fingerprint,
        "evaluation_protocol_fingerprint": evaluation_protocol_fingerprint,
        "evaluation_folds_fingerprint": cv_folds_fingerprint,
        "oof_predictions_sha256": lineage["oof_predictions_sha256"],
        "overall": _evaluate_subset(pd.Series(True, index=oof_df.index)),
        "per_source": {}
    }
    
    for src in sorted(oof_df["source"].unique()):
        evaluation_payload["per_source"][str(src)] = _evaluate_subset(oof_df["source"] == src)
        
    eval_fingerprint = fingerprint(evaluation_payload)
    evaluation_payload["evaluation_fingerprint"] = eval_fingerprint
    write_json(args.metrics_output, evaluation_payload)
    
    lineage["evaluation_fingerprint"] = eval_fingerprint
    lineage["metrics_sha256"] = file_sha256(args.metrics_output)

    # ---------------------------------------------------------
    # FINAL DEPLOYMENT MODEL (Transductive on full dataset)
    # ---------------------------------------------------------
    print("Fitting final deployment model on full dataset...")
    
    # Inductive features on full dataset for final model
    topic_features = fit_topic_features(df[TEXT], N_TOPICS, args.seed)
    for i in range(N_TOPICS):
        col = f"topic_{i}"
        df[col] = topic_features.iloc[:, i].values
        
    X_full, y_full, _ = build_content_model(df, features)
    calibrator, threshold, _, _, _ = fit_calibrator(X_full, y_full, groups, args.seed)
    final_model = train_model(X_full, y_full, args.seed)

    args.model.parent.mkdir(parents=True, exist_ok=True)
    bundle = {
        "model": final_model,
        "calibrator": calibrator,
        "threshold": threshold,
        "features": features,
        "feature_schema": CONTENT_FEATURES,
        "role_feature_contract": role_feature_contract(),
        "dataset_lineage": load_dataset_lineage(dataset_manifest),
        "virality_lineage": dataset_virality_lineage(df),
        "training_config_fingerprint": training_config.get("training_config_fingerprint"),
        "evaluation_protocol_fingerprint": evaluation_protocol_fingerprint,
        "evaluation_folds_fingerprint": cv_folds_fingerprint,
        "experiment_id": experiment_id(lineage),
    }
    joblib.dump(bundle, args.model)
    lineage["model_sha256"] = file_sha256(args.model)
    write_json(args.lineage_output, lineage)
    print(f"Done! Final artifacts saved.")
