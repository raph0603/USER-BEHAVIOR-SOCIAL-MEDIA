"""Train the unified multi-source viral classifier (Stage 1).

One XGBoost model on the unified content features + source one-hot. The viral
label was already defined per-source in preprocess/build_dataset.py.

Split is grouped by author_hash so the same author never appears in both train
and test (avoids identity leakage). Metrics: PR-AUC (primary, data is imbalanced)
+ ROC-AUC. Global SHAP importance (via XGBoost pred_contribs) shows which
features drive virality — the basis for the explanation layer.
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
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    classification_report,
    f1_score,
    roc_auc_score,
)
from sklearn.model_selection import (
    GroupKFold,
    GroupShuffleSplit,
    StratifiedKFold,
    cross_val_predict,
)

ML_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ML_ROOT.parent
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.reproducibility import (
    build_split_manifest,
    compact_lineage,
    experiment_id,
    file_sha256,
    load_json,
    validate_environment_manifest,
    validate_lineage_match,
    validate_split_manifest,
    write_json,
)
from experiment_config import (
    CALIBRATION_FOLDS,
    CONTENT_MODEL_FOLDS,
    DECISION_THRESHOLD,
    DEFAULT_RANDOM_SEED,
    DEFAULT_TEST_SIZE,
    PLATT_CALIBRATION,
    XGBOOST_MODEL,
    resolved_training_config,
    validate_training_config,
)
from features.text_content import build_content_model
from dataset_lineage import load_dataset_lineage, model_lineage_path
from role_contract import role_feature_contract

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
DEFAULT_ENVIRONMENT_MANIFEST = ML_ROOT / "results" / "environment_manifest.json"
DEFAULT_TRAINING_CONFIG = ML_ROOT / "results" / "training_config.json"
DEFAULT_SPLIT_MANIFEST = ML_ROOT / "results" / "split_manifest.json"
DEFAULT_LINEAGE = ML_ROOT / "results" / "experiment_lineage.json"
TEXT = "clean_text"

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
TARGET = "viral"
GROUP = "author_hash"


def validate_dataset_version(df: pd.DataFrame, expected: str | None) -> None:
    """Verify that an official model run uses one exact dataset version."""

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


def split_indices(df: pd.DataFrame, test_size: float, seed: int):
    groups = df[GROUP].fillna(df.index.to_series().astype(str))
    splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=seed)
    return next(splitter.split(df, df[TARGET], groups))


def observation_ids(df: pd.DataFrame, *, official: bool) -> tuple[str, pd.Series]:
    """Resolve stable row identities used to persist the exact split."""

    for column in ("example_id", "content_id", "platform_event_id", "observation_id"):
        if column not in df.columns or df[column].isna().any():
            continue
        values = df[column].astype(str)
        if values.is_unique:
            return column, values
    if official:
        raise ValueError(
            "Official training data requires one complete unique stable identifier column "
            "(preferably example_id)"
        )
    return "row_index_nonofficial", pd.Series(
        [f"row-{index}" for index in range(len(df))], index=df.index, dtype="string"
    )


def content_scores(text: pd.Series, y: pd.Series, train_idx, test_idx, seed: int):
    """Out-of-fold P(viral) for train rows; full-train model applied to test (no leakage)."""
    model = build_content_model(seed)
    cv = StratifiedKFold(n_splits=CONTENT_MODEL_FOLDS, shuffle=True, random_state=seed)
    train_score = cross_val_predict(
        model, text.iloc[train_idx], y.iloc[train_idx], cv=cv, method="predict_proba"
    )[:, 1]
    model.fit(text.iloc[train_idx], y.iloc[train_idx])
    test_score = model.predict_proba(text.iloc[test_idx])[:, 1]
    return train_score, test_score, model


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
    # depth/learning-rate picked by grid search on author-grouped out-of-fold PR-AUC over
    # the train split (0.618 -> 0.631); at ~2.9k rows deeper trees only overfit.
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


def fit_calibrator(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    groups,
    seed: int,
    *,
    model_kwargs: dict | None = None,
    sample_weight=None,
):
    """Platt scaling so the returned probability means what it says.

    `scale_pos_weight` optimises ranking but systematically inflates the scores, and
    `viral_score` is shown to users as a probability. The scaler is fitted on
    out-of-fold scores — folds grouped by author, like the train/test split — so no row
    calibrates against a model that already saw its author, and no row is spent.

    Calibrating also invalidates the 0.5 cut-off: once the scores are honest, few of
    them pass 0.5 when only a quarter of posts go viral, so the decision threshold is
    re-picked here on the same out-of-fold scores and travels with the model.
    """
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


def apply_calibrator(calibrator, proba) -> np.ndarray:
    """Map raw model scores onto calibrated probabilities (monotonic: ranking is kept)."""
    return calibrator.predict_proba(logit(proba).reshape(-1, 1))[:, 1]


def evaluate(
    model: xgb.XGBClassifier,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    calibrator=None,
    threshold: float = 0.5,
) -> None:
    raw = model.predict_proba(X_test)[:, 1]
    proba = apply_calibrator(calibrator, raw) if calibrator is not None else raw
    print(f"PR-AUC : {average_precision_score(y_test, proba):.3f}")
    print(f"ROC-AUC: {roc_auc_score(y_test, proba):.3f}")
    print(f"Baseline viral rate (test): {y_test.mean():.3f}")
    if calibrator is not None:
        print(
            f"Brier  : {brier_score_loss(y_test, proba):.3f} calibrated "
            f"(raw {brier_score_loss(y_test, raw):.3f})"
        )
    print(f"Decision threshold (picked out-of-fold): {threshold:.2f}")
    print(classification_report(y_test, (proba >= threshold).astype(int), digits=3))


def shap_importance(model: xgb.XGBClassifier, X: pd.DataFrame) -> pd.Series:
    booster = model.get_booster()
    contribs = booster.predict(xgb.DMatrix(X), pred_contribs=True)  # last col = bias
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
    split_manifest: dict,
    official_run: bool,
) -> dict:
    code = environment_manifest.get("code", {})
    lineage = {
        "schema_version": "experiment-lineage-v1",
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
        "split_fingerprint": split_manifest.get("split_fingerprint"),
        "virality_contract_fingerprint": training_config.get("virality_contract_fingerprint"),
        "model_sha256": None,
        "determinism_contract": {
            "model_byte_identity_expected": False,
            "reason": (
                "Joblib and XGBoost do not guarantee byte-identical serialization across "
                "builds; replay compares holdout predictions and metrics."
            ),
            "prediction_absolute_tolerance": 1e-12,
            "metric_absolute_tolerance": 1e-12,
        },
    }
    required = (
        "dataset_version",
        "dataset_fingerprint",
        "manifest_sha256",
        "git_commit",
        "environment_fingerprint",
        "training_config_fingerprint",
        "split_fingerprint",
    )
    if official_run and any(not lineage.get(field) for field in required):
        missing = [field for field in required if not lineage.get(field)]
        raise ValueError(f"Official experiment lineage is incomplete: {', '.join(missing)}")
    lineage["experiment_id"] = experiment_id(lineage)
    return lineage


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the unified viral classifier.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument(
        "--bert-model-dir",
        type=Path,
        default=None,
        help="Folder of the Kaggle-trained BERT content model (required if data has content_score_bert).",
    )
    parser.add_argument("--test-size", type=float, default=DEFAULT_TEST_SIZE)
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument(
        "--dataset-version",
        help="Exact lakehouse dataset version embedded in the model artifact.",
    )
    parser.add_argument(
        "--dataset-manifest",
        type=Path,
        help="Validated manifest carrying the pinned Iceberg snapshots for an official run.",
    )
    parser.add_argument("--environment-manifest", type=Path)
    parser.add_argument("--training-config-output", type=Path, default=DEFAULT_TRAINING_CONFIG)
    parser.add_argument("--split-output", type=Path, default=DEFAULT_SPLIT_MANIFEST)
    parser.add_argument("--lineage-output", type=Path, default=DEFAULT_LINEAGE)
    parser.add_argument("--expected-lineage", type=Path)
    parser.add_argument("--official-run", action="store_true")
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
    # The supervised silver role corpus can overlap the outer holdout. Keep role
    # features available for explicit exploratory analyses, but never include them
    # in an official model until their fit is scoped to the outer training boundary.
    features = feature_columns(
        df,
        include_audience=dataset_lineage is None,
        include_roles=not args.official_run,
    )
    train_idx, test_idx = split_indices(df, args.test_size, args.seed)
    id_column, stable_ids = observation_ids(df, official=args.official_run)
    split_manifest = build_split_manifest(
        stable_ids.iloc[train_idx],
        stable_ids.iloc[test_idx],
        strategy="group_shuffle_split",
        group_column=GROUP,
        seed=args.seed,
        test_size=args.test_size,
        id_column=id_column,
    )
    validate_split_manifest(split_manifest)
    write_json(args.split_output, split_manifest)
    y = df[TARGET].astype(int)

    if "content_score_bert" in df.columns:
        if not args.bert_model_dir:
            raise SystemExit(
                "Data has 'content_score_bert' -> pass --bert-model-dir so serving uses the same BERT."
            )
        scores = df["content_score_bert"].to_numpy()  # out-of-fold from Kaggle -> leakage-free
        train_score, test_score = scores[train_idx], scores[test_idx]
        content_bundle = {"content_model_dir": str(args.bert_model_dir)}
        content_backend = "bert_precomputed"
        print("Using precomputed BERT content scores.")
    else:
        train_score, test_score, content_model = content_scores(
            df[TEXT].astype(str), y, train_idx, test_idx, args.seed
        )
        content_bundle = {"content_model": content_model}
        content_backend = "tfidf_logistic_regression"

    X = df[features].astype(float)
    features = features + ["content_score"]
    X_train = X.iloc[train_idx].assign(content_score=train_score)
    X_test = X.iloc[test_idx].assign(content_score=test_score)
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

    dataset_manifest = _dataset_manifest(args.dataset_manifest, args.dataset_version)
    if args.environment_manifest is None:
        if args.official_run:
            raise ValueError("Official training requires --environment-manifest")
        environment_manifest = {
            "code": {},
            "environment_fingerprint": None,
        }
    else:
        environment_manifest = load_json(args.environment_manifest)
        validate_environment_manifest(environment_manifest)
    training_config = resolved_training_config(
        seed=args.seed,
        test_size=args.test_size,
        feature_columns=features,
        feature_versions=(
            sorted(df["feature_version"].dropna().astype(str).unique())
            if "feature_version" in df.columns
            else []
        ),
        dataset_schema_version=str(dataset_manifest.get("schema_version") or "") or None,
        dataset_manifest=dataset_manifest,
        content_backend=content_backend,
        auxiliary_artifacts={
            path.name: file_sha256(path)
            for path in (
                *(
                    (ML_ROOT / "models" / "rhetorical_role.joblib",)
                    if not args.official_run
                    else ()
                ),
                ML_ROOT / "models" / "topic_model.joblib",
            )
            if path.is_file()
        },
        scale_pos_weight=(
            float(len(y_train) - y_train.sum()) / float(y_train.sum())
            if float(y_train.sum())
            else 1.0
        ),
    )
    validate_training_config(training_config)
    write_json(args.training_config_output, training_config)
    lineage = build_experiment_lineage(
        dataset_manifest=dataset_manifest,
        environment_manifest=environment_manifest,
        training_config=training_config,
        split_manifest=split_manifest,
        official_run=args.official_run,
    )
    if args.expected_lineage:
        validate_lineage_match(
            load_json(args.expected_lineage), lineage, context="Replay preflight"
        )
    write_json(args.lineage_output, lineage)

    print(f"Train: {len(X_train)} | Test: {len(X_test)} | Features: {len(features)}")
    model = train_model(X_train, y_train, args.seed)
    train_groups = df[GROUP].fillna(df.index.to_series().astype(str)).iloc[train_idx]
    calibrator, threshold = fit_calibrator(X_train, y_train, train_groups, args.seed)
    evaluate(model, X_test, y_test, calibrator, threshold)

    print("\nTop SHAP feature importance (mean |contribution|):")
    print(shap_importance(model, X_test).head(10).round(4))

    args.model.parent.mkdir(parents=True, exist_ok=True)
    bundle = {
        "schema_version": "model-bundle-v2",
        "model": model,
        "calibrator": calibrator,
        "threshold": threshold,
        "features": features,
        "dataset_version": args.dataset_version,
        "dataset_lineage": dataset_lineage,
        "audience_features_included": dataset_lineage is None,
        "role_feature_contract": role_feature_contract(),
        "lineage": compact_lineage(lineage),
        **content_bundle,
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
            "audience_features_included": False,
            "role_feature_contract": role_feature_contract(),
        }
        lineage_path = model_lineage_path(args.model)
        lineage_path.write_text(
            json.dumps(sidecar, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"Lineage saved -> {lineage_path}")
    print(f"\nSaved -> {args.model}")


if __name__ == "__main__":
    main()
