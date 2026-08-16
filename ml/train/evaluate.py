"""Evaluate the exact serialized Stage-1 model on its persisted holdout split."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import joblib
import pandas as pd
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score

ML_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ML_ROOT.parent
for import_root in (ML_ROOT, PROJECT_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from common.reproducibility import (
    file_sha256,
    load_json,
    validate_split_manifest,
    write_json,
)
from evaluation_artifact import build_evaluation_artifact, validate_evaluation_inputs
from train.train_viral import TARGET, TEXT, apply_calibrator, split_indices
from virality_lineage import dataset_virality_lineage, validate_virality_compatibility

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
DEFAULT_LINEAGE = ML_ROOT / "results" / "experiment_lineage.json"
DEFAULT_SPLIT = ML_ROOT / "results" / "split_manifest.json"
DEFAULT_EVALUATION = ML_ROOT / "results" / "evaluation.json"


def _metrics(y_true: pd.Series, proba: pd.Series, threshold: float) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "n": int(len(y_true)),
        "viral_rate": float(y_true.mean()),
        "pr_auc": float(average_precision_score(y_true, proba)),
        "f1": float(f1_score(y_true, (proba >= threshold).astype(int), zero_division=0)),
        "decision_threshold": float(threshold),
    }
    metrics["roc_auc"] = float(roc_auc_score(y_true, proba)) if y_true.nunique() >= 2 else None
    return metrics


def select_persisted_holdout(df: pd.DataFrame, split_manifest: Mapping[str, Any]) -> pd.DataFrame:
    validate_split_manifest(split_manifest)
    id_column = str(split_manifest["id_column"])
    if id_column not in df.columns:
        raise ValueError(f"Training data does not contain split identifier column {id_column!r}")
    identifiers = df[id_column].astype(str)
    if not identifiers.is_unique:
        raise ValueError(f"Split identifier column {id_column!r} is not unique")
    holdout = set(str(value) for value in split_manifest["holdout_content_ids"])
    selected = df.loc[identifiers.isin(holdout)].copy()
    selected_ids = set(selected[id_column].astype(str))
    if selected_ids != holdout:
        missing = sorted(holdout - selected_ids)
        raise ValueError(f"Persisted holdout contains {len(missing)} IDs absent from the dataset")
    return selected.sort_values(id_column).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate one serialized viral model artifact.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--lineage", type=Path, default=DEFAULT_LINEAGE)
    parser.add_argument("--split-manifest", type=Path, default=DEFAULT_SPLIT)
    parser.add_argument("--dataset-manifest", type=Path)
    parser.add_argument("--environment-manifest", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_EVALUATION)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--virality-contract-fingerprint")
    parser.add_argument("--virality-policy")
    parser.add_argument(
        "--legacy-recompute-split",
        action="store_true",
        help="Compatibility only: recompute a split when evaluating an old bundle.",
    )
    args = parser.parse_args()

    bundle = joblib.load(args.model)
    model, features = bundle["model"], bundle["features"]
    content_model = bundle.get("content_model")
    lineage = load_json(args.lineage)
    dataset_manifest = load_json(args.dataset_manifest) if args.dataset_manifest else None
    environment_manifest = (
        load_json(args.environment_manifest) if args.environment_manifest else None
    )
    actual_model_sha = file_sha256(args.model)
    validate_evaluation_inputs(
        bundle=bundle,
        lineage=lineage,
        dataset_manifest=dataset_manifest,
        environment_manifest=environment_manifest,
        model_sha256=actual_model_sha,
    )

    df = pd.read_parquet(args.data)
    observed_virality_lineage = dataset_virality_lineage(df)
    model_virality_fingerprint = str(bundle.get("virality_contract_fingerprint") or "")
    model_virality_policy = str(bundle.get("virality_policy") or "")
    if not model_virality_fingerprint or not model_virality_policy:
        raise ValueError("Model bundle has no frozen virality contract lineage")
    validate_virality_compatibility(
        observed_virality_lineage,
        expected_fingerprint=(args.virality_contract_fingerprint or model_virality_fingerprint),
        expected_policy=(args.virality_policy or model_virality_policy),
    )
    if args.split_manifest.is_file():
        split_manifest = load_json(args.split_manifest)
        if split_manifest.get("split_fingerprint") != lineage.get("split_fingerprint"):
            raise ValueError("Split sidecar/model fingerprint mismatch")
        test = select_persisted_holdout(df, split_manifest)
        id_column = str(split_manifest["id_column"])
    elif args.legacy_recompute_split:
        _, test_idx = split_indices(df, args.test_size, args.seed)
        test = df.iloc[test_idx].reset_index(drop=True)
        id_column = "legacy_row_index"
        test[id_column] = [f"row-{index}" for index in test_idx]
    else:
        raise FileNotFoundError(
            "Persisted split manifest is required; use --legacy-recompute-split only for old artifacts"
        )

    X = test.reindex(
        columns=[column for column in features if column != "content_score"], fill_value=0.0
    ).astype(float)
    if "content_score" in features:
        if content_model is None:
            raise SystemExit(
                "Bundle has no content_model (BERT mode); evaluate with the BERT serving wrapper."
            )
        X["content_score"] = content_model.predict_proba(test[TEXT].astype(str))[:, 1]
    X = X.reindex(columns=features, fill_value=0.0)

    raw_proba = model.predict_proba(X)[:, 1]
    calibrator = bundle.get("calibrator")
    proba = apply_calibrator(calibrator, raw_proba) if calibrator is not None else raw_proba
    threshold = float(
        bundle.get("classification_probability_threshold", bundle.get("threshold", 0.5))
    )
    y = test[TARGET].astype(int)
    overall = _metrics(y, pd.Series(proba), threshold)
    by_source: dict[str, Any] = {}
    for source in sorted(test["source"].dropna().astype(str).unique()):
        mask = test["source"].astype(str).eq(source).to_numpy()
        by_source[source] = _metrics(y[mask], pd.Series(proba[mask]), threshold)
    predictions = [
        {
            "content_id": str(content_id),
            "source": str(source),
            "label": int(label),
            "probability": float(probability),
        }
        for content_id, source, label, probability in zip(
            test[id_column], test["source"], y, proba, strict=True
        )
    ]
    artifact = build_evaluation_artifact(
        lineage=lineage,
        model_sha256=actual_model_sha,
        overall_metrics=overall,
        source_metrics=by_source,
        predictions=predictions,
        additional_metadata={
            "dataset_lineage": bundle.get("dataset_lineage"),
            **observed_virality_lineage,
            "classification_probability_threshold": threshold,
            "model_artifact": {
                "audience_features_included": bool(bundle.get("audience_features_included", True)),
                "role_feature_contract": bundle.get("role_feature_contract"),
            },
        },
    )
    write_json(args.output, artifact)

    print("=== Overall (persisted holdout) ===")
    print(overall)
    print("\n=== Per source ===")
    print(pd.DataFrame(by_source).transpose())
    print(f"\nSaved -> {args.output}")


if __name__ == "__main__":
    main()
