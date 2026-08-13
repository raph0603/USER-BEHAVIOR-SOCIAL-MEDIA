"""Evaluate the saved viral model overall AND per source.

Reuses the exact same holdout split as training (GroupShuffleSplit by author),
rebuilds the test-time content_score from the saved content model, then reports
PR-AUC / ROC-AUC / F1 overall and broken down by platform — to see whether the
model works evenly across YouTube / X / Reddit (it usually does not).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from dataset_lineage import load_dataset_lineage
from train.train_viral import (
    TARGET,
    TEXT,
    apply_calibrator,
    split_indices,
    validate_dataset_version,
)

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
DEFAULT_OUTPUT = ML_ROOT / "results" / "stage1_evaluation.json"


def _metrics(y_true, proba, threshold: float) -> dict:
    return {
        "n": int(len(y_true)),
        "viral_rate": round(float(y_true.mean()), 3),
        "pr_auc": round(float(average_precision_score(y_true, proba)), 3),
        "roc_auc": round(float(roc_auc_score(y_true, proba)), 3),
        "f1": round(
            float(f1_score(y_true, (proba >= threshold).astype(int), zero_division=0)), 3
        ),
        "decision_threshold": round(float(threshold), 6),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate the viral model overall and per source.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dataset-manifest", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    bundle = joblib.load(args.model)
    model, features = bundle["model"], bundle["features"]
    content_model = bundle.get("content_model")
    calibrator = bundle.get("calibrator")
    threshold = float(bundle.get("threshold", 0.5))
    dataset_lineage = bundle.get("dataset_lineage")
    if args.dataset_manifest:
        _, expected_lineage = load_dataset_lineage(args.dataset_manifest)
        if dataset_lineage != expected_lineage:
            raise ValueError("Model artifact lineage does not match --dataset-manifest")
    elif bundle.get("dataset_version") and not dataset_lineage:
        raise ValueError("Versioned model artifact is missing its dataset lineage")

    df = pd.read_parquet(args.data)
    validate_dataset_version(df, bundle.get("dataset_version"))
    _, test_idx = split_indices(df, args.test_size, args.seed)
    test = df.iloc[test_idx].reset_index(drop=True)

    X = test.reindex(columns=[c for c in features if c != "content_score"], fill_value=0.0).astype(float)
    if "content_score" in features:
        if content_model is None:
            raise SystemExit("Bundle has no content_model (BERT mode); evaluate with the BERT serving wrapper.")
        X["content_score"] = content_model.predict_proba(test[TEXT].astype(str))[:, 1]
    X = X.reindex(columns=features, fill_value=0.0)

    raw_proba = model.predict_proba(X)[:, 1]
    proba = apply_calibrator(calibrator, raw_proba) if calibrator is not None else raw_proba
    y = test[TARGET].astype(int)

    overall = _metrics(y, pd.Series(proba), threshold)
    print("=== Overall (test) ===")
    print(overall)

    print("\n=== Per source ===")
    rows = []
    for src in sorted(test["source"].dropna().unique()):
        mask = (test["source"] == src).to_numpy()
        if y[mask].nunique() < 2:
            print(f"{src}: skipped (only one class in test)")
            continue
        m = _metrics(y[mask], pd.Series(proba[mask]), threshold)
        m["source"] = src
        rows.append(m)
    print(pd.DataFrame(rows).set_index("source"))
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_lineage": dataset_lineage,
        "model_artifact": {
            "file": args.model.name,
            "sha256": hashlib.sha256(args.model.read_bytes()).hexdigest(),
            "audience_features_included": bool(
                bundle.get("audience_features_included", True)
            ),
            "role_feature_contract": bundle.get("role_feature_contract"),
        },
        "evaluation": {
            "split": "GroupShuffleSplit by author_hash",
            "seed": args.seed,
            "test_size": args.test_size,
            "overall": overall,
            "per_source": {str(row.pop("source")): row for row in rows},
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"\nMetrics with snapshot lineage saved -> {args.output}")


if __name__ == "__main__":
    main()
