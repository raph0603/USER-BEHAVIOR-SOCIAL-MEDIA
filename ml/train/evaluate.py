"""Evaluate the saved viral model overall AND per source.

Reuses the exact same holdout split as training (GroupShuffleSplit by author),
rebuilds the test-time content_score from the saved content model, then reports
PR-AUC / ROC-AUC / F1 overall and broken down by platform — to see whether the
model works evenly across YouTube / X / Reddit (it usually does not).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from train.train_viral import GROUP, TARGET, TEXT, feature_columns, split_indices
from virality_lineage import dataset_virality_lineage, validate_virality_compatibility

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"


def _metrics(y_true, proba) -> dict:
    return {
        "n": int(len(y_true)),
        "viral_rate": round(float(y_true.mean()), 3),
        "pr_auc": round(float(average_precision_score(y_true, proba)), 3),
        "roc_auc": round(float(roc_auc_score(y_true, proba)), 3),
        "f1@0.5": round(float(f1_score(y_true, (proba >= 0.5).astype(int), zero_division=0)), 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate the viral model overall and per source.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--virality-contract-fingerprint")
    parser.add_argument("--virality-policy")
    parser.add_argument("--evaluation-output", type=Path)
    args = parser.parse_args()

    bundle = joblib.load(args.model)
    model, features = bundle["model"], bundle["features"]
    content_model = bundle.get("content_model")

    df = pd.read_parquet(args.data)
    observed_lineage = dataset_virality_lineage(df)
    model_fingerprint = str(bundle.get("virality_contract_fingerprint") or "")
    model_policy = str(bundle.get("virality_policy") or "")
    if not model_fingerprint or not model_policy:
        raise ValueError("Model bundle has no frozen virality contract lineage")
    validate_virality_compatibility(
        observed_lineage,
        expected_fingerprint=(args.virality_contract_fingerprint or model_fingerprint),
        expected_policy=(args.virality_policy or model_policy),
    )
    _, test_idx = split_indices(df, args.test_size, args.seed)
    test = df.iloc[test_idx].reset_index(drop=True)

    X = test.reindex(columns=[c for c in features if c != "content_score"], fill_value=0.0).astype(
        float
    )
    if "content_score" in features:
        if content_model is None:
            raise SystemExit(
                "Bundle has no content_model (BERT mode); evaluate with the BERT serving wrapper."
            )
        X["content_score"] = content_model.predict_proba(test[TEXT].astype(str))[:, 1]
    X = X.reindex(columns=features, fill_value=0.0)

    proba = model.predict_proba(X)[:, 1]
    y = test[TARGET].astype(int)

    print("=== Overall (test) ===")
    print(_metrics(y, pd.Series(proba)))

    print("\n=== Per source ===")
    rows = []
    for src in sorted(test["source"].dropna().unique()):
        mask = (test["source"] == src).to_numpy()
        if y[mask].nunique() < 2:
            print(f"{src}: skipped (only one class in test)")
            continue
        m = _metrics(y[mask], pd.Series(proba[mask]))
        m["source"] = src
        rows.append(m)
    per_source = pd.DataFrame(rows).set_index("source")
    print(per_source)
    evaluation_output = args.evaluation_output or args.model.with_suffix(".evaluation.json")
    evaluation_output.parent.mkdir(parents=True, exist_ok=True)
    evaluation_output.write_text(
        json.dumps(
            {
                "dataset_version": bundle.get("dataset_version"),
                **observed_lineage,
                "classification_probability_threshold": bundle.get(
                    "classification_probability_threshold", bundle.get("threshold")
                ),
                "overall": _metrics(y, pd.Series(proba)),
                "per_source": rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
