"""Generate a consolidated stats + quality report for the viral pipeline.

Gathers dataset statistics, model quality (overall + per source) and the
content-model comparison into one Markdown report for presentation/submission.
Writes ml/data/report.md (gitignored, regenerable) and prints a short summary.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict

ML_ROOT = Path(__file__).resolve().parents[0]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from features.text_content import build_content_model
from dataset_lineage import load_dataset_lineage
from train.train_viral import (
    TARGET,
    TEXT,
    apply_calibrator,
    split_indices,
    validate_dataset_version,
)

DATA = ML_ROOT / "data" / "train_dataset.parquet"
MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
BERT_METRICS = ML_ROOT / "data" / "bert_content_metrics.json"
OUT = ML_ROOT / "data" / "report.md"


def _metrics(y, proba, threshold: float = 0.5) -> dict:
    return {
        "n": int(len(y)),
        "viral_rate": round(float(y.mean()), 3),
        "pr_auc": round(float(average_precision_score(y, proba)), 3),
        "roc_auc": round(float(roc_auc_score(y, proba)), 3),
        "f1": round(float(f1_score(y, (proba >= threshold).astype(int), zero_division=0)), 3),
    }


def section_dataset(df: pd.DataFrame) -> str:
    by_source = df.groupby("source")[TARGET].agg(n="count", viral_rate="mean").round(3)
    vi = int(df["is_vietnamese"].sum())
    lines = [
        "## 1. Dataset overview",
        f"- Total rows: **{len(df)}** | overall viral rate: **{df[TARGET].mean():.3f}**",
        f"- Language: VI **{vi}** / EN **{len(df) - vi}**",
        f"- Text length: avg {df['char_count'].mean():.0f} chars / {df['word_count'].mean():.0f} words",
        "",
        "| source | n | viral_rate |",
        "|---|---|---|",
        *[f"| {s} | {int(r.n)} | {r.viral_rate} |" for s, r in by_source.iterrows()],
    ]
    return "\n".join(lines)


def section_lineage(lineage: dict | None) -> str:
    if not lineage:
        return "## Reproducibility lineage\n- Non-official run: no pinned lakehouse manifest."
    snapshots = lineage["iceberg_snapshot_ids"]
    lines = [
        "## Reproducibility lineage",
        f"- Dataset version: `{lineage['dataset_version']}`",
        f"- Dataset fingerprint: `{lineage['dataset_fingerprint']}`",
        f"- Manifest SHA-256: `{lineage['manifest_sha256']}`",
        (
            f"- Training input: `{lineage['training_table']}` pinned at Gold snapshot "
            f"`{lineage['training_snapshot_id']}`"
        ),
        "",
        "| Source Iceberg table | pinned snapshot ID |",
        "|---|---:|",
        *[f"| `{table}` | `{snapshot_id}` |" for table, snapshot_id in snapshots.items()],
    ]
    return "\n".join(lines)


def section_roles(df: pd.DataFrame) -> str:
    role_cols = sorted(c for c in df.columns if c.startswith("role_n_") and c != "role_n_segments")
    if not role_cols or "role_n_segments" not in df.columns:
        return "## 2. Marketing roles\n(no role features in dataset)"
    total_seg = float(df["role_n_segments"].sum()) or 1.0
    rows = sorted(((c.replace("role_n_", ""), int(df[c].sum())) for c in role_cols),
                  key=lambda x: -x[1])
    lines = ["## 2. Marketing-role distribution (over all segments)",
             "| role | segments | % |", "|---|---|---|",
             *[f"| {role} | {n} | {n / total_seg:.1%} |" for role, n in rows]]
    return "\n".join(lines)


def section_quality(df: pd.DataFrame) -> str:
    bundle = joblib.load(MODEL)
    model, features = bundle["model"], bundle["features"]
    content_model = bundle.get("content_model")
    threshold = float(bundle.get("threshold", 0.5))
    _, test_idx = split_indices(df, 0.2, 42)
    test = df.iloc[test_idx].reset_index(drop=True)

    X = test.reindex(columns=[c for c in features if c != "content_score"], fill_value=0.0).astype(float)
    if "content_score" in features and content_model is not None:
        X["content_score"] = content_model.predict_proba(test[TEXT].astype(str))[:, 1]
    X = X.reindex(columns=features, fill_value=0.0)
    proba = model.predict_proba(X)[:, 1]
    if bundle.get("calibrator") is not None:
        proba = apply_calibrator(bundle["calibrator"], proba)
    y = test[TARGET].astype(int)

    overall = _metrics(y, pd.Series(proba), threshold)
    lines = ["## 3. Viral-model quality (test set)",
             f"- Overall: PR-AUC **{overall['pr_auc']}** | ROC-AUC **{overall['roc_auc']}** | F1 {overall['f1']} (n={overall['n']})",
             "", "| source | n | viral_rate | PR-AUC | ROC-AUC | F1 |", "|---|---|---|---|---|---|"]
    for src in sorted(test["source"].dropna().unique()):
        mask = (test["source"] == src).to_numpy()
        if y[mask].nunique() < 2:
            continue
        m = _metrics(y[mask], pd.Series(proba[mask]), threshold)
        lines.append(f"| {src} | {m['n']} | {m['viral_rate']} | {m['pr_auc']} | {m['roc_auc']} | {m['f1']} |")
    return "\n".join(lines)


def section_content(df: pd.DataFrame) -> str:
    X, y = df[TEXT].fillna("").astype(str), df[TARGET].astype(int)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    oof = cross_val_predict(build_content_model(), X, y, cv=cv, method="predict_proba")[:, 1]
    tfidf = _metrics(y, pd.Series(oof))

    bert = "PR-AUC 0.428 | ROC-AUC 0.687 (Kaggle 2026-06-29; drop bert_content_metrics.json into ml/data/ to refresh)"
    if BERT_METRICS.exists():
        m = json.loads(BERT_METRICS.read_text(encoding="utf-8"))
        bert = f"PR-AUC {m.get('oof_pr_auc')} | ROC-AUC {m.get('oof_roc_auc')}"
    return "\n".join([
        "## 4. Content-model comparison (OOF, same 5-fold)",
        f"- TF-IDF + LogReg: PR-AUC **{tfidf['pr_auc']}** | ROC-AUC {tfidf['roc_auc']}",
        f"- BERT (XLM-R): {bert}",
        "- -> Keep TF-IDF while data is small; revisit BERT once data grows.",
    ])


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the consolidated pipeline report.")
    parser.add_argument("--data", type=Path, default=DATA)
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--dataset-manifest", type=Path)
    args = parser.parse_args()

    df = pd.read_parquet(args.data)
    bundle = joblib.load(MODEL)
    lineage = bundle.get("dataset_lineage")
    if args.dataset_manifest:
        _, expected_lineage = load_dataset_lineage(args.dataset_manifest)
        if lineage != expected_lineage:
            raise ValueError("Model artifact lineage does not match --dataset-manifest")
    validate_dataset_version(df, bundle.get("dataset_version"))
    report = "\n\n".join([
        "# AI viral-prediction pipeline — report",
        section_lineage(lineage),
        section_dataset(df),
        section_roles(df),
        section_quality(df),
        section_content(df),
    ]) + "\n"

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report, encoding="utf-8")
    print(report)
    print(f"Saved -> {args.out}")


if __name__ == "__main__":
    main()
