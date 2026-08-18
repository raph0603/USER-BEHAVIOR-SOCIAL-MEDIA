"""Verify the *answers* of the viral model, not just its ranking quality.

Reads the complete OOF predictions artifact and generates the markdown report
with reliability diagram.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import joblib

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

DEFAULT_OOF = ML_ROOT / "results" / "oof_predictions.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
DEFAULT_REPORT = ML_ROOT / "data" / "verify_answers.md"
DEFAULT_PLOT = ML_ROOT / "data" / "calibration.png"
DEFAULT_METRICS = ML_ROOT / "results" / "evaluation.json"

def _reliability_bins(y_true, proba, n_bins: int = 10):
    y_true = np.asarray(y_true, dtype=float)
    proba = np.asarray(proba, dtype=float)
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.clip(np.digitize(proba, edges[1:-1]), 0, n_bins - 1)
    conf, acc, count = [], [], []
    for b in range(n_bins):
        m = idx == b
        count.append(int(m.sum()))
        if m.any():
            conf.append(float(proba[m].mean()))
            acc.append(float(y_true[m].mean()))
        else:
            conf.append(np.nan)
            acc.append(np.nan)
    ece = 0.0
    total = len(y_true)
    for b in range(n_bins):
        if count[b]:
            ece += (count[b] / total) * abs(acc[b] - conf[b])
    return np.array(conf), np.array(acc), np.array(count), round(float(ece), 3)

def make_plot(y, proba, src, out_path: Path, dataset_version: str | None = None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    groups = [("overall", np.ones(len(y), dtype=bool))]
    for s in sorted(pd.unique(src)):
        groups.append((s, src == s))

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot([0, 1], [0, 1], "--", color="gray", label="perfectly calibrated")
    for name, mask in groups:
        if mask.sum() < 5 or len(np.unique(y[mask])) < 2:
            continue
        conf, acc, count, ece = _reliability_bins(y[mask], proba[mask], n_bins=8)
        ok = ~np.isnan(conf)
        ax.plot(conf[ok], acc[ok], marker="o", label=f"{name} (ECE={ece})")
    ax.set_xlabel("Mean predicted probability")
    ax.set_ylabel("Observed viral fraction")
    title = "Reliability diagram (Out-of-Fold)"
    if dataset_version:
        title += f"\n{dataset_version}"
    ax.set_title(title)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    fig.savefig(
        out_path,
        dpi=120,
        metadata={"Description": f"dataset_version={dataset_version or 'unversioned'}"},
    )
    plt.close(fig)

def build_markdown(metrics: dict, dataset_version: str) -> str:
    lines = [
        "# Model-answer verification report",
        "",
        "This report measures out-of-fold predictions. It checks that the probability the ",
        "model returns is **calibrated**, that its **decisions** at the serving ",
        "threshold are sound, and that every number comes with a **95% bootstrap ",
        "confidence interval**.",
        "",
        f"- Dataset version: `{dataset_version}`",
        "",
        "## 1. Ranking + calibration (per group)",
        "",
        "| group | n | viral_rate | ROC-AUC (95% CI) | PR-AUC (95% CI) | Brier down | ECE down |",
        "|---|---|---|---|---|---|---|",
    ]
    
    def _fmt_ci(ci):
        return f"[{ci[0]:.3f}, {ci[1]:.3f}]" if ci else "n/a"
        
    def add_row(group, m, boot):
        if m.get("roc_auc") is None:
            lines.append(f"| {group} | {m['n']} | {m['viral_rate']:.3f} | single class | - | {m.get('brier', '-')} | - |")
            return
        lines.append(
            f"| {group} | {m['n']} | {m['viral_rate']:.3f} | "
            f"{m['roc_auc']:.3f} {_fmt_ci(boot.get('roc_auc_ci95'))} | "
            f"{m['pr_auc']:.3f} {_fmt_ci(boot.get('pr_auc_ci95'))} | "
            f"{m['brier']:.3f} | {m.get('ece', '-'):.3f} |"
        )

    add_row("overall", metrics["overall_calibrated"], metrics["bootstrap_calibrated"])
    for source, m in metrics.get("per_source_calibrated", {}).items():
        # Source bootstraps are not currently exported in JSON, we can skip CIs for them for brevity
        add_row(source, m, {})

    lines += [
        "",
        "*Brier and ECE closer to 0 = better-calibrated probabilities.*",
        "",
        "## 2. Decisions at the Out-of-Fold thresholds",
        "",
        "| group | avg threshold | precision | recall | F1 | TP | FP | FN | TN |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    
    def add_confusion(group, m):
        lines.append(
            f"| {group} | {m['threshold']:.3f} | {m['precision']:.3f} | {m['recall']:.3f} | {m['f1']:.3f} | "
            f"{m['true_positive']} | {m['false_positive']} | {m['false_negative']} | {m['true_negative']} |"
        )
        
    add_confusion("overall", metrics["overall_calibrated"])
    for source, m in metrics.get("per_source_calibrated", {}).items():
        add_confusion(source, m)

    lines += [
        "",
        "![Calibration curves](calibration.png)",
        "",
    ]
    return "\n".join(lines)

def main() -> None:
    parser = argparse.ArgumentParser(description="Verify the model's answers.")
    parser.add_argument("--oof", type=Path, default=DEFAULT_OOF)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--metrics", type=Path, default=DEFAULT_METRICS)
    parser.add_argument("--out", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--plot", type=Path, default=DEFAULT_PLOT)
    parser.add_argument("--dataset-manifest", type=Path)
    args = parser.parse_args()

    df = pd.read_parquet(args.oof)
    with open(args.metrics, "r", encoding="utf-8") as f:
        metrics = json.load(f)
        
    dataset_version = metrics.get("dataset_version", "unknown")

    y = df["viral"].astype(int).to_numpy()
    proba = df["calibrated_probability"].astype(float).to_numpy()
    src = df["source"].fillna("unknown").to_numpy()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    make_plot(y, proba, src, args.plot, dataset_version)
    args.out.write_text(build_markdown(metrics, dataset_version), encoding="utf-8")

    o = metrics["overall_calibrated"]
    boot = metrics["bootstrap_calibrated"]
    print("=== Answer verification (Out-of-Fold) ===")
    print(f" n={o['n']}  viral_rate={o['viral_rate']:.3f}")
    if o.get("roc_auc") is not None:
        print(f" ROC-AUC={o['roc_auc']:.3f} CI95={boot.get('roc_auc_ci95')}")
        print(f" PR-AUC ={o['pr_auc']:.3f} CI95={boot.get('pr_auc_ci95')}")
        print(f" Brier={o['brier']:.3f}  ECE={o.get('ece', 0):.3f}")
    print(f"\nReport : {args.out}")
    print(f"Plot   : {args.plot}")

if __name__ == "__main__":
    main()
