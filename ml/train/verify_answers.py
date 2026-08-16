"""Verify the *answers* of the viral model, not just its ranking quality.

Motivation (tutor feedback: "metrics to verify the answer of the model"):
The existing evaluation reports PR-AUC / ROC-AUC / F1@0.5, which measure how well
the model *ranks* posts. They do NOT tell us whether the probability it returns
(the `viral_score` shown to the user) is trustworthy, nor how stable the numbers
are on our small per-source test sets. This script adds the missing checks:

  1. Calibration      - Brier score + reliability curve (is a "0.7" really ~70%?)
  2. Decision quality  - confusion matrix, precision/recall at the serving threshold
                         stored in the model bundle, plus the best-F1 threshold
                         (diagnostic - it is tuned on the test set).
  3. Stability         - 95% bootstrap confidence intervals on ROC-AUC / PR-AUC,
                         so a single number is never reported without an interval.

Everything is computed overall AND per source (YouTube / X / Reddit), on the
exact same author-grouped holdout split used for training (no leakage).

Usage
-----
    python ml/train/verify_answers.py                 # default model + data
    python ml/train/verify_answers.py --n-boot 2000   # tighter CIs (slower)

Outputs
-------
    ml/data/verify_answers.md        - the full report (gitignored, regenerable)
    ml/data/calibration.png          - reliability diagram (overall + per source)
and a short summary is printed to stdout.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from train.train_viral import TARGET, TEXT, apply_calibrator, split_indices  # noqa: E402

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
DEFAULT_REPORT = ML_ROOT / "data" / "verify_answers.md"
DEFAULT_PLOT = ML_ROOT / "data" / "calibration.png"

RNG = np.random.default_rng(42)


# --------------------------------------------------------------------------- #
# Core metric helpers
# --------------------------------------------------------------------------- #
def _bootstrap_ci(y_true, proba, metric_fn, n_boot: int, alpha: float = 0.05):
    """Percentile bootstrap CI for a ranking metric. Returns (low, high) or None."""
    y_true = np.asarray(y_true)
    proba = np.asarray(proba)
    n = len(y_true)
    if n < 2 or len(np.unique(y_true)) < 2:
        return None
    stats = []
    for _ in range(n_boot):
        idx = RNG.integers(0, n, n)
        yt, pr = y_true[idx], proba[idx]
        if len(np.unique(yt)) < 2:  # a resample can be single-class; skip it
            continue
        stats.append(metric_fn(yt, pr))
    if not stats:
        return None
    lo, hi = np.percentile(stats, [100 * alpha / 2, 100 * (1 - alpha / 2)])
    return round(float(lo), 3), round(float(hi), 3)


def _confusion_at(y_true, proba, thr: float) -> dict:
    pred = (np.asarray(proba) >= thr).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
    return {
        "threshold": round(float(thr), 3),
        "tp": int(tp),
        "fp": int(fp),
        "fn": int(fn),
        "tn": int(tn),
        "precision": round(float(precision_score(y_true, pred, zero_division=0)), 3),
        "recall": round(float(recall_score(y_true, pred, zero_division=0)), 3),
        "f1": round(float(f1_score(y_true, pred, zero_division=0)), 3),
    }


def _best_f1_threshold(y_true, proba) -> float:
    """Threshold maximising F1 (DIAGNOSTIC ONLY - it is tuned on the test set)."""
    grid = np.linspace(0.05, 0.95, 19)
    scores = [f1_score(y_true, (np.asarray(proba) >= t).astype(int), zero_division=0) for t in grid]
    return float(grid[int(np.argmax(scores))])


def _reliability_bins(y_true, proba, n_bins: int = 10):
    """Manual calibration curve that keeps empty bins as NaN (robust on small n)."""
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


def evaluate_group(y_true, proba, n_boot: int, threshold: float = 0.5) -> dict:
    y_true = np.asarray(y_true, dtype=int)
    proba = np.asarray(proba, dtype=float)
    out = {"n": int(len(y_true)), "viral_rate": round(float(y_true.mean()), 3)}
    if len(np.unique(y_true)) < 2:
        out["note"] = "single class in test - ranking metrics undefined"
        out["brier"] = (
            round(float(brier_score_loss(y_true, proba, pos_label=1)), 3) if len(y_true) else None
        )
        out["confusion_serving"] = _confusion_at(y_true, proba, threshold)
        return out
    out["pr_auc"] = round(float(average_precision_score(y_true, proba)), 3)
    out["roc_auc"] = round(float(roc_auc_score(y_true, proba)), 3)
    out["brier"] = round(float(brier_score_loss(y_true, proba)), 3)
    out["pr_auc_ci95"] = _bootstrap_ci(y_true, proba, average_precision_score, n_boot)
    out["roc_auc_ci95"] = _bootstrap_ci(y_true, proba, roc_auc_score, n_boot)
    out["confusion_serving"] = _confusion_at(y_true, proba, threshold)
    best_t = _best_f1_threshold(y_true, proba)
    out["confusion_bestF1"] = _confusion_at(y_true, proba, best_t)
    _, _, _, ece = _reliability_bins(y_true, proba)
    out["ece"] = ece
    return out


# --------------------------------------------------------------------------- #
# Model scoring (mirrors train/evaluate.py exactly)
# --------------------------------------------------------------------------- #
def score_test_set(data_path: Path, model_path: Path, test_size: float, seed: int):
    bundle = joblib.load(model_path)
    model, features = bundle["model"], bundle["features"]
    content_model = bundle.get("content_model")

    df = pd.read_parquet(data_path)
    _, test_idx = split_indices(df, test_size, seed)
    test = df.iloc[test_idx].reset_index(drop=True)

    X = test.reindex(columns=[c for c in features if c != "content_score"], fill_value=0.0).astype(
        float
    )
    if "content_score" in features:
        if content_model is None:
            raise SystemExit(
                "Bundle has no content_model (BERT mode); score with the BERT wrapper."
            )
        X["content_score"] = content_model.predict_proba(test[TEXT].astype(str))[:, 1]
    X = X.reindex(columns=features, fill_value=0.0)

    proba = model.predict_proba(X)[:, 1]
    if bundle.get("calibrator") is not None:  # measure what serving actually returns
        proba = apply_calibrator(bundle["calibrator"], proba)
    y = test[TARGET].astype(int).to_numpy()
    src = test["source"].fillna("unknown").to_numpy()
    threshold = float(
        bundle.get("classification_probability_threshold", bundle.get("threshold")) or 0.5
    )
    return y, np.asarray(proba, dtype=float), src, threshold


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def _fmt_ci(ci):
    return f"[{ci[0]}, {ci[1]}]" if ci else "n/a"


def build_markdown(results: dict, threshold: float = 0.5) -> str:
    lines = [
        "# Model-answer verification report",
        "",
        "Beyond ranking (PR-AUC / ROC-AUC), this checks that the probability the "
        "model returns is **calibrated**, that its **decisions** at the serving "
        "threshold are sound, and that every number comes with a **95% bootstrap "
        "confidence interval**. Same author-grouped holdout split as training.",
        "",
        "## 1. Ranking + calibration (per group)",
        "",
        "| group | n | viral_rate | ROC-AUC (95% CI) | PR-AUC (95% CI) | Brier down | ECE down |",
        "|---|---|---|---|---|---|---|",
    ]
    for g, m in results.items():
        if "roc_auc" not in m:
            lines.append(
                f"| {g} | {m['n']} | {m['viral_rate']} | {m.get('note', 'single class')} | - | {m.get('brier', '-')} | - |"
            )
            continue
        lines.append(
            f"| {g} | {m['n']} | {m['viral_rate']} | "
            f"{m['roc_auc']} {_fmt_ci(m.get('roc_auc_ci95'))} | "
            f"{m['pr_auc']} {_fmt_ci(m.get('pr_auc_ci95'))} | "
            f"{m['brier']} | {m.get('ece', '-')} |"
        )
    lines += [
        "",
        "*Brier and ECE closer to 0 = better-calibrated probabilities. "
        "A wide CI (esp. for X) means the metric is not reliable on that sample size.*",
        "",
        f"## 2. Decisions at the {threshold:.2f} serving threshold",
        "",
        "| group | precision | recall | F1 | TP | FP | FN | TN |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for g, m in results.items():
        c = m["confusion_serving"]
        lines.append(
            f"| {g} | {c['precision']} | {c['recall']} | {c['f1']} | "
            f"{c['tp']} | {c['fp']} | {c['fn']} | {c['tn']} |"
        )
    lines += [
        "",
        "## 3. Best-F1 threshold (diagnostic only - tuned on test, do not deploy as-is)",
        "",
        "| group | best thr | precision | recall | F1 |",
        "|---|---|---|---|---|",
    ]
    for g, m in results.items():
        if "confusion_bestF1" not in m:
            continue
        c = m["confusion_bestF1"]
        lines.append(f"| {g} | {c['threshold']} | {c['precision']} | {c['recall']} | {c['f1']} |")
    lines += [
        "",
        f"The serving threshold ({threshold:.2f}) is picked out-of-fold on the training "
        "rows and stored in the model bundle, so this section is a check, not a tuning "
        "knob. A best-F1 threshold far from it means the split is unstable.",
        "",
        "![Calibration curves](calibration.png)",
        "",
    ]
    return "\n".join(lines)


def make_plot(y, proba, src, out_path: Path):
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
    ax.set_title("Reliability diagram")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify the model's answers (calibration, decisions, stability)."
    )
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-boot", type=int, default=1000)
    parser.add_argument("--out", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--plot", type=Path, default=DEFAULT_PLOT)
    args = parser.parse_args()

    y, proba, src, threshold = score_test_set(args.data, args.model, args.test_size, args.seed)

    results = {"overall": evaluate_group(y, proba, args.n_boot, threshold)}
    for s in sorted(pd.unique(src)):
        mask = src == s
        results[s] = evaluate_group(y[mask], proba[mask], args.n_boot, threshold)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    make_plot(y, proba, src, args.plot)
    args.out.write_text(build_markdown(results, threshold), encoding="utf-8")

    o = results["overall"]
    print("=== Answer verification (overall) ===")
    print(f" n={o['n']}  viral_rate={o['viral_rate']}")
    if "roc_auc" in o:
        print(f" ROC-AUC={o['roc_auc']} CI95={_fmt_ci(o.get('roc_auc_ci95'))}")
        print(f" PR-AUC ={o['pr_auc']} CI95={_fmt_ci(o.get('pr_auc_ci95'))}")
        print(f" Brier={o['brier']}  ECE={o.get('ece')}")
        c = o["confusion_serving"]
        print(f" @{threshold:.2f}: precision={c['precision']} recall={c['recall']} f1={c['f1']}")
    print(f"\nReport : {args.out}")
    print(f"Plot   : {args.plot}")


if __name__ == "__main__":
    main()
