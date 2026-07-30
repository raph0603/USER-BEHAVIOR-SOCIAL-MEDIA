"""Cross-validate Reddit virality models with and without audience features.

Both variants use the same stratified, author-grouped folds, text content
scores and XGBoost hyperparameters. Class imbalance is handled only inside
each training fold through XGBoost's ``scale_pos_weight``; validation folds
are never resampled. The only experimental variable is the presence of
``chan_*`` audience features.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedGroupKFold, cross_val_predict

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset.parquet"
DEFAULT_OUTPUT = ML_ROOT / "results" / "reddit_audience_ablation.json"
DEFAULT_MODEL_DIR = ML_ROOT / "models"
AUDIENCE_PREFIX = "chan_"
SOURCE = "reddit"
TARGET = "viral"
GROUP = "author_hash"
TEXT = "clean_text"
DEFAULT_FOLDS = 5
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


def feature_columns(df: pd.DataFrame) -> list[str]:
    """Mirror the Stage-1 feature contract without importing the heavy trainer."""

    extra = sorted(c for c in df.columns if c.startswith(("src_", "role_", "topic_", "chan_")))
    return CONTENT_FEATURES + extra


def reddit_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return only labelled Reddit rows and fail clearly on unusable input."""

    required = {"source", TARGET, GROUP, TEXT}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Training data is missing required columns: {missing}")

    reddit = df.loc[df["source"].astype(str).str.lower().eq(SOURCE)].copy()
    reddit = reddit.loc[reddit[TARGET].notna()].reset_index(drop=True)
    if reddit.empty:
        raise ValueError("Training data contains no labelled Reddit rows")
    if reddit[TARGET].nunique() < 2:
        raise ValueError("Reddit training data must contain both viral classes")
    return reddit


def ablation_feature_sets(df: pd.DataFrame) -> dict[str, list[str]]:
    """Build feature lists whose only difference is the ``chan_*`` family."""

    all_features = feature_columns(df)
    audience = [column for column in all_features if column.startswith(AUDIENCE_PREFIX)]
    if not audience:
        raise ValueError(
            "No chan_* audience features found. Rebuild the dataset from an export "
            "containing subreddit_member_count or audience_count."
        )
    if "chan_log_audience" not in audience:
        raise ValueError("Audience ablation requires chan_log_audience")
    known = pd.to_numeric(df["chan_log_audience"], errors="coerce").notna()
    if not known.any():
        raise ValueError(
            "Reddit audience is unknown for every row; a with-audience comparison "
            "would be meaningless."
        )
    if df.loc[known, "chan_log_audience"].nunique() < 2:
        raise ValueError(
            "Reddit chan_log_audience has fewer than two observed values; "
            "the data cannot identify an audience-size effect. Do not inject "
            "one current subreddit count into every row."
        )
    return {
        "with_audience": all_features,
        "without_audience": [column for column in all_features if column not in audience],
    }


def author_groups(df: pd.DataFrame) -> pd.Series:
    """Return stable author groups, falling back to one group per unowned row."""

    fallback = df.index.to_series().map(lambda index: f"missing-author-{index}")
    return df[GROUP].astype("string").fillna(fallback)


def stratified_group_folds(
    df: pd.DataFrame,
    *,
    n_splits: int,
    seed: int,
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Build reproducible class-stratified folds with no author overlap."""

    if n_splits < 2:
        raise ValueError("K-fold validation requires at least two folds")
    y = df[TARGET].astype(int)
    groups = author_groups(df)
    if groups.nunique() < n_splits:
        raise ValueError(
            f"K-fold validation needs at least {n_splits} distinct authors; "
            f"received {groups.nunique()}"
        )
    if int(y.value_counts().min()) < n_splits:
        raise ValueError(
            f"K-fold validation needs at least {n_splits} rows in each class"
        )

    splitter = StratifiedGroupKFold(
        n_splits=n_splits,
        shuffle=True,
        random_state=seed,
    )
    folds = list(splitter.split(df, y, groups))
    for fold_number, (train_idx, validation_idx) in enumerate(folds, start=1):
        train_groups = set(groups.iloc[train_idx])
        validation_groups = set(groups.iloc[validation_idx])
        if train_groups & validation_groups:
            raise RuntimeError(f"Author leakage detected in fold {fold_number}")
        validate_split(
            y.iloc[train_idx],
            y.iloc[validation_idx],
            groups.iloc[train_idx],
            split_name=f"fold {fold_number}",
        )
    return folds


def validate_split(
    y_train: pd.Series,
    y_test: pd.Series,
    groups: pd.Series,
    *,
    split_name: str = "split",
) -> None:
    """Validate the minimum class/group support required by the shared trainers."""

    for name, labels in (("train", y_train), ("test", y_test)):
        if labels.nunique() < 2:
            raise ValueError(
                f"Reddit {split_name} {name} partition contains only one class"
            )
    if int(y_train.value_counts().min()) < 5:
        raise ValueError(
            f"Reddit {split_name} train partition needs at least five rows in each class"
        )
    if groups.nunique() < 5:
        raise ValueError(
            f"Reddit {split_name} train partition needs at least five distinct authors"
        )


def metric_summary(
    y_true: pd.Series,
    proba: np.ndarray,
    threshold: float | np.ndarray,
) -> dict[str, float | int]:
    """Compute ranking, calibration and decision metrics for one variant."""

    threshold_values = np.asarray(threshold, dtype=float)
    predicted = (proba >= threshold_values).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, predicted, labels=[0, 1]).ravel()
    return {
        "n": int(len(y_true)),
        "viral_rate": float(y_true.mean()),
        "roc_auc": float(roc_auc_score(y_true, proba)),
        "pr_auc": float(average_precision_score(y_true, proba)),
        "brier": float(brier_score_loss(y_true, proba)),
        "accuracy": float(accuracy_score(y_true, predicted)),
        "precision": float(precision_score(y_true, predicted, zero_division=0)),
        "recall": float(recall_score(y_true, predicted, zero_division=0)),
        "f1": float(f1_score(y_true, predicted, zero_division=0)),
        "threshold": float(threshold_values.mean()),
        "true_positive": int(tp),
        "false_positive": int(fp),
        "false_negative": int(fn),
        "true_negative": int(tn),
    }


def fold_balance(y_train: pd.Series, y_validation: pd.Series) -> dict[str, float | int]:
    """Describe untouched fold distributions and the train-only class weight."""

    train_positive = int(y_train.sum())
    train_negative = int(len(y_train) - train_positive)
    return {
        "train_rows": int(len(y_train)),
        "validation_rows": int(len(y_validation)),
        "train_viral": train_positive,
        "train_non_viral": train_negative,
        "validation_viral": int(y_validation.sum()),
        "validation_non_viral": int(len(y_validation) - y_validation.sum()),
        "train_viral_rate": float(y_train.mean()),
        "validation_viral_rate": float(y_validation.mean()),
        "scale_pos_weight": float(train_negative / train_positive),
    }


def aggregate_fold_metrics(
    fold_metrics: dict[str, list[dict[str, float | int]]],
) -> dict[str, dict[str, dict[str, float]]]:
    """Return mean and sample standard deviation for every fold metric."""

    aggregated: dict[str, dict[str, dict[str, float]]] = {}
    for variant, rows in fold_metrics.items():
        frame = pd.DataFrame(rows)
        numeric = frame.select_dtypes(include=[np.number])
        aggregated[variant] = {
            column: {
                "mean": float(numeric[column].mean()),
                "std": float(numeric[column].std(ddof=1)),
            }
            for column in numeric.columns
        }
    return aggregated


def bootstrap_intervals(
    y_true: pd.Series,
    probabilities: dict[str, np.ndarray],
    *,
    n_boot: int,
    seed: int,
) -> tuple[dict[str, dict[str, list[float]]], dict[str, list[float]]]:
    """Return 95% CIs and paired audience-minus-no-audience deltas."""

    if n_boot <= 0:
        return {}, {}
    y = y_true.to_numpy()
    rng = np.random.default_rng(seed)
    samples: dict[str, dict[str, list[float]]] = {
        variant: {"roc_auc": [], "pr_auc": [], "brier": []}
        for variant in probabilities
    }
    deltas = {"roc_auc": [], "pr_auc": [], "brier": []}

    accepted = 0
    attempts = 0
    while accepted < n_boot and attempts < n_boot * 10:
        attempts += 1
        indices = rng.integers(0, len(y), size=len(y))
        y_sample = y[indices]
        if np.unique(y_sample).size < 2:
            continue
        current: dict[str, dict[str, float]] = {}
        for variant, proba in probabilities.items():
            sampled_proba = proba[indices]
            current[variant] = {
                "roc_auc": float(roc_auc_score(y_sample, sampled_proba)),
                "pr_auc": float(average_precision_score(y_sample, sampled_proba)),
                "brier": float(brier_score_loss(y_sample, sampled_proba)),
            }
            for metric, value in current[variant].items():
                samples[variant][metric].append(value)
        for metric in deltas:
            deltas[metric].append(
                current["with_audience"][metric] - current["without_audience"][metric]
            )
        accepted += 1

    def interval(values: list[float]) -> list[float]:
        low, high = np.quantile(values, [0.025, 0.975])
        return [float(low), float(high)]

    intervals = {
        variant: {metric: interval(values) for metric, values in metrics.items()}
        for variant, metrics in samples.items()
    }
    return intervals, {metric: interval(values) for metric, values in deltas.items()}


def print_report(report: dict) -> None:
    """Print a compact comparison suitable for a terminal or CI log."""

    print(
        "\n=== Reddit audience ablation "
        f"({report['validation']['folds']}-fold stratified group CV) ==="
    )
    columns = [
        "n",
        "viral_rate",
        "roc_auc",
        "pr_auc",
        "brier",
        "precision",
        "recall",
        "f1",
        "threshold",
    ]
    rows = {
        variant: {column: metrics[column] for column in columns}
        for variant, metrics in report["metrics"].items()
    }
    print(pd.DataFrame.from_dict(rows, orient="index").round(4))
    print("\nFold mean ± standard deviation:")
    fold_rows = {}
    for variant, metrics in report["fold_metrics_summary"].items():
        fold_rows[variant] = {
            metric: f"{metrics[metric]['mean']:.4f} ± {metrics[metric]['std']:.4f}"
            for metric in ("roc_auc", "pr_auc", "brier", "f1")
        }
    print(pd.DataFrame.from_dict(fold_rows, orient="index"))
    print("\nDelta (with audience - without audience):")
    print(pd.Series(report["delta"]).round(4))


def main() -> None:
    # Keep XGBoost and the text pipeline out of module import so metric helpers
    # remain testable in lightweight environments.
    from features.text_content import build_content_model
    from train.train_viral import (
        apply_calibrator,
        fit_calibrator,
        train_model,
        validate_dataset_version,
    )

    parser = argparse.ArgumentParser(
        description=(
            "Cross-validate Reddit models with and without audience features "
            "using author-grouped, class-stratified folds."
        )
    )
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--bert-model-dir", type=Path, default=None)
    parser.add_argument("--folds", type=int, default=DEFAULT_FOLDS)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-boot", type=int, default=1000)
    parser.add_argument("--dataset-version")
    args = parser.parse_args()

    full = pd.read_parquet(args.data)
    validate_dataset_version(full, args.dataset_version)
    df = reddit_rows(full)
    feature_sets = ablation_feature_sets(df)
    y = df[TARGET].astype(int)
    groups = author_groups(df)
    folds = stratified_group_folds(df, n_splits=args.folds, seed=args.seed)
    text = df[TEXT].astype(str)

    if "content_score_bert" in df.columns:
        if not args.bert_model_dir:
            raise SystemExit("Data has content_score_bert; pass --bert-model-dir for parity.")
        bert_scores = df["content_score_bert"].to_numpy()
        content_bundle = {"content_model_dir": str(args.bert_model_dir)}
    else:
        bert_scores = None
        content_bundle = {}

    probabilities = {
        variant: np.full(len(df), np.nan, dtype=float)
        for variant in feature_sets
    }
    thresholds = {
        variant: np.full(len(df), np.nan, dtype=float)
        for variant in feature_sets
    }
    fold_metrics: dict[str, list[dict[str, float | int]]] = {
        variant: [] for variant in feature_sets
    }
    balance_report: list[dict[str, float | int]] = []
    model_kwargs = {"colsample_bytree": 1.0}
    args.model_dir.mkdir(parents=True, exist_ok=True)

    for fold_number, (train_idx, validation_idx) in enumerate(folds, start=1):
        fold_seed = args.seed + fold_number
        y_train = y.iloc[train_idx]
        y_validation = y.iloc[validation_idx]
        train_groups = groups.iloc[train_idx]
        validation_groups = groups.iloc[validation_idx]
        balance_report.append(
            {
                "fold": fold_number,
                "train_authors": int(train_groups.nunique()),
                "validation_authors": int(validation_groups.nunique()),
                **fold_balance(y_train, y_validation),
            }
        )

        if bert_scores is not None:
            train_score = bert_scores[train_idx]
            validation_score = bert_scores[validation_idx]
        else:
            inner_splits = min(
                DEFAULT_FOLDS,
                int(train_groups.nunique()),
                int(y_train.value_counts().min()),
            )
            if inner_splits < 2:
                raise ValueError(
                    f"Fold {fold_number} cannot build leakage-free content scores"
                )
            content_model = build_content_model()
            inner_cv = StratifiedGroupKFold(
                n_splits=inner_splits,
                shuffle=True,
                random_state=fold_seed,
            )
            train_score = cross_val_predict(
                content_model,
                text.iloc[train_idx],
                y_train,
                groups=train_groups,
                cv=inner_cv,
                method="predict_proba",
            )[:, 1]
            content_model.fit(text.iloc[train_idx], y_train)
            validation_score = content_model.predict_proba(
                text.iloc[validation_idx]
            )[:, 1]

        for variant, base_features in feature_sets.items():
            numeric = df[base_features].astype(float)
            X_train = numeric.iloc[train_idx].assign(content_score=train_score)
            X_validation = numeric.iloc[validation_idx].assign(
                content_score=validation_score
            )
            # Every fold computes its own scale_pos_weight from y_train. The
            # untouched validation fold therefore keeps the real class rate.
            model = train_model(X_train, y_train, fold_seed, **model_kwargs)
            calibrator, threshold = fit_calibrator(
                X_train,
                y_train,
                train_groups,
                fold_seed,
                model_kwargs=model_kwargs,
            )
            raw = model.predict_proba(X_validation)[:, 1]
            proba = apply_calibrator(calibrator, raw)
            probabilities[variant][validation_idx] = proba
            thresholds[variant][validation_idx] = threshold
            fold_metrics[variant].append(
                {
                    "fold": fold_number,
                    **metric_summary(y_validation, proba, threshold),
                }
            )

    for variant, values in probabilities.items():
        if np.isnan(values).any() or np.isnan(thresholds[variant]).any():
            raise RuntimeError(f"Incomplete out-of-fold predictions for {variant}")

    metrics = {
        variant: metric_summary(y, probabilities[variant], thresholds[variant])
        for variant in feature_sets
    }
    fold_metrics_summary = aggregate_fold_metrics(fold_metrics)
    intervals, delta_intervals = bootstrap_intervals(
        y,
        probabilities,
        n_boot=args.n_boot,
        seed=args.seed,
    )
    delta = {
        metric: float(metrics["with_audience"][metric] - metrics["without_audience"][metric])
        for metric in ("roc_auc", "pr_auc", "brier", "accuracy", "precision", "recall", "f1")
    }

    # Train deployable final models on every Reddit row only after the
    # cross-validated experiment is complete.
    if bert_scores is not None:
        final_content_score = bert_scores
    else:
        final_content_model = build_content_model()
        final_content_cv = StratifiedGroupKFold(
            n_splits=args.folds,
            shuffle=True,
            random_state=args.seed,
        )
        final_content_score = cross_val_predict(
            final_content_model,
            text,
            y,
            groups=groups,
            cv=final_content_cv,
            method="predict_proba",
        )[:, 1]
        final_content_model.fit(text, y)
        content_bundle = {"content_model": final_content_model}

    for variant, base_features in feature_sets.items():
        features = [*base_features, "content_score"]
        X_all = df[base_features].astype(float).assign(
            content_score=final_content_score
        )
        model = train_model(X_all, y, args.seed, **model_kwargs)
        calibrator, threshold = fit_calibrator(
            X_all,
            y,
            groups,
            args.seed,
            model_kwargs=model_kwargs,
        )
        joblib.dump(
            {
                "model": model,
                "calibrator": calibrator,
                "threshold": threshold,
                "features": features,
                "source": SOURCE,
                "audience_mode": variant,
                "dataset_version": args.dataset_version,
                "validation": "StratifiedGroupKFold",
                "validation_folds": args.folds,
                "balance_strategy": "fold-local scale_pos_weight",
                **content_bundle,
            },
            args.model_dir / f"reddit_{variant}.joblib",
        )

    report = {
        "source": SOURCE,
        "dataset": str(args.data),
        "dataset_version": args.dataset_version,
        "seed": args.seed,
        "known_audience_rows": int(df["chan_log_audience"].notna().sum()),
        "audience_unique_values": int(df["chan_log_audience"].nunique(dropna=True)),
        "total_rows": int(len(df)),
        "validation": {
            "scheme": "StratifiedGroupKFold",
            "folds": args.folds,
            "group": GROUP,
            "stratified_target": TARGET,
            "balance_strategy": "fold-local scale_pos_weight",
            "validation_resampled": False,
            "fold_balance": balance_report,
        },
        "metrics": metrics,
        "fold_metrics": fold_metrics,
        "fold_metrics_summary": fold_metrics_summary,
        "confidence_intervals_95": intervals,
        "delta": delta,
        "delta_confidence_intervals_95": delta_intervals,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print_report(report)
    print(f"\nModels saved -> {args.model_dir}")
    print(f"Metrics saved -> {args.output}")


if __name__ == "__main__":
    main()
