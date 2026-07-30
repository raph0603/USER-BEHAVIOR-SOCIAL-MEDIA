"""Train and evaluate Stage 1 with balanced, grouped K-fold validation.

The input is undersampled to the smallest source while preserving each source's
viral rate. The resulting dataset therefore contains exactly as many YouTube,
Reddit and X rows. K-fold validation then preserves the joint ``source × viral``
distribution and keeps every author in exactly one fold.
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

DEFAULT_DATA = ML_ROOT / "data" / "train_dataset_multisource.parquet"
DEFAULT_BALANCED_DATA = ML_ROOT / "data" / "train_dataset_multisource_balanced.parquet"
DEFAULT_MODEL = ML_ROOT / "models" / "stage1_multisource.joblib"
DEFAULT_OUTPUT = ML_ROOT / "results" / "multisource_cv_metrics.json"
DEFAULT_FOLDS = 5
SOURCE = "source"
TARGET = "viral"
GROUP = "author_hash"
TEXT = "clean_text"


def author_groups(df: pd.DataFrame) -> pd.Series:
    """Return author groups, with one private group for every missing author."""

    fallback = df.index.to_series().map(lambda index: f"missing-author-{index}")
    return df[GROUP].astype("string").fillna(fallback)


def source_class_strata(df: pd.DataFrame) -> pd.Series:
    """Create the composite label whose proportions every fold must preserve."""

    sources = df[SOURCE].astype("string").str.strip().str.lower()
    labels = df[TARGET].astype(int).astype(str)
    return sources + "|" + labels


def balance_sources_for_cv(df: pd.DataFrame, *, seed: int) -> pd.DataFrame:
    """Undersample every source to the minimum count, stratified by viral label."""

    normalized = df[SOURCE].astype("string").str.strip().str.lower()
    counts = normalized.value_counts()
    if len(counts) < 2:
        raise ValueError("Source balancing requires at least two sources")
    target_rows = int(counts.min())
    balanced_parts = []

    for offset, source in enumerate(sorted(counts.index.astype(str))):
        source_rows = df.loc[normalized.eq(source)]
        label_counts = source_rows[TARGET].astype(int).value_counts().sort_index()
        exact_quotas = label_counts / len(source_rows) * target_rows
        quotas = np.floor(exact_quotas).astype(int)
        remaining = target_rows - int(quotas.sum())
        fractions = (exact_quotas - quotas).sort_values(ascending=False)
        for label in fractions.index[:remaining]:
            quotas.loc[label] += 1

        sampled_labels = []
        for label, quota in quotas.items():
            candidates = source_rows.loc[source_rows[TARGET].astype(int).eq(int(label))]
            sampled_labels.append(
                candidates.sample(
                    n=int(quota),
                    replace=False,
                    random_state=seed + offset * 100 + int(label),
                )
            )
        balanced_parts.append(pd.concat(sampled_labels))

    balanced = pd.concat(balanced_parts)
    balanced = balanced.sample(frac=1.0, random_state=seed).reset_index(drop=True)
    balanced_counts = source_counts(balanced)
    if len(set(balanced_counts.values())) != 1:
        raise RuntimeError(f"Source balancing failed: {balanced_counts}")
    return balanced


def source_balance_weights(sources: pd.Series) -> pd.Series:
    """Correct small fold-level count differences caused by grouped splitting."""

    normalized = sources.astype("string").str.strip().str.lower()
    counts = normalized.value_counts()
    if len(counts) < 2:
        raise ValueError("Source balancing requires at least two sources")
    per_source_total = len(normalized) / len(counts)
    weights = normalized.map(per_source_total / counts)
    if weights.isna().any():
        raise ValueError("Unable to compute a source weight for every training row")
    return weights.astype(float)


def stratified_source_folds(
    df: pd.DataFrame,
    *,
    n_splits: int,
    seed: int,
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Return source/class-stratified folds with author-level isolation."""

    if n_splits < 2:
        raise ValueError("K-fold validation requires at least two folds")
    groups = author_groups(df)
    strata = source_class_strata(df)
    support = strata.value_counts()
    if int(support.min()) < n_splits:
        raise ValueError(
            f"Each source/class stratum needs at least {n_splits} rows; "
            f"minimum support is {int(support.min())}"
        )
    if groups.nunique() < n_splits:
        raise ValueError(
            f"K-fold validation needs at least {n_splits} authors; "
            f"received {groups.nunique()}"
        )

    splitter = StratifiedGroupKFold(
        n_splits=n_splits,
        shuffle=True,
        random_state=seed,
    )
    folds = list(splitter.split(df, strata, groups))
    validation_rows: list[int] = []
    for fold_number, (train_idx, validation_idx) in enumerate(folds, start=1):
        if set(groups.iloc[train_idx]) & set(groups.iloc[validation_idx]):
            raise RuntimeError(f"Author leakage detected in fold {fold_number}")
        if set(df.iloc[validation_idx][SOURCE].astype(str).str.lower()) != set(
            df[SOURCE].astype(str).str.lower()
        ):
            raise ValueError(f"Fold {fold_number} does not contain every source")
        if df.iloc[validation_idx][TARGET].nunique() < 2:
            raise ValueError(f"Fold {fold_number} does not contain both viral classes")
        validation_rows.extend(validation_idx.tolist())
    if sorted(validation_rows) != list(range(len(df))):
        raise RuntimeError("Validation folds do not cover every row exactly once")
    return folds


def metric_summary(
    y_true: pd.Series,
    probabilities: np.ndarray,
    thresholds: float | np.ndarray,
) -> dict[str, float | int]:
    """Compute ranking, calibration and thresholded decision metrics."""

    threshold_values = np.asarray(thresholds, dtype=float)
    predicted = (probabilities >= threshold_values).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, predicted, labels=[0, 1]).ravel()
    return {
        "n": int(len(y_true)),
        "viral_rate": float(y_true.mean()),
        "roc_auc": float(roc_auc_score(y_true, probabilities)),
        "pr_auc": float(average_precision_score(y_true, probabilities)),
        "brier": float(brier_score_loss(y_true, probabilities)),
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


def aggregate_fold_metrics(
    rows: list[dict[str, float | int]],
) -> dict[str, dict[str, float]]:
    """Summarize fold metrics as mean and sample standard deviation."""

    frame = pd.DataFrame(rows)
    result = {}
    for column in (
        "roc_auc",
        "pr_auc",
        "brier",
        "accuracy",
        "precision",
        "recall",
        "f1",
        "threshold",
    ):
        result[column] = {
            "mean": float(frame[column].mean()),
            "std": float(frame[column].std(ddof=1)),
        }
    return result


def source_counts(frame: pd.DataFrame) -> dict[str, int]:
    """Return stable lowercase source counts."""

    return {
        str(source): int(count)
        for source, count in frame[SOURCE]
        .astype("string")
        .str.lower()
        .value_counts()
        .sort_index()
        .items()
    }


def source_weight_totals(
    sources: pd.Series,
    weights: pd.Series,
) -> dict[str, float]:
    """Report the effective training contribution of every source."""

    frame = pd.DataFrame(
        {
            "source": sources.astype("string").str.lower().to_numpy(),
            "weight": weights.to_numpy(),
        }
    )
    return {
        str(source): float(value)
        for source, value in frame.groupby("source")["weight"].sum().sort_index().items()
    }


def _print_report(report: dict) -> None:
    print(
        "\n=== Multi-source Stage-1 "
        f"({report['validation']['folds']}-fold source-balanced CV) ==="
    )
    overall = pd.DataFrame([report["metrics"]["overall"]], index=["overall"])
    per_source = pd.DataFrame.from_dict(report["metrics"]["per_source"], orient="index")
    columns = [
        "n",
        "viral_rate",
        "roc_auc",
        "pr_auc",
        "brier",
        "precision",
        "recall",
        "f1",
    ]
    print(pd.concat([overall, per_source])[columns].round(4))
    print("\nFold mean ± standard deviation:")
    fold_summary = report["fold_metrics_summary"]
    print(
        pd.Series(
            {
                metric: f"{fold_summary[metric]['mean']:.4f} ± "
                f"{fold_summary[metric]['std']:.4f}"
                for metric in ("roc_auc", "pr_auc", "brier", "f1")
            }
        )
    )


def main() -> None:
    # Heavy ML imports stay local so the fold/weight helpers remain lightweight
    # and independently testable.
    from features.text_content import build_content_model
    from train.train_viral import (
        apply_calibrator,
        feature_columns,
        fit_calibrator,
        train_model,
        validate_dataset_version,
    )

    parser = argparse.ArgumentParser(
        description=(
            "Train Stage-1 with source/class-stratified author-grouped K-fold "
            "validation and equal source contribution."
        )
    )
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--balanced-data", type=Path, default=DEFAULT_BALANCED_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--folds", type=int, default=DEFAULT_FOLDS)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dataset-version")
    args = parser.parse_args()

    input_df = pd.read_parquet(args.data)
    validate_dataset_version(input_df, args.dataset_version)
    required = {SOURCE, TARGET, GROUP, TEXT}
    missing = sorted(required - set(input_df.columns))
    if missing:
        raise ValueError(f"Training data is missing required columns: {missing}")
    input_df = input_df.reset_index(drop=True)
    input_source_counts = source_counts(input_df)
    df = balance_sources_for_cv(input_df, seed=args.seed)
    args.balanced_data.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.balanced_data, index=False)
    y = df[TARGET].astype(int)
    groups = author_groups(df)
    strata = source_class_strata(df)
    folds = stratified_source_folds(df, n_splits=args.folds, seed=args.seed)
    base_features = feature_columns(df)
    numeric = df[base_features].astype(float)
    text = df[TEXT].astype(str)

    probabilities = np.full(len(df), np.nan, dtype=float)
    thresholds = np.full(len(df), np.nan, dtype=float)
    fold_metrics: list[dict[str, float | int]] = []
    fold_details: list[dict] = []

    for fold_number, (train_idx, validation_idx) in enumerate(folds, start=1):
        fold_seed = args.seed + fold_number
        y_train = y.iloc[train_idx]
        y_validation = y.iloc[validation_idx]
        train_groups = groups.iloc[train_idx]
        train_sources = df.iloc[train_idx][SOURCE]
        weights = source_balance_weights(train_sources)

        content_model = build_content_model()
        inner_cv = StratifiedGroupKFold(
            n_splits=args.folds,
            shuffle=True,
            random_state=fold_seed,
        )
        train_content_score = cross_val_predict(
            content_model,
            text.iloc[train_idx],
            y_train,
            groups=train_groups,
            cv=inner_cv,
            method="predict_proba",
        )[:, 1]
        content_model.fit(text.iloc[train_idx], y_train)
        validation_content_score = content_model.predict_proba(
            text.iloc[validation_idx]
        )[:, 1]

        X_train = numeric.iloc[train_idx].assign(
            content_score=train_content_score
        )
        X_validation = numeric.iloc[validation_idx].assign(
            content_score=validation_content_score
        )
        model = train_model(
            X_train,
            y_train,
            fold_seed,
            sample_weight=weights,
        )
        calibrator, threshold = fit_calibrator(
            X_train,
            y_train,
            train_groups,
            fold_seed,
            sample_weight=weights,
        )
        raw = model.predict_proba(X_validation)[:, 1]
        proba = apply_calibrator(calibrator, raw)
        probabilities[validation_idx] = proba
        thresholds[validation_idx] = threshold
        fold_metrics.append(
            {
                "fold": fold_number,
                **metric_summary(y_validation, proba, threshold),
            }
        )
        fold_details.append(
            {
                "fold": fold_number,
                "train_rows": int(len(train_idx)),
                "validation_rows": int(len(validation_idx)),
                "train_authors": int(train_groups.nunique()),
                "validation_authors": int(groups.iloc[validation_idx].nunique()),
                "train_source_counts": source_counts(df.iloc[train_idx]),
                "validation_source_counts": source_counts(df.iloc[validation_idx]),
                "effective_source_weight": source_weight_totals(
                    train_sources,
                    weights,
                ),
                "train_source_class_counts": {
                    str(key): int(value)
                    for key, value in strata.iloc[train_idx]
                    .value_counts()
                    .sort_index()
                    .items()
                },
                "validation_source_class_counts": {
                    str(key): int(value)
                    for key, value in strata.iloc[validation_idx]
                    .value_counts()
                    .sort_index()
                    .items()
                },
            }
        )

    if np.isnan(probabilities).any() or np.isnan(thresholds).any():
        raise RuntimeError("Cross-validation did not score every training row")

    overall_metrics = metric_summary(y, probabilities, thresholds)
    per_source_metrics = {}
    normalized_sources = df[SOURCE].astype("string").str.lower()
    for source in sorted(normalized_sources.unique()):
        mask = normalized_sources.eq(source).to_numpy()
        per_source_metrics[str(source)] = metric_summary(
            y.loc[mask],
            probabilities[mask],
            thresholds[mask],
        )

    # Fit one deployable model after all out-of-fold metrics are frozen.
    final_content_model = build_content_model()
    final_cv = StratifiedGroupKFold(
        n_splits=args.folds,
        shuffle=True,
        random_state=args.seed,
    )
    final_content_score = cross_val_predict(
        final_content_model,
        text,
        y,
        groups=groups,
        cv=final_cv,
        method="predict_proba",
    )[:, 1]
    final_content_model.fit(text, y)
    X_all = numeric.assign(content_score=final_content_score)
    final_weights = source_balance_weights(df[SOURCE])
    final_model = train_model(
        X_all,
        y,
        args.seed,
        sample_weight=final_weights,
    )
    final_calibrator, final_threshold = fit_calibrator(
        X_all,
        y,
        groups,
        args.seed,
        sample_weight=final_weights,
    )

    args.model.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "model": final_model,
            "content_model": final_content_model,
            "calibrator": final_calibrator,
            "threshold": final_threshold,
            "features": [*base_features, "content_score"],
            "dataset_version": args.dataset_version,
            "validation": "StratifiedGroupKFold(source × viral)",
            "validation_folds": args.folds,
            "source_balance": (
                "stratified undersampling to the minimum source count, "
                "with fold-local residual equal-total weights"
            ),
            "source_counts": source_counts(df),
            "source_weight_totals": source_weight_totals(
                df[SOURCE],
                final_weights,
            ),
        },
        args.model,
    )

    report = {
        "dataset": str(args.data),
        "balanced_dataset": str(args.balanced_data),
        "dataset_version": args.dataset_version,
        "seed": args.seed,
        "input_rows": int(len(input_df)),
        "input_source_counts": input_source_counts,
        "rows": int(len(df)),
        "source_counts": source_counts(df),
        "known_audience_by_source": {
            str(source): int(
                df.loc[normalized_sources.eq(source), "chan_log_audience"].notna().sum()
            )
            if "chan_log_audience" in df.columns
            else 0
            for source in sorted(normalized_sources.unique())
        },
        "validation": {
            "scheme": "StratifiedGroupKFold",
            "stratification": "source × viral",
            "group": GROUP,
            "folds": args.folds,
            "source_balance": (
                "stratified undersampling to the minimum source count, "
                "with fold-local residual equal-total weights"
            ),
            "source_balancing_applied_before_folds": True,
            "fold_validation_resampled": False,
            "folds_detail": fold_details,
        },
        "metrics": {
            "overall": overall_metrics,
            "per_source": per_source_metrics,
        },
        "fold_metrics": fold_metrics,
        "fold_metrics_summary": aggregate_fold_metrics(fold_metrics),
        "final_model_threshold": float(final_threshold),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _print_report(report)
    print(f"\nModel saved -> {args.model}")
    print(f"Metrics saved -> {args.output}")


if __name__ == "__main__":
    main()
