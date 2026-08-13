"""Train the rhetorical-role classifier from the silver annotation dataset.

Silver = LLM/heuristic labels with confidence >= 0.75 (relatively clean). The
"gold" file is NOT human-verified, so we evaluate on a stratified split of silver
itself. Roles with fewer than MIN_CLASS samples are dropped (cannot be learned or
evaluated reliably) and logged. The saved model feeds rhetorical_roles.py, which
turns predicted segment roles into per-post features for the viral model.

Coverage caveat: silver only contains the roles that passed the 0.75 confidence
bar, so solution/benefit/educational/scarcity are absent here — the classifier
cannot predict them until the annotation set improves.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import joblib
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from role_contract import role_feature_contract

# Official annotation dataset committed at repo root (merged from main); reproducible for everyone.
DEFAULT_SILVER = ML_ROOT.parent / "annotation_marketing" / "silver_dataset.jsonl"
DEFAULT_MODEL = ML_ROOT / "models" / "rhetorical_role.joblib"
MIN_CLASS = 10


def build_role_model() -> Pipeline:
    return Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(ngram_range=(1, 2), min_df=2, sublinear_tf=True, max_features=20000),
            ),
            ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]
    )


def load_silver(path: Path, min_class: int) -> pd.DataFrame:
    df = pd.read_json(path, lines=True)
    counts = df["primary_role"].value_counts()
    dropped = counts[counts < min_class]
    if len(dropped):
        print(f"Dropped rare roles (<{min_class} samples): {dropped.to_dict()}")
    keep = counts[counts >= min_class].index
    return df[df["primary_role"].isin(keep)].reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the rhetorical-role classifier from silver.")
    parser.add_argument("--silver", type=Path, default=DEFAULT_SILVER)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--min-class", type=int, default=MIN_CLASS)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    df = load_silver(args.silver, args.min_class)
    X, y = df["text"].astype(str), df["primary_role"]
    print(f"Rows: {len(df)} | Roles: {sorted(y.unique())}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, stratify=y, random_state=args.seed
    )
    model = build_role_model()
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    macro_f1 = float(f1_score(y_test, pred, average="macro", zero_division=0))
    print(f"Macro-F1: {macro_f1:.3f}")
    print(classification_report(y_test, pred, digits=3, zero_division=0))
    print(
        "Exploratory component: this score measures agreement with held-out "
        "heuristic silver labels, not accuracy against human-validated gold labels."
    )

    model.fit(X, y)  # refit on all silver for the final artifact
    args.model.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "model": model,
            "roles": sorted(y.unique()),
            "role_feature_contract": role_feature_contract(),
            "evaluation": {
                "macro_f1": macro_f1,
                "reference_labels": "held_out_automated_heuristic_silver",
                "human_gold_validated": False,
                "test_size": args.test_size,
                "seed": args.seed,
            },
        },
        args.model,
    )
    print(f"Saved -> {args.model}")


if __name__ == "__main__":
    main()
