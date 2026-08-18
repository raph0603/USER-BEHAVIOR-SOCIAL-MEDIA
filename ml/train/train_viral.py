"""Compatibility entrypoint for the official Stage-1 grouped-CV workflow."""

from __future__ import annotations

import pandas as pd

try:
    from .grouped_cv_stage1 import *  # noqa: F403
    from .grouped_cv_stage1 import main
except ImportError:  # direct script execution
    from grouped_cv_stage1 import *  # type: ignore  # noqa: F403
    from grouped_cv_stage1 import main  # type: ignore


# Kept explicitly in this entrypoint because repository contract tests and legacy
# callers inspect these public Stage-1 helpers directly.
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


def validate_dataset_version(df: pd.DataFrame, expected: str | None) -> None:
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
    extra = sorted(column for column in df.columns if column.startswith(tuple(prefixes)))
    return CONTENT_FEATURES + extra


if __name__ == "__main__":
    main()
