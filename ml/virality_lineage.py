"""Compatibility checks for ground-truth virality contract lineage."""

from __future__ import annotations

import re
from typing import Mapping

import pandas as pd


FINGERPRINT_PATTERN = re.compile(r"^[a-f0-9]{64}$")
LEGACY_POLICY = "legacy_dataset_relative_top_quartile"


def dataset_virality_lineage(frame: pd.DataFrame) -> dict[str, str]:
    required = {"virality_policy", "virality_contract_fingerprint"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(
            "Dataset is a legacy artifact without frozen virality contract lineage: "
            + ", ".join(missing)
        )
    policies = sorted(frame["virality_policy"].dropna().astype(str).unique())
    fingerprints = sorted(frame["virality_contract_fingerprint"].dropna().astype(str).unique())
    if len(policies) != 1 or len(fingerprints) != 1:
        raise ValueError(
            "Dataset must contain exactly one virality policy and contract fingerprint"
        )
    if policies[0] == LEGACY_POLICY:
        raise ValueError("Legacy dataset-relative labels are not valid for an official model run")
    if not FINGERPRINT_PATTERN.fullmatch(fingerprints[0]):
        raise ValueError("Dataset virality_contract_fingerprint must be a SHA-256 hex digest")
    return {
        "virality_policy": policies[0],
        "virality_contract_fingerprint": fingerprints[0],
    }


def validate_virality_compatibility(
    observed: Mapping[str, str],
    *,
    expected_fingerprint: str | None,
    expected_policy: str | None,
) -> None:
    if expected_fingerprint and observed["virality_contract_fingerprint"] != expected_fingerprint:
        raise ValueError(
            "Virality contract mismatch: expected "
            f"{expected_fingerprint}, received {observed['virality_contract_fingerprint']}"
        )
    if expected_policy and observed["virality_policy"] != expected_policy:
        raise ValueError(
            f"Virality policy mismatch: expected {expected_policy}, "
            f"received {observed['virality_policy']}"
        )
