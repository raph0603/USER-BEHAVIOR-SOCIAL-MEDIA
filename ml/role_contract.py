"""Shared methodological contract for rhetorical-role features."""

from __future__ import annotations


ROLE_COMPONENT_STATUS = "exploratory"
ROLE_LABEL_PROVENANCE = "automated_heuristic_silver"
ROLE_METRIC_INTERPRETATION = "agreement_with_held_out_silver_labels"


def role_feature_contract() -> dict:
    """Return JSON-serializable limits attached to role-derived artifacts."""

    return {
        "status": ROLE_COMPONENT_STATUS,
        "intended_use": "qualitative_feature_level_interpretability",
        "label_provenance": ROLE_LABEL_PROVENANCE,
        "human_gold_validated": False,
        "metric_interpretation": ROLE_METRIC_INTERPRETATION,
        "linguistic_claims_permitted": False,
    }
