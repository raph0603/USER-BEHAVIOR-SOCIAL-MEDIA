"""Explainable viral prediction (Stage 1) — the novelty: not just a score, but WHY.

Loads the fused multi-source model (stage1_multisource.joblib), rebuilds the exact
training features for one post, predicts P(viral), then uses XGBoost per-prediction
SHAP contributions to surface the top drivers as human-readable factors. Emits the
structured JSON the dashboard parses:
  {viral_score, label, confidence, top_factors[], explanation_text, suggestions[]}
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from features.rhetorical_roles import RoleFeaturizer
from features.topics import TopicFeaturizer
from preprocess.build_dataset import add_text_features, clean_text
from train.train_viral import apply_calibrator

DEFAULT_MODEL_PATH = ML_ROOT / "models" / "stage1_multisource.joblib"
MODEL_PATH_ENV = "AI_MODEL_PATH"
MODEL_ALIASES = {
    "legacy": ("AI_MODEL_LEGACY_PATH", DEFAULT_MODEL_PATH),
    "audience-x90": (
        "AI_MODEL_X90_PATH",
        ML_ROOT / "models" / "stage1_multisource_audience_x90.joblib",
    ),
}
DECISION_THRESHOLD = 0.50
TOP_K = 5


def configured_model_path(model_name: str | None = None) -> Path:
    """Return the configured path for a public model alias."""

    if model_name is not None:
        try:
            environment_name, default_path = MODEL_ALIASES[model_name]
        except KeyError as exc:
            raise ValueError(f"Unknown model {model_name!r}") from exc
        configured = os.getenv(environment_name, "").strip()
        return Path(configured).expanduser() if configured else default_path

    configured = os.getenv(MODEL_PATH_ENV, "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_MODEL_PATH

# Map raw feature names -> human-readable labels for the explanation.
FEATURE_LABELS = {
    "content_score": "Post content/topic",
    "char_count": "Post length (chars)",
    "word_count": "Word count",
    "has_question": "Contains a question",
    "is_vietnamese": "Written in Vietnamese",
    "f_word": "Hard vocabulary",
    "f_sent": "Long sentences",
    "f_clause": "Many clauses per sentence",
    "f_info": "Technical-term density",
    "f_visual": "Visual emphasis (CAPS/!!!/emoji/#)",
    "cognitive_friction_score": "Overall reading difficulty",
    "role_diversity": "Marketing-role diversity",
    "role_n_segments": "Number of segments",
    "chan_log_audience": "Channel audience size (followers/subscribers)",
    "chan_has_audience": "Channel audience known",
    "chan_audience_available": "Channel audience known",
    "chan_audience_is_zero": "Channel has no audience",
}
_ROLE_LABELS = {
    "cta": "call to action (CTA)",
    "hook": "opening hook",
    "proof": "proof/metrics",
    "social_proof": "social proof",
    "pain_point": "pain point",
    "urgency": "urgency",
}


def _label_for(feature: str) -> str:
    if feature in FEATURE_LABELS:
        return FEATURE_LABELS[feature]
    if feature.startswith("src_"):
        return f"Platform {feature[4:]}"
    if feature.startswith("topic_"):
        return f"Topic #{feature[6:]}"
    for prefix in ("role_n_", "role_ratio_"):
        if feature.startswith(prefix):
            role = feature[len(prefix) :]
            name = _ROLE_LABELS.get(role, role)
            kind = "Count of" if prefix == "role_n_" else "Ratio of"
            return f"Exploratory role cue: {kind.lower()} {name}"
    return feature


class ViralExplainer:
    def __init__(self, model_path: Path | None = None):
        model_path = configured_model_path() if model_path is None else Path(model_path)
        bundle = joblib.load(model_path)
        self.model_path = model_path
        self.model = bundle["model"]
        self.features = bundle["features"]
        # Both absent in models trained before calibration; 0.5 was the old default.
        self.calibrator = bundle.get("calibrator")
        self.threshold = float(
            bundle.get("classification_probability_threshold", bundle.get("threshold"))
            or DECISION_THRESHOLD
        )
        if "content_model" in bundle:
            self.content_model = bundle["content_model"]
        else:  # BERT backend: rebuild from the saved model folder
            from features.bert_content import BertContentModel

            self.content_model = BertContentModel(bundle["content_model_dir"])
        self.roles = (
            RoleFeaturizer()
            if any(feature.startswith("role_") for feature in self.features)
            else None
        )
        self.topics = (
            TopicFeaturizer()
            if any(feature.startswith("topic_") for feature in self.features)
            else None
        )

    def _feature_row(self, text: str, source: str, audience: float | None = None):
        cleaned = clean_text(text)
        df = add_text_features(pd.DataFrame({"clean_text": [cleaned]}))
        if self.roles is not None:
            for name, value in self.roles.post_features(cleaned).items():
                df[name] = value
        if self.topics is not None:
            for name, value in self.topics.transform([cleaned]).iloc[0].items():
                df[name] = value
        df["content_score"] = self.content_model.predict_proba([cleaned])[:, 1]
        src = (source or "").strip().lower()
        for col in self.features:
            if col.startswith("src_"):
                df[col] = 1.0 if col == f"src_{src}" else 0.0
        # Mirror preprocess.build_dataset.add_channel_features: Reddit's subreddit size
        # is a community-level value, not an author audience, so it stays unavailable.
        # For the other sources, an unknown audience is NaN and 0 means an author with
        # no followers; serving must not collapse those two cases.
        if "chan_log_audience" in self.features:
            known = audience is not None and src != "reddit"
            aud = max(float(audience), 0.0) if known else np.nan
            df["chan_log_audience"] = np.log1p(aud) if known else np.nan
            df["chan_has_audience"] = 1.0 if known else 0.0
            df["chan_audience_available"] = 1.0 if known else 0.0
            df["chan_audience_is_zero"] = 1.0 if known and aud == 0 else 0.0
        return df.reindex(columns=self.features, fill_value=0.0).astype(float)

    def _contributions(self, X: pd.DataFrame) -> pd.Series:
        contribs = self.model.get_booster().predict(xgb.DMatrix(X), pred_contribs=True)
        return pd.Series(contribs[0][:-1], index=self.features)  # drop bias term

    def _suggestions(self, X: pd.DataFrame, is_viral: int) -> list[str]:
        row = X.iloc[0]
        tips = []
        # Role assignments are exploratory heuristic signals. Keep them visible in
        # TreeSHAP factors, but do not turn an unvalidated absence into prescriptive advice.
        if row.get("cognitive_friction_score", 0) >= 0.5:
            tips.append("Lower reading difficulty: shorter sentences, less jargon.")
        return tips[:3]

    def explain(
        self,
        text: str,
        source: str = "",
        audience: float | None = None,
        threshold: float | None = None,
    ) -> dict:
        threshold = self.threshold if threshold is None else threshold
        X = self._feature_row(text, source, audience)
        score = float(self.model.predict_proba(X)[0, 1])
        if self.calibrator is not None:
            # Monotonic, so the SHAP factors below still explain the reported score.
            score = float(apply_calibrator(self.calibrator, [score])[0])
        contribs = self._contributions(X)

        top = contribs.reindex(contribs.abs().sort_values(ascending=False).index).head(TOP_K)
        top_factors = [
            {
                "feature": feat,
                "label": _label_for(feat),
                "value": None if pd.isna(X.iloc[0][feat]) else round(float(X.iloc[0][feat]), 4),
                "contribution": round(float(c), 4),
                "direction": "up" if c > 0 else "down",
            }
            for feat, c in top.items()
        ]
        up = [f["label"] for f in top_factors if f["direction"] == "up"][:3]
        down = [f["label"] for f in top_factors if f["direction"] == "down"][:3]
        is_viral = int(score >= threshold)

        # State the threshold: only a quarter of posts go viral, so the boundary sits
        # well below 50% and "likely viral at 34%" reads like a bug without it.
        parts = [
            f"Prediction: {'likely viral' if is_viral else 'unlikely viral'} "
            f"(probability {score:.0%}, decision threshold {threshold:.0%})."
        ]
        if up:
            parts.append("Factors increasing it: " + ", ".join(up) + ".")
        if down:
            parts.append("Factors decreasing it: " + ", ".join(down) + ".")

        return {
            "viral_score": round(score, 3),
            "label": "viral-likely" if is_viral else "not-viral",
            # Distance from the decision boundary, not from 0.5: the calibrated
            # threshold sits near the base rate, so 0.5 is no longer the tipping point.
            "confidence": round(abs(score - threshold) / max(threshold, 1 - threshold), 3),
            "top_factors": top_factors,
            "explanation_text": " ".join(parts),
            "suggestions": self._suggestions(X, is_viral),
        }


_explainer: ViralExplainer | None = None
_explainers: dict[str, ViralExplainer] = {}


def explain_post(
    text: str,
    source: str = "",
    audience: float | None = None,
    model_name: str | None = None,
) -> dict:
    global _explainer
    cache_key = model_name or "default"
    explainer = _explainers.get(cache_key)
    if explainer is None:
        explainer = ViralExplainer(configured_model_path(model_name))
        _explainers[cache_key] = explainer
        if model_name is None:
            _explainer = explainer
    result = explainer.explain(text, source, audience)
    result["model"] = model_name or "legacy"
    return result


if __name__ == "__main__":
    import json

    samples = [
        (
            "This EV has insane range. Over 300,000 drivers already switched. Limited offer — order today!",
            "x",
        ),
        ("Pin xe điện hết giữa đường, chờ 3 tiếng mới có cứu hộ.", "reddit"),
    ]
    for text, src in samples:
        print(json.dumps(explain_post(text, src), ensure_ascii=False, indent=2))
        print("-" * 60)
