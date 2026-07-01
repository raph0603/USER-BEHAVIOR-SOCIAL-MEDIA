"""Per-post rhetorical-role features for the viral model.

Segments a post, classifies each segment's marketing role with the trained
rhetorical_role model, then aggregates to post-level features: count and ratio
per role, number of segments, and role diversity. Pure function of the text, so
it is safe to use at both train and serve time (no engagement leakage).

If the role model is missing the features are skipped with a warning, so the
pipeline still runs without the annotation artifact.
"""
from __future__ import annotations

import re
from pathlib import Path

import joblib
import pandas as pd

ML_ROOT = Path(__file__).resolve().parents[1]
ROLE_MODEL_PATH = ML_ROOT / "models" / "rhetorical_role.joblib"

MIN_SEGMENT_CHARS = 3
_SEGMENT_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+|\s+•\s+")


def segment_text(text: object) -> list[str]:
    parts = _SEGMENT_SPLIT.split(str(text or "").strip())
    return [p.strip() for p in parts if len(p.strip()) >= MIN_SEGMENT_CHARS]


class RoleFeaturizer:
    def __init__(self, model_path: Path = ROLE_MODEL_PATH):
        bundle = joblib.load(model_path)
        self.model = bundle["model"]
        self.roles = list(bundle["roles"])

    def _empty(self) -> dict:
        feats = {f"role_n_{r}": 0 for r in self.roles}
        feats.update({f"role_ratio_{r}": 0.0 for r in self.roles})
        feats["role_n_segments"] = 0
        feats["role_diversity"] = 0
        return feats

    def post_features(self, text: object) -> dict:
        segments = segment_text(text)
        feats = self._empty()
        if not segments:
            return feats
        counts = pd.Series(self.model.predict(segments)).value_counts()
        n = len(segments)
        for role in self.roles:
            c = int(counts.get(role, 0))
            feats[f"role_n_{role}"] = c
            feats[f"role_ratio_{role}"] = c / n
        feats["role_n_segments"] = n
        feats["role_diversity"] = int(len(counts))
        return feats

    def transform(self, texts) -> pd.DataFrame:
        return pd.DataFrame([self.post_features(t) for t in texts])


def add_role_features(df: pd.DataFrame, text_col: str = "clean_text",
                      model_path: Path = ROLE_MODEL_PATH) -> pd.DataFrame:
    if not model_path.exists():
        print(f"[rhetorical_roles] model not found at {model_path}; skipping role features.")
        return df
    feats = RoleFeaturizer(model_path).transform(df[text_col])
    return pd.concat([df.reset_index(drop=True), feats.reset_index(drop=True)], axis=1)


if __name__ == "__main__":
    demo = [
        "Tu passes 3h à créer tes campagnes ? Notre IA génère 10 hooks en 30 secondes. Teste-la gratuitement aujourd'hui.",
        "This EV has amazing range. Over 300 000 drivers already switched. Limited offer, order today!",
    ]
    fz = RoleFeaturizer()
    for t in demo:
        print(t[:60], "->", {k: v for k, v in fz.post_features(t).items() if v})
