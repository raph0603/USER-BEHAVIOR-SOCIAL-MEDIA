"""Stage 1 (pre-launch) viral predictor for an ad / piece of content.

LONG-TERM DESIGN (why this survives model upgrades)
---------------------------------------------------
- The content scorer is ANY object exposing ``.predict_proba(list[str]) -> ndarray``.
  Today that artifact is a TF-IDF + LogisticRegression sklearn Pipeline. To upgrade to
  sentence-embeddings or a fine-tuned BERT/ViralBERT later, train a drop-in object with the
  SAME interface, save it to the SAME path, and this serving file does not change.
- Components are decoupled and loaded independently. If the fusion artifact is missing the
  predictor degrades gracefully to the content score (currently the strongest single signal).
- ``CONTENT_MODEL_PATH`` is the single switch: point it at a new artifact to swap the backend.

So "is TF-IDF good long term?" -> the *backend* is replaceable; the *interface* is what we commit to.
"""
from __future__ import annotations

from pathlib import Path
import joblib

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "ml" / "models"

# --- single switch for the content backend (swap to an embedding/BERT artifact later) ---
CONTENT_MODEL_PATH = MODELS / "stage1_content_model.joblib"
DECISION_THRESHOLD = 0.50          # P(viral) cutoff; tune on a validation set / business need


class StageOnePredictor:
    """Loads the trained Stage-1 artifacts and scores new content."""

    def __init__(self, content_path: Path = CONTENT_MODEL_PATH):
        # content backend: only requirement is .predict_proba(list[str])
        self.content = joblib.load(content_path)

    @staticmethod
    def _combine(title: str, description: str = "", transcript: str = "") -> str:
        # must match the training-time text assembly (see 02_features.ipynb -> text_all)
        return f"{title or ''} . {description or ''} . {transcript or ''}".strip()

    def content_score(self, title: str, description: str = "", transcript: str = "") -> float:
        """P(viral) from the content text alone, in [0, 1]."""
        text = self._combine(title, description, transcript)
        return float(self.content.predict_proba([text])[0, 1])

    def predict(self, title: str, description: str = "", transcript: str = "",
                threshold: float = DECISION_THRESHOLD) -> dict:
        """Return the viral prediction for one ad."""
        score = self.content_score(title, description, transcript)
        return {
            "content_score": round(score, 3),
            "is_viral": int(score >= threshold),
            "label": "viral-likely" if score >= threshold else "not-viral",
            "threshold": threshold,
        }


# module-level singleton + convenience function
_predictor: StageOnePredictor | None = None


def predict_ad(title: str, description: str = "", transcript: str = "") -> dict:
    """Score a single ad. Loads the model once and caches it."""
    global _predictor
    if _predictor is None:
        _predictor = StageOnePredictor()
    return _predictor.predict(title, description, transcript)


if __name__ == "__main__":
    examples = [
        ("The 2024 Chevrolet Equinox EV review: range, price and charging explained",
         "We test the new affordable electric SUV, real-world range and charging cost vs gas.",
         "so today we are driving the equinox ev and talking about battery range price and energy ..."),
        ("Super Bowl EV commercial 2025",
         "Official ad spot.",
         ""),
    ]
    p = StageOnePredictor()
    for title, desc, tr in examples:
        out = p.predict(title, desc, tr)
        print(f"\n{title[:55]:55s} -> {out}")
