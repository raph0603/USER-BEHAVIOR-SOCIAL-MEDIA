"""TF-IDF + LogisticRegression content model: P(viral) from the post text alone.

Exposes the same ``.predict_proba(list[str])`` interface that ml/serve/predict.py
relies on, so its score can be fused as a single feature and the backend can be
swapped for embeddings/BERT later without touching callers.

Word ngrams (1,2) keep it language-agnostic for the EN+VI mix; accents are kept
so Vietnamese tokens stay intact.
"""
from __future__ import annotations

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline


def build_content_model() -> Pipeline:
    return Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    ngram_range=(1, 2),
                    min_df=2,
                    max_features=20000,
                    sublinear_tf=True,
                    strip_accents=None,
                ),
            ),
            ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]
    )
