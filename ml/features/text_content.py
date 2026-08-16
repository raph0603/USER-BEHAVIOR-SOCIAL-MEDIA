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

from experiment_config import CONTENT_MODEL, DEFAULT_RANDOM_SEED


def build_content_model(seed: int = DEFAULT_RANDOM_SEED) -> Pipeline:
    tfidf = CONTENT_MODEL["tfidf"]
    logistic = CONTENT_MODEL["logistic_regression"]
    return Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    ngram_range=tuple(tfidf["ngram_range"]),
                    min_df=tfidf["min_df"],
                    max_features=tfidf["max_features"],
                    sublinear_tf=tfidf["sublinear_tf"],
                    strip_accents=tfidf["strip_accents"],
                ),
            ),
            (
                "clf",
                LogisticRegression(
                    max_iter=logistic["max_iter"],
                    class_weight=logistic["class_weight"],
                    solver=logistic["solver"],
                    random_state=seed,
                ),
            ),
        ]
    )
