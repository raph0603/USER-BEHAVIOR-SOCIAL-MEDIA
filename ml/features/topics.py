"""Topic features: per-post topic distribution via NMF over TF-IDF.

Unsupervised (no label) -> no leakage. Fit on the corpus, emit topic_0..topic_{k-1}
as a soft topic-membership distribution for the fusion model. This fills the
"topic" component of the Stage-1 design; BERTopic (embeddings + UMAP + HDBSCAN) is
the heavier upgrade for when data grows, behind this same feature interface.
"""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.decomposition import NMF
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline

from experiment_config import DEFAULT_RANDOM_SEED, TOPIC_MODEL

ML_ROOT = Path(__file__).resolve().parents[1]
TOPIC_MODEL_PATH = ML_ROOT / "models" / "topic_model.joblib"
N_TOPICS = int(TOPIC_MODEL["n_topics"])


def build_topic_model(n_topics: int = N_TOPICS, seed: int = DEFAULT_RANDOM_SEED) -> Pipeline:
    tfidf = TOPIC_MODEL["tfidf"]
    nmf = TOPIC_MODEL["nmf"]
    return Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    min_df=tfidf["min_df"],
                    max_features=tfidf["max_features"],
                    sublinear_tf=tfidf["sublinear_tf"],
                ),
            ),
            (
                "nmf",
                NMF(
                    n_components=n_topics,
                    init=nmf["init"],
                    random_state=seed,
                    max_iter=nmf["max_iter"],
                ),
            ),
        ]
    )


def _to_frame(matrix) -> pd.DataFrame:
    matrix = np.asarray(matrix, dtype=float)
    row_sum = matrix.sum(axis=1, keepdims=True)
    row_sum[row_sum == 0] = 1.0
    matrix = matrix / row_sum  # normalize to a topic distribution
    return pd.DataFrame(matrix, columns=[f"topic_{i}" for i in range(matrix.shape[1])])


def fit_topic_features(
    texts,
    n_topics: int = N_TOPICS,
    model_path: Path = TOPIC_MODEL_PATH,
    seed: int = DEFAULT_RANDOM_SEED,
) -> pd.DataFrame:
    model = build_topic_model(n_topics, seed)
    matrix = model.fit_transform(pd.Series(texts).fillna("").astype(str))
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)
    return _to_frame(matrix)


class TopicFeaturizer:
    def __init__(self, model_path: Path = TOPIC_MODEL_PATH):
        self.model = joblib.load(model_path)

    def transform(self, texts) -> pd.DataFrame:
        matrix = self.model.transform(pd.Series(texts).fillna("").astype(str))
        return _to_frame(matrix)


def add_topic_features(
    df: pd.DataFrame,
    text_col: str = "clean_text",
    n_topics: int = N_TOPICS,
    model_path: Path = TOPIC_MODEL_PATH,
    seed: int = DEFAULT_RANDOM_SEED,
) -> pd.DataFrame:
    feats = fit_topic_features(df[text_col], n_topics, model_path, seed)
    return pd.concat([df.reset_index(drop=True), feats.reset_index(drop=True)], axis=1)


def top_words(n: int = 8) -> dict:
    """Top terms per topic (for inspection / labelling the explanation)."""
    model = joblib.load(TOPIC_MODEL_PATH)
    vocab = np.array(model.named_steps["tfidf"].get_feature_names_out())
    H = model.named_steps["nmf"].components_
    return {f"topic_{i}": vocab[row.argsort()[::-1][:n]].tolist() for i, row in enumerate(H)}


if __name__ == "__main__":
    for t, words in top_words().items():
        print(t, "->", ", ".join(words))
