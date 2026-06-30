"""Serving wrapper for the Kaggle-trained BERT content model.

Same ``.predict_proba(list[str]) -> ndarray`` interface as the TF-IDF content
model, so it drops into the fusion/serving unchanged. Needs `transformers` +
`torch` (CPU is fine for single-post inference); the imports are lazy so the
TF-IDF path never requires them.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np


class BertContentModel:
    def __init__(self, model_dir: str | Path, max_len: int = 192, batch_size: int = 32):
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self._torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.tokenizer = AutoTokenizer.from_pretrained(str(model_dir))
        self.model = AutoModelForSequenceClassification.from_pretrained(str(model_dir)).to(self.device).eval()
        self.max_len = max_len
        self.batch_size = batch_size

    def predict_proba(self, texts) -> np.ndarray:
        torch = self._torch
        texts = [str(t or "") for t in texts]
        chunks = []
        for i in range(0, len(texts), self.batch_size):
            enc = self.tokenizer(
                texts[i:i + self.batch_size],
                truncation=True,
                max_length=self.max_len,
                padding=True,
                return_tensors="pt",
            ).to(self.device)
            with torch.no_grad():
                logits = self.model(**enc).logits
            chunks.append(torch.softmax(logits, 1)[:, 1].cpu().numpy())
        viral = np.concatenate(chunks) if chunks else np.zeros(0)
        return np.column_stack([1.0 - viral, viral])
