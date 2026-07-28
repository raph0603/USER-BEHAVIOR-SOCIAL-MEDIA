"""Fine-tune XLM-R on the paper's exact split, so its score sits in the same table as TF-IDF.

The other Kaggle script, train_bert_content.py, runs 5-fold StratifiedKFold over every row
to produce an out-of-fold `content_score_bert` for the fusion model. That number cannot be
compared with the TF-IDF baseline reported in the paper, for two reasons:

  1. It is an out-of-fold score over all 3,798 rows; the paper's baseline is measured on the
     870-row held-out test split.
  2. Stratified folds let the same author appear in a training fold and a validation fold,
     so the score is inflated by author identity -- the very leak the paper's split avoids.

This script therefore reproduces `ml/train/train_viral.py:split_indices` exactly: one
GroupShuffleSplit on author_hash, test_size 0.2, seed 42. Train on the 2,928 training rows,
predict the 870 test rows, report average precision on them. That figure, and only that
figure, is comparable with the 0.412 the sparse lexical baseline reaches.

Kaggle setup:
  1. Upload ml/data/train_dataset.parquet as a Kaggle Dataset and point DATA at it.
  2. Settings -> Accelerator -> GPU T4. Then Save Version (Save & Run All).
Runtime is roughly 15-25 minutes on a T4.
"""

import json
import random
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import GroupShuffleSplit
from torch.utils.data import DataLoader, TensorDataset
from transformers import AutoModelForSequenceClassification, AutoTokenizer

DATA_DIR = "/kaggle/input/viral-train-dataset"
# Either the full training table or the three-column export made for this run.
DATA_CANDIDATES = ["train_dataset_kaggle.parquet", "train_dataset.parquet"]
TEXT_COL, LABEL_COL, GROUP_COL = "clean_text", "viral", "author_hash"

MODEL_NAME = "xlm-roberta-base"
MAXLEN, EPOCHS, BS, LR = 192, 4, 16, 2e-5
TEST_SIZE, SEED = 0.2, 42          # must match ml/train/train_viral.py
TFIDF_BASELINE_AP = 0.412          # what this run has to beat, same rows

WORK = Path("/kaggle/working")
OUT_METRICS = WORK / "bert_holdout_metrics.json"
OUT_SCORES = WORK / "bert_holdout_test_scores.csv"


def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


def log(message: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {message}", flush=True)


set_seed(SEED)
device = "cuda" if torch.cuda.is_available() else "cpu"
log(f"device: {device}")
if device == "cpu":
    log("WARNING: no GPU. Enable Settings -> Accelerator -> GPU T4, or this takes hours.")

data_path = next((p for p in (Path(DATA_DIR) / name for name in DATA_CANDIDATES) if p.exists()), None)
if data_path is None:
    raise SystemExit(
        f"No dataset found in {DATA_DIR}. Expected one of {DATA_CANDIDATES}. "
        "Add the Kaggle Dataset as an input, and check its folder name."
    )
log(f"reading {data_path}")
df = pd.read_parquet(data_path)
for column in (TEXT_COL, LABEL_COL, GROUP_COL):
    if column not in df.columns:
        raise SystemExit(f"The dataset is missing the column '{column}'.")

texts = df[TEXT_COL].fillna("").astype(str).tolist()
y = df[LABEL_COL].astype(int).to_numpy()

# Exactly ml/train/train_viral.py:split_indices -- same splitter, same fallback, same seed.
groups = df[GROUP_COL].fillna(pd.Series(df.index.astype(str), index=df.index))
train_idx, test_idx = next(
    GroupShuffleSplit(n_splits=1, test_size=TEST_SIZE, random_state=SEED).split(df, y, groups)
)
log(f"rows {len(df)} | train {len(train_idx)} | test {len(test_idx)}")
log(f"viral rate: train {y[train_idx].mean():.4f} | test {y[test_idx].mean():.4f}")
shared = set(groups.iloc[train_idx]) & set(groups.iloc[test_idx])
log(f"authors present on both sides of the split: {len(shared)} (must be 0)")

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
encoded = tokenizer(texts, truncation=True, padding="max_length", max_length=MAXLEN,
                    return_tensors="pt")
input_ids, attention_mask = encoded["input_ids"], encoded["attention_mask"]
labels = torch.tensor(y)
truncated = float((encoded["attention_mask"].sum(dim=1) == MAXLEN).float().mean())
log(f"posts hitting the {MAXLEN}-token limit: {truncated:.1%}")


def loader(indices, shuffle: bool) -> DataLoader:
    index = torch.tensor(indices)
    dataset = TensorDataset(input_ids[index], attention_mask[index], labels[index])
    return DataLoader(dataset, batch_size=BS, shuffle=shuffle)


model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2).to(device)
optimiser = torch.optim.AdamW(model.parameters(), lr=LR)

# The training set is imbalanced the same way the fusion model's is; weighting the loss keeps
# the comparison against the balanced-class-weight baseline fair.
positives = int(y[train_idx].sum())
weights = torch.tensor(
    [len(train_idx) / (2 * (len(train_idx) - positives)), len(train_idx) / (2 * positives)],
    dtype=torch.float, device=device,
)
loss_fn = torch.nn.CrossEntropyLoss(weight=weights)

for epoch in range(EPOCHS):
    model.train()
    running = 0.0
    batches = loader(train_idx, shuffle=True)
    for ids, mask, target in batches:
        optimiser.zero_grad()
        logits = model(input_ids=ids.to(device), attention_mask=mask.to(device)).logits
        loss = loss_fn(logits, target.to(device))
        loss.backward()
        optimiser.step()
        running += float(loss)
    log(f"epoch {epoch + 1}/{EPOCHS} | mean loss {running / len(batches):.4f}")

model.eval()
scores = []
with torch.no_grad():
    for ids, mask, _ in loader(test_idx, shuffle=False):
        logits = model(input_ids=ids.to(device), attention_mask=mask.to(device)).logits
        scores.append(torch.softmax(logits, dim=1)[:, 1].cpu().numpy())
proba = np.concatenate(scores)

ap = float(average_precision_score(y[test_idx], proba))
roc = float(roc_auc_score(y[test_idx], proba))

print("\n" + "=" * 62)
print("  Held-out test split, author-disjoint, identical to the paper")
print("=" * 62)
print(f"  fine-tuned {MODEL_NAME:<24} average precision {ap:.4f}   ROC-AUC {roc:.4f}")
print(f"  TF-IDF baseline (same rows)              average precision {TFIDF_BASELINE_AP:.4f}")
print(f"  difference                               {ap - TFIDF_BASELINE_AP:+.4f}")
print("=" * 62)
print("  A negative difference is a result, not a failure: it says the corpus is too")
print("  small for a 278M-parameter encoder, which is what the paper reports.")
print("=" * 62 + "\n")

pd.DataFrame({"row": test_idx, "y_true": y[test_idx], "bert_proba": proba}).to_csv(
    OUT_SCORES, index=False
)
OUT_METRICS.write_text(
    json.dumps(
        {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "protocol": "GroupShuffleSplit on author_hash, test_size=0.2, seed=42",
            "config": {"model": MODEL_NAME, "maxlen": MAXLEN, "epochs": EPOCHS,
                       "batch_size": BS, "lr": LR, "seed": SEED},
            "n_train": int(len(train_idx)), "n_test": int(len(test_idx)),
            "test_viral_rate": round(float(y[test_idx].mean()), 4),
            "truncation_rate": round(truncated, 4),
            "bert_average_precision": round(ap, 4),
            "bert_roc_auc": round(roc, 4),
            "tfidf_average_precision": TFIDF_BASELINE_AP,
        },
        indent=2,
    ),
    encoding="utf-8",
)
log(f"saved -> {OUT_METRICS} and {OUT_SCORES}")
