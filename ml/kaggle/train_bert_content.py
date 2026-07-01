"""Fine-tune a multilingual content model (XLM-R) for viral prediction — run on Kaggle GPU.

Upgrades the TF-IDF content_score to a BERT score that handles EN + VI. Produces an
out-of-fold `content_score_bert` for every row (leakage-free → usable as a fusion
feature) and a final model for serving. Drop-in: same role as content_score.

Everything is written to /kaggle/working/, which Kaggle persists when you
"Save Version" (Save & Run All) — so logs, metrics and artifacts are all kept:
    train_dataset_bert.parquet   dataset + content_score_bert column   -> ml/data/
    bert_content/                fine-tuned model + tokenizer          -> ml/models/
    bert_content_metrics.json    config + per-fold + OOF metrics
    bert_content_report.md       short human-readable report

Setup on Kaggle:
1. Upload `ml/data/train_dataset.parquet` as a Kaggle Dataset; set DATA below.
2. Settings -> Accelerator -> GPU (T4 is enough); Save Version (Save & Run All).
"""
import json
import random
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold
from torch.utils.data import DataLoader, TensorDataset
from transformers import AutoModelForSequenceClassification, AutoTokenizer

DATA = "/kaggle/input/viral-train-dataset/train_dataset.parquet"
TEXT_COL, LABEL_COL = "clean_text", "viral"
MODEL_NAME, MAXLEN, EPOCHS, BS, LR = "xlm-roberta-base", 192, 4, 16, 2e-5
N_FOLDS, SEED = 5, 42

WORK = Path("/kaggle/working")
OUT_MODEL = WORK / "bert_content"
OUT_PARQUET = WORK / "train_dataset_bert.parquet"
OUT_METRICS = WORK / "bert_content_metrics.json"
OUT_REPORT = WORK / "bert_content_report.md"


def set_seed(seed: int):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


def log(msg: str):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


set_seed(SEED)
device = "cuda" if torch.cuda.is_available() else "cpu"
log(f"device: {device}")

df = pd.read_parquet(DATA)
texts = df[TEXT_COL].fillna("").astype(str).tolist()
y = df[LABEL_COL].astype(int).to_numpy()
log(f"{len(texts)} docs | viral rate: {float(y.mean()):.3f}")

tok = AutoTokenizer.from_pretrained(MODEL_NAME)


def loader(idx, shuffle):
    enc = tok([texts[i] for i in idx], truncation=True, max_length=MAXLEN,
              padding="max_length", return_tensors="pt")
    ds = TensorDataset(enc["input_ids"], enc["attention_mask"], torch.tensor(y[idx]))
    return DataLoader(ds, batch_size=BS, shuffle=shuffle)


def train_one(tr_idx, te_idx, tag=""):
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2).to(device)
    ytr = y[tr_idx]
    weight = torch.tensor([1.0, float((ytr == 0).sum() / max((ytr == 1).sum(), 1))]).to(device)
    loss_fn = torch.nn.CrossEntropyLoss(weight=weight)
    opt = torch.optim.AdamW(model.parameters(), lr=LR)

    model.train()
    for ep in range(EPOCHS):
        total, n = 0.0, 0
        for ids, mask, lab in loader(tr_idx, True):
            opt.zero_grad()
            logits = model(input_ids=ids.to(device), attention_mask=mask.to(device)).logits
            loss = loss_fn(logits, lab.to(device))
            loss.backward()
            opt.step()
            total += float(loss) * len(lab)
            n += len(lab)
        log(f"  {tag} epoch {ep + 1}/{EPOCHS} loss {total / max(n, 1):.4f}")

    model.eval()
    probs = []
    with torch.no_grad():
        for ids, mask, _ in loader(te_idx, False):
            logits = model(input_ids=ids.to(device), attention_mask=mask.to(device)).logits
            probs.append(torch.softmax(logits, 1)[:, 1].cpu().numpy())
    return np.concatenate(probs), model


cv = StratifiedKFold(n_splits=N_FOLDS, shuffle=True, random_state=SEED)
oof = np.zeros(len(y))
fold_metrics = []
for k, (tr, te) in enumerate(cv.split(texts, y)):
    log(f"fold {k + 1}/{N_FOLDS} training ...")
    oof[te], _ = train_one(tr, te, tag=f"fold{k + 1}")
    pr = float(average_precision_score(y[te], oof[te]))
    roc = float(roc_auc_score(y[te], oof[te]))
    fold_metrics.append({"fold": k + 1, "pr_auc": round(pr, 4), "roc_auc": round(roc, 4)})
    log(f"fold {k + 1} PR-AUC {pr:.3f} | ROC-AUC {roc:.3f}")

oof_pr = float(average_precision_score(y, oof))
oof_roc = float(roc_auc_score(y, oof))
log(f"OOF  PR-AUC {oof_pr:.3f} | ROC-AUC {oof_roc:.3f}")

df["content_score_bert"] = oof
df.to_parquet(OUT_PARQUET, index=False)

log("training final model on all data ...")
_, final_model = train_one(np.arange(len(y)), np.arange(len(y)), tag="final")
final_model.save_pretrained(OUT_MODEL)
tok.save_pretrained(OUT_MODEL)

metrics = {
    "created_utc": datetime.now(timezone.utc).isoformat(),
    "config": {"model": MODEL_NAME, "maxlen": MAXLEN, "epochs": EPOCHS,
               "batch_size": BS, "lr": LR, "n_folds": N_FOLDS, "seed": SEED},
    "n_docs": int(len(y)),
    "viral_rate": round(float(y.mean()), 4),
    "fold_metrics": fold_metrics,
    "oof_pr_auc": round(oof_pr, 4),
    "oof_roc_auc": round(oof_roc, 4),
}
OUT_METRICS.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")

OUT_REPORT.write_text(
    f"# BERT content model — report\n\n"
    f"- Model: `{MODEL_NAME}` | maxlen {MAXLEN} | epochs {EPOCHS} | bs {BS} | lr {LR}\n"
    f"- Docs: {len(y)} | viral rate: {y.mean():.3f}\n"
    f"- **OOF PR-AUC: {oof_pr:.3f} | ROC-AUC: {oof_roc:.3f}**\n\n"
    f"| fold | PR-AUC | ROC-AUC |\n|---|---|---|\n"
    + "".join(f"| {m['fold']} | {m['pr_auc']} | {m['roc_auc']} |\n" for m in fold_metrics),
    encoding="utf-8",
)

log(f"Saved -> {OUT_PARQUET}")
log(f"Saved -> {OUT_MODEL}")
log(f"Saved -> {OUT_METRICS}")
log(f"Saved -> {OUT_REPORT}")
log("Done. Use 'Save Version' so /kaggle/working artifacts persist in the notebook output.")
