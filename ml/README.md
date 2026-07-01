# `ml/` — Viral prediction & explanation (Stage 1)

Predicts whether a social-media post (EV domain) is likely to go **viral**, and explains
**why** — to power content/marketing recommendations. Multi-source: **YouTube · X · Reddit**.

## Pipeline

```
filtered_events.csv (exported from the Silver lakehouse)
        │
        ▼  preprocess/build_dataset.py
  clean text → unified content features + PER-SOURCE viral label + source one-hot
        │                                 + rhetorical-role features + topic features
        ▼  train/train_viral.py
  content_model (TF-IDF → content_score)  ─┐
  + structural features + src_* + role_* + topic_* + chan_*  ─┴─►  XGBoost fusion  →  P(viral)
        │
        ▼  serve/explain_viral.py
  per-prediction SHAP → JSON {viral_score, label, confidence, top_factors, explanation_text, suggestions}
```

## Key design decisions

- **Unified content features** across all 3 sources (pure functions of the text): `cognitive_friction` + `char/word/has_question/is_vietnamese`.
- **Per-source viral label**: within each source, z-score `log1p` of that platform's engagement metrics → the top `--quantile` (default 0.75) is labelled viral. Engagement columns build the label only — never features (avoids leakage).
- **`content_score`** = TF-IDF + LogReg over the text, fused as a single feature (built out-of-fold to avoid leakage). Interface `.predict_proba(list[str])` → swapping in BERT later needs no other change.
- **`role_*`** = rhetorical marketing roles (cta/hook/proof/…) from the `feature/annotation-roles-marketing` branch; mainly aid explainability.
- **`topic_*`** = NMF topic distribution over TF-IDF (fills the "topic" component of the design; BERTopic is the heavier upgrade).
- **`chan_*`** = channel/author audience size (`follower/subscriber/subreddit_member` unified via log1p) — a *pre-launch* author property (not engagement), so it's a valid feature, not leakage. Best-effort: only added when the export carries those columns.
- **Explanation** = SHAP (XGBoost `pred_contribs`) → maps features to readable reasons + suggestions.

## Run (use the `ml/.venv` Python)

```powershell
$env:PYTHONIOENCODING='utf-8'            # Windows: avoid console encoding issues
# whole training chain in one command:
& ".\ml\.venv\Scripts\python.exe" ml/run_pipeline.py            # role -> dataset -> train -> evaluate
& ".\ml\.venv\Scripts\python.exe" ml/run_pipeline.py --report   # also build the report
# explain a single post:
& ".\ml\.venv\Scripts\python.exe" ml/serve/explain_viral.py
# batch-score a CSV -> JSONL:
& ".\ml\.venv\Scripts\python.exe" ml/serve/score_batch.py --input posts.csv --output out.jsonl
```

Reuse in code: `from serve.explain_viral import explain_post; explain_post(text, source)`.

## File structure

| File | Role |
|---|---|
| `preprocess/build_dataset.py` | raw → training dataset (clean, features, per-source label) |
| `preprocess/enrich_youtube_subscribers.py` | add real YouTube `subscriber_count` (channel audience) to the events CSV |
| `features/cognitive_friction.py` | reading-difficulty feature (EN + VI) |
| `features/text_content.py` | TF-IDF + LogReg content model (`.predict_proba`) |
| `features/rhetorical_roles.py` | segment → per-post role features |
| `features/topics.py` | NMF topic-distribution features |
| `features/bert_content.py` | optional BERT content backend (Kaggle-trained) |
| `train/train_roles.py` | role classifier from the silver annotation set |
| `train/train_viral.py` | XGBoost fusion + evaluation + save model |
| `train/evaluate.py` | overall + per-source metrics |
| `serve/explain_viral.py` | predict + SHAP → explanation JSON |
| `serve/score_batch.py` | batch scoring CSV → JSONL |
| `run_pipeline.py` | run the whole chain end-to-end |
| `models/*.parquet`, `data/*` | artifacts (gitignored) |

## Current results

Content-only baseline (48 features, no channel data):

- Viral model (overall): **PR-AUC ~0.585 · ROC-AUC ~0.77** (`content_score` strongest; topics add a clear lift).
- Role classifier: **macro-F1 ~0.50** over 12 roles.
- Content model: **TF-IDF (0.499) > BERT (0.428)** at this data size → keep TF-IDF for now.
- Per source: strong on **YouTube/Reddit**, near-random on **X** (only ~350 rows).

Adding real channel audience (50 features; YouTube `subscriber_count` fetched via
`preprocess/enrich_youtube_subscribers.py`, 1156/2112 rows):

| metric | baseline (48) | + channel (50) |
|---|---|---|
| overall PR-AUC | 0.585 | **0.773** |
| overall ROC-AUC | 0.770 | **0.871** |
| YouTube PR-AUC | 0.605 | **0.884** |
| YouTube ROC-AUC | 0.796 | **0.931** |

`chan_log_audience` becomes the **#1 SHAP feature** (above `content_score`) — channel
audience is the strongest single predictor of virality, i.e. the model clearly learns
from user/channel-specific signal. Reddit/X are ~unchanged (no follower data yet).

> ⚠️ **Temporal-leakage caveat:** subscriber counts are fetched *now*, not at post time.
> A post that went viral likely *gained* subscribers, so current audience is partly a
> *consequence* of virality → the YouTube gain (0.93) is optimistic. A clean setup needs
> the subscriber count snapshotted at publish time (a fresh/retrieve layer with a TTL).
> Audience size is a valid pre-launch feature in principle; only the measurement is inflated.

## Limitations & next steps

- Content model is **TF-IDF** → Vietnamese is still weak; upgrade to **multilingual BERT** (train on Kaggle GPU, same interface).
- Roles use heuristic labels; no human-verified gold set for a clean evaluation.
- **Channel/author features** are live for **YouTube** (real `subscriber_count` via `preprocess/enrich_youtube_subscribers.py`) and the biggest single lift so far — but see the temporal-leakage caveat above; the honest next step is a *historical* subscriber snapshot at post time. **Reddit** (`subreddit_member_count`) and **X** (`follower_count`) are still dormant: Reddit's `.json` is 403-blocked without OAuth and X needs login, so those rows carry no audience yet.
- Collect more data (especially X/Reddit) to balance sources and raise `--quantile` toward the paper standard (0.75 → 0.90).
- **Stage 2** (post-launch engagement time series → LSTM/GNN) not built — needs time-series engagement data.

> Architecture diagram: `ml/ARCHITECTURE.md`. Handoff for the API/UI tasks: `ml/HANDOFF.md`.
