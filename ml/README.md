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
        │                                                          → Platt calibration → threshold
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
- **`chan_*`** = channel/author audience size (`follower/subscriber/subreddit_member` unified via log1p) — a *pre-launch* author property (not engagement), so it's a valid feature, not leakage. Best-effort: only added when the export carries those columns. An **unknown audience is `NaN`, never `0`** — `0` means an author with no followers, and collapsing the two lets the feature stand in for the platform instead of the signal. A positive value is trusted even when the export's `*_available` flag disagrees, because that flag describes one observation while the value may come from another collector.
- **Explanation** = SHAP (XGBoost `pred_contribs`) → maps features to readable reasons + suggestions.
- **Calibrated probability**: `scale_pos_weight` sharpens ranking but inflates the scores, so a Platt scaler fitted on author-grouped out-of-fold predictions maps them back to honest probabilities (ECE 0.123 → 0.016). It is monotonic, so ranking metrics and the SHAP ordering are untouched. Calibrating also moves the decision boundary — with a 0.25 base rate, few honest scores pass 0.5 — so the threshold is re-picked out-of-fold (currently **0.29**) and stored in the model bundle.

## Run (use the `ml/.venv` Python)

```powershell
$env:PYTHONIOENCODING='utf-8'            # Windows: avoid console encoding issues
# whole training chain: role -> dataset -> train -> evaluate.
# An official run needs the manifest of one exact lakehouse dataset version:
& ".\ml\.venv\Scripts\python.exe" ml/run_pipeline.py --lakehouse-manifest <manifest.json>
# compatibility path for a CSV you have on disk (prints a "not official" warning;
# add --allow-stale-input if the file is older than 24h):
& ".\ml\.venv\Scripts\python.exe" ml/run_pipeline.py --manual-csv-input data/samples/filtered_events.csv
# fill in YouTube channel audience; --limit 0 applies the cache without any request:
& ".\ml\.venv\Scripts\python.exe" ml/preprocess/enrich_youtube_subscribers.py --limit 0
# calibration, decisions and bootstrap CIs for the trained model:
& ".\ml\.venv\Scripts\python.exe" ml/train/verify_answers.py --n-boot 2000
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

3798 labelled rows (2928 train / 870 test), 52 features, 2367 rows with a known channel
audience across all three sources. Every number below comes from
`train/verify_answers.py`, which scores exactly what serving returns (calibrated
probability, bundled threshold) and reports a 95% bootstrap CI.

| group | n | ROC-AUC (95% CI) | PR-AUC (95% CI) | ECE |
|---|---|---|---|---|
| overall | 870 | 0.793 [0.759, 0.824] | 0.603 [0.534, 0.666] | 0.016 |
| youtube | 401 | 0.881 [0.840, 0.918] | 0.751 [0.669, 0.825] | 0.037 |
| x | 150 | 0.750 [0.663, 0.827] | 0.508 [0.366, 0.658] | 0.060 |
| reddit | 319 | 0.669 [0.601, 0.731] | 0.429 [0.339, 0.542] | 0.025 |

- Role classifier: **macro-F1 ~0.50** over 12 roles.
- Content model: **TF-IDF (0.499) > BERT (0.428)** at this data size → keep TF-IDF for now.
- Every source clears random (Reddit's CI lower bound is 0.601); X's interval is wide
  because it only has 150 test rows.
- `chan_log_audience` and `content_score` are the top two SHAP features, close together —
  the model reads the post, not just the channel.

Two earlier numbers in this file were wrong and are worth knowing about, because they
still circulate: an overall PR-AUC of **0.773** and a YouTube ROC-AUC of **0.931**. Both
came from filling an unknown audience with `0`, which made the feature a near-perfect
stand-in for "is this YouTube?" rather than a virality signal. Unknown audiences are now
`NaN` and the honest figures are the ones tabled above.

> ⚠️ **Temporal-leakage caveat:** subscriber counts are fetched *now*, not at post time.
> A post that went viral likely *gained* subscribers, so current audience is partly a
> *consequence* of virality → the YouTube figures are still optimistic. A clean setup
> needs the subscriber count snapshotted at publish time (a fresh/retrieve layer with a
> TTL). Audience size is a valid pre-launch feature in principle; only the measurement is
> inflated.

## Limitations & next steps

- Content model is **TF-IDF** → Vietnamese is still weak; upgrade to **multilingual BERT** (train on Kaggle GPU, same interface).
- Roles use heuristic labels; no human-verified gold set for a clean evaluation.
- **Channel/author features** are now live on all three sources (`subscriber_count`, `follower_count`, `subreddit_member_count`) and are the biggest single lift — but see the temporal-leakage caveat above; the honest next step is a *historical* subscriber snapshot at post time.
- Audience coverage is still thin: 2367 of 4357 rows. 442 YouTube channels are absent from the local cache and Reddit/X carry a value on well under half their rows.
- Collect more data (especially X/Reddit) to balance sources and raise `--quantile` toward the paper standard (0.75 → 0.90). X has only 150 test rows, which is why its CI spans 0.16.
- **Stage 2** (post-launch engagement time series → LSTM/GNN) not built — needs time-series engagement data. The `silver.engagement_snapshots` append-only table on `feat/youtube-metadata-evolution` is the input it has been waiting for.

> Architecture diagram: `ml/ARCHITECTURE.md`. Handoff for the API/UI tasks: `ml/HANDOFF.md`.
