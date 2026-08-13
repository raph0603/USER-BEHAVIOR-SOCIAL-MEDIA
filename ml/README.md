# `ml/` — Viral prediction & explanation (Stage 1)

Predicts whether a social-media post (EV domain) is likely to go **viral**, and explains
**why** — to power content/marketing recommendations. Multi-source: **YouTube · X · Reddit**.

## Pipeline

```
Silver post_features + engagement_snapshots (pinned snapshot IDs)
        │
        ▼  maintenance/build_training_dataset.py
Gold training_examples write → pinned Gold snapshot read
        │
        ▼  versioned Parquet export + validated lineage manifest
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

Stage 2 answers the other question — *is it actually taking off?* — after the post is live:

```
silver.engagement_snapshots (one row per observation of a post)
        │
        ▼  preprocess/build_stage2_dataset.py
  per-post sequence, split in time: features from observations <= --horizon-hours,
  label from an observation >= --label-hours, posts missing either side dropped
        │
        ▼  train/train_stage2.py
  seq_* velocity / acceleration / ratios  +  stage1_score  ──►  XGBoost  →  P(viral)
```

The Stage-1 probability enters as one more feature, so Stage 2 **corrects** the prior rather
than replacing it. Joining the Stage-1 dataset by URL also supplies the `author_hash` the
snapshot table has no column for, without which train and test could share an author.

## Key design decisions

- **Unified content features** across all 3 sources (pure functions of the text): `cognitive_friction` + `char/word/has_question/is_vietnamese`.
- **Per-source viral label**: within each source, z-score `log1p` of that platform's engagement metrics → the top `--quantile` (default 0.75) is labelled viral. Engagement columns build the label only — never features (avoids leakage).
- **`content_score`** = TF-IDF + LogReg over the text, fused as a single feature (built out-of-fold to avoid leakage). Interface `.predict_proba(list[str])` → swapping in BERT later needs no other change.
- **`role_*`** = rhetorical marketing roles (cta/hook/proof/…) from the `feature/annotation-roles-marketing` branch; mainly aid explainability.
- **`topic_*`** = NMF topic distribution over TF-IDF (fills the "topic" component of the design; BERTopic is the heavier upgrade).
- **`chan_*`** = channel/author audience size (`follower/subscriber/subreddit_member` unified via log1p) — a *pre-launch* author property (not engagement), so it's a valid feature, not leakage. Best-effort: only added when the export carries those columns. An **unknown audience is `NaN`, never `0`** — `0` means an author with no followers, and collapsing the two lets the feature stand in for the platform instead of the signal. A positive value is trusted even when the export's `*_available` flag disagrees, because that flag describes one observation while the value may come from another collector.
- **Explanation** = SHAP (XGBoost `pred_contribs`) → maps features to readable reasons + suggestions.
- **Calibrated probability**: `scale_pos_weight` sharpens ranking but inflates the scores, so a Platt scaler fitted on author-grouped out-of-fold predictions maps them back to honest probabilities. It is monotonic, so ranking metrics and the SHAP ordering are untouched. Calibrating also moves the decision boundary — with a 0.25 base rate, few honest scores pass 0.5 — so the threshold is re-picked out-of-fold (currently **0.33**) and stored in the model bundle.

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
# stage 2, once silver.engagement_snapshots carries several observations per post:
& ".\ml\.venv\Scripts\python.exe" ml/preprocess/build_stage2_dataset.py --input <snapshots.parquet>
& ".\ml\.venv\Scripts\python.exe" ml/train/train_stage2.py `
    --stage1-data ml/data/train_dataset.parquet --stage1-model ml/models/stage1_multisource.joblib
# explain a single post:
& ".\ml\.venv\Scripts\python.exe" ml/serve/explain_viral.py
# batch-score a CSV -> JSONL:
& ".\ml\.venv\Scripts\python.exe" ml/serve/score_batch.py --input posts.csv --output out.jsonl
```

The model bundle, its `.lineage.json` sidecar, evaluation JSON, and generated report
all retain the same dataset fingerprint, Silver source snapshots, and pinned Gold
training snapshot. See
[`docs/REPRODUCIBLE_TRAINING.md`](../docs/REPRODUCIBLE_TRAINING.md) for snapshot replay
and the reporting contract.

Reuse in code: `from serve.explain_viral import explain_post; explain_post(text, source)`.

## Tests

`requirements-train.txt` carries only what training and serving need, so install the dev
extras once before running the suite:

```powershell
& ".\ml\.venv\Scripts\pip.exe" install -r ml/requirements-dev.txt
& ".\ml\.venv\Scripts\pip.exe" install -r ml/requirements-notebooks.txt   # only for ml/notebooks/
```

```powershell
# the AI surface -- audience contract, train/serve parity, stage-2 time split, the API:
& ".\ml\.venv\Scripts\python.exe" -m pytest tests/scripts/test_lakehouse_training_dataset.py `
    tests/scripts/test_serve_train_parity.py tests/scripts/test_stage2_sequences.py `
    ml/server/test_app.py -q
# the whole repo, minus the Spark jobs that need pyspark + a container (466 passed):
& ".\ml\.venv\Scripts\python.exe" -m pytest tests/scripts -q `
    --ignore=tests/scripts/test_engagement_snapshots.py `
    --ignore=tests/scripts/test_silver_post_features.py `
    --ignore=tests/scripts/test_pipeline_monitoring.py
```

`test_serve_train_parity.py` skips itself when `models/stage1_multisource.joblib` is
absent (it is gitignored), so a run reporting skips means the model has not been trained
on this machine yet.

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
| `preprocess/build_stage2_dataset.py` | engagement snapshots → per-post sequences, split in time |
| `train/train_roles.py` | role classifier from the silver annotation set |
| `train/train_viral.py` | XGBoost fusion + evaluation + save model |
| `train/train_stage2.py` | Stage-2 model over the sequences, fused with the Stage-1 score |
| `train/evaluate.py` | overall + per-source metrics |
| `serve/explain_viral.py` | predict + SHAP → explanation JSON |
| `serve/score_batch.py` | batch scoring CSV → JSONL |
| `run_pipeline.py` | run the whole chain end-to-end |
| `models/*.parquet`, `data/*` | artifacts (gitignored) |

## Current results

> Summary only. `ml/RESULTS.md` carries the full picture: artifacts, per-family and
> per-feature weights, the decision table, how each figure was reached, and what the numbers
> do not say.

The current official run contains 197 labelled rows from pinned Iceberg snapshots and
uses an author-grouped 41-row test split. Every number below comes from
`train/verify_answers.py`, which scores exactly what serving returns and embeds the
dataset version in the report and calibration figure.

| group | n | ROC-AUC (95% CI) | PR-AUC (95% CI) | ECE |
|---|---|---|---|---|
| overall | 41 | 0.623 [0.150, 0.957] | 0.196 [0.029, 0.696] | 0.208 |
| youtube | 6 | 0.200 [0.000, 0.600] | 0.200 [0.167, 0.600] | 0.093 |
| x | 35 | 0.818 [0.588, 0.992] | 0.250 [0.071, 1.000] | 0.227 |

- Role classifier: **macro-F1 ~0.50** over 12 roles.
- Content model: **TF-IDF (0.499) > BERT (0.428)** at this data size → keep TF-IDF for now.
- The official sample is too small for strong claims: overall and per-source intervals
  are wide, and Reddit has no eligible row in this pinned dataset version.
- Dataset version `dataset-v2-52ef5b2d5b113e3a377e` and Gold snapshot
  `4034905545805767069` are bound to the model and figures;
  full snapshot IDs and artifact digests are in `ml/results/`.

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
- **Stage 2** is written but has never seen real data. `silver.engagement_snapshots` lives on the unmerged `feat/youtube-metadata-evolution` branch and, once merged, still needs days of polling before a post carries several observations. Until then the only evidence the layer is correct is its tests.

> Full results, feature weights and caveats: `ml/RESULTS.md`. Architecture diagram:
> `ml/ARCHITECTURE.md`. Handoff for the API/UI tasks: `ml/HANDOFF.md`.
