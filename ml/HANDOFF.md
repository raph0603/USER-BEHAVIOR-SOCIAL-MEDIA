# Handoff Guide — AI Stage 1 (viral prediction & explanation)

> For the team building **Task A (a HuggingFace model that turns the analysis into a report UI)**
> and **Task B (an AI-server that exposes the model via an API)**.
> The main model (Stage 1) is finished. Both tasks are **independent** of the model internals —
> they only need to consume the JSON output described in Section 2.

---

## 1. What the system does

Input = one social-media post (`text` + `source`). Output = a **viral prediction + an explanation of WHY + improvement suggestions**.

```
text, source ──► clean text ──► features (content TF-IDF · marketing roles · topics · source one-hot)
            ──► XGBoost fusion ──► P(viral) ──► per-prediction SHAP ──► structured JSON
```

- **content** = how the wording/topic itself predicts virality (TF-IDF + Logistic Regression score).
- **marketing roles** = share of hook / cta / proof / pain_point … in the post (from a role classifier).
- **topics** = which EV topic the post is about (NMF topic distribution).
- **SHAP** = a standard method that attributes the prediction to each feature, so we can say *why*.

Current quality on the 870-row test set: **PR-AUC 0.603 · ROC-AUC 0.793**, and the returned
probability is calibrated (ECE 0.016). Best on YouTube (ROC 0.881), weakest on Reddit
(0.669); X sits at 0.750 but on only 150 rows, so treat it as indicative.
Full per-source table with confidence intervals: run `python ml/train/verify_answers.py`.
Architecture diagram: `ml/ARCHITECTURE.md`. (A detailed engineering log is kept locally and is not required to use this guide.)

---

## 2. The integration contract (most important section)

Everything you build consumes this one function:

```python
# file: ml/serve/explain_viral.py   (make sure the ml/ folder is on sys.path)
from serve.explain_viral import explain_post

result = explain_post(text="...", source="youtube")   # source ∈ {"youtube", "x", "reddit", ""}
result = explain_post(text="...", source="youtube", audience=120_000)   # if you know it
```

`audience` is the author's follower/subscriber count. **Leave it out when you do not know
it** — the model was trained with unknown audiences as missing, and passing `0` instead
tells it the author has no followers at all, which drags the score toward zero.

It returns a JSON-serializable `dict` with exactly this schema:

| Field | Type | Meaning |
|---|---|---|
| `viral_score` | float in [0,1] | calibrated probability the post is viral — a 0.30 really does mean ~30% |
| `label` | `"viral-likely"` \| `"not-viral"` | class at the model's own threshold, **not 0.5** (see below) |
| `confidence` | float in [0,1] | distance from the decision threshold, rescaled to [0,1] |
| `top_factors` | list | the most influential features, already sorted by impact |
| `top_factors[].feature` | string | raw feature name (e.g. `content_score`, `topic_1`) |
| `top_factors[].label` | string | human-readable label (e.g. "Post content/topic") |
| `top_factors[].value` | float \| null | the feature's value for this post (null if not applicable) |
| `top_factors[].contribution` | float | SHAP contribution (signed) |
| `top_factors[].direction` | `"up"` \| `"down"` | whether it pushes the score up or down |
| `explanation_text` | string | one-paragraph human-readable explanation |
| `suggestions` | list[string] | concrete tips to improve the post |

> ⚠️ **`viral_score` below 0.5 can still be `viral-likely`.** Only ~25% of posts are viral,
> so an honest probability rarely passes 0.5. The threshold is picked out-of-fold during
> training and stored in the model bundle (currently **0.29**); `explanation_text` always
> states it. Do not hard-code 0.5 in your UI — read `label`, or pass your own
> `threshold=` to `explain_post` if your use case wants to trade recall for precision.

**Real example output** (`source="x"`, no `audience` passed, `top_factors` trimmed to 3):
```json
{
  "viral_score": 0.345,
  "label": "viral-likely",
  "confidence": 0.077,
  "top_factors": [
    {"feature": "topic_4",             "label": "Topic #4",                 "value": 0.6174, "contribution": -0.2564, "direction": "down"},
    {"feature": "role_ratio_urgency",  "label": "Ratio of urgency",         "value": 0.3333, "contribution":  0.2039, "direction": "up"},
    {"feature": "chan_log_audience",   "label": "Channel audience size",    "value": null,   "contribution": -0.1836, "direction": "down"}
  ],
  "explanation_text": "Prediction: likely viral (probability 34%, decision threshold 29%). Factors increasing it: Ratio of urgency, Post content/topic, Overall reading difficulty. Factors decreasing it: Topic #4, Channel audience size (followers/subscribers).",
  "suggestions": ["Add a clear call to action (CTA).", "Open with an attention-grabbing hook."]
}
```

A `top_factors[].value` of `null` means the feature is unknown for this post — above, no
`audience` was passed. The model handles it as missing rather than as zero.

Batch scoring (a CSV of many posts → one JSON object per line):
```
python ml/serve/score_batch.py --input posts.csv --output out.jsonl
```
The CSV needs a `text` column and an optional `source` column.

---

## 3. How to run / retrain (reference)

```powershell
# role classifier -> dataset -> train -> evaluate. The input must be named explicitly:
python ml/run_pipeline.py --lakehouse-manifest <manifest.json>          # official run
python ml/run_pipeline.py --manual-csv-input <events.csv>               # compatibility
python ml/run_pipeline.py --export                                      # export a CSV first
# check the answers, not just the ranking (calibration, decisions, bootstrap CIs):
python ml/train/verify_answers.py --n-boot 2000
```
- Trained model: `ml/models/stage1_multisource.joblib` — a dict
  `{model, calibrator, threshold, content_model, features}`. `calibrator` and `threshold`
  are absent in models trained before calibration; serving falls back to 0.5 for those.
- Minimal dependencies: `ml/requirements-train.txt`.

You do **not** need to retrain for Task A/B — just call `explain_post`.

---

## 4. TASK A — Pick a HuggingFace model to build the report UI

**Goal:** take the JSON from Section 2 and turn it into a **human-friendly report or UI** (for a marketer).

**Input / Output:**
- Input = the `explain_post(...)` JSON (one post, or a list of them).
- Output = a natural-language report (or HTML/markdown UI).

**Candidate models (prefer MULTILINGUAL EN+VI — reports may be in Vietnamese):**

| Use case | Candidates | Notes |
|---|---|---|
| Text → report (instruction LLM) | `Qwen2.5-7B-Instruct`, `Llama-3.1-8B-Instruct`, `Mistral-7B-Instruct` | strong multilingual; use a 1.5B–3B variant if GPU/VRAM is limited |
| Generate UI code | `Qwen2.5-Coder`, `deepseek-coder` | if you want the model to emit UI code (HTML/React) |

**Comparison criteria (fill these in when you choose):** output quality · Vietnamese support · model size / VRAM · latency · license.

**Suggested approach (not mandatory):** pass the JSON as input context and
request a fixed-structure report. Example request:
```
Create a marketing performance report from this social-post analysis (JSON).
Write a short report: (1) viral likelihood, (2) the main reasons, (3) 2-3 improvement tips.
JSON: {...}
```
→ **No change to the main model is needed.** You only consume its JSON.

---

## 5. TASK B — Build the AI-server (API)

**Goal:** wrap the main model behind a REST API so other services in the learning platform can call it.

**Status: built.** `ml/server/app.py` (FastAPI + uvicorn, `ml/server/Dockerfile`, tests in
`ml/server/test_app.py`). It delegates to `explain_post`, so it inherits the schema in
Section 2 automatically.

| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/predict` | `{"text": "...", "source": "youtube", "audience": 120000}` | the JSON schema from Section 2 |
| POST | `/predict/batch` | `{"items": [{text, source, audience}, ...]}` | list of JSON objects |
| GET | `/health` | — | `{"status": "ok"}` |

- `audience` is optional on every endpoint; omit it rather than sending `0` (see Section 2).
- The model loads **at startup** (it is heavy).
- Note: if a BERT content backend is used later, the server also needs `transformers` + `torch` (the current TF-IDF backend does **not**).

---

## 6. Limitations & data dependencies

- **Stage 2** is written but untrained on real data — see Section 6.1. Nothing in Task A/B depends on it; `explain_post` stays the Stage-1 entry point.
- **Channel/author features** are live on all three sources, but only 2367 of 4357 rows
  carry a value, and the counts are read *today* rather than at post time — a post that
  went viral has since gained subscribers, so the YouTube figures are optimistic.
- **Reddit is the weakest source** (ROC 0.669) and **X rests on 150 test rows**, so its
  0.750 has a CI of [0.663, 0.827]. Collect more X/Reddit data before trusting either.
- Host vs container results differ slightly due to un-pinned transitive deps
  (scipy/BLAS); functionality is unaffected.

### 6.1 Stage 2: written, waiting on data

Stage 1 predicts **before** a post is launched, from the **text only**.
Stage 2 is meant to predict **after** launch, from **how engagement grows over time** — e.g. likes/views/comments measured at hour 1, 2, 3, … — and feed that **time series** into an LSTM/GNN.

The blocker is data shape:
- The exported dataset (`filtered_events.csv`) has **one engagement snapshot per post** (a single `like_count`, `view_count`, … taken at export time).
- Stage 2 needs **many snapshots per post over time** (a sequence). We do not store that sequence yet.

So Stage 2 is **blocked on data collection**, not on modelling. To unblock it you must first capture engagement repeatedly over time per post (e.g. re-poll each post every N minutes/hours and store each reading), then build sequences `[(t1, likes1), (t2, likes2), …]` per post.

**That collection now exists, on a branch that is not merged yet.**
`feat/youtube-metadata-evolution` adds `spark/jobs/batch/engagement_snapshots.py`, which
appends to `lakehouse.silver.engagement_snapshots` — an **append-only** table where every
refresh writes a new row instead of overwriting one, keyed by
`observed_at` + `platform_event_id`, expressly so that T+1h / T+6h / T+24h labels can be
built later. It also ships `youtube_engagement_velocity.py` (velocity and acceleration).

**The modelling side is written and tested.** `preprocess/build_stage2_dataset.py` turns
those observations into one row per post, split in time — features only from readings at or
before `--horizon-hours`, the label only from a reading at or after `--label-hours`, and a
post missing either side is dropped rather than guessed. `train/train_stage2.py` then trains
over the resulting velocity / acceleration / ratio features **with the Stage-1 probability
as one more input**, so Stage 2 corrects the prior instead of replacing it. Joining the
Stage-1 dataset by URL does double duty: it supplies that feature and the `author_hash` the
snapshot table lacks, without which train and test could share an author.

We use gradient boosting rather than the LSTM/GNN this document originally named. A
trajectory here is 3-4 observations already reduced to summary statistics; a recurrent model
needs long sequences and far more posts than we will have for months. Worth revisiting once
posts routinely carry dozens of readings.

Because the table is still empty, the layer is verified only against synthetic trajectories
(`tests/scripts/test_stage2_sequences.py`) — **no Stage-2 performance number should be
quoted yet**.

There is also a framing trap worth knowing before the first real run. Engagement counters are
cumulative, so whatever a post has accumulated by the horizon is a lower bound on what it
shows at the label horizon: "big now" predicts "big later" almost for free. On the synthetic
trajectories, ranking posts by the 6-hour view count alone scored ROC-AUC **0.829** while the
trained model scored **0.806** — the baseline won. `train_stage2.py` therefore prints that
baseline next to every result. A Stage-2 number that does not clear it says nothing about the
shape of the curve, which is the only thing Stage 2 is for.

What remains is therefore operational, not modelling: merge that branch and let it
accumulate readings, then run the two scripts above and finally report a real number.

---

## 7. File map (`ml/`)

| File | Role |
|---|---|
| `serve/explain_viral.py` | **`explain_post` — the entry point for Task A/B** |
| `serve/score_batch.py` | batch scoring: CSV → JSONL |
| `server/app.py` | the AI-server (Task B) — FastAPI wrapper over `explain_post` |
| `report_ui/generate_report.py` | the report generator (Task A) |
| `run_pipeline.py` | run the whole training chain with one command |
| `train/train_viral.py`, `train/train_roles.py`, `train/evaluate.py` | training + evaluation |
| `train/verify_answers.py` | calibration, decision quality and bootstrap CIs |
| `preprocess/build_stage2_dataset.py`, `train/train_stage2.py` | Stage 2 — written, untrained (§6.1) |
| `preprocess/build_dataset.py` | preprocessing + label + features |
| `features/` | `cognitive_friction`, `text_content`, `rhetorical_roles`, `topics`, `bert_content` |
| `models/*.joblib` | trained models (gitignored) |
| `ARCHITECTURE.md` | architecture diagram (committed) |
