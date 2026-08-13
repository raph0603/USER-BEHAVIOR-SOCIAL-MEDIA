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
- **marketing-role cues** = exploratory shares of hook / cta / proof / pain_point …
  inferred by a classifier trained on automated heuristic silver labels. They support
  qualitative TreeSHAP interpretation and are not validated linguistic conclusions.
- **topics** = which EV topic the post is about (NMF topic distribution).
- **SHAP** = a standard method that attributes the prediction to each feature, so we can say *why*.

Current official quality on the 41-row author-grouped test set is **PR-AUC 0.193 ·
ROC-AUC 0.623**, with ECE 0.207. These figures come from the pinned Gold dataset and
exclude audience features; the intervals are wide, so treat them as preliminary.
Full per-source table with confidence intervals: run `python ml/train/verify_answers.py`.
Architecture diagram: `ml/ARCHITECTURE.md`. (A detailed engineering log is kept locally and is not required to use this guide.)

---

## 2. The integration contract (most important section)

Everything you build consumes this one function:

```python
# file: ml/serve/explain_viral.py   (make sure the ml/ folder is on sys.path)
from serve.explain_viral import explain_post

result = explain_post(text="...", source="youtube")   # source ∈ {"youtube", "x", "reddit", ""}
```

The legacy `audience` argument remains accepted for API compatibility, but the official
model ignores it. Current follower/subscriber counts are not guaranteed to predate the
post and are therefore excluded to prevent look-ahead bias.

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

Any factor whose raw name starts with `role_` is an exploratory heuristic cue. Its label
is prefixed with `Exploratory role cue`, and clients must not present it as a definitive
linguistic diagnosis. Role assignments no longer generate prescriptive suggestions.

> ⚠️ **`viral_score` below 0.5 can still be `viral-likely`.** Only ~25% of posts are viral,
> so an honest probability rarely passes 0.5. The threshold is picked out-of-fold during
> training and stored in the model bundle (currently **0.26**); `explanation_text` always
> states it. Do not hard-code 0.5 in your UI — read `label`, or pass your own
> `threshold=` to `explain_post` if your use case wants to trade recall for precision.

**Illustrative output schema** (`source="x"`, `top_factors` trimmed):
```json
{
  "viral_score": 0.345,
  "label": "viral-likely",
  "confidence": 0.077,
  "top_factors": [
    {"feature": "topic_4",             "label": "Topic #4",                 "value": 0.6174, "contribution": -0.2564, "direction": "down"},
    {"feature": "role_ratio_urgency",  "label": "Exploratory role cue: ratio of urgency", "value": 0.3333, "contribution": 0.2039, "direction": "up"}
  ],
  "explanation_text": "Prediction: likely viral. Factors increasing it: Exploratory role cue: ratio of urgency and post content/topic. Factors decreasing it: Topic #4.",
  "suggestions": []
}
```

A `top_factors[].value` of `null` means the feature is unknown for this post.

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
# compare the downstream model with and without exploratory role features:
python ml/train/evaluate_role_ablation.py --n-boot 2000
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

- `audience` is retained as an optional compatibility field but is ignored by the official model.
- The model loads **at startup** (it is heavy).
- Note: if a BERT content backend is used later, the server also needs `transformers` + `torch` (the current TF-IDF backend does **not**).

---

## 6. Limitations & data dependencies

- **Stage 2** is written but untrained on real data — see Section 6.1. Nothing in Task A/B depends on it; `explain_post` stays the Stage-1 entry point.
- **Audience is disabled in the official model.** It can return only after timestamped
  reputation history supports `reputation_observed_at <= post_published_at`; see
  `docs/PREPUBLICATION_REPUTATION.md`.
- The official test set has only 41 rows and no eligible Reddit row. Collect more data
  before drawing platform-specific conclusions.
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
