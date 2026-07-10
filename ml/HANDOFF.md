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

Current quality on the test set: **PR-AUC ≈ 0.55 · ROC-AUC ≈ 0.76** (good on YouTube/Reddit, weak on X — X has too little data).
Architecture diagram: `ml/ARCHITECTURE.md`. (A detailed engineering log is kept locally and is not required to use this guide.)

---

## 2. The integration contract (most important section)

Everything you build consumes this one function:

```python
# file: ml/serve/explain_viral.py   (make sure the ml/ folder is on sys.path)
from serve.explain_viral import explain_post

result = explain_post(text="...", source="youtube")   # source ∈ {"youtube", "x", "reddit", ""}
```

It returns a JSON-serializable `dict` with exactly this schema:

| Field | Type | Meaning |
|---|---|---|
| `viral_score` | float in [0,1] | probability the post is viral |
| `label` | `"viral-likely"` \| `"not-viral"` | class at threshold 0.5 |
| `confidence` | float in [0,1] | how sure the model is (distance from 0.5, scaled) |
| `top_factors` | list | the most influential features, already sorted by impact |
| `top_factors[].feature` | string | raw feature name (e.g. `content_score`, `topic_1`) |
| `top_factors[].label` | string | human-readable label (e.g. "Post content/topic") |
| `top_factors[].value` | float \| null | the feature's value for this post (null if not applicable) |
| `top_factors[].contribution` | float | SHAP contribution (signed) |
| `top_factors[].direction` | `"up"` \| `"down"` | whether it pushes the score up or down |
| `explanation_text` | string | one-paragraph human-readable explanation |
| `suggestions` | list[string] | concrete tips to improve the post |

**Real example output:**
```json
{
  "viral_score": 0.671,
  "label": "viral-likely",
  "confidence": 0.342,
  "top_factors": [
    {"feature": "content_score",   "label": "Post content/topic",     "value": 0.74, "contribution": 0.81, "direction": "up"},
    {"feature": "topic_6",          "label": "Topic #6",               "value": 0.55, "contribution": 0.30, "direction": "up"},
    {"feature": "role_ratio_hook",  "label": "Ratio of opening hook",  "value": 0.50, "contribution": 0.14, "direction": "up"}
  ],
  "explanation_text": "Prediction: likely viral (probability 67%). Factors increasing it: Post content/topic, Topic #6, Ratio of opening hook.",
  "suggestions": ["Add a clear call to action (CTA).", "Add concrete numbers or proof."]
}
```

Batch scoring (a CSV of many posts → one JSON object per line):
```
python ml/serve/score_batch.py --input posts.csv --output out.jsonl
```
The CSV needs a `text` column and an optional `source` column.

---

## 3. How to run / retrain (reference)

```powershell
# run the whole training chain with one command
python ml/run_pipeline.py            # role classifier -> dataset -> train -> evaluate
python ml/run_pipeline.py --export   # also pull a fresh export from the lakehouse first
```
- Trained model: `ml/models/stage1_multisource.joblib` — a dict `{model, content_model, features}`.
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

**The handler already exists:** `explain_post(text, source)` (it loads the model once via a singleton). You only need to expose it over HTTP.

**Suggested endpoints:**
| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/predict` | `{"text": "...", "source": "youtube"}` | the JSON schema from Section 2 |
| POST | `/predict/batch` | `{"items": [{text, source}, ...]}` | list of JSON objects (reuse `score_batch.py` logic) |
| GET | `/health` | — | `{"status": "ok"}` |

**Suggested stack:** FastAPI + uvicorn; containerize it (use `ml/Dockerfile` as a template and add `fastapi`/`uvicorn`).
- Load the model **at startup** (it is heavy).
- The **API contract = the schema in Section 2** — share that with the consuming services.
- Note: if a BERT content backend is used later, the server also needs `transformers` + `torch` (the current TF-IDF backend does **not**).

---

## 6. Limitations & data dependencies

- **Stage 2** (post-launch engagement over time → LSTM/GNN) **is not built** — see Section 6.1.
- **Channel/author features** (subscriber/follower counts) are **not available** — need a crawler that fetches follower counts.
- **X is weak** (ROC ≈ 0.43, near random) because it has only 348 rows — collect more X/Reddit data.
- Host vs container results differ slightly (0.585 vs 0.555) due to un-pinned transitive deps (scipy/BLAS); functionality is unaffected.

### 6.1 Why Stage 2 cannot run yet

Stage 1 predicts **before** a post is launched, from the **text only**.
Stage 2 is meant to predict **after** launch, from **how engagement grows over time** — e.g. likes/views/comments measured at hour 1, 2, 3, … — and feed that **time series** into an LSTM/GNN.

The blocker is data shape:
- The exported dataset (`filtered_events.csv`) has **one engagement snapshot per post** (a single `like_count`, `view_count`, … taken at export time).
- Stage 2 needs **many snapshots per post over time** (a sequence). We do not store that sequence yet.

So Stage 2 is **blocked on data collection**, not on modelling. To unblock it you must first capture engagement repeatedly over time per post (e.g. re-poll each post every N minutes/hours and store each reading), then build sequences `[(t1, likes1), (t2, likes2), …]` per post. The pipeline already has a `refresh_recent_engagement_insights` job and a `metadata_refreshed_at` column, which is the natural place to start accumulating these readings — but today it overwrites a single snapshot rather than appending a history.

---

## 7. File map (`ml/`)

| File | Role |
|---|---|
| `serve/explain_viral.py` | **`explain_post` — the entry point for Task A/B** |
| `serve/score_batch.py` | batch scoring: CSV → JSONL |
| `run_pipeline.py` | run the whole training chain with one command |
| `train/train_viral.py`, `train/train_roles.py`, `train/evaluate.py` | training + evaluation |
| `preprocess/build_dataset.py` | preprocessing + label + features |
| `features/` | `cognitive_friction`, `text_content`, `rhetorical_roles`, `topics`, `bert_content` |
| `models/*.joblib` | trained models (gitignored) |
| `ARCHITECTURE.md` | architecture diagram (committed) |
