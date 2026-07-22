# AI Functions & 24/7 Infrastructure Sizing

Analysis of the project's AI/ML capabilities and a proposed server
configuration for running them continuously (24/7).

All "AI" lives in [`ml/`](../ml/) — the **viral prediction & explanation
(Stage 1)** system for EV-domain social posts across **YouTube · X · Reddit**.

---

## 1. AI functions in the project

The AI splits into four function groups with very different resource profiles:

| # | Function | Code | Stack | Resource profile |
|---|---|---|---|---|
| **A** | **Inference / serving** — P(viral) + SHAP explanation + suggestions | [`serve/explain_viral.py`](../ml/serve/explain_viral.py), [`serve/predict.py`](../ml/serve/predict.py) | XGBoost + SHAP + scikit-learn (TF-IDF/LogReg), NMF topics, role classifier | **CPU-only, light.** Model loaded once (singleton), ~tens of ms per request, ~1–2 GB RAM |
| **B** | **BERT content backend (optional)** | [`features/bert_content.py`](../ml/features/bert_content.py) | `torch` + `transformers`, model fine-tuned on Kaggle | CPU works for single-post; large batches want a GPU. +2–4 GB RAM |
| **C** | **Training / retrain** — role → dataset → fusion → evaluate → report | [`run_pipeline.py`](../ml/run_pipeline.py), [`train/train_viral.py`](../ml/train/train_viral.py), DAG [`ai_train_pipeline.py`](../orchestrator/dags/ai_train_pipeline.py) | scikit-learn + XGBoost; Spark job builds the dataset | **Periodic batch, CPU-only.** `schedule=None` (manual), timeout up to **4h**. BERT training runs on **Kaggle GPU (off-server)** |
| **D** | **Batch scoring** | [`serve/score_batch.py`](../ml/serve/score_batch.py) | Calls (A) row-by-row, CSV → JSONL | CPU, linear in the number of posts |

### The AI pipeline (Stage 1)

```
filtered_events.csv (exported from the Silver lakehouse)
        │  preprocess/build_dataset.py
        ▼  clean text → content features + per-source viral label + role/topic/source features
  content_model (TF-IDF → content_score) ─┐
  + structural + src_* + role_* + topic_* + chan_*  ─┴─►  XGBoost fusion  →  P(viral)
        │  serve/explain_viral.py
        ▼  per-prediction SHAP → JSON {viral_score, label, confidence, top_factors, explanation_text, suggestions}
```

### Facts that drive the infra decision

- **The current backend is TF-IDF, not BERT/LLM** → the core function **needs no
  GPU**. Per the README, TF-IDF (0.499) currently *beats* BERT (0.428) at this
  data size.
- **No API server exists yet.** [`HANDOFF.md`](../ml/HANDOFF.md) describes
  *Task B* (a FastAPI wrapper around `explain_post`) and *Task A* (a 7–8B LLM
  that turns the analysis JSON into a marketing report) as **planned work**.
  These are the two potential 24/7 workloads to provision for.
- **The heavy training is already offloaded** to Kaggle GPU. The server only
  runs CPU training for XGBoost/scikit-learn.
- **AI does not run in isolation** — it consumes data from the lakehouse
  pipeline (Spark: 4 workers × 2 cores / 2 GB, Kafka, MinIO, Airflow, Playwright
  collectors, Streamlit dashboard) which already runs 24/7. That is the real
  resource driver today.

---

## 2. Workloads vs. the "run 24/7" requirement

| Workload | Runs 24/7? | Resource weight |
|---|---|---|
| **AI serving** (Task B — when built) | ✅ Yes | Very light (CPU, 1–2 GB) |
| **LLM report generation** (Task A — if self-hosted) | ✅ Yes | **Very heavy if self-hosted** (GPU 16–24 GB VRAM) |
| Data pipeline feeding the AI (Spark/Kafka/MinIO/Airflow/collectors/dashboard) | ✅ Yes | **Heaviest today**: ~8 cores + ~24–28 GB RAM |
| Periodic retrain (`ai-trainer` + Spark dataset build) | ⏱ Batch (manual/scheduled) | CPU burst up to 4h; needs RAM headroom |

> **Key point:** the 24/7 load **today** is the *data pipeline*, not the AI. The
> 24/7 load **tomorrow** hinges on **one decision**: self-host the Task A LLM
> (needs a GPU) or call an external API (no GPU).

---

## 3. Proposed server configuration (3 tiers)

The whole system is **single-host Docker Compose** → the natural deployment is
one sufficiently large server. Three tiers depending on strategy:

### Tier 1 — Minimum (keep TF-IDF, call an external API for reports) ✅ *recommended starting point*

| | |
|---|---|
| **vCPU** | 8 (floor: Spark workers alone need 8 cores) |
| **RAM** | **32 GB** (pipeline ~26–28 GB + AI serving ~2 GB + OS/Docker ~3 GB) |
| **Disk** | 250 GB **NVMe SSD** (MinIO Iceberg + Spark/Playwright images + Kafka retention + Airflow logs) |
| **GPU** | Not needed |
| **Cloud equivalent** | AWS `m6i.2xlarge` · GCP `n2-standard-8` · Hetzner CCX33 / bare-metal |

Note: retrain overlapping heavy collection gets tight → temporarily lower
`SPARK_WORKER_COUNT`.

### Tier 2 — Comfortable operations (room for BERT inference + concurrent training)

| | |
|---|---|
| **vCPU** | 16 |
| **RAM** | **64 GB** |
| **Disk** | 500 GB NVMe SSD |
| **GPU** | Optional **1× L4/T4 24 GB** if enabling high-throughput BERT batch |
| **Cloud equivalent** | AWS `m6i.4xlarge` (+ `g6.xlarge` for GPU) · GCP `n2-standard-16` |

### Tier 3 — If **self-hosting the report LLM (Task A)** on-prem

| | |
|---|---|
| **vCPU** | 16 |
| **RAM** | 64–96 GB |
| **Disk** | 500 GB – 1 TB NVMe |
| **GPU** | **1× 24 GB VRAM** — RTX 4090 / A10 / L4 (runs Qwen2.5-7B / Llama-3.1-8B via vLLM/TGI/Ollama; 4-bit needs only ~6–8 GB VRAM) |
| **Cloud equivalent** | AWS `g5.xlarge` (A10 24 GB) · GCP `g2-standard-16` (L4) |

> **Cost tip:** don't co-locate a 7B LLM on the pipeline host if it's used only
> occasionally. It is far cheaper to **call an external API (Claude/OpenAI) for
> Task A** (stay on Tier 1/2, zero GPU) and **keep BERT training on Kaggle** as
> today. Move to Tier 3 only when data must stay on-prem or report traffic is
> high.

---

## 4. Architecture & hardening for 24/7

1. **Separate serving from training.** Serving (24/7, low latency) and retrain
   (4h burst) should not fight for CPU. On one host, set
   `deploy.resources.limits` on `ai-trainer` / Spark. Better: push retrain to
   **Kaggle / a spot instance / a separate cron**, keeping the main host for
   pipeline + serving.
2. **Set per-service resource limits.** Compose currently has **no**
   `mem_limit` / `cpus` (only the `SPARK_WORKER_MEMORY` env) → one service can
   OOM the whole host. Add limits so a single service can't take down the stack.
3. **Move off the OneDrive path.** The README warns about I/O errors when the
   project sits under OneDrive. A 24/7 server must use a **native Linux
   filesystem** with Docker volumes for state.
4. **Secure external exposure.** Only set `HOST_BIND_ADDRESS=0.0.0.0` **after**
   rotating all default credentials (Airflow/MinIO), behind a reverse proxy +
   TLS + firewall.
5. **Durability:** `restart: unless-stopped` on every service (Compose already
   has some), plus scheduled backups of **MinIO (lakehouse)** and
   **airflow-postgres**. Keep the `docker_storage_maintenance` DAG enabled to
   cap disk growth.
6. **Monitoring:** `prometheus_client` is already a dependency → wire up
   Prometheus + Grafana for CPU/RAM and serving latency.

---

## 5. One-line summary

The core AI (TF-IDF + XGBoost + SHAP) is lightweight and **CPU-only**, so a sane
24/7 starting point is **8 vCPU / 32 GB / 250 GB NVMe, no GPU (Tier 1)**. You
only need a 24 GB GPU once you decide to **self-host the report-generation LLM
(Task A)**.
