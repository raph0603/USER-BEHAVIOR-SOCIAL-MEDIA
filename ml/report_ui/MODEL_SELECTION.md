# Task A — HuggingFace model selection for the report generator

**Goal (from `ml/HANDOFF.md` §4):** take the JSON returned by `explain_post()` and
turn it into a human-friendly marketing report. Reports may be in **English or
Vietnamese**, so the model must be solidly **bilingual EN + VI**.

The main model is unchanged — this layer only *consumes* its JSON output.

## Selection criteria (from the HANDOFF)

Output quality · Vietnamese support · model size / VRAM · latency · license.

## Candidates compared (July 2026)

| Model | VI support | Size / min VRAM (Q4) | License | Verdict |
|---|---|---|---|---|
| **Qwen2.5-7B-Instruct** | Excellent (general model with the best VI) | 7B / ~6 GB | **Apache 2.0** | **Recommended** |
| Qwen2.5-1.5B / 3B-Instruct | Very good | 1.5–3B / ~2–3 GB | Apache 2.0 (1.5B) · Qwen Research (3B) | Low-VRAM fallback |
| Qwen2.5-Coder-7B-Instruct | Good | 7B / ~6 GB | Apache 2.0 | If we later want the model to emit UI code (HTML/React) |
| SeaLLMs-v3-7B | Excellent (SE-Asia specialised, 12 langs incl. VI) | 7B / ~6 GB | SeaLLM license (check commercial terms) | Strong VI alternative |
| Vistral-7B-Chat | Excellent (VI-specialised, Mistral-based) | 7B / ~5 GB | Mistral-derived | Older (2023), VI-focused fallback |
| Meta-Llama-3.1-8B-Instruct | Good | 8B / ~7 GB | Llama 3.1 Community (restrictions) | Viable but licence less permissive |
| Mistral-7B-Instruct-v0.3 | Weak in VI (European focus) | 7B / ~5 GB | Apache 2.0 | Not ideal for VI reports |
| PhoGPT-4B-Chat | VI only (monolingual) | 4B / ~3 GB | Apache 2.0 | Can't handle EN — too narrow for bilingual reports |

## Recommendation

**Primary: `Qwen2.5-7B-Instruct`.** Best Vietnamese quality among general instruct
models, permissive **Apache 2.0** licence (safe for the project), strong at
following a fixed report structure, and available out-of-the-box in Ollama
(`qwen2.5:7b`) and on the HuggingFace Hub.

**VI-specialised alternative:** `SeaLLMs-v3-7B` — if blind tests show Qwen's
Vietnamese phrasing isn't good enough for marketers, switch to it (verify its
licence terms for our use first).

**If we later want the model to write the UI itself** (HTML/React components):
`Qwen2.5-Coder-7B-Instruct`.

## Runtime recommendation (machine-dependent — pick by available VRAM/RAM)

| Your machine | Recommended runtime | Model tag |
|---|---|---|
| GPU ≥ 8 GB VRAM (or ≥ 16 GB RAM) | **Ollama** (offline, simplest) | `qwen2.5:7b` |
| GPU 4–6 GB / modest RAM | Ollama, smaller model | `qwen2.5:3b` |
| No GPU / weak laptop | **HuggingFace Inference API** (needs `HF_TOKEN`) | `Qwen/Qwen2.5-7B-Instruct` |

The prototype (`generate_report.py`) is **backend-agnostic**: it runs today with a
deterministic **template** backend (no model, no GPU needed) and switches to
`ollama` or `hf` by changing one flag — so we can validate the report format now
and plug the chosen model in later.

## Sources

- [Best Open Source LLM for Vietnamese in 2026 — SiliconFlow](https://www.siliconflow.com/articles/en/best-open-source-LLM-for-Vietnamese)
- [Llama 4 vs Qwen 3.5 vs Mistral: Best Open LLM 2026 — Tech-Insider](https://tech-insider.org/llama-4-vs-qwen-vs-mistral-2026/)
- [SeaLLMs / SeaLLM-7B — HuggingFace](https://huggingface.co/SeaLLMs/SeaLLM-7B-v2)
- [PhoGPT: Generative pre-training for Vietnamese (arXiv)](https://arxiv.org/pdf/2311.02945)
- [Awesome Vietnamese NLP — GitHub](https://github.com/vndee/awsome-vietnamese-nlp)
- [VMLU Vietnamese LLM leaderboard](https://vmlu.ai/leaderboard)
