"""Task A prototype - turn the viral-model JSON into a marketing report.

Consumes the exact JSON schema produced by ``ml/serve/explain_viral.explain_post``
(see ml/HANDOFF.md section 2) and renders a short, fixed-structure report for a
marketer, in English or Vietnamese.

It is **backend-agnostic** so we can validate the report format today and plug in
the chosen HuggingFace model (see MODEL_SELECTION.md -> Qwen2.5-7B-Instruct) later:

  - template : deterministic, no model, no GPU. Always works. (default)
  - ollama   : local model via the configurable Ollama HTTP API.
  - hf       : HuggingFace Inference API (needs env var HF_TOKEN).

Usage
-----
    # works out of the box, no model needed:
    python ml/report_ui/generate_report.py --input ml/report_ui/example_input.json

    # Vietnamese, local model via Ollama:
    python ml/report_ui/generate_report.py -i example_input.json --lang vi --backend ollama

    # HuggingFace Inference API:
    set HF_TOKEN=hf_xxx
    python ml/report_ui/generate_report.py -i example_input.json --backend hf

The report is printed to stdout and saved next to the input as ``*_report.md``.
"""
from __future__ import annotations

import argparse
import json
import os
import textwrap
from pathlib import Path

DEFAULT_MODEL_OLLAMA = "qwen2.5:7b"
DEFAULT_MODEL_HF = "Qwen/Qwen2.5-7B-Instruct"
DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"

# --------------------------------------------------------------------------- #
# Prompt (shared by the LLM backends) - adapted from ml/HANDOFF.md section 4
# --------------------------------------------------------------------------- #
PROMPT_TEMPLATES = {
    # The verdict is handed over, never re-derived: the decision threshold sits near the
    # base rate (~0.23), so a small model asked to compare the probability against it
    # reliably concludes the opposite of `label` and contradicts the very JSON it quotes.
    "en": (
        "You are a marketing assistant. Below is the analysis of one social-media "
        "post (JSON) about an electric-vehicle ad campaign. Write a SHORT report "
        "with exactly three sections:\n"
        "1. Viral likelihood: quote `viral_score` as a percentage, then state the verdict "
        "EXACTLY as the `label` field gives it -- \"viral-likely\" means likely to go "
        "viral, \"not-viral\" means unlikely. Never work the verdict out yourself from the "
        "probability or the threshold; the model already did that.\n"
        "2. Main reasons (explain the top factors in plain language).\n"
        "3. 2-3 concrete improvement tips.\n"
        "Keep it under 180 words. Do not invent numbers not present in the JSON.\n\n"
        "JSON:\n{payload}\n"
    ),
    "vi": (
        "Bạn là trợ lý marketing. Dưới đây là phân tích của một bài đăng mạng xã hội "
        "(JSON) về chiến dịch quảng cáo xe điện. Hãy viết một BÁO CÁO NGẮN gồm đúng "
        "ba phần:\n"
        "1. Khả năng lan truyền: nêu `viral_score` dưới dạng phần trăm, rồi kết luận ĐÚNG "
        "theo trường `label` -- \"viral-likely\" nghĩa là CÓ khả năng lan truyền, "
        "\"not-viral\" nghĩa là KHÔNG có khả năng lan truyền. Tuyệt đối không tự suy ra kết "
        "luận từ xác suất hay ngưỡng; mô hình đã quyết định rồi.\n"
        "2. Lý do chính (giải thích các yếu tố quan trọng bằng ngôn ngữ dễ hiểu).\n"
        "3. 2-3 gợi ý cải thiện cụ thể.\n"
        "Giữ dưới 180 từ. Không bịa ra số liệu không có trong JSON.\n\n"
        "JSON:\n{payload}\n"
    ),
}

L = {
    "en": {
        "title": "Marketing report — viral analysis",
        "s1": "1. Viral likelihood",
        "s2": "2. Main reasons",
        "s3": "3. Improvement tips",
        "likely": "This post is **likely to go viral**",
        "unlikely": "This post is **unlikely to go viral**",
        "prob": "estimated probability",
        "conf": "model confidence",
        "push_up": "pushes the score up",
        "push_down": "pulls the score down",
        "no_factors": "No dominant factor stood out.",
        "no_tips": "No specific suggestion.",
    },
    "vi": {
        "title": "Báo cáo marketing — phân tích lan truyền",
        "s1": "1. Khả năng lan truyền",
        "s2": "2. Lý do chính",
        "s3": "3. Gợi ý cải thiện",
        "likely": "Bài đăng này **có khả năng lan truyền**",
        "unlikely": "Bài đăng này **ít khả năng lan truyền**",
        "prob": "xác suất ước tính",
        "conf": "độ tin cậy của mô hình",
        "push_up": "làm tăng điểm",
        "push_down": "làm giảm điểm",
        "no_factors": "Không có yếu tố nổi trội.",
        "no_tips": "Không có gợi ý cụ thể.",
    },
}


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def load_result(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    required = {"viral_score", "label", "top_factors", "suggestions"}
    missing = required - data.keys()
    if missing:
        raise SystemExit(f"Input JSON is missing required keys: {sorted(missing)}")
    return data


# --------------------------------------------------------------------------- #
# Backend 1 - deterministic template (no model needed)
# --------------------------------------------------------------------------- #
def render_template(result: dict, lang: str) -> str:
    t = L[lang]
    score = float(result["viral_score"])
    conf = float(result.get("confidence", 0.0))
    likely = result["label"] == "viral-likely" or score >= 0.5

    header = t["likely"] if likely else t["unlikely"]
    s1 = f"{header} ({t['prob']}: **{score:.0%}**, {t['conf']}: {conf:.0%})."

    factors = result.get("top_factors") or []
    if factors:
        bullets = []
        for f in factors[:3]:
            arrow = t["push_up"] if f.get("direction") == "up" else t["push_down"]
            bullets.append(f"- **{f.get('label', f.get('feature'))}** {arrow}.")
        s2 = "\n".join(bullets)
    else:
        s2 = t["no_factors"]

    tips = result.get("suggestions") or []
    s3 = "\n".join(f"- {tip}" for tip in tips) if tips else t["no_tips"]

    return (
        f"# {t['title']}\n\n"
        f"### {t['s1']}\n{s1}\n\n"
        f"### {t['s2']}\n{s2}\n\n"
        f"### {t['s3']}\n{s3}\n"
    )


# --------------------------------------------------------------------------- #
# Backend 2 - Ollama (local)
# --------------------------------------------------------------------------- #
def render_ollama(result: dict, lang: str, model: str) -> str:
    import urllib.request

    prompt = PROMPT_TEMPLATES[lang].format(payload=json.dumps(result, ensure_ascii=False, indent=2))
    # Low temperature: the report restates a fixed analysis, so sampling variety only buys
    # us paraphrases of the same facts and more chances to drift away from them.
    body = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.2},
    }).encode("utf-8")
    base_url = os.environ.get("OLLAMA_BASE_URL", DEFAULT_OLLAMA_BASE_URL).rstrip("/")
    req = urllib.request.Request(
        f"{base_url}/api/generate", data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())["response"].strip()


# --------------------------------------------------------------------------- #
# Backend 3 - HuggingFace Inference API
# --------------------------------------------------------------------------- #
def render_hf(result: dict, lang: str, model: str) -> str:
    import urllib.request

    token = os.environ.get("HF_TOKEN")
    if not token:
        raise SystemExit("Set the HF_TOKEN environment variable to use the 'hf' backend.")
    prompt = PROMPT_TEMPLATES[lang].format(payload=json.dumps(result, ensure_ascii=False, indent=2))
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 400,
        "temperature": 0.3,
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://router.huggingface.co/v1/chat/completions", data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())["choices"][0]["message"]["content"].strip()


BACKENDS = {"template": render_template, "ollama": render_ollama, "hf": render_hf}


def generate(result: dict, backend: str, lang: str, model: str | None) -> str:
    if backend == "template":
        return render_template(result, lang)
    if backend == "ollama":
        return render_ollama(result, lang, model or DEFAULT_MODEL_OLLAMA)
    if backend == "hf":
        return render_hf(result, lang, model or DEFAULT_MODEL_HF)
    raise SystemExit(f"Unknown backend: {backend}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a marketing report from the viral-model JSON.")
    parser.add_argument("-i", "--input", type=Path, required=True, help="JSON file from explain_post().")
    parser.add_argument("--backend", choices=list(BACKENDS), default="template")
    parser.add_argument("--lang", choices=["en", "vi"], default="en")
    parser.add_argument("--model", default=None, help="Override model tag (ollama/hf backends).")
    parser.add_argument("-o", "--output", type=Path, default=None)
    args = parser.parse_args()

    result = load_result(args.input)
    try:
        report = generate(result, args.backend, args.lang, args.model)
    except Exception as exc:  # noqa: BLE001 - prototype: degrade gracefully to template
        print(f"[warn] backend '{args.backend}' failed ({exc}); falling back to template.\n")
        report = render_template(result, args.lang)

    out = args.output or args.input.with_name(args.input.stem + "_report.md")
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\n[saved] {out}")


if __name__ == "__main__":
    main()
