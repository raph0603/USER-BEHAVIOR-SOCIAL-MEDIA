"""Task A prototype - turn the viral-model JSON into a marketing report.

Consumes the exact JSON schema produced by ``ml/serve/explain_viral.explain_post``
(see ml/HANDOFF.md section 2) and renders a short, fixed-structure report for a
marketer, in English or Vietnamese.

It is **backend-agnostic** so we can validate the report format today and plug in
the chosen HuggingFace model (see MODEL_SELECTION.md -> Qwen2.5-7B-Instruct) later:

  - template : deterministic, no model, no GPU. Always works. (default)
  - ollama   : local model via Ollama HTTP API (http://localhost:11434).
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

# --------------------------------------------------------------------------- #
# Prompt (shared by the LLM backends) - adapted from ml/HANDOFF.md section 4
# --------------------------------------------------------------------------- #
PROMPT_TEMPLATES = {
    "en": (
        "You are a senior marketing advisor writing for a busy marketer with NO technical "
        "background. Below is the JSON analysis of one social-media post advertising an "
        "electric vehicle. The JSON includes the post text under \"post_text\". Write a "
        "helpful, SPECIFIC report in Markdown with EXACTLY these four sections and headings:\n\n"
        "#### Verdict\n"
        "2-3 sentences: will this post likely take off on its platform or not, and the main "
        "reason why, in everyday words. Do NOT mention probabilities, scores, percentages, "
        "model names or numbers.\n\n"
        "#### What's working\n"
        "3-5 bullet points on the strengths. For EACH point, briefly explain WHY it helps the "
        "post perform. Translate any technical factor into plain marketing language. No jargon, "
        "no raw feature names, no values.\n\n"
        "#### What to improve\n"
        "3-5 concrete bullet points. Refer to the ACTUAL wording of this post (post_text) and, "
        "where useful, give a short concrete example of the fix. Make each tip directly "
        "actionable.\n\n"
        "#### Bottom line\n"
        "One sentence: the single most important change to make before publishing.\n\n"
        "Rules: 220-320 words total; warm, constructive, concrete tone; base every point on the "
        "post and the JSON, never invent facts. READ post_text CAREFULLY and be factually "
        "accurate: NEVER say an element is missing if it is actually present — if the post "
        "already contains a call-to-action, a hook, proof or social proof, acknowledge it and "
        "only suggest making it stronger; never tell them to 'add' something that is already "
        "there. Never call plain counts (e.g. '3,000 stations', '25,000 customers') "
        "'percentages'. Write ONLY in English. Keep any numbers and units from the post EXACTLY "
        "as written — never convert or change them (keep 'km' as 'km'). Never show numbers or "
        "internal feature names. Do NOT wrap the report in code fences or backticks; start "
        "directly with '#### Verdict'. End immediately after the Bottom line sentence — add no closing remarks, summaries or backticks. Output ONLY the Markdown report. JSON:\n{payload}\n"
    ),
    "vi": (
        "Bạn là cố vấn marketing cấp cao, viết cho một marketer bận rộn KHÔNG có nền tảng kỹ "
        "thuật. Dưới đây là phân tích JSON của một bài đăng mạng xã hội quảng cáo xe điện. "
        "JSON có chứa nội dung bài trong \"post_text\". Hãy viết một báo cáo HỮU ÍCH, CỤ THỂ ở "
        "định dạng Markdown với ĐÚNG bốn mục và tiêu đề sau:\n\n"
        "#### Nhận định\n"
        "2-3 câu: bài này có khả năng lan truyền trên nền tảng của nó hay không, và lý do "
        "chính, bằng lời lẽ đời thường. KHÔNG nêu xác suất, điểm số, phần trăm, tên mô hình "
        "hay con số.\n\n"
        "#### Điểm mạnh\n"
        "3-5 gạch đầu dòng về điểm mạnh. Với MỖI điểm, giải thích ngắn gọn VÌ SAO nó giúp bài "
        "hiệu quả. Diễn giải mọi yếu tố kỹ thuật thành ngôn ngữ marketing dễ hiểu. Không thuật "
        "ngữ, không tên đặc trưng thô, không giá trị số.\n\n"
        "#### Cần cải thiện\n"
        "3-5 gạch đầu dòng cụ thể. Hãy tham chiếu ĐÚNG câu chữ của bài này (post_text) và, khi "
        "hữu ích, đưa ví dụ cụ thể để sửa. Mỗi gợi ý phải khả thi ngay.\n\n"
        "#### Kết luận\n"
        "Một câu: thay đổi quan trọng nhất cần làm trước khi đăng.\n\n"
        "Quy tắc: 220-320 từ; giọng ấm áp, xây dựng, cụ thể; dựa mọi ý vào bài và JSON, không "
        "bịa thông tin. ĐỌC KỸ post_text và chính xác về nội dung: TUYỆT ĐỐI không nói một yếu "
        "tố còn thiếu nếu nó thực sự đã có — nếu bài đã có câu kêu gọi hành động, câu mở đầu, "
        "bằng chứng hay bằng chứng xã hội, hãy ghi nhận và chỉ gợi ý làm mạnh hơn; đừng bảo "
        "'thêm' thứ đã có sẵn. Không gọi các con số đếm (vd. '3.000 trạm', '25.000 khách hàng') "
        "là 'phần trăm'. CHỈ viết bằng tiếng Việt; TUYỆT ĐỐI không dùng ký tự tiếng Trung hay "
        "ngôn ngữ khác. Giữ NGUYÊN mọi con số và đơn vị (giữ 'km' là 'km'). Không hiển thị con "
        "số hay tên đặc trưng nội bộ. KHÔNG bọc báo cáo trong khối mã (```); bắt đầu trực tiếp "
        "bằng '#### Nhận định'. Kết thúc ngay sau câu ở mục Kết luận — không thêm lời bình, tóm tắt hay dấu backtick. Chỉ xuất ra báo cáo Markdown. JSON:\n{payload}\n"
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
    body = json.dumps({"model": model, "prompt": prompt, "stream": False, "keep_alive": "3h"}).encode("utf-8")    
    req = urllib.request.Request(
        "http://localhost:11434/api/generate", data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=300) as resp:
        text = json.loads(resp.read())["response"].strip()
    # Safety net: drop any code-fence marker lines the model may add anywhere
    fence = {"```", "```markdown", "```md"}
    text = "\n".join(ln for ln in text.splitlines() if ln.strip() not in fence).strip()
    return text


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
