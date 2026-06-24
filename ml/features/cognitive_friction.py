"""Static content feature: cognitive friction (reading effort), EN + VI.

A pure function of the post text, so it is known pre-launch and computes the same
value at train and serve time. Higher score = harder to read, which tends to
suppress engagement — a useful, explainable signal for marketers running green/EV
campaigns ("this ad is hard to read, so it is less likely to spread").

The function auto-detects English vs Vietnamese and routes the language-specific
parts (lexical difficulty, jargon, connectives) accordingly; the rest of the
signals are language-agnostic. Each sub-signal is clamped to a fixed [lo, hi]
range (not min-max over the dataset) to keep the function stateless and drift-free.
"""
from __future__ import annotations

import re

__all__ = ["cognitive_friction"]

_VOWELS = "aeiouy"
_WORD = re.compile(r"[^\W\d_]+", re.UNICODE)          # Unicode letters -> works for EN and VI
_EMOJI = re.compile("[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF]")
_VI_CHARS = set("ăâđêôơưáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợùúủũụứừửữựỳýỷỹỵ")

# HARD technical terms only — these raise reading effort. Common green-marketing words
# (ev, green, battery, xe điện, pin, sạch...) are deliberately excluded: they belong to a
# topic-relevance feature, not friction, and would wrongly penalise short on-topic ads.
_JARGON_EN = re.compile(
    r"\b(kwh?|kilowatt|regenerative|drivetrain|powertrain|lithium|electrolyte|inverter|"
    r"voltage|torque|thermal|aerodynamic|coefficient|payload|autonomy)\b", re.I)
_JARGON_VI = re.compile(
    r"(truyền động|biến tần|điện áp|mô[- ]?men( xoắn)?|hệ số|khí động|nhiệt động|"
    r"mật độ năng lượng|tái tạo|lithium|công suất|hiệu suất|mã lực|"
    r"kilowatt[- ]?giờ|vòng/phút)", re.I)

_CONJ_EN = re.compile(
    r"\b(and|but|or|because|although|which|that|while|however|therefore|whereas)\b", re.I)
_CONJ_VI = re.compile(
    r"(và|nhưng|hoặc|vì|bởi|mà|nên|tuy nhiên|do đó|mặc dù|cho nên|vì vậy|trong khi)", re.I)

_SENT_SPLIT = re.compile(r"[.!?]+(?=\s|$)")   # split on sentence punctuation, keep "77.4" intact
_SHOUT_PUNCT = re.compile(r"[!?]{2,}")        # any run of !/? >= 2, incl. mixed "?!", "!?!?"
# number + technical unit (kWh, Nm, %, km, mã lực...) — drift-proof tech-spec marker, language-agnostic.
# Counts specs, NOT casual numbers ("3 tiếng", "2024"), so simple sentences don't spike f_info.
_UNITS = re.compile(r"\d[\d.,]*\s*-?\s*(kwh|kw|wh|nm|hp|rpm|km/h|km|mph|kv|mã lực|kilowatt[- ]?giờ|%)", re.I)


def _scale(x: float, lo: float, hi: float) -> float:
    """Clamp x to [lo, hi] and rescale to [0, 1]."""
    return 0.0 if hi == lo else min(1.0, max(0.0, (x - lo) / (hi - lo)))


def _syllables(word: str) -> int:
    """Heuristic English syllable count (vowel groups)."""
    word = word.lower()
    count, prev_vowel = 0, False
    for ch in word:
        vowel = ch in _VOWELS
        count += vowel and not prev_vowel
        prev_vowel = vowel
    if word.endswith("e"):
        count -= 1
    return max(1, count)


def _detect_lang(text: str) -> str:
    """Return 'vi' if Vietnamese diacritics are common enough, else 'en'."""
    letters = [c.lower() for c in text if c.isalpha()]
    if not letters:
        return "en"
    return "vi" if sum(c in _VI_CHARS for c in letters) / len(letters) > 0.03 else "en"


def cognitive_friction(text: str) -> dict:
    """Return sub-signals + composite score (all in [0, 1]) and the detected lang.

    f_word is NaN for Vietnamese (excluded from the composite). f_info counts
    technical specs (number+unit) + hard jargon, not casual numbers.
    """
    keys = ("f_word", "f_sent", "f_clause", "f_info", "f_visual")
    text = (text or "").strip()
    if not text:
        return {**dict.fromkeys(keys, 0.0), "cognitive_friction_score": 0.0, "lang": "en"}

    lang = _detect_lang(text)
    words = _WORD.findall(text)
    n_words = max(len(words), 1)
    sentences = [s for s in _SENT_SPLIT.split(text) if s.strip()]
    n_sent = max(len(sentences), 1)

    jargon = len((_JARGON_VI if lang == "vi" else _JARGON_EN).findall(text))
    conj = len((_CONJ_VI if lang == "vi" else _CONJ_EN).findall(text))
    specs = len(_UNITS.findall(text))                        # number+unit tech specs (drift-proof)
    shout = sum(len(w) >= 3 and w.isupper() for w in words)  # ALL-CAPS words, not acronyms

    parts = {
        # lexical difficulty: EN polysyllabic-word ratio. VI -> NaN (word length is not a valid
        # difficulty proxy for a monosyllabic language), so it is excluded from the composite.
        "f_word": _scale(sum(_syllables(w) >= 3 for w in words) / n_words, 0.0, 0.30)
                  if lang == "en" else float("nan"),
        "f_sent": _scale(n_words / n_sent, 8.0, 30.0),                       # long sentences
        "f_clause": _scale((conj + text.count(",")) / n_sent, 0.0, 3.0),     # nested clauses
        "f_info": _scale((specs + jargon) / n_words, 0.0, 0.12),             # technical-spec density
        "f_visual": (                                                        # shouting + !!! + emoji/#
            _scale(shout / n_words, 0.0, 0.30)
            + _scale(len(_SHOUT_PUNCT.findall(text)), 0, 5)
            + _scale(len(_EMOJI.findall(text)) + text.count("#"), 0, 10)
        ) / 3,
    }
    measured = [v for v in parts.values() if v == v]         # drop NaN (VI f_word)
    parts["cognitive_friction_score"] = round(sum(measured) / len(measured), 4)
    parts["lang"] = lang
    return parts


if __name__ == "__main__":
    samples = {
        "EN low ": "This EV is fun. It drives well. We love it.",
        "EN high": ("Notwithstanding the regenerative drivetrain's 77.4 kWh lithium-ion architecture, "
                    "the thermal inverter coefficient, which fluctuates considerably, undermines payload "
                    "autonomy by 23.6% — A TRULY ALARMING RESULT!!! #EV #battery"),
        "VI low ": "Xe điện này vui. Nó chạy tốt. Mình thích nó.",
        "VI emo ": "Pin xe hết sạch. Chết máy giữa đường. Mất 3 tiếng chờ.",  # low friction = correct
        "VI high": ("Mặc dù kiến trúc pin lithium-ion 77.4 kWh của hệ truyền động tái tạo, hệ số biến tần "
                    "nhiệt — vốn dao động đáng kể — làm giảm quãng đường tới 23.6%, MỘT KẾT QUẢ ĐÁNG BÁO "
                    "ĐỘNG!!! #xeđiện #pin"),
    }
    for name, t in samples.items():
        print(name, cognitive_friction(t))
