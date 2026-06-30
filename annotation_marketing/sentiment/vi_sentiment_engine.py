"""
Moteur de sentiment vietnamien — écrit entièrement à la main (pas de port
d'une librairie existante : aucun équivalent VADER mûr et gratuit n'existe
pour le vietnamien).

Architecture délibérément inspirée de VADER (mêmes mécanismes, lexique et
règles différents) :
  1. Lexique de polarité mot-à-mot, échelle -4..+4 (même échelle que VADER
     pour rester comparable et fusionnable avec le pipeline EN).
  2. Négateurs ("không", "chẳng", "chả", "đừng", "chưa") : inversent et
     atténuent le score du mot qui suit dans une fenêtre de 3 mots, comme la
     règle de négation de VADER en anglais.
  3. Intensificateurs ("rất", "quá", "cực", "cực kỳ", "vô cùng", cũng "hơi",
     "khá" qui ATTÉNUENT) : multiplient le score du mot suivant.
  4. Compound score normalisé entre -1 et +1 (formule identique à VADER :
     x / sqrt(x² + alpha), alpha=15) pour rester directement comparable au
     score EN dans les exports finaux.

Limites assumées (à documenter honnêtement) : lexique volontairement plus
restreint que VADER EN (~150 entrées vs ~7500), pas de gestion de l'ironie,
pas de gestion des emojis vietnamiens spécifiques, segmentation par espaces
uniquement (pas de tokenizer vietnamien dédié type VnCoreNLP — un mot
vietnamien composé de plusieurs syllabes séparées par des espaces, ex.
"an toàn", est donc traité comme une expression multi-mots explicite dans
le lexique, pas reconnu automatiquement par segmentation).
"""

import math
import re

ALPHA = 15.0

# --- Lexique de polarité (mots simples ET expressions, espaces conservés) ---
VI_LEXICON = {
    # Positif — général
    "tốt": 1.8, "tuyệt": 2.6, "tuyệt vời": 3.0, "hài lòng": 2.0,
    "thích": 1.6, "yêu": 2.2, "đẹp": 1.6, "mạnh": 1.4, "nhanh": 1.2,
    "rẻ": 1.6, "an toàn": 1.8, "bền": 1.6, "hiện đại": 1.4,
    "ưu đãi": 1.8, "tiết kiệm": 1.8, "êm": 1.4, "mượt": 1.6,
    "đáng mua": 2.2, "đáng tiền": 2.0, "ấn tượng": 1.8,
    # Positif — EV spécifique
    "trợ giá": 1.6, "sạc nhanh": 1.6, "miễn phí sạc": 2.0,
    "vận hành êm": 1.6, "tiết kiệm xăng": 1.8, "thân thiện môi trường": 2.0,

    # Négatif — général
    "tệ": -2.0, "kém": -1.8, "chán": -1.6, "dở": -1.8, "xấu": -1.6,
    "đắt": -1.6, "chậm": -1.4, "hỏng": -2.2, "lỗi": -1.8,
    "nguy hiểm": -2.4, "cháy": -2.8, "thất vọng": -2.2,
    "khó chịu": -1.6, "kém chất lượng": -2.2, "lo lắng": -1.8,
    "đáng thất vọng": -2.4, "tồi tệ": -2.6, "phiền": -1.4,
    # Négatif — EV spécifique
    "hết pin": -2.0, "sạc lâu": -1.8, "pin yếu": -2.0,
    "cháy pin": -3.2, "lo lắng quãng đường": -2.2, "trạm sạc hỏng": -2.2,
    "giảm trợ giá": -1.8, "thu hồi": -2.0, "lỗi phần mềm": -1.8,
}

# --- Négateurs : inversent + atténuent (fenêtre de 3 tokens suivants) ---
NEGATORS = {"không", "chẳng", "chả", "đừng", "chưa", "khỏi"}
NEGATION_SHIFT = -0.74  # même logique que le facteur de VADER (N_SCALAR)

# --- Intensificateurs : multiplient le mot suivant ---
BOOSTERS_INCR = {"rất": 0.293, "quá": 0.293, "cực": 0.35, "cực kỳ": 0.4,
                 "vô cùng": 0.35, "hết sức": 0.3}
BOOSTERS_DECR = {"hơi": -0.25, "khá": -0.15, "tạm": -0.2}


def _tokenize(text: str):
    text = text.lower()
    return re.findall(r"[\wàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]+", text)


def _match_lexicon_terms(tokens):
    """Repère les expressions multi-mots du lexique dans la séquence de tokens,
    renvoie une liste de (start_idx, end_idx, score) sans recouvrement."""
    matches = []
    i = 0
    n = len(tokens)
    # trie les clés multi-mots par longueur décroissante (priorité aux expressions longues)
    multi_keys = sorted([k for k in VI_LEXICON if " " in k],
                         key=lambda k: -len(k.split()))
    while i < n:
        matched = False
        for key in multi_keys:
            parts = key.split()
            L = len(parts)
            if tokens[i:i + L] == parts:
                matches.append((i, i + L, VI_LEXICON[key]))
                i += L
                matched = True
                break
        if not matched:
            if tokens[i] in VI_LEXICON:
                matches.append((i, i + 1, VI_LEXICON[tokens[i]]))
            i += 1
    return matches


def vi_polarity_scores(text: str) -> dict:
    """Retourne {neg, neu, pos, compound} sur le même format que VADER EN."""
    tokens = _tokenize(text)
    matches = _match_lexicon_terms(tokens)

    raw_scores = []
    for start, end, score in matches:
        adj_score = score
        # intensificateur juste avant le terme
        if start - 1 >= 0:
            prev = tokens[start - 1]
            if prev in BOOSTERS_INCR:
                adj_score += math.copysign(BOOSTERS_INCR[prev] * 4, adj_score)
            elif prev in BOOSTERS_DECR:
                adj_score += math.copysign(abs(BOOSTERS_DECR[prev]) * 4, adj_score) * -1
        # négation dans les 3 tokens précédents
        window_start = max(0, start - 3)
        if any(t in NEGATORS for t in tokens[window_start:start]):
            adj_score = adj_score * -1 + math.copysign(NEGATION_SHIFT, -adj_score)
        raw_scores.append(adj_score)

    if not raw_scores:
        return {"neg": 0.0, "neu": 1.0, "pos": 0.0, "compound": 0.0}

    total = sum(raw_scores)
    compound = total / math.sqrt(total * total + ALPHA)
    compound = max(-1.0, min(1.0, compound))

    pos_sum = sum(s for s in raw_scores if s > 0)
    neg_sum = sum(abs(s) for s in raw_scores if s < 0)
    n_tokens = max(len(tokens), 1)
    # Normalisation proportionnelle (pos/neg répartis selon leur poids relatif,
    # neu = ce qui reste compte tenu de la fraction de tokens réellement matchés)
    pos_ratio = pos_sum / (pos_sum + neg_sum) if (pos_sum + neg_sum) > 0 else 0.0
    neg_ratio = neg_sum / (pos_sum + neg_sum) if (pos_sum + neg_sum) > 0 else 0.0
    matched_frac = len(matches) / n_tokens
    pos = round(pos_ratio * matched_frac, 3)
    neg = round(neg_ratio * matched_frac, 3)
    neu = round(max(0.0, 1.0 - pos - neg), 3)

    return {"neg": neg, "neu": neu, "pos": pos, "compound": round(compound, 4)}
