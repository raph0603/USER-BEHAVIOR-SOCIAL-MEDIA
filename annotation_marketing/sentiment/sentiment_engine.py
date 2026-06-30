"""
Moteur de sentiment unifié — point d'entrée unique pour le pipeline.

Combine :
  - EN : VADER (vaderSentiment) + extension lexicale domaine VE (ev_lexicon_en.py)
  - VI : moteur de règles écrit à la main (vi_sentiment_engine.py)
  - Pondération conditionnée par le rôle rhétorique (posts uniquement, déjà
    annoté dans silver/gold_dataset.jsonl) — voir `adjust_for_role`.
  - Stance pour les commentaires (réaction du public, voir `compute_stance`).

Contribution originale (au-delà du simple usage de VADER, en réponse à la
consigne du tuteur de ne pas se contenter d'une librairie existante) :
  1. Extension lexicale du domaine VE (ev_lexicon_en.py).
  2. Moteur vietnamien from scratch, architecture inspirée VADER mais lexique
     et règles propres (vi_sentiment_engine.py) — aucun équivalent existant
     gratuit pour le vietnamien.
  3. Pondération par rôle rhétorique : utilise une information dont VADER ne
     dispose pas (le rôle marketing du segment, déjà annoté dans ce projet)
     pour corriger des faux positifs/négatifs systématiques (ex. la négation
     dans un `objection_handling` est de la réassurance, pas un vrai sentiment
     négatif envers le produit).
  4. Stance pour les commentaires : VADER ne distingue pas l'adhésion au
     message de l'hostilité à son propos — ajout d'une couche dédiée.
"""

import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from ev_lexicon_en import EV_LEXICON_EN
from vi_sentiment_engine import vi_polarity_scores

_analyzer = SentimentIntensityAnalyzer()
_analyzer.lexicon.update(EV_LEXICON_EN)

# Multi-word EV terms (avec espace) -> token unique pour que VADER les voie
# comme une seule unité au lieu de scorer chaque mot séparément.
_MULTIWORD_TERMS = sorted(
    [k for k in EV_LEXICON_EN if "_" in k],
    key=lambda k: -len(k.split("_")),
)


def _substitute_multiword_terms(text: str) -> str:
    """Remplace les expressions EV multi-mots ("range anxiety") par leur clé
    lexicale ("range_anxiety") pour que VADER les traite comme un seul terme."""
    out = text
    for key in _MULTIWORD_TERMS:
        phrase = key.replace("_", " ")
        out = re.sub(re.escape(phrase), key, out, flags=re.IGNORECASE)
    return out


def _to_ternary(compound: float) -> str:
    if compound >= 0.05:
        return "positive"
    if compound <= -0.05:
        return "negative"
    return "neutral"


def score_text(text: str, language: str) -> dict:
    """Renvoie {sentiment_label, sentiment_score, neg, neu, pos, compound, engine}."""
    if not isinstance(text, str) or not text.strip():
        return {"sentiment_label": "neutral", "sentiment_score": 0.0,
                "neg": 0.0, "neu": 1.0, "pos": 0.0, "compound": 0.0, "engine": "empty"}

    lang = (language or "en").lower()
    if lang in ("vi", "vietnamese", "viet"):
        scores = vi_polarity_scores(text)
        engine = "vi_rule_engine_v1"
    else:
        prepped = _substitute_multiword_terms(text)
        scores = _analyzer.polarity_scores(prepped)
        engine = "vader_ev_extended"

    return {
        "sentiment_label": _to_ternary(scores["compound"]),
        "sentiment_score": round(scores["compound"], 4),
        "neg": scores["neg"], "neu": scores["neu"], "pos": scores["pos"],
        "compound": scores["compound"], "engine": engine,
    }


# --- Pondération par rôle rhétorique (posts uniquement) ---
# Chaque rôle a un comportement de négation/intensité différent du sens
# littéral. Facteur appliqué au compound score AVANT reclassification ternaire.
ROLE_ADJUSTMENT = {
    # La négation dans une levée d'objection est de la réassurance
    # ("No, the battery won't degrade after 5 years") : on atténue le négatif.
    "objection_handling": {"if_negative_mult": 0.45},
    # Les pain points sont volontairement négatifs (description d'un problème
    # pour mieux vendre la solution) : sentiment réel, mais on ajoute un flag
    # plutôt qu'une correction — ce n'est pas un faux négatif.
    "pain_point": {"flag_only": "intentional_negative_rhetoric"},
    # Urgency/scarcity utilisent un vocabulaire de manque ("don't miss out",
    # "limited", "running out") que VADER lit parfois trop négativement.
    "urgency": {"if_negative_mult": 0.7},
    "scarcity": {"if_negative_mult": 0.7},
}


def adjust_for_role(base: dict, role: str) -> dict:
    """Applique la correction de rôle à un résultat de score_text(). Renvoie
    un nouveau dict avec sentiment_score/label ajustés + champ role_flag."""
    out = dict(base)
    out["role_flag"] = None
    rule = ROLE_ADJUSTMENT.get(role)
    if not rule:
        return out

    if "flag_only" in rule:
        out["role_flag"] = rule["flag_only"]
        return out

    if "if_negative_mult" in rule and out["compound"] < 0:
        adjusted = out["compound"] * rule["if_negative_mult"]
        out["compound"] = round(adjusted, 4)
        out["sentiment_score"] = round(adjusted, 4)
        out["sentiment_label"] = _to_ternary(adjusted)
        out["role_flag"] = f"adjusted_for_role:{role}"
    return out


# --- Stance pour les commentaires (réaction du public au message, pas juste
# son ton émotionnel) ---
_ADHERE_MARKERS_EN = ["i agree", "exactly", "this", "true", "facts", "well said",
                      "i want one", "i'm buying", "love this", "makes sense"]
_SCEPTIC_MARKERS_EN = ["i doubt", "not sure", "really?", "sounds too good",
                       "we'll see", "i don't believe", "skeptical", "show me proof"]
_HOSTILE_MARKERS_EN = ["this is a scam", "lies", "bullshit", "fake", "propaganda",
                       "shill", "stupid", "garbage", "scam"]

_ADHERE_MARKERS_VI = ["đồng ý", "chuẩn", "chính xác", "muốn mua", "ưng quá"]
_SCEPTIC_MARKERS_VI = ["chưa chắc", "không tin", "nghi ngờ", "liệu có"]
_HOSTILE_MARKERS_VI = ["lừa đảo", "dối trá", "vô lý", "ngu", "rác"]


def compute_stance(text: str, language: str, sentiment_label: str) -> str:
    """adhère / sceptique / hostile / neutre — combine marqueurs lexicaux
    explicites + repli sur le sentiment_label si aucun marqueur trouvé."""
    if not isinstance(text, str):
        return "neutre"
    low = text.lower()
    lang = (language or "en").lower()

    if lang in ("vi", "vietnamese", "viet"):
        adhere, sceptic, hostile = _ADHERE_MARKERS_VI, _SCEPTIC_MARKERS_VI, _HOSTILE_MARKERS_VI
    else:
        adhere, sceptic, hostile = _ADHERE_MARKERS_EN, _SCEPTIC_MARKERS_EN, _HOSTILE_MARKERS_EN

    if any(m in low for m in hostile):
        return "hostile"
    if any(m in low for m in sceptic):
        return "sceptique"
    if any(m in low for m in adhere):
        return "adhère"

    # Repli : pas de marqueur explicite -> dérivé du sentiment général
    if sentiment_label == "positive":
        return "adhère"
    if sentiment_label == "negative":
        return "sceptique"
    return "neutre"
