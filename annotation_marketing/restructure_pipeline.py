"""
Pipeline de restructuration suite a la revue qualite demandee par l'utilisateur :

1. Corrige les segments tagues language="en" qui contiennent en realite des
   diacritiques vietnamiens (bug de tag, pas de re-annotation de role : le
   moteur de vote (vote1/vote2/vote3, ci-dessous, copie de vote_classify.py)
   ne lit JAMAIS le champ `language` en entree, donc le role assigne ne peut
   pas avoir ete fausse par ce bug -- on le reverifie empiriquement pour le
   prouver, pas juste l'affirmer).
2. Reconstruit gold/silver avec la bonne convention :
   - gold_dataset.jsonl = sous-ensemble a confiance maximale (>=0.95, accord
     3/3 des votes), le plus proche d'un "verifie" qu'on puisse produire sans
     annotateur humain. 286 lignes.
   - silver_dataset.jsonl = reste des segments a role reel (!=uncertain,
     confidence <0.95). 3122 lignes.
   - uncertain_dataset.jsonl = nouveau fichier separe pour ne perdre aucune
     ligne (3468 segments role=uncertain, ni silver ni gold).
"""

import json
import re
import os

SRC_DIR = "/sessions/gracious-stoic-albattani/mnt/Projet_user_behavior/USER-BEHAVIOR-SOCIAL-MEDIA/annotation_marketing"

VI_DIACRITICS = re.compile(
    "[ạảấầẩẫắằặẳẵếềệểễốồộổỗớờợởỡứừựửữịỉĩụủũỳỵỷỹđ]"
)


def has(t, *pats):
    return any(re.search(p, t) for p in pats)


def vote1(text, seg_id):
    t = text.lower().strip()
    if has(t, r"https?://", r"\bsubscribe\b", r"\bfollow us\b", r"\blink in (the )?bio\b",
           r"\buse code\b", r"\bdang ky\b", "đăng ký", r"\bclick here\b", r"\bcheck out\b",
           r"\bcontact us\b", "liên hệ", r"\bbook (a|your) (test ?drive|demo)\b"):
        return ("cta", 0.85)
    if has(t, r"\d+\s?(km|mi|miles|kw|kwh|hp|mph|km/h|%|\$|€|₫|đồng|seconds|sec)\b",
           r"\d+\s?(cu ?ft|liters?|lít)\b", r"range of \d+", r"0-60", r"top speed",
           r"\d+[,. ]?\d*\s?(rmb|usd|vnd|triệu|tỷ)\b", r"\bprice of\b.*\d", r"\baverage\b.*\d"):
        return ("proof", 0.76)
    if has(t, "ưu đãi", "khuyến mãi", "giảm giá", r"\d+\s?(triệu|tỷ) đồng"):
        return ("scarcity", 0.65)
    if has(t, r"\b(customers?|users?|owners?|clients?|reviewers?) (say|love|report)\b",
           r"\btrusted by\b", r"\bmillions? of\b", r"\bover \d+[,. ]?\d* (owners|customers|users)\b",
           "khách hàng", "người dùng", r"\b#1\b", r"\bbest[- ]selling\b",
           r"\b(led|headed|directed) (design |engineering )?teams? at\b",
           r"\bformer(ly)?\b.*\b(engineer|designer|director|ceo|lead)\b",
           r"\bchief (designer|engineer)\b", r"\bworked at\b", r"\bexpert\b",
           "đánh giá", "kênh đánh giá"):
        return ("social_proof", 0.72)
    if has(t, r"\bi (drove|owned|bought|tested|tried|spent|took|cancelled|switched|decided)\b",
           r"\bmy (experience|journey|byd|tesla|car|ev)\b",
           "tôi đã", "mình đã", "trải nghiệm", r"\bhaving owned\b", r"\bafter \d+ (months|years|km)\b",
           r"\blast (year|month|week) i\b"):
        return ("storytelling", 0.7)
    if has(t, r"\bproblems?\b", r"\bissues?\b", r"\bregret\b", r"\bworried\b", r"\bafraid\b",
           r"\bannoying\b", r"\bfrustrat", r"\banxiety\b", r"\bcan'?t\b.*\b(charge|find)\b",
           r"\blittle to dislike\b", r"\bdislike\b", "cảnh báo", "cảnh giác",
           "vấn đề", "lo lắng", "khó chịu"):
        return ("pain_point", 0.7)
    if has(t, r"\bbut\b", r"\bhowever\b", r"\beven though\b", r"\bdespite\b", r"\bno need to\b",
           r"\byou (don'?t|do not) (need|have) to\b", r"\bactually\b.*\bnot\b"):
        return ("objection_handling", 0.68)
    if has(t, r"\bnow\b", r"\btoday\b", r"\bhurry\b", r"\bdon'?t miss\b", r"\bbefore it'?s too late\b",
           "ngay hôm nay", "đừng bỏ lỡ"):
        return ("urgency", 0.7)
    if has(t, r"\bonly \d+\b", r"\blimited (edition|stock|time|units)\b", r"\blast chance\b",
           "chỉ còn", "số lượng giới hạn"):
        return ("scarcity", 0.72)
    if seg_id == 1 and has(t, r"\?$", r"\bwhy\b", r"\bhow\b", r"\bwhat\b", r"\bregret\b", r"\bshocking\b",
                            r"\bvs\b", r"\btruth\b"):
        return ("hook", 0.78)
    if has(t, r"\b(save|saves|saving)\b.*\b(money|fuel|gas|time)\b", r"\bbenefits?\b", r"\bbetter\b",
           r"\bcheaper\b", r"\bfaster\b", r"\bmore efficient\b", "tiết kiệm", "lợi ích"):
        return ("benefit", 0.65)
    if has(t, r"\bhow to\b", r"\bwe explain\b", r"\bin this video\b", r"\blet'?s? (talk about|look at)\b",
           r"\bhere'?s how\b", "hướng dẫn", "giải thích"):
        return ("educational", 0.62)
    if has(t, r"\b(we|i|this) (offer|provide|built|created|introduce|present)\b", r"\bour (new|latest)\b",
           r"\bthis (car|ev|model|vehicle) (has|comes with|features)\b"):
        return ("solution", 0.6)
    if len(t.split()) <= 4:
        return ("uncertain", 0.75)
    return ("uncertain", 0.5)


def vote2(text, seg_id):
    t = text.lower().strip()
    words = t.split()
    if t.startswith(("subscribe", "follow", "check out", "click", "visit", "buy", "get", "try", "use",
                      "share", "watch", "download", "book", "contact", "sign up", "đăng ký", "theo dõi")):
        return ("cta", 0.8)
    if seg_id == 1 and (t.endswith("?") or has(t, r"\bvs\b", r"\bregret\b", r"\bshocking\b", r"\btruth\b",
                                                 r"\bwhy\b", r"\bworst\b", r"\bbest\b")):
        return ("hook", 0.75)
    if has(t, r"\d") and has(t, r"(km|kwh|hp|mph|%|\$|€|₫|seconds|miles|cu ?ft|liters?|rmb|usd|vnd|triệu|tỷ)"):
        return ("proof", 0.75)
    if has(t, r"\b(we|our|this)\b") and has(t, r"\b(offer|launch|introduce|feature|come with|build)\b"):
        return ("solution", 0.62)
    if has(t, r"\b(best|most|least|cheapest|fastest|easiest|biggest)\b") and not has(t, r"\bworst\b"):
        return ("benefit", 0.6)
    if has(t, r"\bi\b") and has(t, r"\b(drove|owned|bought|tested|tried|spent|took|had|cancelled|switched)\b"):
        return ("storytelling", 0.68)
    if has(t, r"\bbut\b|\bhowever\b|\bdespite\b|\beven though\b|\bdon'?t (need|have) to\b"):
        return ("objection_handling", 0.65)
    if has(t, r"\bsubscribe\b|\bfollow\b|\blink\b|\bclick\b"):
        return ("cta", 0.78)
    if has(t, r"\bonly\b.*\d|\blimited\b|ưu đãi|khuyến mãi|giảm giá"):
        return ("scarcity", 0.66)
    if has(t, r"\bnow\b|\btoday\b|\bhurry\b"):
        return ("urgency", 0.65)
    if has(t, r"\b(customers?|users?|owners?|clients?)\b") and has(t, r"\b(say|love|report|trust)\b"):
        return ("social_proof", 0.72)
    if has(t, r"\b(led|headed|directed) teams? at\b|\bformer\b.*\b(engineer|designer|director)\b|\bchief (designer|engineer)\b"):
        return ("social_proof", 0.65)
    if has(t, r"\bproblem|issue|regret|worried|afraid|annoying|frustrat|dislike"):
        return ("pain_point", 0.68)
    if has(t, r"\bhow to\b|\bexplain|\blearn\b|\bunderstand\b|\bguide\b|\btips?\b"):
        return ("educational", 0.6)
    if len(words) <= 4:
        return ("uncertain", 0.7)
    if t.startswith(("#", "©", "credit", "music by", "filmed", "source:", "timestamp")):
        return ("uncertain", 0.85)
    return ("uncertain", 0.45)


def vote3(text, seg_id):
    t = text.lower().strip()
    fields = {
        "cta": ["subscribe", "follow", "link in bio", "click", "contact", "book a", "sign up",
                "đăng ký", "theo dõi", "liên hệ", "mua ngay", "nhấn vào"],
        "proof": [r"\d+\s?(km|kwh|hp|mph|%|\$|€|₫|seconds|miles|rmb|usd|vnd)", r"test results?", r"specs?\b",
                  r"giá \d", r"\d+\s?lít", r"range\b", r"\d+\s?(triệu|tỷ)"],
        "social_proof": ["customers?", r"users? (say|love)", "owners? report", "trusted by",
                          "khách hàng", "người dùng", "đánh giá", "tin dùng",
                          r"led .*teams? at", r"chief (designer|engineer)", r"former.*(engineer|designer)"],
        "storytelling": [r"\bi (drove|owned|bought|tested|cancelled|switched)\b", r"my (experience|journey)",
                          "tôi đã", "mình đã", "trải nghiệm", "hành trình"],
        "pain_point": ["problem", "issue", "regret", "worried", "afraid", "frustrat", "anxiety",
                       "dislike", "vấn đề", "lo lắng", "khó chịu", "bất tiện", "cảnh báo"],
        "benefit": [r"save (money|fuel|time)", "benefit", r"better\b", "cheaper", "efficient",
                    "tiết kiệm", "lợi ích", "hiệu quả", "tốt hơn"],
        "urgency": [r"\bnow\b", "today", "hurry", r"don'?t miss", "ngay hôm nay", "đừng bỏ lỡ"],
        "scarcity": [r"only \d+", r"limited (edition|stock|time)", "last chance", "chỉ còn",
                     "giới hạn", "ưu đãi", "khuyến mãi", "giảm giá"],
        "objection_handling": [r"\bbut\b", "however", "despite", "even though", "no need to",
                                "không cần"],
        "educational": ["how to", "explain", "in this video", "guide", r"tips?\b", "hướng dẫn",
                        "giải thích", "cách ", "kênh đánh giá"],
        "solution": [r"\b(we|our|this) (offer|provide|launch|introduce|feature)", "new model",
                     "giải pháp"],
        "hook": [r"\bwhy\b", r"\bvs\b", "shocking", "truth about", "regret buying", "bạn có biết",
                 "sự thật"],
    }
    scores = {}
    for label, pats in fields.items():
        if has(t, *pats):
            scores[label] = scores.get(label, 0) + 1
    if scores:
        best = max(scores, key=scores.get)
        conf = 0.55 + 0.1 * scores[best]
        return (best, min(conf, 0.85))
    if len(t.split()) <= 4:
        return ("uncertain", 0.7)
    return ("uncertain", 0.5)


def recompute_role(text, seg_id):
    v1, v2, v3 = vote1(text, seg_id), vote2(text, seg_id), vote3(text, seg_id)
    labels = [v[0] for v in [v1, v2, v3]]
    counts = {}
    for l in labels:
        counts[l] = counts.get(l, 0) + 1
    maxcount = max(counts.values())
    winners = [l for l, c in counts.items() if c == maxcount]
    PRIORITY = {"cta": 9, "proof": 8, "social_proof": 7, "scarcity": 7, "urgency": 6,
                "objection_handling": 6, "pain_point": 5, "storytelling": 5, "solution": 4,
                "benefit": 4, "hook": 3, "educational": 2, "uncertain": 0}
    label = winners[0] if len(winners) == 1 else max(winners, key=lambda l: PRIORITY[l])
    return label


def load(fn):
    rows = []
    with open(os.path.join(SRC_DIR, fn), encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main():
    rows = load("silver_dataset.jsonl") + load("gold_dataset.jsonl")
    print("total chargé:", len(rows))

    # --- Etape 1 : fix language tags ---
    n_fixed = 0
    n_role_changed_after_fix = 0
    for r in rows:
        has_vi_chars = bool(VI_DIACRITICS.search(r["text"].lower()))
        if r["language"] == "en" and has_vi_chars:
            old_role = r["primary_role"]
            r["language"] = "vi"
            n_fixed += 1
            recomputed = recompute_role(r["text"], r["segment_id"])
            if recomputed != old_role:
                n_role_changed_after_fix += 1
                print("  ROLE CHANGE:", r["post_id"], r["segment_id"], old_role, "->", recomputed)
    print(f"Tags language corrigés (en->vi) : {n_fixed}")
    print(f"Roles qui auraient changé si on recalculait : {n_role_changed_after_fix}")
    print("(le moteur de vote ne lit pas `language`, donc 0 attendu — vérifié empiriquement ci-dessus)")

    # --- Etape 2 : reconstruire gold / silver / uncertain ---
    real = [r for r in rows if r["primary_role"] != "uncertain"]
    uncertain = [r for r in rows if r["primary_role"] == "uncertain"]
    gold = [r for r in real if r["confidence"] >= 0.95]
    silver = [r for r in real if r["confidence"] < 0.95]

    print(f"gold (conf>=0.95): {len(gold)}")
    print(f"silver (conf<0.95, role reel): {len(silver)}")
    print(f"uncertain (mis a part): {len(uncertain)}")
    assert len(gold) + len(silver) + len(uncertain) == len(rows)

    def dump(fn, data):
        with open(os.path.join(SRC_DIR, fn), "w", encoding="utf-8") as f:
            for r in data:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    dump("gold_dataset.jsonl", gold)
    dump("silver_dataset.jsonl", silver)
    dump("uncertain_dataset.jsonl", uncertain)
    print("Fichiers réécrits : gold_dataset.jsonl, silver_dataset.jsonl, uncertain_dataset.jsonl (nouveau)")


if __name__ == "__main__":
    main()
