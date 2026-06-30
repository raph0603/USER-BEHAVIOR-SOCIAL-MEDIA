"""
Filtre les segments "bruit" non-marketing avant l'annotation (cf. section 8 du
protocole : "Rejeter ou re-segmenter : segment illisible, vide, dupliqué, trop
générique ou impossible à rattacher au post").

Cas rejetés :
- Timestamps de chapitres YouTube ("0:00 Subscribe!", "12:07 Problems").
- URLs nues ou quasi-nues (avec ou sans préfixe "Facebook:", "Site:", etc.).
- Lignes hashtags uniquement ("#ev #review #mg4").
- Fragments trop courts pour porter du sens marketing (< 8 caractères, ou
  juste un mot-étiquette comme "Credits:", "Chapters", "Skip Ahead:").
- Adresses email seules.

Entrée : segments_a_annoter.jsonl
Sorties :
- segments_a_annoter_clean.jsonl (segments conservés pour annotation)
- segments_rejetes.jsonl (segments rejetés, avec le motif du rejet)
"""
import json
import re
from pathlib import Path
from collections import Counter

OUT = Path("/sessions/gracious-stoic-albattani/mnt/outputs")

TIMESTAMP_RE = re.compile(r'^\s*\d{1,2}:\d{2}(:\d{2})?\s')
URL_RE = re.compile(r'https?://\S+|\bwww\.\S+')
HASHTAG_ONLY_RE = re.compile(r'^(\s*#\S+\s*)+$')
EMAIL_ONLY_RE = re.compile(r'^\s*[\w.+-]+@[\w-]+\.[\w.-]+\s*$')
LABEL_PREFIX_RE = re.compile(
    r'^(facebook|twitter|instagram|tiktok|linkedin|youtube|website|site|subscribe|email|contact|'
    r'liên hệ|fanpage|kênh|crédits|credits|chapters|skip ahead|timestamps?|social)\s*[:\|]?\s*$',
    re.IGNORECASE,
)


def reject_reason(text: str):
    t = text.strip()
    if len(t) < 8:
        return "too_short"
    if TIMESTAMP_RE.match(t):
        return "chapter_timestamp"
    if HASHTAG_ONLY_RE.match(t):
        return "hashtag_only"
    if EMAIL_ONLY_RE.match(t):
        return "email_only"
    if LABEL_PREFIX_RE.match(t):
        return "label_only"
    # URL qui occupe la quasi-totalité du segment (lien nu, avec ou sans petit préfixe)
    has_url = bool(URL_RE.search(t))
    if has_url:
        without_url = URL_RE.sub('', t).strip(' :|-–—.,')
        if len(without_url) < 15:
            return "url_only"
    return None


def main():
    kept, rejected = [], []
    reasons = Counter()

    with open(OUT / "segments_a_annoter.jsonl", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            reason = reject_reason(d["text"])
            if reason:
                d["reject_reason"] = reason
                rejected.append(d)
                reasons[reason] += 1
            else:
                kept.append(d)

    with open(OUT / "segments_a_annoter_clean.jsonl", "w", encoding="utf-8") as f:
        for d in kept:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    with open(OUT / "segments_rejetes.jsonl", "w", encoding="utf-8") as f:
        for d in rejected:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    print(f"Segments conservés : {len(kept)}")
    print(f"Segments rejetés   : {len(rejected)} ({100*len(rejected)/(len(kept)+len(rejected)):.1f}%)")
    print("Motifs de rejet :")
    for reason, n in reasons.most_common():
        print(f"  {reason:20s} : {n}")


if __name__ == "__main__":
    main()
