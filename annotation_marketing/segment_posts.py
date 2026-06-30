"""
Segmentation des posts en segments courts (étape 3 de la fiche d'instructions).

Règles appliquées :
1) Découpage en phrases sur la ponctuation forte (. ! ? saut de ligne).
2) Découpage supplémentaire des phrases qui contiennent une conjonction de
   coordination (et / and / mais / but) séparant deux propositions
   suffisamment longues (>= 4 mots chacune) -> cas "deux fonctions dans une
   phrase" décrit dans la fiche.
3) Nettoyage : suppression des segments vides, doublons consécutifs, segments
   < 3 caractères.

C'est une PREMIÈRE PASSE mécanique : elle prépare le travail pour
l'annotation LLM, qui pourra encore affiner certaines frontières de segments
si besoin (la fiche le permet : le LLM reçoit les segments proposés mais peut
signaler un découpage imparfait via confidence basse / uncertain).
"""
import json
import re
from pathlib import Path

OUT = Path("/sessions/gracious-stoic-albattani/mnt/outputs")

SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
COORD_SPLIT_RE = re.compile(r"\b(et|and|mais|but)\b", flags=re.IGNORECASE)


def split_sentences(text: str):
    text = text.strip()
    if not text:
        return []
    parts = SENTENCE_SPLIT_RE.split(text)
    return [p.strip() for p in parts if p.strip()]


def maybe_split_coordination(sentence: str):
    m = COORD_SPLIT_RE.search(sentence)
    if not m:
        return [sentence]
    left = sentence[: m.start()].strip(" ,.;")
    right = sentence[m.end():].strip(" ,.;")
    if len(left.split()) >= 4 and len(right.split()) >= 4:
        if not left.endswith((".", "!", "?")):
            left += "."
        if not right.endswith((".", "!", "?")):
            right += "."
        return [left, right]
    return [sentence]


def segment_text(text: str):
    segments = []
    for sentence in split_sentences(text):
        segments.extend(maybe_split_coordination(sentence))
    # nettoyage final
    cleaned = []
    seen = set()
    for s in segments:
        s = s.strip()
        if len(s) < 3:
            continue
        if s in seen:
            continue
        seen.add(s)
        cleaned.append(s)
    return cleaned


def main():
    in_file = OUT / "posts_originaux_selection.jsonl"
    out_file = OUT / "segments_a_annoter.jsonl"

    n_posts = 0
    n_segments = 0
    seg_per_post = []

    with open(in_file, encoding="utf-8") as fin, open(out_file, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            post = json.loads(line)
            segments = segment_text(post["text"])
            if not segments:
                continue
            n_posts += 1
            n_segments += len(segments)
            seg_per_post.append(len(segments))
            for i, seg_text in enumerate(segments, start=1):
                fout.write(json.dumps({
                    "post_id": post["post_id"],
                    "platform": post["platform"],
                    "language": post["language"],
                    "segment_id": i,
                    "text": seg_text,
                    "full_text": post["text"],
                    "n_segments_in_post": len(segments),
                }, ensure_ascii=False) + "\n")

    avg = sum(seg_per_post) / len(seg_per_post) if seg_per_post else 0
    print(f"Posts segmentés : {n_posts}")
    print(f"Segments produits : {n_segments}")
    print(f"Moyenne segments/post : {avg:.2f}")
    print(f"-> {out_file}")


if __name__ == "__main__":
    main()
