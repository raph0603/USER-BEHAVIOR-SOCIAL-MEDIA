"""
Construit l'échantillon équilibré : pour chaque langue, prend le réseau qui a
le moins de posts disponibles, puis échantillonne ce même nombre dans les 2
autres réseaux (tirage aléatoire reproductible, seed fixe).
Entrée : all_posts_raw.jsonl (post_id, platform, language, text)
Sortie : posts_originaux_selection.jsonl + résumé des comptages avant/après.
"""
import json
import random
from collections import defaultdict
from pathlib import Path

OUT = Path("/sessions/gracious-stoic-albattani/mnt/outputs")
SEED = 42

MIN_TEXT_LEN = 20    # filtre qualité : on rejette les posts trop courts pour être annotables
MAX_TEXT_LEN = 3000  # filtre qualité : on rejette les posts trop longs (essais/articles, pas des
                      # posts marketing courts) qui fausseraient la segmentation et l'équilibrage


def load_posts(path):
    posts = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            posts.append(json.loads(line))
    return posts


def main():
    posts = load_posts(OUT / "all_posts_raw.jsonl")

    # filtre qualité minimal
    before = len(posts)
    posts = [p for p in posts if MIN_TEXT_LEN <= len(p["text"].strip()) <= MAX_TEXT_LEN]
    rejected = before - len(posts)

    by_lang_platform = defaultdict(list)
    for p in posts:
        by_lang_platform[(p["language"], p["platform"])].append(p)

    print("=== Disponible après filtre qualité (%d <= texte <= %d caractères) ===" % (MIN_TEXT_LEN, MAX_TEXT_LEN))
    for (lang, plat), lst in sorted(by_lang_platform.items()):
        print(f"  {plat:10s} {lang:3s} : {len(lst)}")
    print(f"  (rejetés trop courts/trop longs : {rejected})\n")

    rng = random.Random(SEED)
    selected = []
    summary = []

    languages = sorted(set(l for l, _ in by_lang_platform.keys()))
    for lang in languages:
        platforms = {plat: lst for (l, plat), lst in by_lang_platform.items() if l == lang}
        if not platforms:
            continue
        min_count = min(len(lst) for lst in platforms.values())
        for plat, lst in platforms.items():
            rng.shuffle(lst)
            chosen = lst[:min_count]
            selected.extend(chosen)
            summary.append((plat, lang, len(lst), min_count))

    out_file = OUT / "posts_originaux_selection.jsonl"
    with open(out_file, "w", encoding="utf-8") as f:
        for p in selected:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    print("=== Échantillon équilibré final ===")
    for plat, lang, available, taken in sorted(summary):
        print(f"  {plat:10s} {lang:3s} : {taken} retenus / {available} disponibles")
    print(f"\nTotal sélectionné : {len(selected)} posts -> {out_file}")


if __name__ == "__main__":
    main()
