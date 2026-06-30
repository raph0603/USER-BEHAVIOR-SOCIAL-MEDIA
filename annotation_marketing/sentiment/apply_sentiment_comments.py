"""
Applique le moteur de sentiment + stance sur les 3 corpus de commentaires
bruts retrouves dans dashboard/data/ (reddit.csv, x.csv, youtube.csv).

Detection de langue : pas de colonne langue pour Reddit/YouTube -> detection
heuristique par presence de diacritiques vietnamiens specifiques (regex).
Pour X, la colonne `lang` existe deja (detection faite par X/Twitter) ; tout
code != 'vi' est traite par le moteur EN par defaut (le corpus X est
quasi-exclusivement anglais : 13800/16500 'en', seulement 8 'vi').

Concu pour tourner par lots successifs limites dans le temps (le sandbox
tue les process en arriere-plan entre deux appels) : un budget de temps
(TIME_BUDGET_S) interrompt proprement le traitement et un fichier de
checkpoint (.checkpoint_<name>.txt) retient le nombre de lignes deja
traitees, pour reprendre exactement ou on s'est arrete a l'appel suivant.

Sortie : un CSV reduit par plateforme (id, language, sentiment_label,
sentiment_score, sentiment_engine, stance), joinable aux fichiers Features
existants par leur cle (comment_id / status_id).
"""

import re
import sys
import os
import time
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from sentiment_engine import score_text, compute_stance

DATA_DIR = "/sessions/gracious-stoic-albattani/mnt/Projet_user_behavior/dashboard/data"
OUT_DIR = "/sessions/gracious-stoic-albattani/mnt/Projet_user_behavior/USER-BEHAVIOR-SOCIAL-MEDIA/annotation_marketing/sentiment/output"
os.makedirs(OUT_DIR, exist_ok=True)

TIME_BUDGET_S = 32
CHUNKSIZE = 5000

VI_DIACRITICS = re.compile(
    "[ạảấầẩẫắằặẳẵếềệểễốồộổỗớờợởỡứừựửữịỉĩụủũỳỵỷỹđ]"
)


def detect_lang(text):
    if not isinstance(text, str) or not text:
        return "en"
    return "vi" if VI_DIACRITICS.search(text.lower()) else "en"


def checkpoint_path(name):
    return os.path.join(OUT_DIR, ".checkpoint_" + name + ".txt")


def read_checkpoint(name):
    p = checkpoint_path(name)
    if os.path.exists(p):
        return int(open(p).read().strip())
    return 0


def write_checkpoint(name, n):
    f = open(checkpoint_path(name), "w")
    f.write(str(n))
    f.close()


def process_platform(name, csv_path, id_col, text_col, lang_col=None):
    out_path = os.path.join(OUT_DIR, name + "_sentiment.csv")
    skip = read_checkpoint(name)
    cols = [c for c in [id_col, text_col, lang_col] if c]
    t0 = time.time()
    n_done_this_call = 0

    skiprows = list(range(1, skip + 1)) if skip else None
    reader = pd.read_csv(csv_path, usecols=cols, chunksize=CHUNKSIZE, skiprows=skiprows)

    for chunk in reader:
        if lang_col:
            chunk["language"] = chunk[lang_col].apply(lambda v: "vi" if v == "vi" else "en")
        else:
            chunk["language"] = chunk[text_col].apply(detect_lang)

        scored = chunk.apply(lambda r: score_text(r[text_col], r["language"]), axis=1)
        chunk["sentiment_label"] = scored.apply(lambda d: d["sentiment_label"])
        chunk["sentiment_score"] = scored.apply(lambda d: d["sentiment_score"])
        chunk["sentiment_engine"] = scored.apply(lambda d: d["engine"])
        chunk["stance"] = chunk.apply(
            lambda r: compute_stance(r[text_col], r["language"], r["sentiment_label"]), axis=1
        )

        out_cols = [id_col, "language", "sentiment_label", "sentiment_score", "sentiment_engine", "stance"]
        write_header = (skip == 0 and n_done_this_call == 0)
        mode = "w" if write_header else "a"
        chunk[out_cols].to_csv(out_path, mode=mode, header=write_header, index=False)

        n_done_this_call += len(chunk)
        skip += len(chunk)
        write_checkpoint(name, skip)

        elapsed = time.time() - t0
        if elapsed > TIME_BUDGET_S:
            print("[" + name + "] pause budget temps : " + str(skip) + " lignes au total")
            return skip, False

    print("[" + name + "] TERMINE : " + str(skip) + " lignes au total")
    return skip, True


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    plan = []
    if target in ("reddit", "all"):
        plan.append(("reddit", os.path.join(DATA_DIR, "reddit.csv"), "comment_id", "comment_text", None))
    if target in ("x", "all"):
        plan.append(("x", os.path.join(DATA_DIR, "x.csv"), "status_id", "tweet_text", "lang"))
    if target in ("youtube", "all"):
        plan.append(("youtube", os.path.join(DATA_DIR, "youtube.csv"), "comment_id", "text", None))

    for name, path, idc, txtc, langc in plan:
        total, done = process_platform(name, path, idc, txtc, lang_col=langc)
        status = "COMPLET" if done else "A_REPRENDRE"
        print("### " + name + ": " + str(total) + " lignes, statut=" + status)
