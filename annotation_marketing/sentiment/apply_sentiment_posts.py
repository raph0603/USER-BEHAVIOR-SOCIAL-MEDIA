"""
Applique le moteur de sentiment (EN VADER+EV / VI from-scratch) + la
pondération par rôle rhétorique sur silver_dataset.jsonl et gold_dataset.jsonl.

Champs ajoutés à chaque ligne :
  - sentiment_label : positive / negative / neutral
  - sentiment_score : compound -1..1 (après ajustement de rôle)
  - sentiment_engine : vader_ev_extended / vi_rule_engine_v1
  - role_flag : None, "adjusted_for_role:<role>", ou "intentional_negative_rhetoric"

Sortie : silver_dataset_sentiment.jsonl, gold_dataset_sentiment.jsonl,
dans le même dossier que les fichiers source.
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from sentiment_engine import score_text, adjust_for_role

SRC_DIR = "/sessions/gracious-stoic-albattani/mnt/Projet_user_behavior/USER-BEHAVIOR-SOCIAL-MEDIA/annotation_marketing"


def process(in_path, out_path):
    n = 0
    label_counts = {}
    flag_counts = {}
    with open(in_path, encoding="utf-8") as fin, open(out_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            base = score_text(row.get("text", ""), row.get("language", "en"))
            adjusted = adjust_for_role(base, row.get("primary_role"))
            row["sentiment_label"] = adjusted["sentiment_label"]
            row["sentiment_score"] = adjusted["sentiment_score"]
            row["sentiment_engine"] = adjusted["engine"]
            row["role_flag"] = adjusted["role_flag"]
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
            label_counts[row["sentiment_label"]] = label_counts.get(row["sentiment_label"], 0) + 1
            if row["role_flag"]:
                flag_counts[row["role_flag"].split(":")[0]] = flag_counts.get(row["role_flag"].split(":")[0], 0) + 1
    print(f"{in_path} -> {out_path} : {n} lignes")
    print("  labels:", label_counts)
    print("  role_flags:", flag_counts)


if __name__ == "__main__":
    process(os.path.join(SRC_DIR, "silver_dataset.jsonl"),
             os.path.join(SRC_DIR, "silver_dataset_sentiment.jsonl"))
    process(os.path.join(SRC_DIR, "gold_dataset.jsonl"),
             os.path.join(SRC_DIR, "gold_dataset_sentiment.jsonl"))
