"""Batch-score posts and save explanations to a JSONL file.

Reads a CSV with a text column (and optional source column), runs the explainable
viral predictor on each row, and writes one JSON object per line (input + result):
  {text, source, viral_score, label, confidence, top_factors, explanation_text, suggestions}

Usage:
  python ml/serve/score_batch.py --input data/samples/filtered_events.csv --limit 20
  python ml/serve/score_batch.py --input my_posts.csv --output ml/data/predictions.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from serve.explain_viral import explain_post

DEFAULT_OUTPUT = ML_ROOT / "data" / "predictions.jsonl"


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch-score posts and save explanations to JSONL.")
    parser.add_argument("--input", type=Path, required=True, help="CSV with a text column.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--text-col", default="text")
    parser.add_argument("--source-col", default="source")
    parser.add_argument("--limit", type=int, default=None, help="Only score the first N rows.")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    if args.limit:
        df = df.head(args.limit)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    n_viral = 0
    with open(args.output, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            text = str(row.get(args.text_col, "") or "")
            source = str(row.get(args.source_col, "") or "")
            result = explain_post(text, source)
            n_viral += result["label"] == "viral-likely"
            record = {"text": text, "source": source, **result}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"Scored {len(df)} posts | viral-likely: {n_viral} | saved -> {args.output}")


if __name__ == "__main__":
    main()
