"""Run the full Stage-1 AI pipeline end-to-end.

Order: train role classifier -> build dataset -> train viral model -> evaluate.
Each step runs as a subprocess with the current Python, so it mirrors running the
scripts by hand and stops on the first error.

  python ml/run_pipeline.py                  # ML chain (assumes filtered_events.csv exists)
  python ml/run_pipeline.py --export         # pull a fresh Silver export from the lakehouse first
  python ml/run_pipeline.py --report         # also build the consolidated report
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ML_ROOT = Path(__file__).resolve().parents[0]
PY = sys.executable

STEPS = [
    ("Train role classifier", ML_ROOT / "train" / "train_roles.py"),
    ("Build dataset", ML_ROOT / "preprocess" / "build_dataset.py"),
    ("Train viral model", ML_ROOT / "train" / "train_viral.py"),
    ("Evaluate per source", ML_ROOT / "train" / "evaluate.py"),
]

ENV = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}


def run(title: str, script: Path) -> None:
    print(f"\n{'=' * 60}\n>>> {title}\n{'=' * 60}", flush=True)
    subprocess.run([PY, str(script)], check=True, env=ENV)


def export_from_lakehouse(out: Path) -> None:
    print(f"\n{'=' * 60}\n>>> Export Silver -> {out}\n{'=' * 60}", flush=True)
    subprocess.run(
        ["docker", "exec", "dashboard", "python", "/app/scripts/data_cli.py",
         "export", "--format", "csv", "--output", "/app/filtered_events.csv"],
        check=True,
    )
    subprocess.run(["docker", "cp", "dashboard:/app/filtered_events.csv", str(out)], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Stage-1 AI pipeline end-to-end.")
    parser.add_argument("--export", action="store_true",
                        help="Pull a fresh filtered_events.csv from the running lakehouse first.")
    parser.add_argument("--export-out", type=Path,
                        default=ML_ROOT.parent / "data" / "samples" / "filtered_events.csv")
    parser.add_argument("--report", action="store_true", help="Also build the consolidated report.")
    args = parser.parse_args()

    if args.export:
        export_from_lakehouse(args.export_out)

    for title, script in STEPS:
        run(title, script)
    if args.report:
        run("Build report", ML_ROOT / "report.py")

    print("\nPipeline finished.")


if __name__ == "__main__":
    main()
