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
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
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


def run(title: str, script: Path, *arguments: str) -> None:
    print(f"\n{'=' * 60}\n>>> {title}\n{'=' * 60}", flush=True)
    subprocess.run([PY, str(script), *arguments], check=True, env=ENV)


def export_from_lakehouse(out: Path) -> None:
    print(f"\n{'=' * 60}\n>>> Export Silver -> {out}\n{'=' * 60}", flush=True)
    subprocess.run(
        ["docker", "exec", "dashboard", "python", "/app/scripts/data_cli.py",
         "export", "--format", "csv", "--output", "/app/filtered_events.csv"],
        check=True,
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["docker", "cp", "dashboard:/app/filtered_events.csv", str(out)], check=True)
    digest = hashlib.sha256(out.read_bytes()).hexdigest()
    exported_at = datetime.now(timezone.utc)
    version = f"{exported_at:%Y%m%dT%H%M%SZ}-{digest[:12]}"
    versioned_path = out.parent / "exports" / f"filtered_events-{version}.csv"
    versioned_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(out, versioned_path)
    manifest = {
        "version": version,
        "exported_at": exported_at.isoformat(),
        "source_table": "lakehouse.silver.events",
        "sha256": digest,
        "current_path": str(out),
        "versioned_path": str(versioned_path),
    }
    out.with_suffix(".manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def validate_training_input(
    path: Path,
    *,
    max_age_hours: float,
    allow_stale: bool,
) -> None:
    if not path.is_file():
        raise FileNotFoundError(
            f"Training input is missing: {path}. Run with --export first."
        )
    age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    max_age_seconds = max_age_hours * 3600
    if not allow_stale and age_seconds > max_age_seconds:
        age_hours = age_seconds / 3600
        raise RuntimeError(
            f"Training input is {age_hours:.1f} hours old; maximum is "
            f"{max_age_hours:.1f}. Run with --export or pass --allow-stale-input."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Stage-1 AI pipeline end-to-end.")
    parser.add_argument("--export", action="store_true",
                        help="Pull a fresh filtered_events.csv from the running lakehouse first.")
    parser.add_argument("--export-out", type=Path,
                        default=ML_ROOT.parent / "data" / "samples" / "filtered_events.csv")
    parser.add_argument("--report", action="store_true", help="Also build the consolidated report.")
    parser.add_argument(
        "--max-input-age-hours",
        type=float,
        default=24.0,
        help="Reject an existing export older than this threshold.",
    )
    parser.add_argument(
        "--allow-stale-input",
        action="store_true",
        help="Permit an older export for deliberate reproducibility runs.",
    )
    args = parser.parse_args()

    if args.export:
        export_from_lakehouse(args.export_out)
    validate_training_input(
        args.export_out,
        max_age_hours=args.max_input_age_hours,
        allow_stale=args.allow_stale_input,
    )

    for title, script in STEPS:
        arguments = (
            ("--input", str(args.export_out))
            if script.name == "build_dataset.py"
            else ()
        )
        run(title, script, *arguments)
    if args.report:
        run("Build report", ML_ROOT / "report.py")

    print("\nPipeline finished.")


if __name__ == "__main__":
    main()
