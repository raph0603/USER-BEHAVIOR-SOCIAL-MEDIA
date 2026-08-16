"""Run the full Stage-1 AI pipeline end-to-end.

Order: validate/build dataset -> train role classifier -> train viral model -> evaluate.
Each step runs as a subprocess with the current Python, so it mirrors running the
scripts by hand and stops on the first error.

  python ml/run_pipeline.py --lakehouse-manifest /path/to/manifest.json
  python ml/run_pipeline.py --manual-csv-input /path/to/export.csv
  python ml/run_pipeline.py --export         # manual CSV export compatibility path
  python ml/run_pipeline.py --report         # also build the consolidated report
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ML_ROOT = Path(__file__).resolve().parents[0]
PY = sys.executable
PIPELINE_ROOT = ML_ROOT.parent / "spark" / "jobs"
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.virality_contract import ViralityContract

STEPS = [
    ("Build dataset", ML_ROOT / "preprocess" / "build_dataset.py"),
    ("Train role classifier", ML_ROOT / "train" / "train_roles.py"),
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
        [
            "docker",
            "exec",
            "dashboard",
            "python",
            "/app/scripts/data_cli.py",
            "export",
            "--format",
            "csv",
            "--output",
            "/app/filtered_events.csv",
        ],
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
    if not path.exists():
        raise FileNotFoundError(f"Training input is missing: {path}. Run with --export first.")
    age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    max_age_seconds = max_age_hours * 3600
    if not allow_stale and age_seconds > max_age_seconds:
        age_hours = age_seconds / 3600
        raise RuntimeError(
            f"Training input is {age_hours:.1f} hours old; maximum is "
            f"{max_age_hours:.1f}. Run with --export or pass --allow-stale-input."
        )


def load_lakehouse_manifest(
    path: Path,
    *,
    expected_dataset_version: str | None = None,
) -> tuple[Path, str]:
    if not path.is_file():
        raise FileNotFoundError(f"Lakehouse dataset manifest is missing: {path}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("Lakehouse dataset manifest must be a JSON object")
    version = str(manifest.get("dataset_version") or "").strip()
    fingerprint = str(manifest.get("dataset_fingerprint") or "").strip()
    relative_path = str(manifest.get("dataset_relative_path") or "").strip()
    if manifest.get("official_input") is not True:
        raise ValueError("Manifest is not marked as an official lakehouse input")
    if not version or not relative_path:
        raise ValueError("Manifest must contain dataset_version and dataset_relative_path")
    if re.fullmatch(r"dataset-v2-[a-f0-9]{20}", version):
        raise ValueError(
            "Legacy dataset-v2 uses dataset-relative top-quartile labels and cannot be "
            "treated as a frozen virality contract"
        )
    if not re.fullmatch(r"dataset-v3-[a-f0-9]{20}", version):
        raise ValueError("Manifest dataset_version has an invalid format")
    if not re.fullmatch(r"[a-f0-9]{64}", fingerprint):
        raise ValueError("Manifest dataset_fingerprint must be a SHA-256 hex digest")
    if version != f"dataset-v3-{fingerprint[:20]}":
        raise ValueError("Manifest dataset_version does not match dataset_fingerprint")
    labeling = manifest.get("labeling")
    if not isinstance(labeling, dict):
        raise ValueError("Official manifest must contain frozen virality labeling lineage")
    virality_fingerprint = str(labeling.get("virality_contract_fingerprint") or "")
    if not re.fullmatch(r"[a-f0-9]{64}", virality_fingerprint):
        raise ValueError("Manifest virality_contract_fingerprint must be a SHA-256 hex digest")
    if labeling.get("policy") not in {
        "platform_reference_quantile",
        "training_reference_quantile",
    }:
        raise ValueError("Manifest has an unsupported virality labeling policy")
    if expected_dataset_version and version != expected_dataset_version:
        raise ValueError(
            f"Expected lakehouse dataset {expected_dataset_version}, received {version}"
        )
    relative_dataset_path = Path(relative_path)
    if relative_dataset_path.is_absolute():
        raise ValueError("Manifest dataset_relative_path must be relative")
    export_root = (path.parent.parent if path.parent.name == "runs" else path.parent).resolve()
    dataset_path = (path.parent / relative_dataset_path).resolve()
    if not dataset_path.is_relative_to(export_root):
        raise ValueError("Manifest dataset_relative_path escapes the export root")
    if dataset_path.name != version:
        raise ValueError("Manifest dataset path does not match dataset_version")
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Versioned lakehouse dataset is missing for {version}: {dataset_path}"
        )
    if str(manifest.get("format") or "").lower() != "parquet":
        raise ValueError("Official lakehouse training input must use Parquet")
    relative_contract_path = Path(str(labeling.get("contract_relative_path") or ""))
    if not str(relative_contract_path) or relative_contract_path.is_absolute():
        raise ValueError("Manifest labeling must reference a relative virality contract sidecar")
    contract_path = (path.parent / relative_contract_path).resolve()
    if not contract_path.is_relative_to(export_root):
        raise ValueError("Manifest virality contract path escapes the export root")
    if not contract_path.is_file():
        raise FileNotFoundError(f"Frozen virality contract sidecar is missing: {contract_path}")
    contract = ViralityContract.load(contract_path)
    if contract.fingerprint != virality_fingerprint:
        raise ValueError("Manifest and sidecar virality contract fingerprints do not match")
    return dataset_path, version


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Stage-1 AI pipeline end-to-end.")
    parser.add_argument(
        "--export",
        action="store_true",
        help="Create a manual CSV export; never used by the official training DAG.",
    )
    parser.add_argument(
        "--export-out",
        type=Path,
        default=ML_ROOT.parent / "data" / "samples" / "filtered_events.csv",
    )
    parser.add_argument("--report", action="store_true", help="Also build the consolidated report.")
    parser.add_argument(
        "--lakehouse-manifest",
        type=Path,
        help="Manifest for the exact official lakehouse dataset version to train.",
    )
    parser.add_argument(
        "--dataset-version",
        help="Optional exact version assertion for --lakehouse-manifest.",
    )
    parser.add_argument(
        "--manual-csv-input",
        type=Path,
        help="Explicit non-official CSV compatibility input.",
    )
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

    if args.dataset_version and not args.lakehouse_manifest:
        raise ValueError("--dataset-version requires --lakehouse-manifest")
    if args.lakehouse_manifest and (args.export or args.manual_csv_input):
        raise ValueError(
            "Choose the official --lakehouse-manifest path or a manual CSV path, not both"
        )
    if args.export:
        export_from_lakehouse(args.export_out)
        args.manual_csv_input = args.export_out
    if args.lakehouse_manifest:
        training_input, dataset_version = load_lakehouse_manifest(
            args.lakehouse_manifest,
            expected_dataset_version=args.dataset_version,
        )
        official_input = True
        lakehouse_payload = json.loads(args.lakehouse_manifest.read_text(encoding="utf-8"))
        virality_fingerprint = lakehouse_payload["labeling"]["virality_contract_fingerprint"]
        virality_policy = lakehouse_payload["labeling"]["policy"]
    elif args.manual_csv_input:
        training_input = args.manual_csv_input
        dataset_version = None
        official_input = False
        virality_fingerprint = None
        virality_policy = None
        validate_training_input(
            training_input,
            max_age_hours=args.max_input_age_hours,
            allow_stale=args.allow_stale_input,
        )
        print(
            "WARNING: using an explicit manual CSV compatibility input; "
            "this is not an official training run."
        )
    else:
        raise ValueError(
            "Official training requires --lakehouse-manifest. "
            "Use --manual-csv-input only for deliberate compatibility runs."
        )

    for title, script in STEPS:
        arguments: tuple[str, ...] = ()
        if script.name == "build_dataset.py":
            arguments = ("--input", str(training_input))
            if dataset_version:
                arguments += ("--dataset-version", dataset_version)
        elif script.name in {"train_viral.py", "evaluate.py"} and dataset_version:
            arguments = (
                "--virality-contract-fingerprint",
                virality_fingerprint,
                "--virality-policy",
                virality_policy,
            )
            if script.name == "train_viral.py":
                arguments += ("--dataset-version", dataset_version)
        run(title, script, *arguments)
    if args.report:
        run("Build report", ML_ROOT / "report.py")

    print(
        "\nPipeline finished. "
        + (
            f"Official dataset: {dataset_version}."
            if official_input
            else "Manual compatibility input; no official dataset version."
        )
    )


if __name__ == "__main__":
    main()
