"""Run the full Stage-1 AI pipeline end-to-end.

Order: validate input -> train role classifier -> build features -> train viral model -> evaluate.
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
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dataset_lineage import load_dataset_lineage

ML_ROOT = Path(__file__).resolve().parents[0]
PROJECT_ROOT = ML_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.reproducibility import (
    capture_environment_manifest,
    capture_git_identity,
    fingerprint,
    load_json,
    manifest_sha256,
    require_official_git,
    write_json,
)

PY = sys.executable

ENVIRONMENT_MANIFEST = ML_ROOT / "results" / "environment_manifest.json"
TRAINING_CONFIG = ML_ROOT / "results" / "training_config.json"
SPLIT_MANIFEST = ML_ROOT / "results" / "split_manifest.json"
EXPERIMENT_LINEAGE = ML_ROOT / "results" / "experiment_lineage.json"
EVALUATION_ARTIFACT = ML_ROOT / "results" / "evaluation.json"
MODEL_ARTIFACT = ML_ROOT / "models" / "stage1_multisource.joblib"

STEPS = [
    ("Train role classifier", ML_ROOT / "train" / "train_roles.py"),
    ("Build dataset", ML_ROOT / "preprocess" / "build_dataset.py"),
    ("Train viral model", ML_ROOT / "train" / "train_viral.py"),
    ("Evaluate per source", ML_ROOT / "train" / "evaluate.py"),
    ("Verify performance figures", ML_ROOT / "train" / "verify_answers.py"),
    ("Evaluate exploratory role-feature ablation", ML_ROOT / "train" / "evaluate_role_ablation.py"),
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
    dataset_path, lineage = load_dataset_lineage(
        path,
        expected_dataset_version=expected_dataset_version,
    )
    assert dataset_path is not None
    return dataset_path, str(lineage["dataset_version"])


def validate_official_manifest(manifest: dict) -> list[str]:
    """Return missing immutable identities required by the current official schema."""

    missing = []
    for field in (
        "manifest_sha256",
        "gold_snapshot_id",
        "gold_table",
        "build_environment",
        "build_environment_fingerprint",
    ):
        if not manifest.get(field):
            missing.append(field)
    snapshots = manifest.get("iceberg_snapshots_json", manifest.get("source_snapshots"))
    if not snapshots:
        missing.append("iceberg_snapshots_json")
    return missing


def validate_dataset_build_environment(manifest: dict, git_commit: str) -> None:
    build_environment = manifest.get("build_environment")
    if not isinstance(build_environment, dict):
        raise ValueError("Dataset manifest has no build environment")
    expected = str(build_environment.get("environment_fingerprint") or "")
    identity = {
        key: value for key, value in build_environment.items() if key != "environment_fingerprint"
    }
    if (
        expected != fingerprint(identity)
        or manifest.get("build_environment_fingerprint") != expected
    ):
        raise ValueError("Dataset build-environment fingerprint is invalid")
    code = build_environment.get("code", {})
    if code.get("git_commit") != git_commit or code.get("git_dirty") is not False:
        raise ValueError("Dataset was not built from the clean Git revision used for training")
    container = build_environment.get("container", {})
    if not container.get("digest") or not container.get("executor_digest"):
        raise ValueError("Official dataset build has no immutable Spark driver/executor digest")


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
    parser.add_argument(
        "--allow-dirty-nonofficial",
        action="store_true",
        help="Allow a dirty tree while explicitly marking the experiment non-official.",
    )
    parser.add_argument(
        "--allow-legacy-manifest-nonofficial",
        action="store_true",
        help="Accept a legacy dataset manifest without complete immutable lineage as non-official.",
    )
    parser.add_argument(
        "--expected-lineage",
        type=Path,
        help="Replay preflight: require dataset, code, environment, config, and split identities to match.",
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
        training_input, dataset_lineage = load_dataset_lineage(
            args.lakehouse_manifest,
            expected_dataset_version=args.dataset_version,
        )
        assert training_input is not None
        dataset_version = str(dataset_lineage["dataset_version"])
        dataset_manifest_path = args.lakehouse_manifest.resolve()
        dataset_manifest_payload = load_json(dataset_manifest_path)
        missing_manifest_fields = validate_official_manifest(dataset_manifest_payload)
        if missing_manifest_fields and not args.allow_legacy_manifest_nonofficial:
            raise ValueError(
                "Official dataset manifest lacks immutable lineage fields: "
                + ", ".join(missing_manifest_fields)
                + ". Re-export it or pass --allow-legacy-manifest-nonofficial."
            )
        official_input = not missing_manifest_fields
    elif args.manual_csv_input:
        training_input = args.manual_csv_input
        dataset_version = None
        dataset_manifest_path = None
        dataset_manifest_payload = {}
        official_input = False
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

    git_identity = capture_git_identity(PROJECT_ROOT)
    clean_for_official = (
        require_official_git(
            git_identity,
            allow_dirty_nonofficial=args.allow_dirty_nonofficial,
        )
        if official_input
        else False
    )
    official_run = bool(official_input and clean_for_official)
    if official_run:
        validate_dataset_build_environment(dataset_manifest_payload, git_identity.git_commit)
    environment_manifest = capture_environment_manifest(
        PROJECT_ROOT,
        git_identity,
        dependency_lock=ML_ROOT / "requirements-train.txt",
        distributions=(
            "pandas",
            "numpy",
            "scikit-learn",
            "scipy",
            "xgboost",
            "pyarrow",
            "joblib",
        ),
        require_container_digest=official_run,
        components=(
            {"dataset_build": dataset_manifest_payload["build_environment"]}
            if dataset_manifest_payload.get("build_environment")
            else None
        ),
    )
    write_json(ENVIRONMENT_MANIFEST, environment_manifest)

    for title, script in STEPS:
        arguments: tuple[str, ...] = ()
        if script.name == "build_dataset.py":
            arguments = ("--input", str(training_input), "--seed", "42")
            if dataset_version:
                arguments += ("--dataset-version", dataset_version)
        elif script.name == "train_roles.py":
            arguments = ("--seed", "42")
        elif script.name == "train_viral.py":
            arguments = (
                "--environment-manifest",
                str(ENVIRONMENT_MANIFEST),
                "--training-config-output",
                str(TRAINING_CONFIG),
                "--split-output",
                str(SPLIT_MANIFEST),
                "--lineage-output",
                str(EXPERIMENT_LINEAGE),
            )
            if dataset_version:
                arguments += ("--dataset-version", dataset_version)
            if args.lakehouse_manifest:
                arguments += ("--dataset-manifest", str(args.lakehouse_manifest))
            if args.expected_lineage:
                arguments += ("--expected-lineage", str(args.expected_lineage))
            if official_run:
                arguments += ("--official-run",)
        elif script.name == "evaluate.py":
            arguments = (
                "--model",
                str(MODEL_ARTIFACT),
                "--lineage",
                str(EXPERIMENT_LINEAGE),
                "--split-manifest",
                str(SPLIT_MANIFEST),
                "--environment-manifest",
                str(ENVIRONMENT_MANIFEST),
                "--output",
                str(EVALUATION_ARTIFACT),
            )
            if args.lakehouse_manifest:
                arguments += ("--dataset-manifest", str(args.lakehouse_manifest))
        elif script.name in {"verify_answers.py", "evaluate_role_ablation.py"}:
            if dataset_manifest_path:
                arguments = ("--dataset-manifest", str(dataset_manifest_path))
        run(title, script, *arguments)
    if args.report:
        report_arguments = (
            ("--dataset-manifest", str(dataset_manifest_path))
            if dataset_manifest_path
            else ()
        )
        run("Build report", ML_ROOT / "report.py", *report_arguments)

    print(
        "\nPipeline finished. "
        + (
            f"Official dataset: {dataset_version}."
            if official_run
            else "Non-official run; inspect experiment_lineage.json for the recorded identities."
        )
    )


if __name__ == "__main__":
    main()
