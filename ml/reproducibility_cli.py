"""Validate and compare complete experiment identities and replay outputs."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Mapping

import joblib
import pandas as pd

ML_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ML_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.reproducibility import (
    file_sha256,
    fingerprint,
    load_json,
    manifest_sha256,
    validate_environment_manifest,
    validate_lineage_match,
    validate_split_manifest,
    write_json,
)
from experiment_config import validate_training_config

def _evaluation_identity(artifact: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in artifact.items()
        if key not in {"generated_at", "evaluation_fingerprint"}
    }

def _source_snapshots(manifest: Mapping[str, Any]) -> dict[str, int]:
    raw = manifest.get("iceberg_snapshots_json", manifest.get("source_snapshots", {}))
    snapshots = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(snapshots, Mapping) or not snapshots:
        raise ValueError("no pinned Silver snapshots")
    return {str(key): int(value) for key, value in sorted(snapshots.items())}

def _dataset_fingerprint(manifest: Mapping[str, Any]) -> str:
    raw_filters = manifest.get("filters_json", manifest.get("filters", {}))
    filters = json.loads(raw_filters) if isinstance(raw_filters, str) else raw_filters
    if not isinstance(filters, Mapping):
        raise ValueError("dataset filters are not an object")
    schema_version = str(manifest.get("schema_version") or "")
    if not schema_version:
        raise ValueError("dataset schema version is missing")
    return fingerprint(
        {
            "schema_version": schema_version,
            "source_snapshots": _source_snapshots(manifest),
            "filters": dict(sorted(filters.items())),
        }
    )

def paper_ready_identity(
    dataset_manifest: Mapping[str, Any],
    lineage: Mapping[str, Any],
    evaluation: Mapping[str, Any],
) -> dict[str, Any]:
    snapshots = _source_snapshots(dataset_manifest)
    identity = {
        "dataset_version": lineage.get("dataset_version"),
        "dataset_fingerprint": lineage.get("dataset_fingerprint"),
        "silver_post_features_snapshot": snapshots.get("lakehouse.silver.post_features"),
        "silver_engagement_snapshot": snapshots.get("lakehouse.silver.engagement_snapshots"),
        "gold_snapshot": lineage.get("gold_snapshot_id"),
        "manifest_sha256": lineage.get("manifest_sha256"),
        "git_commit": lineage.get("git_commit"),
        "environment_fingerprint": lineage.get("environment_fingerprint"),
        "training_config_fingerprint": lineage.get("training_config_fingerprint"),
        "experiment_id": lineage.get("experiment_id"),
        "model_sha256": lineage.get("model_sha256"),
        "evaluation_fingerprint": evaluation.get("evaluation_fingerprint"),
    }
    
    if "evaluation_protocol_fingerprint" in lineage:
        identity["evaluation_protocol_fingerprint"] = lineage.get("evaluation_protocol_fingerprint")
        identity["evaluation_folds_fingerprint"] = lineage.get("evaluation_folds_fingerprint")
        identity["oof_predictions_sha256"] = lineage.get("oof_predictions_sha256")
        identity["metrics_sha256"] = lineage.get("metrics_sha256")
    else:
        identity["split_fingerprint"] = lineage.get("split_fingerprint")
        
    return identity

def _write_markdown(path: Path, title: str, rows: list[tuple[str, str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"# {title}", "", "| Check | Status | Detail |", "|---|---:|---|"]
    for label, status, detail in rows:
        escaped_detail = detail.replace("|", "\\|")
        lines.append(f"| {label} | {status} | {escaped_detail} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def validate_artifacts(
    *,
    dataset_manifest: Mapping[str, Any],
    environment_manifest: Mapping[str, Any],
    training_config: Mapping[str, Any],
    split_manifest: Mapping[str, Any] | None,
    evaluation_protocol: Mapping[str, Any] | None,
    cv_folds_manifest: Mapping[str, Any] | None,
    oof_predictions_path: Path | None,
    metrics_path: Path,
    lineage: Mapping[str, Any],
    bundle: Mapping[str, Any],
    model_sha256: str,
    evaluation: Mapping[str, Any],
) -> dict[str, tuple[bool | None, str]]:
    results: dict[str, tuple[bool | None, str]] = {}

    def check(label: str, operation) -> None:
        try:
            operation()
            results[label] = (True, "")
        except Exception as exc:
            results[label] = (False, str(exc))

    check("Dataset fingerprint", lambda: (
        None if dataset_manifest.get("dataset_fingerprint") == _dataset_fingerprint(dataset_manifest)
        and lineage.get("dataset_fingerprint") == dataset_manifest.get("dataset_fingerprint")
        else (_ for _ in ()).throw(ValueError("dataset or manifest identity mismatch"))
    ))
    check("Manifest SHA-256", lambda: (
        None if dataset_manifest.get("manifest_sha256") == manifest_sha256(dataset_manifest)
        and lineage.get("manifest_sha256") == dataset_manifest.get("manifest_sha256")
        else (_ for _ in ()).throw(ValueError("manifest identity mismatch"))
    ))

    def validate_silver_snapshots() -> None:
        if lineage.get("silver_snapshot_ids") != _source_snapshots(dataset_manifest):
            raise ValueError("Silver snapshot IDs differ between dataset and lineage")
    check("Silver snapshots", validate_silver_snapshots)
    check("Gold snapshot", lambda: (
        None if int(lineage.get("gold_snapshot_id") or 0) == int(dataset_manifest.get("gold_snapshot_id") or -1) > 0
        else (_ for _ in ()).throw(ValueError("Gold snapshot mismatch"))
    ))

    def validate_git() -> None:
        code = environment_manifest.get("code", {})
        commit = str(code.get("git_commit") or "")
        if not re.fullmatch(r"[a-f0-9]{40}", commit) or lineage.get("git_commit") != commit:
            raise ValueError("Git revision mismatch")
        if lineage.get("official_run") and code.get("git_dirty") is not False:
            raise ValueError("official run was captured from a dirty working tree")
    check("Git revision", validate_git)

    def validate_environment() -> None:
        validate_environment_manifest(environment_manifest)
        if lineage.get("environment_fingerprint") != environment_manifest.get("environment_fingerprint"):
            raise ValueError("environment/lineage fingerprint mismatch")
    check("Environment fingerprint", validate_environment)

    def validate_config() -> None:
        validate_training_config(training_config)
        if lineage.get("training_config_fingerprint") != training_config.get("training_config_fingerprint"):
            raise ValueError("training config/lineage fingerprint mismatch")
    check("Training config", validate_config)

    def validate_feature_schema() -> None:
        configured = training_config.get("feature_schema", {}).get("model_columns")
        bundled = bundle.get("features")
        if not isinstance(configured, list) or not configured:
            raise ValueError("resolved feature schema is missing")
        if bundled != configured:
            raise ValueError("model bundle feature order differs from training configuration")
    check("Feature schema", validate_feature_schema)

    def validate_model() -> None:
        if lineage.get("model_sha256") != model_sha256:
            raise ValueError("model SHA-256 mismatch")
    check("Model identity", validate_model)

    is_grouped_cv = bool(evaluation_protocol and cv_folds_manifest and oof_predictions_path)
    
    if not is_grouped_cv:
        def validate_split() -> None:
            validate_split_manifest(split_manifest)
            if lineage.get("split_fingerprint") != split_manifest.get("split_fingerprint"):
                raise ValueError("split/lineage fingerprint mismatch")
        check("Split fingerprint", validate_split)
        
        def validate_evaluation() -> None:
            if evaluation.get("evaluation_fingerprint") != fingerprint(_evaluation_identity(evaluation)):
                raise ValueError("evaluation fingerprint mismatch")
        check("Evaluation metrics", validate_evaluation)
        
    else:
        def validate_grouped_cv_identities() -> None:
            ep_fp = fingerprint({k: v for k, v in evaluation_protocol.items() if k != "evaluation_protocol_fingerprint"})
            if evaluation_protocol.get("evaluation_protocol_fingerprint") != ep_fp:
                raise ValueError("evaluation_protocol_fingerprint tampered")
            if lineage.get("evaluation_protocol_fingerprint") != ep_fp:
                raise ValueError("lineage evaluation_protocol_fingerprint mismatch")
                
            folds_fp = fingerprint({k: v for k, v in cv_folds_manifest.items() if k != "evaluation_folds_fingerprint"})
            if cv_folds_manifest.get("evaluation_folds_fingerprint") != folds_fp:
                raise ValueError("evaluation_folds_fingerprint tampered")
            if lineage.get("evaluation_folds_fingerprint") != folds_fp:
                raise ValueError("lineage evaluation_folds_fingerprint mismatch")
                
            eval_fp = fingerprint(_evaluation_identity(evaluation))
            if evaluation.get("evaluation_fingerprint") != eval_fp:
                raise ValueError("evaluation fingerprint tampered")
            if lineage.get("evaluation_fingerprint") != eval_fp:
                raise ValueError("lineage evaluation_fingerprint mismatch")
                
            if lineage.get("oof_predictions_sha256") != file_sha256(oof_predictions_path):
                raise ValueError("OOF parquet SHA-256 mismatch")
            if lineage.get("metrics_sha256") != file_sha256(metrics_path):
                raise ValueError("Metrics JSON SHA-256 mismatch")
                
        check("Grouped CV Fingerprints", validate_grouped_cv_identities)
        
        def validate_oof_invariants() -> None:
            df = pd.read_parquet(oof_predictions_path)
            if df["example_id"].duplicated().any():
                raise ValueError("OOF predictions contain duplicate example_id")
            if df.isnull().any().any():
                raise ValueError("OOF predictions contain missing values")
            if not ((df["raw_probability"] >= 0) & (df["raw_probability"] <= 1)).all():
                raise ValueError("OOF raw_probability out of bounds")
            if not ((df["calibrated_probability"] >= 0) & (df["calibrated_probability"] <= 1)).all():
                raise ValueError("OOF calibrated_probability out of bounds")
            
            expected_labels = df["calibrated_probability"] >= df["classification_threshold"]
            if not (df["predicted_label"] == expected_labels).all():
                raise ValueError("predicted_label does not match calibrated_probability >= threshold")
                
            for col in ["dataset_version", "virality_contract_fingerprint", "evaluation_protocol_fingerprint", "evaluation_folds_fingerprint"]:
                if len(df[col].unique()) != 1:
                    raise ValueError(f"OOF column {col} contains multiple values")
            
            if df["evaluation_protocol_fingerprint"].iloc[0] != evaluation_protocol.get("evaluation_protocol_fingerprint"):
                raise ValueError("OOF evaluation_protocol_fingerprint mismatch")
            if df["evaluation_folds_fingerprint"].iloc[0] != cv_folds_manifest.get("evaluation_folds_fingerprint"):
                raise ValueError("OOF evaluation_folds_fingerprint mismatch")
                
            # Verify exact correspondence between cv_folds_manifest sizes and OOF fold sizes
            oof_fold_counts = df["outer_fold"].value_counts().to_dict()
            manifest_folds = cv_folds_manifest.get("folds", {})
            for fold_id, fold_info in manifest_folds.items():
                if oof_fold_counts.get(int(fold_id), 0) != fold_info.get("test_size"):
                    raise ValueError(f"OOF test size for fold {fold_id} differs from cv_folds_manifest")
                    
        check("OOF Invariants", validate_oof_invariants)

    return results

def _compare_numbers(reference: Any, candidate: Any, tolerance: float, path: str) -> list[str]:
    failures: list[str] = []
    if isinstance(reference, Mapping) and isinstance(candidate, Mapping):
        if set(reference) != set(candidate):
            return [f"{path} keys differ"]
        for key in sorted(reference):
            failures.extend(_compare_numbers(reference[key], candidate[key], tolerance, f"{path}.{key}"))
    elif isinstance(reference, list) and isinstance(candidate, list):
        if len(reference) != len(candidate):
            return [f"{path} lengths differ"]
        for index, (left, right) in enumerate(zip(reference, candidate, strict=True)):
            failures.extend(_compare_numbers(left, right, tolerance, f"{path}[{index}]"))
    elif isinstance(reference, (int, float)) and isinstance(candidate, (int, float)):
        if not math.isclose(float(reference), float(candidate), rel_tol=0.0, abs_tol=tolerance):
            failures.append(f"{path}: {reference!r} != {candidate!r}")
    elif reference != candidate:
        failures.append(f"{path}: {reference!r} != {candidate!r}")
    return failures

def compare_replay(
    reference_lineage: Mapping[str, Any],
    candidate_lineage: Mapping[str, Any],
    reference_evaluation: Mapping[str, Any],
    candidate_evaluation: Mapping[str, Any],
    reference_oof_path: Path | None = None,
    candidate_oof_path: Path | None = None,
) -> list[str]:
    failures: list[str] = []
    for field in (
        "silver_snapshot_ids",
        "gold_snapshot_id",
        "dataset_fingerprint",
        "git_commit",
        "environment_fingerprint",
        "training_config_fingerprint",
        "split_fingerprint",
        "evaluation_protocol_fingerprint",
        "evaluation_folds_fingerprint",
    ):
        if reference_lineage.get(field) != candidate_lineage.get(field):
            failures.append(f"lineage.{field} differs")
            
    contract = reference_lineage.get("determinism_contract", {})
    prediction_tolerance = float(contract.get("prediction_absolute_tolerance", 0.0))
    metric_tolerance = float(contract.get("metric_absolute_tolerance", 0.0))
    
    if reference_oof_path and candidate_oof_path and reference_oof_path.exists() and candidate_oof_path.exists():
        ref_df = pd.read_parquet(reference_oof_path).sort_values("example_id").reset_index(drop=True)
        cand_df = pd.read_parquet(candidate_oof_path).sort_values("example_id").reset_index(drop=True)
        if len(ref_df) != len(cand_df) or not (ref_df["example_id"] == cand_df["example_id"]).all():
            failures.append("OOF example_ids do not match")
        else:
            for col in ["raw_probability", "calibrated_probability", "classification_threshold"]:
                diffs = (ref_df[col] - cand_df[col]).abs()
                if diffs.max() > prediction_tolerance:
                    failures.append(f"OOF {col} exceeds tolerance {prediction_tolerance}")
            for col in ["predicted_label", "viral"]:
                if not (ref_df[col] == cand_df[col]).all():
                    failures.append(f"OOF {col} differs")
    else:
        failures.extend(_compare_numbers(
            reference_evaluation.get("predictions", []),
            candidate_evaluation.get("predictions", []),
            prediction_tolerance,
            "predictions",
        ))
        
    failures.extend(_compare_numbers(
        reference_evaluation.get("metrics", {}),
        candidate_evaluation.get("metrics", {}),
        metric_tolerance,
        "metrics",
    ))
    return failures

def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    verify = subparsers.add_parser("verify")
    verify.add_argument("--dataset-manifest", type=Path, required=True)
    verify.add_argument("--environment-manifest", type=Path, default=ML_ROOT / "results/environment_manifest.json")
    verify.add_argument("--training-config", type=Path, default=ML_ROOT / "results/training_config.json")
    verify.add_argument("--split-manifest", type=Path)
    verify.add_argument("--evaluation-protocol", type=Path)
    verify.add_argument("--cv-folds-manifest", type=Path)
    verify.add_argument("--oof-predictions", type=Path)
    verify.add_argument("--lineage", type=Path, default=ML_ROOT / "results/experiment_lineage.json")
    verify.add_argument("--model", type=Path, default=ML_ROOT / "models/stage1_multisource.joblib")
    verify.add_argument("--evaluation", type=Path, default=ML_ROOT / "results/evaluation.json")
    verify.add_argument("--reference-lineage", type=Path)
    verify.add_argument("--reference-evaluation", type=Path)
    verify.add_argument("--reference-oof", type=Path)
    verify.add_argument("--report-json", type=Path)
    verify.add_argument("--report-markdown", type=Path)
    
    report = subparsers.add_parser("report")
    report.add_argument("--dataset-manifest", type=Path, required=True)
    report.add_argument("--lineage", type=Path, default=ML_ROOT / "results/experiment_lineage.json")
    report.add_argument("--evaluation", type=Path, default=ML_ROOT / "results/evaluation.json")
    report.add_argument("--output", type=Path)
    
    args = parser.parse_args()

    if args.command == "report":
        identity = paper_ready_identity(
            load_json(args.dataset_manifest), load_json(args.lineage), load_json(args.evaluation)
        )
        if args.output:
            write_json(args.output, identity)
        print(json.dumps(identity, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    lineage = load_json(args.lineage)
    evaluation = load_json(args.evaluation)
    
    # Auto-detect grouped CV vs Legacy
    is_grouped_cv = "evaluation_protocol_fingerprint" in lineage
    
    results = validate_artifacts(
        dataset_manifest=load_json(args.dataset_manifest),
        environment_manifest=load_json(args.environment_manifest),
        training_config=load_json(args.training_config),
        split_manifest=load_json(args.split_manifest) if not is_grouped_cv and args.split_manifest and args.split_manifest.exists() else None,
        evaluation_protocol=load_json(args.evaluation_protocol) if is_grouped_cv and args.evaluation_protocol and args.evaluation_protocol.exists() else None,
        cv_folds_manifest=load_json(args.cv_folds_manifest) if is_grouped_cv and args.cv_folds_manifest and args.cv_folds_manifest.exists() else None,
        oof_predictions_path=args.oof_predictions if is_grouped_cv else None,
        metrics_path=args.evaluation,
        lineage=lineage,
        bundle=joblib.load(args.model),
        model_sha256=file_sha256(args.model),
        evaluation=evaluation,
    )
    
    if args.reference_lineage:
        replay_failures = compare_replay(
            load_json(args.reference_lineage),
            lineage,
            load_json(args.reference_evaluation),
            evaluation,
            args.reference_oof,
            args.oof_predictions if is_grouped_cv else None,
        )
        results["Replay outputs"] = (not replay_failures, "; ".join(replay_failures[:5]))
    else:
        results["Replay outputs"] = (None, "reference artifacts were not supplied")

    width = max(len(label) for label in results) + 2
    for label, (passed, detail) in results.items():
        status = "PASS" if passed is True else "FAIL" if passed is False else "NOT RUN"
        print(f"{label} {'.' * max(1, width - len(label))} {status}")
        if detail:
            print(f"  {detail}")
    passed = all(result[0] is not False for result in results.values())
    rows = [(label, "PASS" if result is True else "FAIL" if result is False else "NOT RUN", detail) for label, (result, detail) in results.items()]
    if args.report_json:
        write_json(
            args.report_json,
            {
                "schema_version": "reproducibility-check-v2",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "overall_status": "PASS" if passed else "FAIL",
                "checks": [{"name": label, "status": status, "detail": detail} for label, status, detail in rows],
                "identities": paper_ready_identity(load_json(args.dataset_manifest), lineage, evaluation),
            },
        )
    if args.report_markdown:
        _write_markdown(args.report_markdown, "Reproducibility verification", rows)
    print(f"\nREPRODUCIBILITY CHECK: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1

if __name__ == "__main__":
    raise SystemExit(main())
