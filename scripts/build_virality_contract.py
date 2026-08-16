"""Generate a frozen historical virality contract from a pinned scored reference."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = ROOT / "spark" / "jobs"
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.virality_contract import PLATFORM_REFERENCE_POLICY, build_contract


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a platform-reference virality contract from pinned scored rows"
    )
    parser.add_argument("--reference-data", type=Path, required=True)
    parser.add_argument("--reference-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--quantile", type=float, default=0.75)
    parser.add_argument("--horizon-hours", type=int, required=True)
    parser.add_argument("--tolerance-hours", type=int, required=True)
    parser.add_argument("--min-reference-examples-per-platform", type=int, required=True)
    return parser


def _read_reference(path: Path) -> pd.DataFrame:
    frame = (
        pd.read_parquet(path)
        if path.is_dir() or path.suffix.lower() == ".parquet"
        else pd.read_csv(path)
    )
    required = {"source", "engagement_score"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("Reference data is missing columns: " + ", ".join(missing))
    return frame


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    manifest = json.loads(args.reference_manifest.read_text(encoding="utf-8"))
    snapshots = manifest.get("source_snapshots") or manifest.get("iceberg_snapshots")
    if snapshots is None and manifest.get("iceberg_snapshots_json"):
        snapshots = json.loads(manifest["iceberg_snapshots_json"])
    dataset_fingerprint = str(manifest.get("dataset_fingerprint") or "")
    dataset_version = str(manifest.get("dataset_version") or "")
    if not isinstance(snapshots, dict) or not snapshots:
        raise ValueError("Reference manifest must identify exact source snapshot IDs")
    if len(dataset_fingerprint) != 64 or not dataset_version:
        raise ValueError("Reference manifest must identify dataset version and SHA-256 fingerprint")
    frame = _read_reference(args.reference_data)
    scores = {
        str(platform): group["engagement_score"].tolist()
        for platform, group in frame.groupby("source", sort=True)
    }
    contract = build_contract(
        scores,
        policy=PLATFORM_REFERENCE_POLICY,
        quantile=args.quantile,
        reference={
            "dataset_version": dataset_version,
            "dataset_fingerprint": dataset_fingerprint,
            "source_snapshots": snapshots,
        },
        horizon_hours=args.horizon_hours,
        tolerance_hours=args.tolerance_hours,
        eligibility_filters=(
            manifest.get("filters") or json.loads(manifest.get("filters_json") or "{}")
        ),
        min_reference_examples_per_platform=args.min_reference_examples_per_platform,
    )
    contract.write(args.output)
    print("Virality reference contract")
    print("---------------------------")
    print(f"Policy: {contract.policy}")
    print(f"Quantile: {contract.payload['quantile']} ({contract.payload['quantile_method']})")
    for platform, threshold in sorted(contract.thresholds.items()):
        print(
            f"{platform}: reference_rows={threshold['reference_count']} "
            f"virality_engagement_threshold={threshold['value']:.12g}"
        )
    print(f"Contract fingerprint: {contract.fingerprint}")
    print(f"Saved: {args.output}")
    print("Status: VALID")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
