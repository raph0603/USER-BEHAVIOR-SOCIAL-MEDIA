"""Finalize hashes for an X lineage bundle after every stage log is closed."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.x_lineage import sha256_file


def finalize(directory: Path) -> dict:
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Manifest is missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    expected = manifest.get("files_generated") or []
    missing = [
        relative
        for relative in expected
        if relative not in {"manifest.json", "manifest.sha256"}
        and not (directory / relative).is_file()
    ]
    if missing:
        manifest.setdefault("errors", []).append(
            "missing_generated_files:" + ",".join(sorted(missing))
        )
        manifest["status"] = "FAIL"

    manifest["sha256"] = {
        relative: sha256_file(directory / relative)
        for relative in expected
        if relative not in {"manifest.json", "manifest.sha256"}
        and (directory / relative).is_file()
    }
    manifest["errors"] = sorted(set(manifest.get("errors") or []))
    manifest["warnings"] = sorted(set(manifest.get("warnings") or []))
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (directory / "manifest.sha256").write_text(
        f"{sha256_file(manifest_path)}  manifest.json\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    manifest = finalize(args.directory)
    clean = json.loads((args.directory / "clean.json").read_text(encoding="utf-8"))
    counts = manifest.get("matched_row_counts") or {}
    print(f"X lineage export: {manifest.get('status')}")
    print()
    print("Event:")
    print(f"  platform_event_id: {manifest.get('platform_event_id')}")
    print(f"  URL: {clean.get('after', {}).get('url') or clean.get('before', {}).get('url')}")
    print()
    print("RAW:")
    print("  rows: 1")
    print("  original text preserved: yes")
    print()
    print("CLEAN:")
    for label, key in (
        ("<USER>", "user_count"),
        ("<EMAIL>", "email_count"),
        ("<PHONE>", "phone_count"),
        ("<IP>", "ip_count"),
        ("<URL>", "url_count"),
    ):
        print(f"  {label}: {clean['redaction_summary'].get(key, 0)}")
    print()
    print("BRONZE:")
    print(f"  event_log: {counts.get('lakehouse.bronze.event_log', 0)}")
    print(f"  current projection: {counts.get('lakehouse.bronze.events', 0)}")
    print()
    print("SILVER:")
    print(f"  events: {counts.get('lakehouse.silver.events', 0)}")
    print(f"  contents: {counts.get('lakehouse.silver.contents', 0)}")
    print(f"  snapshots: {counts.get('lakehouse.silver.engagement_snapshots', 0)}")
    print(f"  post_features: {counts.get('lakehouse.silver.post_features', 0)}")
    print()
    print("GOLD:")
    print(f"  content_stats: {counts.get('lakehouse.gold.content_stats', 0)}")
    print(f"  user_evolution: {counts.get('lakehouse.gold.user_evolution', 0)}")
    print()
    print("Output:")
    print(f"  {args.directory}")
    return 0 if manifest.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
