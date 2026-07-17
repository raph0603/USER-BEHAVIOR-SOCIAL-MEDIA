"""Validate refresh JSONL files before append and current-state merge."""

from __future__ import annotations

import json
import os
from pathlib import Path


def validate_file(path: Path, source: str) -> int:
    if not path.is_file():
        raise RuntimeError(f"Missing insight refresh output: {path}")
    observations: set[tuple[str, str, str]] = set()
    count = 0
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSON in {path}:{line_number}") from exc
            if event.get("source") != source:
                raise RuntimeError(
                    f"Unexpected source in {path}:{line_number}: {event.get('source')}"
                )
            identity = event.get("platform_event_id") or event.get("url")
            observed_at = event.get("metadata_refreshed_at")
            if not identity or not observed_at:
                raise RuntimeError(
                    f"Missing observation identity in {path}:{line_number}"
                )
            key = (source, str(identity), str(observed_at))
            if key in observations:
                raise RuntimeError(f"Duplicate observation in {path}:{line_number}: {key}")
            observations.add(key)
            count += 1
    return count


def main() -> None:
    output_dir = Path(
        os.getenv("INSIGHT_REFRESH_OUTPUT_DIR", "/app/insight-refresh")
    )
    counts = {
        source: validate_file(output_dir / f"{source}.jsonl", source)
        for source in ("youtube", "x", "reddit")
    }
    print(json.dumps({"event": "insight_refresh_validated", "counts": counts}))


if __name__ == "__main__":
    main()
