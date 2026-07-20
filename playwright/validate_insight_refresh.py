"""Validate refresh JSONL files before append and current-state merge."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path


TRACKED_METRICS = (
    "like_count",
    "view_count",
    "comment_count",
    "reply_count",
    "retweet_count",
    "bookmark_count",
    "score",
    "follower_count",
    "subscriber_count",
    "subreddit_member_count",
)


def deterministic_observation_id(source: str, identity: str, observed_at: str) -> str:
    value = f"{source}\x1f{identity}\x1f{observed_at}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _validate_availability(event: dict, path: Path, line_number: int) -> None:
    explicit: dict[str, bool] = {}
    for metric in TRACKED_METRICS:
        availability = f"{metric}_available"
        if availability not in event:
            continue
        flag = event[availability]
        if not isinstance(flag, bool):
            raise RuntimeError(f"Invalid availability flag in {path}:{line_number}: {availability}")
        if flag != (event.get(metric) is not None):
            raise RuntimeError(f"Metric availability mismatch in {path}:{line_number}: {metric}")
        explicit[metric] = flag

    raw_coverage = event.get("coverage_json")
    if raw_coverage is None:
        return
    try:
        coverage = json.loads(raw_coverage) if isinstance(raw_coverage, str) else raw_coverage
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid coverage JSON in {path}:{line_number}") from exc
    if not isinstance(coverage, dict):
        raise RuntimeError(f"Invalid coverage object in {path}:{line_number}")
    for metric, expected in explicit.items():
        if coverage.get(metric) is not expected:
            raise RuntimeError(f"Coverage mismatch in {path}:{line_number}: {metric}")


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
                raise RuntimeError(f"Missing observation identity in {path}:{line_number}")
            expected_observation_id = deterministic_observation_id(
                source, str(identity), str(observed_at)
            )
            supplied_observation_id = event.get("observation_id")
            if (
                supplied_observation_id is not None
                and supplied_observation_id != expected_observation_id
            ):
                raise RuntimeError(f"Invalid observation_id in {path}:{line_number}")
            envelope_fields = (
                "event_id",
                "payload_fingerprint",
                "producer_name",
                "producer_run_id",
                "collection_method",
                "provenance_json",
            )
            if any(event.get(field) is not None for field in envelope_fields):
                for identifier in ("event_id", "payload_fingerprint"):
                    value = str(event.get(identifier) or "")
                    if len(value) != 64 or any(
                        character not in "0123456789abcdef" for character in value.lower()
                    ):
                        raise RuntimeError(f"Invalid {identifier} in {path}:{line_number}")
                for field in envelope_fields[2:]:
                    if not event.get(field):
                        raise RuntimeError(f"Missing {field} in {path}:{line_number}")
            _validate_availability(event, path, line_number)
            key = (source, str(identity), str(observed_at))
            if key in observations:
                raise RuntimeError(f"Duplicate observation in {path}:{line_number}: {key}")
            observations.add(key)
            count += 1
    return count


def main() -> None:
    output_dir = Path(os.getenv("INSIGHT_REFRESH_OUTPUT_DIR", "/app/insight-refresh"))
    counts = {
        source: validate_file(output_dir / f"{source}.jsonl", source)
        for source in ("youtube", "x", "reddit")
    }
    print(json.dumps({"event": "insight_refresh_validated", "counts": counts}))


if __name__ == "__main__":
    main()
