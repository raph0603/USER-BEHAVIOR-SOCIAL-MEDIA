"""Deterministic workload construction for validation and generated load."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from common.event_envelope import enrich_event_envelope

from .core import canonical_json, workload_fingerprint


BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def generated_events(size: int, platform: str) -> list[dict[str, Any]]:
    if size <= 0:
        raise ValueError("size must be positive")
    events = []
    for index in range(size):
        observed = (BASE_TIME + timedelta(seconds=index)).isoformat()
        source_id = f"benchmark-{platform}-{index:09d}"
        event = {
            "source": platform,
            "platform_event_id": source_id,
            "user_id": f"benchmark-user-{index % 1000:04d}",
            "url": f"https://benchmark.invalid/{platform}/{source_id}",
            "timestamp": observed,
            "observed_at": observed,
            "collected_at": observed,
            "event_type": f"{platform}.benchmark.content",
            "event_version": "v1",
            "title": f"Benchmark content {index}",
            "raw_text": f"Deterministic benchmark payload number {index}",
            "view_count": index % 10_000,
            "like_count": index % 1_000,
        }
        events.append(
            enrich_event_envelope(
                event,
                producer_name="pipeline-benchmark",
                producer_run_id="deterministic-workload-v1",
                collection_method="generated_load",
            )
        )
    return events


def invalid_events(platform: str) -> list[dict[str, Any]]:
    valid = generated_events(1, platform)[0]
    cases: list[dict[str, Any]] = []
    for name, updates in (
        ("missing_user_id", {"user_id": None}),
        ("missing_url", {"url": None}),
        ("missing_timestamp", {"timestamp": None}),
        ("collector_error", {"error": "controlled benchmark collector error"}),
        ("empty_after_clean", {"title": "   ", "raw_text": "   "}),
    ):
        case = dict(valid)
        case.update(updates)
        case["benchmark_invalid_case"] = name
        case["platform_event_id"] = f"benchmark-{platform}-invalid-{name}"
        case["url"] = updates.get("url", f"https://benchmark.invalid/{platform}/invalid-{name}")
        case.pop("event_id", None)
        case.pop("observation_id", None)
        case.pop("payload_fingerprint", None)
        cases.append(
            enrich_event_envelope(
                case,
                producer_name="pipeline-benchmark",
                producer_run_id="deterministic-dlq-v1",
                collection_method="generated_load",
            )
        )
    return cases


def workload_identity(events: list[dict[str, Any]], platform: str) -> dict[str, Any]:
    encoded_bytes = sum(len(canonical_json(event).encode("utf-8")) for event in events)
    unique_contents = len(
        {
            (str(event.get("source") or ""), str(event.get("platform_event_id") or ""))
            for event in events
        }
    )
    return {
        "type": "generated_load",
        "source": platform,
        "input_events": len(events),
        "input_bytes": encoded_bytes,
        "input_fingerprint": workload_fingerprint(events),
        "source_snapshot": None,
        "generation_config": {
            "generator": "generated-events-v1",
            "base_time": BASE_TIME.isoformat(),
            "unique_social_contents": unique_contents,
            "artificially_replicated": unique_contents < len(events),
        },
        "platform_distribution": {platform: len(events)},
    }
