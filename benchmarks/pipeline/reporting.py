"""Machine-readable and paper-convenience benchmark outputs."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True, default=str) + "\n" for row in rows),
        encoding="utf-8",
    )


def write_summary_csv(path: Path, measurements: Sequence[Mapping[str, Any]]) -> None:
    fields = (
        "benchmark_id",
        "workload_type",
        "input_events",
        "run_index",
        "warmup",
        "status",
        "duration_seconds",
        "throughput_events_per_second",
        "bronze_rows",
        "silver_rows",
        "gold_rows",
        "dlq_rows",
        "duplicate_logical_rows",
        "missing_application_proofs",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for item in measurements:
            writer.writerow(
                {
                    "benchmark_id": item["run"]["benchmark_id"],
                    "workload_type": item["workload"]["type"],
                    "input_events": item["workload"]["input_events"],
                    "run_index": item["run"]["repeat"],
                    "warmup": item["warmup"],
                    "status": item["status"],
                    "duration_seconds": item["timings"]["end_to_end_seconds"],
                    "throughput_events_per_second": item["throughput"].get(
                        "end_to_end_events_per_second"
                    ),
                    "bronze_rows": item["counts"]["bronze_logical_rows"],
                    "silver_rows": item["counts"]["silver_rows"],
                    "gold_rows": item["counts"]["gold_rows"],
                    "dlq_rows": item["counts"]["dlq_events"],
                    "duplicate_logical_rows": item["reliability"]["duplicate_logical_rows_created"],
                    "missing_application_proofs": item["reliability"]["missing_application_proofs"],
                }
            )


def write_paper_table(path: Path, summary: Sequence[Mapping[str, Any]]) -> None:
    lines = [
        "| Events | E2E time (s) | Events/s | Bronze rows | Silver rows | DLQ | Storage (MiB) | Replay duplicates |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summary:
        storage = item.get("storage_bytes", {}).get("median")
        storage_mib = "" if storage is None else f"{float(storage) / (1024 * 1024):.3f}"
        lines.append(
            "| {input_events} | {duration:.6f} | {throughput:.3f} | {bronze} | {silver} | {dlq} | {storage} | {duplicates} |".format(
                input_events=item["input_events"],
                duration=item["duration_seconds"]["median"],
                throughput=item["throughput_events_per_second"]["median"],
                bronze=item["bronze_logical_rows"],
                silver=item["silver_rows"],
                dlq=item["dlq_events"],
                storage=storage_mib,
                duplicates=item["duplicate_logical_rows_created"],
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_svg_figures(output_dir: Path, summary: Sequence[Mapping[str, Any]]) -> list[Path]:
    figures = []
    for filename, key, title, ylabel in (
        (
            "input_vs_duration.svg",
            ("duration_seconds", "median"),
            "Input vs processing time",
            "Seconds",
        ),
        (
            "input_vs_throughput.svg",
            ("throughput_events_per_second", "median"),
            "Input vs throughput",
            "Events per second",
        ),
    ):
        points = [(float(item["input_events"]), float(item[key[0]][key[1]])) for item in summary]
        if not points:
            continue
        path = output_dir / filename
        _write_svg(path, points, title, ylabel)
        figures.append(path)
    return figures


def write_storage_figure(output_dir: Path, summary: Sequence[Mapping[str, Any]]) -> Path | None:
    points = [
        (float(item["input_events"]), float(item["storage_bytes"]["median"]))
        for item in summary
        if item.get("storage_bytes", {}).get("median") is not None
    ]
    if not points:
        return None
    path = output_dir / "input_vs_storage.svg"
    _write_svg(path, points, "Input vs physical Iceberg storage", "Bytes")
    return path


def write_reliability_table(path: Path, reliability: Mapping[str, Any]) -> None:
    rows = (
        (
            "Replay idempotence",
            reliability.get("idempotence"),
            reliability.get("idempotence_anomalies", 0),
        ),
        (
            "Bronze/Silver reconciliation",
            reliability.get("reconciliation"),
            reliability.get("reconciliation_anomalies", 0),
        ),
        ("DLQ routing", reliability.get("dlq"), reliability.get("dlq_anomalies", 0)),
        (
            "Controlled anomaly detection",
            reliability.get("controlled_anomaly_detection"),
            reliability.get("controlled_anomaly_count", 0),
        ),
    )
    lines = ["| Test | Result | Observed anomalies |", "|---|---|---:|"]
    lines.extend(f"| {name} | {result} | {anomalies} |" for name, result, anomalies in rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_svg(path: Path, points: Sequence[tuple[float, float]], title: str, ylabel: str) -> None:
    width, height, margin = 800, 480, 70
    max_x = max(x for x, _ in points) or 1
    max_y = max(y for _, y in points) or 1
    coords = [
        (
            margin + (x / max_x) * (width - 2 * margin),
            height - margin - (y / max_y) * (height - 2 * margin),
        )
        for x, y in points
    ]
    polyline = " ".join(f"{x:.2f},{y:.2f}" for x, y in coords)
    circles = "".join(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4"/>' for x, y in coords)
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white"/>
<text x="{width / 2}" y="28" text-anchor="middle" font-family="sans-serif" font-size="20">{title}</text>
<line x1="{margin}" y1="{height - margin}" x2="{width - margin}" y2="{height - margin}" stroke="black"/>
<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height - margin}" stroke="black"/>
<text x="{width / 2}" y="{height - 18}" text-anchor="middle" font-family="sans-serif">Input events</text>
<text x="18" y="{height / 2}" text-anchor="middle" transform="rotate(-90 18 {height / 2})" font-family="sans-serif">{ylabel}</text>
<polyline points="{polyline}" fill="none" stroke="#2563eb" stroke-width="2"/>
<g fill="#2563eb">{circles}</g>
</svg>\n'''
    path.write_text(svg, encoding="utf-8")
