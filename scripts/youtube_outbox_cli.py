"""Inspect and quarantine oversized rows in the durable YouTube outbox."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve()
ROOT = SCRIPT_PATH.parents[1] if SCRIPT_PATH.parent.name == "scripts" else SCRIPT_PATH.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "playwright"))

from common.youtube_outbox import MESSAGE_SIZE_TOO_LARGE
from common.youtube_state import YouTubeStateStore
from youtube_pipeline_events import youtube_kafka_max_event_bytes


UTC = timezone.utc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-db",
        default="/app/state/youtube-pipeline.sqlite",
        help="Path to the shared YouTube SQLite state database.",
    )
    parser.add_argument(
        "--max-event-bytes",
        type=int,
        default=None,
        help="Override YOUTUBE_KAFKA_MAX_EVENT_BYTES for this inspection.",
    )
    parser.add_argument(
        "--include-terminal",
        action="store_true",
        help="Include rows that are already quarantined.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list", help="List oversized retained events without modifying them.")
    quarantine = subparsers.add_parser(
        "quarantine",
        help="Mark oversized retained events as terminal without deleting their payload.",
    )
    quarantine.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the rows that would be quarantined without modifying SQLite.",
    )
    return parser


def _public_row(row: dict) -> dict:
    return {
        "outbox_id": row["outbox_id"],
        "topic": row["topic"],
        "event_type": row.get("event_type"),
        "event_id": row.get("event_id"),
        "video_id": row.get("video_id"),
        "payload_size_bytes": row["payload_size_bytes"],
        "delivery_attempts": int(row.get("delivery_attempts") or 0),
        "status": row.get("status") or "pending",
    }


def run(args: argparse.Namespace) -> int:
    max_event_bytes = youtube_kafka_max_event_bytes(args.max_event_bytes)
    changed = 0
    with YouTubeStateStore(args.state_db) as state:
        rows = state.inspect_outbox(
            max_event_bytes=max_event_bytes,
            include_terminal=args.include_terminal,
        )
        for row in rows:
            print(json.dumps(_public_row(row), ensure_ascii=False, sort_keys=True))
            if args.command == "quarantine" and not args.dry_run:
                state.quarantine_outbox(
                    row["outbox_id"],
                    failed_at=datetime.now(UTC),
                    reason=MESSAGE_SIZE_TOO_LARGE,
                    error=(
                        f"UTF-8 outbox payload is {row['payload_size_bytes']} bytes; "
                        f"limit is {max_event_bytes} bytes"
                    ),
                    payload_size_bytes=row["payload_size_bytes"],
                )
                changed += 1
    print(
        json.dumps(
            {
                "command": args.command,
                "dry_run": bool(getattr(args, "dry_run", False)),
                "max_event_bytes": max_event_bytes,
                "matched": len(rows),
                "quarantined": changed,
            },
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
