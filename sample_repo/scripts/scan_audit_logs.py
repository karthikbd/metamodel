"""
Scan structured audit JSONL logs and print execution summaries.

Usage:
  python -m scripts.scan_audit_logs
  python -m scripts.scan_audit_logs --failed-only
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def _audit_file() -> Path:
    return Path(__file__).resolve().parents[1] / "logs" / "audit_events.jsonl"


def _load_events() -> list[dict]:
    fp = _audit_file()
    if not fp.exists():
        return []
    events: list[dict] = []
    with fp.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize audit_events.jsonl")
    parser.add_argument("--failed-only", action="store_true", help="Show only FAILED events")
    return parser.parse_args()


def _filter_events(events: list[dict], failed_only: bool) -> list[dict]:
    if not failed_only:
        return events
    return [e for e in events if e.get("status") == "FAILED"]


def _print_summary(events: list[dict]) -> None:
    by_type = Counter(e.get("event_type", "unknown") for e in events)
    by_status = Counter(e.get("status", "unknown") for e in events)

    print("=== Audit Event Summary ===")
    print(f"total_events: {len(events)}")
    print("event_types:")
    for k, v in sorted(by_type.items()):
        print(f"  - {k}: {v}")
    print("statuses:")
    for k, v in sorted(by_status.items()):
        print(f"  - {k}: {v}")


def _print_recent(events: list[dict], limit: int = 25) -> None:
    print("\n=== Recent Job/Function Events (last 25) ===")
    for e in events[-limit:]:
        print(
            f"{e.get('ts_utc')} | {e.get('event_type')} | {e.get('status')} | "
            f"job={e.get('job')} | fn={e.get('module')}.{e.get('function')}"
        )


def main() -> None:
    args = _parse_args()

    events = _load_events()
    events = _filter_events(events, failed_only=args.failed_only)

    if not events:
        print("No audit events found.")
        return

    _print_summary(events)
    _print_recent(events)


if __name__ == "__main__":
    main()
