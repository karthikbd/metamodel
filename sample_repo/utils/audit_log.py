"""
Structured audit logging helpers for job/function execution tracing.

Writes JSONL audit events to sample_repo/logs/audit_events.jsonl so downstream
scanners can reconstruct cross-module execution order and outcomes.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


def _repo_root() -> Path:
    # sample_repo/utils/audit_log.py -> sample_repo/
    return Path(__file__).resolve().parents[1]


def _audit_file_path() -> Path:
    logs_dir = _repo_root() / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir / "audit_events.jsonl"


def emit_audit_event(
    event_type: str,
    *,
    module: str,
    function: str | None = None,
    job: str | None = None,
    status: str | None = None,
    duration_ms: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Append a single structured audit event as JSONL.

    The function is intentionally tolerant of filesystem/logging failures so it
    never breaks business execution paths.
    """
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "module": module,
        "function": function,
        "job": job,
        "status": status,
        "duration_ms": duration_ms,
        "metadata": metadata or {},
    }

    try:
        fp = _audit_file_path()
        with fp.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning("Failed to emit audit event (%s): %s", event_type, exc)
