"""
Run Tracker — LangSmith-style per-agent execution log.
Stores runs and their step events in an in-process store during the session.
Persisted to Neo4j ScanRun nodes so history survives restarts.
"""
import time
import uuid
from dataclasses import dataclass, field
from typing import Literal

from graph.neo4j_client import run_write, run_query

Status = Literal["queued", "running", "success", "error", "partial"]


@dataclass
class StepEvent:
    id:      str
    agent:   str
    level:   str
    message: str
    data:    dict
    ts:      float


@dataclass
class AgentRun:
    id:          str
    agent_name:  str
    status:      Status = "queued"
    started_at:  float | None = None
    finished_at: float | None = None
    events:      list[StepEvent] = field(default_factory=list)

    @property
    def duration_ms(self) -> int | None:
        if self.started_at and self.finished_at:
            return int((self.finished_at - self.started_at) * 1000)
        return None

    def to_dict(self) -> dict:
        return {
            "id":          self.id,
            "agent_name":  self.agent_name,
            "status":      self.status,
            "started_at":  self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "event_count": len(self.events),
        }


@dataclass
class PipelineRun:
    id:         str
    phase:      str           # "phase1" | "phase2"
    repo_root:  str
    status:     Status = "queued"
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    agent_runs: list[AgentRun] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id":          self.id,
            "phase":       self.phase,
            "repo_root":   self.repo_root,
            "status":      self.status,
            "started_at":  self.started_at,
            "finished_at": self.finished_at,
            "agent_runs":  [a.to_dict() for a in self.agent_runs],
        }


# ---------------------------------------------------------------------------
# In-memory store (keyed by run_id)
# ---------------------------------------------------------------------------
_runs: dict[str, PipelineRun] = {}


def create_run(phase: str, repo_root: str) -> PipelineRun:
    run = PipelineRun(id=str(uuid.uuid4()), phase=phase, repo_root=repo_root)
    _runs[run.id] = run
    return run


def get_run(run_id: str) -> PipelineRun | None:
    return _runs.get(run_id)


def list_runs() -> list[dict]:
    return [r.to_dict() for r in sorted(_runs.values(), key=lambda r: -r.started_at)]


def add_agent_run(run_id: str, agent_name: str) -> AgentRun:
    ar = AgentRun(id=str(uuid.uuid4()), agent_name=agent_name)
    if run := _runs.get(run_id):
        run.agent_runs.append(ar)
    return ar


def record_event(agent_run: AgentRun, event_dict: dict):
    se = StepEvent(
        id=event_dict["id"],
        agent=event_dict["agent"],
        level=event_dict["level"],
        message=event_dict["message"],
        data=event_dict.get("data", {}),
        ts=event_dict["ts"],
    )
    agent_run.events.append(se)
    if agent_run.status == "queued":
        agent_run.status = "running"
        agent_run.started_at = se.ts


def finish_agent_run(agent_run: AgentRun, success: bool):
    agent_run.status = "success" if success else "error"
    agent_run.finished_at = time.time()


def finish_run(run: PipelineRun, success: bool):
    run.status = "success" if success else "error"
    run.finished_at = time.time()


async def persist_run_to_graph(run: PipelineRun):
    """Write a ScanRun node to Neo4j for durable history."""
    await run_write("""
        MERGE (r:ScanRun {id: $id})
        SET r.phase       = $phase,
            r.repo_root   = $repo_root,
            r.status      = $status,
            r.started_at  = $started_at,
            r.finished_at = $finished_at
    """, {
        "id":          run.id,
        "phase":       run.phase,
        "repo_root":   run.repo_root,
        "status":      run.status,
        "started_at":  run.started_at,
        "finished_at": run.finished_at,
    })
