"""
Pipeline routes — trigger Phase 1 hydration agents.
Each POST streams execution events as SSE for real-time log display.
"""
import json
import uuid
from typing import AsyncGenerator

from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel

from agents.ast_extractor      import ASTExtractorAgent
from agents.cross_ref_resolver import CrossRefResolverAgent
from agents.schema_extractor   import SchemaExtractorAgent
from agents.llm_summariser     import LLMSummariserAgent
from services import run_tracker
from config import settings
# STM seeding is the final step of Phase 1 — must run after schema_extractor
from api.routes.stm import _seed_from_list, DEFAULT_MAPPINGS
# Phase 2 pipeline registration — auto-runs after STM seed
from api.routes.phase2 import _register_demo_pipelines

router = APIRouter()

PHASE1_AGENTS = [
    ("ast_extractor",      ASTExtractorAgent),
    ("cross_ref_resolver", CrossRefResolverAgent),
    ("schema_extractor",   SchemaExtractorAgent),
    ("llm_summariser",     LLMSummariserAgent),
]


class RunRequest(BaseModel):
    repo_root: str | None = None
    agents: list[str] | None = None   # subset of agent names, or None = all
    force: bool = False               # bypass SHA-256 file-hash cache


# ---------------------------------------------------------------------------
# Helpers — each yields SSE dicts so event_stream stays small
# ---------------------------------------------------------------------------

async def _run_agent(
    agent_name: str, AgentClass, scan_run_id: str,
    repo_root: str, force: bool, pipeline_run_id: str,
) -> AsyncGenerator[tuple[dict, bool], None]:
    """Yields (sse_dict, ok) tuples for every event produced by one agent."""
    agent_run = run_tracker.add_agent_run(pipeline_run_id, agent_name)
    yield {"data": json.dumps({"type": "agent_start", "agent": agent_name,
                               "agent_run_id": agent_run.id})}, True

    agent = AgentClass(scan_run_id=scan_run_id, repo_root=repo_root, force=force)
    agent_ok = True
    try:
        async for event in agent.stream():
            ed = event.to_dict()
            run_tracker.record_event(agent_run, ed)
            if event.level == "error":
                agent_ok = False
            yield {"data": json.dumps({"type": "event", **ed})}, agent_ok
    except Exception as exc:
        agent_ok = False
        yield {"data": json.dumps({"type": "event", "level": "error",
                                   "agent": agent_name, "message": str(exc)})}, False

    run_tracker.finish_agent_run(agent_run, agent_ok)
    yield {"data": json.dumps({"type": "agent_end", "agent": agent_name,
                               "status": agent_run.status})}, agent_ok


async def _run_stm_seed() -> AsyncGenerator[tuple[dict, bool], None]:
    """Seed STM mappings.  Completes Code→Column→STM lineage chain."""
    yield {"data": json.dumps({"type": "agent_start", "agent": "stm_seed",
                               "agent_run_id": None})}, True
    try:
        r = await _seed_from_list(DEFAULT_MAPPINGS)
        msg = (f"STM seeded: {r['stm_nodes_created_or_updated']} nodes, "
               f"{r['edges_linked_to_schema']} MAPS_TO edges, "
               f"{r['orphaned_stm_nodes']} orphaned targets")
        yield {"data": json.dumps({"type": "event", "level": "info",
                                   "agent": "stm_seed", "message": msg})}, True
        yield {"data": json.dumps({"type": "agent_end", "agent": "stm_seed",
                                   "status": "success"})}, True
    except Exception as exc:
        yield {"data": json.dumps({"type": "event", "level": "error",
                                   "agent": "stm_seed",
                                   "message": f"STM seed failed: {exc}"})}, False
        yield {"data": json.dumps({"type": "agent_end", "agent": "stm_seed",
                                   "status": "error"})}, False


async def _run_phase2_seed() -> AsyncGenerator[tuple[dict, bool], None]:
    """Auto-register demo Phase 2 pipelines — runs automatically after STM seed."""
    yield {"data": json.dumps({"type": "agent_start", "agent": "phase2_seed",
                               "agent_run_id": None})}, True
    try:
        r    = await _register_demo_pipelines()
        n    = len(r.get("registered", []))
        skip = len(r.get("skipped_scripts_not_found", []))
        msg  = (f"Phase 2 pipelines registered: {n}"
                + (f" ({skip} functions not found — will register on next run)" if skip else ""))
        yield {"data": json.dumps({"type": "event", "level": "info",
                                   "agent": "phase2_seed", "message": msg})}, True
        yield {"data": json.dumps({"type": "agent_end", "agent": "phase2_seed",
                                   "status": "success"})}, True
    except Exception as exc:
        # Phase 2 failure never blocks Phase 1 success
        yield {"data": json.dumps({"type": "event", "level": "warning",
                                   "agent": "phase2_seed",
                                   "message": f"Phase 2 seed skipped: {exc}"})}, True
        yield {"data": json.dumps({"type": "agent_end", "agent": "phase2_seed",
                                   "status": "warning"})}, True


async def _run_phase1_finalize() -> AsyncGenerator[tuple[dict, bool], None]:
    """STM seed + Phase 2 registration — final steps of every Phase 1 run."""
    async for msg, ok in _run_stm_seed():
        yield msg, ok
    async for msg, _ok in _run_phase2_seed():
        yield msg, True   # phase2 never blocks phase1 success


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post("/run/phase1")
async def run_phase1(req: RunRequest):
    repo_root   = req.repo_root or settings.repo_scan_root
    scan_run_id = str(uuid.uuid4())
    pipeline_run = run_tracker.create_run("phase1", repo_root)
    agent_filter = set(req.agents) if req.agents else None

    async def event_stream() -> AsyncGenerator[dict, None]:
        yield {"data": json.dumps({"type": "pipeline_start",
                                   "run_id": pipeline_run.id,
                                   "scan_run_id": scan_run_id})}
        success = True

        for agent_name, AgentClass in PHASE1_AGENTS:
            if agent_filter and agent_name not in agent_filter:
                continue
            async for msg, ok in _run_agent(agent_name, AgentClass, scan_run_id,
                                            repo_root, req.force, pipeline_run.id):
                yield msg
                if not ok:
                    success = False

        async for msg, ok in _run_phase1_finalize():
            yield msg
            if not ok:
                success = False

        run_tracker.finish_run(pipeline_run, success)
        await run_tracker.persist_run_to_graph(pipeline_run)
        yield {"data": json.dumps({"type": "pipeline_end",
                                   "run_id": pipeline_run.id,
                                   "status": pipeline_run.status})}

    return EventSourceResponse(event_stream())


@router.get("/runs")
def get_pipeline_runs():
    return run_tracker.list_runs()
