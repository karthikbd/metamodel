from fastapi import APIRouter, HTTPException
from services import run_tracker

router = APIRouter()


@router.get("/")
def list_runs():
    return run_tracker.list_runs()


@router.get("/{run_id}")
def get_run(run_id: str):
    run = run_tracker.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run.to_dict()


@router.get("/{run_id}/agents")
def get_agent_runs(run_id: str):
    run = run_tracker.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return [a.to_dict() for a in run.agent_runs]


@router.get("/{run_id}/agents/{agent_run_id}/events")
def get_agent_events(run_id: str, agent_run_id: str):
    run = run_tracker.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    for ar in run.agent_runs:
        if ar.id == agent_run_id:
            return [
                {
                    "id": e.id,
                    "agent": e.agent,
                    "level": e.level,
                    "message": e.message,
                    "data": e.data,
                    "ts": e.ts,
                }
                for e in ar.events
            ]
    raise HTTPException(status_code=404, detail="AgentRun not found")
