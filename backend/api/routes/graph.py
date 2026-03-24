from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from graph import queries
from graph import mock_data as _mock
from graph.neo4j_client import run_query

router = APIRouter()


class CypherRequest(BaseModel):
    cypher: str
    params: dict | None = None


# ── Private helpers (keep route handlers' cyclomatic complexity low) ──────

async def _fetch_datasets(name: str | None, limit: int) -> list[dict]:
    try:
        if name:
            rows = await run_query(
                "MATCH (d:Dataset) WHERE d.name = $n RETURN d LIMIT $lim",
                {"n": name, "lim": limit},
            )
        else:
            rows = await run_query("MATCH (d:Dataset) RETURN d LIMIT $lim", {"lim": limit})
        result = [dict(r["d"]) for r in rows]
    except Exception:
        result = []
    return result or _mock.get_datasets(name, limit)


async def _fetch_columns(dataset: str | None, limit: int) -> list[dict]:
    try:
        if dataset:
            rows = await run_query(
                "MATCH (d:Dataset {name: $ds})-[:HAS_COLUMN]->(c:Column) RETURN c LIMIT $lim",
                {"ds": dataset, "lim": limit},
            )
        else:
            rows = await run_query("MATCH (c:Column) RETURN c LIMIT $lim", {"lim": limit})
        result = [dict(r["c"]) for r in rows]
    except Exception:
        result = []
    return result or _mock.get_columns(dataset, limit)


async def _fetch_jobs(search: str | None, limit: int) -> list[dict]:
    try:
        if search:
            rows = await run_query(
                "MATCH (j:Job) WHERE j.name CONTAINS $s RETURN j LIMIT $lim",
                {"s": search, "lim": limit},
            )
        else:
            rows = await run_query("MATCH (j:Job) RETURN j LIMIT $lim", {"lim": limit})
        result = [dict(r["j"]) for r in rows]
    except Exception:
        result = []
    return result or _mock.get_jobs(search, limit)


# ── Routes ────────────────────────────────────────────────────────────────

@router.post("/query")
async def run_cypher(req: CypherRequest):
    try:
        rows = await queries.run_raw_cypher(req.cypher, req.params)
        return {"rows": rows, "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/nodes/labels")
async def get_labels():
    """Return all node labels with counts."""
    return await queries.get_node_label_counts()


@router.get("/datasets")
async def list_datasets(name: str | None = None, limit: int = 100):
    """List Dataset nodes; optionally filter by name."""
    return await _fetch_datasets(name, limit)


@router.get("/columns")
async def list_columns(dataset: str | None = None, limit: int = 100):
    """List Column nodes; optionally filter by parent dataset name."""
    return await _fetch_columns(dataset, limit)


@router.get("/jobs")
async def list_jobs(limit: int = 100, search: str | None = None):
    """List Job nodes; optionally filter by name."""
    return await _fetch_jobs(search, limit)


@router.get("/job/{job_id}/reads")
async def get_reads(job_id: str):
    return await queries.resolve_reads(job_id)


@router.get("/job/{job_id}/writes")
async def get_writes(job_id: str):
    return await queries.resolve_writes(job_id)


@router.get("/job/{job_id}/dataflows")
async def get_dataflows(job_id: str):
    return await queries.resolve_dataflows(job_id)
