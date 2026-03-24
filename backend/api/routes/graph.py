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


# ── Visual Cypher Endpoint ────────────────────────────────────────────────

def _neo4j_node_id(val: dict) -> str:
    """Return a stable string id for a Neo4j node dict."""
    return str(val.get("element_id") or val.get("id") or id(val))


def _is_neo4j_node(val: dict) -> bool:
    return "element_id" in val or "labels" in val


def _is_neo4j_rel(val: dict) -> bool:
    return "start_node_element_id" in val or "type" in val


def _maybe_add_node(nid: str, val: dict, col_name: str, node_map: dict) -> None:
    """Insert a Neo4j node-dict into node_map if not already present."""
    if nid in node_map:
        return
    labels = val.get("labels", [])
    node_map[nid] = {
        "id":   nid,
        "type": labels[0] if labels else col_name,
        "data": {k: v for k, v in val.items() if k not in ("element_id", "labels")},
    }


def _append_relationship(val: dict, edge_list: list) -> None:
    """Append a Neo4j relationship dict as a ReactFlow edge."""
    edge_list.append({
        "id":     str(val.get("element_id", len(edge_list))),
        "source": str(val.get("start_node_element_id", "")),
        "target": str(val.get("end_node_element_id", "")),
        "label":  val.get("type", ""),
    })


def _process_row(row: dict, node_map: dict, edge_list: list) -> bool:
    """Return True if the row contained at least one graph-typed value."""
    found = False
    for col_name, val in row.items():
        if not isinstance(val, dict):
            continue
        if _is_neo4j_node(val):
            _maybe_add_node(_neo4j_node_id(val), val, col_name, node_map)
            found = True
        elif _is_neo4j_rel(val):
            _append_relationship(val, edge_list)
            found = True
    return found


def _ensure_node(key: str, node_map: dict) -> None:
    if key not in node_map:
        node_map[key] = {"id": key, "type": "Node", "data": {"name": key, "label": key}}


def _row_tgt_val(row: dict, keys: list) -> str | None:
    raw = row[keys[-1]]
    return str(raw) if raw is not None else None


def _row_edge_label(row: dict, keys: list) -> str:
    return str(list(row.values())[1]) if len(keys) == 3 else "→"


def _scalar_row_to_graph(row: dict, idx: int, node_map: dict, edge_list: list) -> None:
    """Convert a scalar row (src / rel / tgt columns) into synthetic graph objects."""
    if not row:
        return
    keys = list(row.keys())
    if len(keys) == 1:
        _ensure_node(str(list(row.values())[0] or f"node_{idx}"), node_map)
        return
    src_val = str(row[keys[0]]) if row[keys[0]] is not None else f"node_{idx}_s"
    tgt_val = _row_tgt_val(row, keys)
    _ensure_node(src_val, node_map)
    if tgt_val and tgt_val != src_val:
        _ensure_node(tgt_val, node_map)
        edge_list.append({"id": f"e-{idx}", "source": src_val, "target": tgt_val,
                          "label": _row_edge_label(row, keys)})


def _build_from_scalars(scalar_rows: list[dict], node_map: dict, edge_list: list) -> None:
    for i, row in enumerate(scalar_rows):
        _scalar_row_to_graph(row, i, node_map, edge_list)


def _to_visual(rows: list[dict]) -> dict:
    """Convert raw Cypher result rows into ReactFlow {nodes, edges} format."""
    node_map: dict[str, dict] = {}
    edge_list: list[dict] = []
    scalar_rows: list[dict] = []

    for row in rows:
        if not _process_row(row, node_map, edge_list):
            scalar_rows.append(row)

    if not node_map and scalar_rows:
        _build_from_scalars(scalar_rows, node_map, edge_list)

    return {"nodes": list(node_map.values()), "edges": edge_list}


@router.post("/visual")
async def run_cypher_visual(req: CypherRequest):
    """
    Execute a Cypher query and return the results as a ReactFlow-compatible
    {nodes, edges} structure.  Falls back to the mock job-graph when the query
    is empty or Neo4j is unreachable.
    """
    if not req.cypher.strip():
        return _mock.get_all_job_graph_mock()

    try:
        rows = await queries.run_raw_cypher(req.cypher, req.params)
    except Exception:
        rows = []

    if not rows:
        return _mock.get_all_job_graph_mock()

    return _to_visual(rows)
