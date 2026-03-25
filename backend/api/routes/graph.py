import uuid
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from neo4j.graph import Node, Relationship
from graph import queries
from graph import mock_data as _mock
from graph.neo4j_client import run_query, get_driver, _session_kwargs
from graph.writer import write_columns_batch, write_rels_batch, write_datasets_batch, write_jobs_batch

router = APIRouter()


class CypherRequest(BaseModel):
    cypher: str
    params: dict | None = None


def _serialize_val(v):
    """Recursively convert any value to JSON-safe primitives.
    Neo4j temporal types (DateTime, Date, Duration, etc.) become ISO strings."""
    if isinstance(v, (str, int, float, bool, type(None))):
        return v
    if isinstance(v, list):
        return [_serialize_val(i) for i in v]
    if isinstance(v, dict):
        return {k: _serialize_val(vv) for k, vv in v.items()}
    # Catch-all: neo4j temporal / spatial types, unknown objects → isoformat or str
    if hasattr(v, 'isoformat'):
        return v.isoformat()
    return str(v)


def _serialize_props(props: dict) -> dict:
    return {k: _serialize_val(v) for k, v in props.items()}


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


@router.post("/vis-query")
async def vis_query_for_network(req: CypherRequest):
    """Execute Cypher and return { nodes, edges } for direct vis-network rendering.

    Each node:  { id, label, labels, props }
    Each edge:  { id, from, to, type, props }
    """
    try:
        driver = await get_driver()
        async with driver.session(**_session_kwargs()) as session:
            result = await session.run(req.cypher, req.params or {})
            records = [r async for r in result]

        nodes_map: dict[str, dict] = {}
        edges_list: list[dict] = []
        edge_ids: set[str] = set()

        for rec in records:
            for key in rec.keys():
                val = rec[key]
                if isinstance(val, Node):
                    nid = val.element_id
                    if nid not in nodes_map:
                        lbls = list(val.labels)
                        nodes_map[nid] = {
                            "id":     nid,
                            "label":  lbls[0] if lbls else "Node",
                            "labels": lbls,
                            "props":  _serialize_props(dict(val)),
                        }
                elif isinstance(val, Relationship):
                    eid = val.element_id
                    if eid not in edge_ids:
                        edge_ids.add(eid)
                        # Ensure start/end nodes are in the map too
                        for n in (val.start_node, val.end_node):
                            nid = n.element_id
                            if nid not in nodes_map:
                                lbls = list(n.labels)
                                nodes_map[nid] = {
                                    "id":     nid,
                                    "label":  lbls[0] if lbls else "Node",
                                    "labels": lbls,
                                    "props":  _serialize_props(dict(n)),
                                }
                        edges_list.append({
                            "id":    eid,
                            "from":  val.start_node.element_id,
                            "to":    val.end_node.element_id,
                            "type":  val.type,
                            "props": _serialize_props(dict(val)),
                        })

        return {"nodes": list(nodes_map.values()), "edges": edges_list}
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


@router.post("/seed-mock")
async def seed_mock_graph():
    """
    Write ALL mock data (Datasets, Jobs, Job edges, Dataset FK joins,
    Column nodes + HAS_COLUMN edges, Column DERIVED_FROM edges) into AuraDB.
    Safe to call multiple times — every write is a MERGE (idempotent).
    """
    run_id = f"seed-mock-{uuid.uuid4().hex[:8]}"
    errors: list[str] = []
    columns_flat = await _seed_columns(run_id, errors)
    col_pairs    = await _seed_col_edges(errors)
    await _seed_datasets(run_id, errors)
    await _seed_jobs(run_id, errors)
    await _seed_job_edges(errors)
    await _seed_dataset_joins(errors)
    return {
        "status": "partial" if errors else "ok",
        "run_id": run_id,
        "seeded": {
            "datasets":      len(_mock.DATASETS),
            "jobs":          len(_mock.JOBS),
            "columns":       len(columns_flat),
            "col_edges":     len(col_pairs),
            "job_edges":     len(_mock.JOB_EDGES),
            "dataset_joins": len(_mock.DATASET_JOINS),
        },
        "errors": errors,
    }


# ── Seed helpers (one logical step each, extracted to keep CCN low) ────────

async def _seed_datasets(run_id: str, errors: list) -> None:
    try:
        await write_datasets_batch(_mock.DATASETS, run_id)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"datasets: {exc}")


async def _seed_jobs(run_id: str, errors: list) -> None:
    try:
        await write_jobs_batch(_mock.JOBS, run_id)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"jobs: {exc}")


async def _seed_columns(run_id: str, errors: list) -> list[dict]:
    ds_name_to_id = {d["name"]: d["id"] for d in _mock.DATASETS}
    columns_flat: list[dict] = []
    for ds_name, cols in _mock.COLUMNS.items():
        ds_id = ds_name_to_id.get(ds_name, "")
        for c in cols:
            columns_flat.append({
                "id":             c["id"],
                "name":           c["name"],
                "qualified_name": f"{ds_name}.{c['name']}",
                "dataset_id":     ds_id,
                "data_type":      c["dtype"],
                "pii_flag":       c.get("pii", False),
                "sensitive_flag": c.get("pii", False),
            })
    try:
        await write_columns_batch(columns_flat, run_id)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"columns: {exc}")
    return columns_flat


async def _seed_col_edges(errors: list) -> list[dict]:
    col_pairs = [
        {"from_id": e["src"], "to_id": e["tgt"],
         "confidence": e.get("confidence", "verified"),
         "expression": e.get("expression", "")}
        for e in _mock.COLUMN_EDGES
    ]
    try:
        await write_rels_batch("Column", "DERIVED_FROM", "Column", col_pairs)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"column_derived_from: {exc}")
    return col_pairs


async def _seed_job_edges(errors: list) -> None:
    def _pairs(rel_name: str) -> list[dict]:
        return [
            {"from_id": e["src"], "to_id": e["tgt"],
             "confidence": e.get("conf", "verified")}
            for e in _mock.JOB_EDGES if e["rel"] == rel_name
        ]
    try:
        reads  = _pairs("READS_FROM")
        writes = _pairs("WRITES_TO")
        deps   = _pairs("DEPENDS_ON")
        if reads:
            await write_rels_batch("Job", "READS_FROM", "Dataset", reads)
        if writes:
            await write_rels_batch("Job", "WRITES_TO",  "Dataset", writes)
        if deps:
            await write_rels_batch("Job", "DEPENDS_ON", "Job",     deps)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"job_edges: {exc}")


async def _seed_dataset_joins(errors: list) -> None:
    def _join_pairs(rel_name: str) -> list[dict]:
        return [
            {"from_id": j["src"], "to_id": j["tgt"],
             "join_key": j["join_key"], "join_type": j["join_type"]}
            for j in _mock.DATASET_JOINS if j["rel"] == rel_name
        ]
    try:
        refs    = _join_pairs("REFERENCES")
        derived = _join_pairs("DERIVED_FROM")
        joins   = _join_pairs("JOINS_WITH")
        if refs:
            await write_rels_batch("Dataset", "REFERENCES",  "Dataset", refs)
        if derived:
            await write_rels_batch("Dataset", "DERIVED_FROM", "Dataset", derived)
        if joins:
            await write_rels_batch("Dataset", "JOINS_WITH",  "Dataset", joins)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"dataset_joins: {exc}")


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
