import uuid
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from neo4j.graph import Node, Relationship
from neo4j.exceptions import ServiceUnavailable, SessionExpired
from graph import queries
from graph import mock_data as _mock
from graph.neo4j_client import run_query, run_write, get_driver, _session_kwargs, _reset_driver
from graph.writer import write_columns_batch, write_rels_batch, write_datasets_batch, write_jobs_batch, write_business_rule

router = APIRouter()


class CypherRequest(BaseModel):
    cypher: str
    params: dict | None = None


def _serialize_val(v):
    """Recursively convert any value to JSON-safe primitives.
    Handles Neo4j Node / Relationship objects as well as temporal types."""
    if isinstance(v, (str, int, float, bool, type(None))):
        return v
    if isinstance(v, Node):
        # Flatten properties; attach label list so the frontend can render nicely
        return {"_labels": list(v.labels), **{k: _serialize_val(vv) for k, vv in v.items()}}
    if isinstance(v, Relationship):
        # Flatten properties; attach relationship type
        return {"_relType": v.type, **{k: _serialize_val(vv) for k, vv in v.items()}}
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
    """Execute Cypher and return tabular rows. Node/Relationship values are
    flattened to plain dicts so the response is always JSON-serializable.
    A LIMIT clause is auto-injected when absent to prevent runaway scans."""
    try:
        guarded = _guard_cypher(req.cypher)
        rows = await queries.run_raw_cypher(guarded, req.params)
        safe = [{k: _serialize_val(v) for k, v in row.items()} for row in rows]
        return {"rows": safe, "count": len(safe)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


import re

# Hard caps — prevent runaway queries from exhausting memory or stalling
_MAX_NODES   = 400
_MAX_EDGES   = 600
_QUERY_LIMIT = 500          # auto-injected when the Cypher has no LIMIT
_TIMEOUT_S   = 30           # Neo4j server-side query timeout in seconds

_RE_LIMIT = re.compile(r'\bLIMIT\s+\d+', re.IGNORECASE)
# Unbounded variable-length path:  [*]  [:T*]  [r:T*]  — NOT already bounded like *1..5
_RE_UNBOUND_PATH = re.compile(r'\*(?![\d.])(\s*\])')


_RE_CALL = re.compile(r'^\s*CALL\s+', re.IGNORECASE)


def _guard_cypher(cypher: str) -> str:
    """Return the Cypher with safety guardrails applied:

    1. Bounds any unbounded variable-length paths  [*]  [:T*]  →  *1..5
       to prevent full-graph traversals that stall AuraDB for 30 s.
    2. Auto-injects LIMIT {_QUERY_LIMIT} when none is present.
    3. Skips LIMIT injection for CALL procedure statements (invalid syntax).
    """
    # Bound unbounded var-length paths before any other transforms
    cypher = _RE_UNBOUND_PATH.sub(r'*1..5\1', cypher)

    if _RE_CALL.match(cypher):
        return cypher.rstrip().rstrip(';')
    if not _RE_LIMIT.search(cypher):
        return cypher.rstrip().rstrip(';') + f' LIMIT {_QUERY_LIMIT}'
    return cypher


@router.post("/vis-query")
async def vis_query_for_network(req: CypherRequest):
    """Execute Cypher and return { nodes, edges, truncated } for vis-network rendering.

    Complexity guardrails
    ─────────────────────
    • Auto-injects LIMIT {_QUERY_LIMIT} when the user query has none            O(N) → O(cap)
    • Server-side query timeout ({_TIMEOUT_S}s) — avoids indefinite AuraDB hang
    • Hard caps on node/edge maps (_MAX_NODES/_MAX_EDGES) — O(1) space ceiling
    • Streams records lazily; stops consuming once both caps are hit
    • Auto-reconnects once on ServiceUnavailable / defunct connection errors
    """
    try:
        guarded = _guard_cypher(req.cypher)

        nodes_map: dict[str, dict] = {}
        edges_list: list[dict]     = []
        edge_ids:   set[str]       = set()
        rows_list:  list[dict]     = []
        truncated                  = False

        # Retry once on defunct/stale connection errors (AuraDB drops idle sockets)
        for attempt in range(2):
            try:
                driver = await get_driver()
                async with driver.session(**_session_kwargs()) as session:
                    result = await session.run(
                        guarded,
                        req.params or {},
                        timeout=_TIMEOUT_S,
                    )

                    async for rec in result:
                        # Stop streaming once both caps are saturated
                        if len(nodes_map) >= _MAX_NODES and len(edges_list) >= _MAX_EDGES:
                            truncated = True
                            # Consume remaining rows without processing to avoid cursor leak
                            async for _ in result:
                                pass
                            break

                        # Collect serialised row for the table (mirrors /query endpoint)
                        rows_list.append({k: _serialize_val(v) for k, v in rec.items()})

                        for val in rec.values():
                            # db.schema.visualization() returns nodes/rels as lists —
                            # unpack them so the same extraction logic applies.
                            items = val if isinstance(val, list) else [val]
                            for item in items:
                                if isinstance(item, Node):
                                    nid = item.element_id
                                    if nid not in nodes_map:
                                        if len(nodes_map) >= _MAX_NODES:
                                            truncated = True
                                            continue
                                        lbls = list(item.labels)
                                        nodes_map[nid] = {
                                            "id":     nid,
                                            "label":  lbls[0] if lbls else "Node",
                                            "labels": lbls,
                                            "props":  _serialize_props(dict(item)),
                                        }
                                elif isinstance(item, Relationship):
                                    eid = item.element_id
                                    if eid not in edge_ids:
                                        if len(edges_list) >= _MAX_EDGES:
                                            truncated = True
                                            continue
                                        edge_ids.add(eid)
                                        # Ensure endpoint nodes are in the map
                                        for n in (item.start_node, item.end_node):
                                            nid = n.element_id
                                            if nid not in nodes_map and len(nodes_map) < _MAX_NODES:
                                                lbls = list(n.labels)
                                                nodes_map[nid] = {
                                                    "id":     nid,
                                                    "label":  lbls[0] if lbls else "Node",
                                                    "labels": lbls,
                                                    "props":  _serialize_props(dict(n)),
                                                }
                                        edges_list.append({
                                            "id":    eid,
                                            "from":  item.start_node.element_id,
                                            "to":    item.end_node.element_id,
                                            "type":  item.type,
                                            "props": _serialize_props(dict(item)),
                                        })
                break  # success — exit retry loop
            except (ServiceUnavailable, SessionExpired) as conn_exc:
                import logging
                logging.getLogger(__name__).warning(
                    "vis-query: defunct connection (attempt %d): %s", attempt + 1, conn_exc
                )
                await _reset_driver()
                if attempt == 1:
                    raise

        return {
            "nodes":     list(nodes_map.values()),
            "edges":     edges_list,
            "rows":      rows_list,
            "count":     len(rows_list),
            "truncated": truncated,
        }
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
    await _seed_business_rules(run_id, errors)
    await _seed_mark_deprecated(errors)
    await _seed_scheduler(run_id, errors)
    await _seed_scanrun_links(run_id, errors)
    return {
        "status": "partial" if errors else "ok",
        "run_id": run_id,
        "seeded": {
            "datasets":           len(_mock.DATASETS),
            "jobs":               len(_mock.JOBS),
            "columns":            len(columns_flat),
            "col_edges":          len(col_pairs),
            "job_edges":          len(_mock.JOB_EDGES),
            "dataset_joins":      len(_mock.DATASET_JOINS),
            "business_rules":     len(_mock.BUSINESS_RULES),
            "deprecated_columns": sum(1 for cols in _mock.COLUMNS.values() for c in cols if c.get("deprecated")),
            "scheduler_jobs":     len(_mock.SCHEDULER_JOBS),
            "scheduler_edges":    len(_mock.SCHEDULER_JOB_EDGES),
            "scanrun_links":      1,
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
                "deprecated":     c.get("deprecated", False),
            })
    try:
        await write_columns_batch(columns_flat, run_id)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"columns: {exc}")
    return columns_flat


async def _seed_mark_deprecated(errors: list) -> None:
    """Mark the 3 known deprecated columns directly by id — avoids qualified_name constraint."""
    deprecated_ids = [c["id"] for cols in _mock.COLUMNS.values()
                      for c in cols if c.get("deprecated")]
    if not deprecated_ids:
        return
    try:
        await run_write(
            "UNWIND $ids AS cid "
            "MATCH (n:Column {id: cid}) "
            "SET n.deprecated = true, n.status = 'deprecated'",
            {"ids": deprecated_ids},
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"mark_deprecated: {exc}")


async def _seed_business_rules(run_id: str, errors: list) -> None:
    for br in _mock.BUSINESS_RULES:
        try:
            await write_business_rule(br, run_id)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"business_rule {br['id']}: {exc}")


async def _seed_mark_deprecated(errors: list) -> None:
    """Explicitly mark all mock-deprecated columns by name.

    The MERGE in write_columns_batch operates on col-xxx-xxx IDs, but real
    pipeline runs may have created UUID-based Column nodes with the same names.
    This step finds *all* Column nodes with a matching name and forces the
    correct deprecated / status values, making the seed idempotent regardless
    of which node ID set is present in the database.
    """
    deprecated_names = [
        c["name"]
        for cols in _mock.COLUMNS.values()
        for c in cols
        if c.get("deprecated", False)
    ]
    if not deprecated_names:
        return
    try:
        await run_write(
            "UNWIND $names AS n "
            "MATCH (c:Column {name: n}) "
            "SET c.deprecated = true, c.status = 'deprecated'",
            {"names": deprecated_names},
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"mark_deprecated: {exc}")


def _scheduler_job_rows(run_id: str) -> list[dict]:
    return [
        {
            "id":          j["id"],
            "name":        j["name"],
            "domain":      j.get("domain", ""),
            "type":        j.get("type", "scheduled"),
            "status":      j.get("status", "active"),
            "source":      j.get("source", "scheduler"),
            "module":      j.get("module", ""),
            "function":    j.get("function", ""),
            "path":        j.get("path", ""),
            "risk_tags":   j.get("risk_tags", []),
            "scan_run_id": run_id,
        }
        for j in _mock.SCHEDULER_JOBS
    ]


def _scheduler_dep_pairs() -> list[dict]:
    return [
        {"from_id": e["src"], "to_id": e["tgt"], "confidence": e.get("conf", "verified")}
        for e in _mock.SCHEDULER_JOB_EDGES
    ]


def _scheduler_dataset_pairs(rel_name: str) -> list[dict]:
    pairs: list[dict] = []
    for sched_id, core_job_id in _mock.SCHEDULER_TO_CORE_JOB.items():
        for edge in _mock.JOB_EDGES:
            if edge.get("src") != core_job_id or edge.get("rel") != rel_name:
                continue
            pairs.append({
                "from_id": sched_id,
                "to_id": edge["tgt"],
                "confidence": edge.get("conf", "verified"),
            })
    return pairs


async def _seed_scheduler(run_id: str, errors: list) -> None:
    """Seed the scheduler DAG Job nodes and their DEPENDS_ON edges.

    Creates 7 Job nodes (source='scheduler') from job_scheduler.py plus 8
    DEPENDS_ON edges that match the JobSpec.depends_on lists exactly.
    Safe to re-run — every write is a MERGE.
    """
    try:
        rows = _scheduler_job_rows(run_id)
        await run_write(
            """
            UNWIND $rows AS row
            MERGE (n:Job {id: row.id})
            ON CREATE SET n = row
            ON MATCH  SET n += row
            """,
            {"rows": rows},
        )
        deps = _scheduler_dep_pairs()
        if deps:
            await write_rels_batch("Job", "DEPENDS_ON", "Job", deps)

        # Bridge scheduler jobs into the dataset lineage by inheriting
        # READS_FROM/WRITES_TO from their mapped core jobs.
        # This avoids detached scheduler islands in graph visualization.
        reads = _scheduler_dataset_pairs("READS_FROM")
        writes = _scheduler_dataset_pairs("WRITES_TO")
        if reads:
            await write_rels_batch("Job", "READS_FROM", "Dataset", reads)
        if writes:
            await write_rels_batch("Job", "WRITES_TO", "Dataset", writes)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"scheduler: {exc}")


async def _seed_scanrun_links(run_id: str, errors: list) -> None:
    """Create/update ScanRun nodes and backfill SCANNED edges by scan_run_id."""
    try:
        await run_write(
            """
            MERGE (sr:ScanRun {id: $run_id})
            ON CREATE SET sr.phase = 'seed-mock', sr.status = 'success', sr.repo_root = 'meta_model',
                          sr.started_at = timestamp(), sr.finished_at = timestamp()
            ON MATCH  SET sr.phase = 'seed-mock', sr.status = 'success', sr.finished_at = timestamp()
            WITH sr
            MATCH (n)
            WHERE n.scan_run_id = $run_id AND NOT n:ScanRun
            MERGE (sr)-[:SCANNED]->(n)
            WITH 1 AS _
            MATCH (hist:ScanRun)
            MATCH (n)
            WHERE n.scan_run_id = hist.id AND NOT n:ScanRun
            MERGE (hist)-[:SCANNED]->(n)
            """,
            {"run_id": run_id},
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"scanrun_links: {exc}")


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
