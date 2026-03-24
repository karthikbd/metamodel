"""
Graph Writer — the single write surface for all hydration agents.

Rules enforced here:
  1. Every write is a MERGE keyed on a stable identifier — no duplicates.
  2. Every write batch carries a scan_run_id.
  3. Nodes not touched in a run are marked DEPRECATED, never deleted.
  4. All facts from agents carry a confidence property (verified / inferred).
"""
import logging
from datetime import datetime, timezone

from graph.neo4j_client import run_write
from graph.schema import VERIFIED, INFERRED, NodeLabel, RelType, Prop

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Generic MERGE helpers
# ---------------------------------------------------------------------------

async def merge_node(label: str, match_props: dict, set_props: dict, scan_run_id: str) -> dict:
    """MERGE a node, set properties, stamp scan_run_id and updated_at."""
    set_clause = ", ".join(f"n.{k} = ${k}" for k in set_props)
    params = {**match_props, **set_props, "scan_run_id": scan_run_id, "updated_at": _now()}
    cypher = f"""
        MERGE (n:{label} {{{', '.join(f'{k}: ${k}' for k in match_props)}}})
        ON CREATE SET {set_clause}, n.scan_run_id = $scan_run_id,
                      n.created_at = $updated_at, n.status = 'active'
        ON MATCH  SET {set_clause}, n.scan_run_id = $scan_run_id,
                      n.updated_at = $updated_at, n.status = 'active'
        RETURN n
    """
    try:
        rows = await run_write(cypher, params)
        return rows[0]["n"] if rows else {}
    except Exception as exc:  # noqa: BLE001
        log.warning("merge_node skipped (%s): %s", label, exc)
        return {}


async def merge_rel(
    from_label: str,
    from_id_prop: str,
    from_id_val: str,
    rel_type: str,
    to_label: str,
    to_id_prop: str,
    to_id_val: str,
    rel_props: dict | None = None,
) -> None:
    """MERGE a directed relationship between two nodes by their id props."""
    rel_props = rel_props or {}
    set_clause = (
        "SET " + ", ".join(f"r.{k} = $rel_{k}" for k in rel_props)
        if rel_props else ""
    )
    params = {
        "from_id": from_id_val,
        "to_id": to_id_val,
        **{f"rel_{k}": v for k, v in rel_props.items()},
    }
    cypher = f"""
        MATCH (a:{from_label} {{{from_id_prop}: $from_id}})
        MATCH (b:{to_label}   {{{to_id_prop}:   $to_id}})
        MERGE (a)-[r:{rel_type}]->(b)
        {set_clause}
    """
    try:
        await run_write(cypher, params)
    except Exception as exc:  # noqa: BLE001
        log.warning("merge_rel skipped (%s→%s): %s", from_label, to_label, exc)


# ---------------------------------------------------------------------------
# Batch UNWIND writers — O(1) round trips regardless of item count
# Use these in agents instead of calling merge_node / merge_rel per item.
# ---------------------------------------------------------------------------

async def write_scripts_batch(scripts: list[dict], scan_run_id: str) -> None:
    """Write all Script nodes for a scan in one UNWIND round trip."""
    if not scripts:
        return
    now = _now()
    rows = [
        {
            "id": s["id"], "name": s.get("name", ""), "path": s.get("path", ""),
            "repository": s.get("repository", ""), "language": s.get("language", "Python"),
            "hash": s.get("hash", ""), "execution_engine": s.get("execution_engine", "python"),
            "risk_tags": s.get("risk_tags", []), "confidence": VERIFIED,
            "scan_run_id": scan_run_id, "updated_at": now,
        }
        for s in scripts
    ]
    cypher = """
        UNWIND $rows AS row
        MERGE (n:Script {id: row.id})
        ON CREATE SET n.name = row.name, n.path = row.path,
                      n.repository = row.repository, n.language = row.language,
                      n.hash = row.hash, n.execution_engine = row.execution_engine,
                      n.risk_tags = row.risk_tags, n.confidence = row.confidence,
                      n.scan_run_id = row.scan_run_id,
                      n.created_at = row.updated_at, n.status = 'active'
        ON MATCH  SET n.name = row.name, n.path = row.path, n.hash = row.hash,
                      n.risk_tags = row.risk_tags, n.scan_run_id = row.scan_run_id,
                      n.updated_at = row.updated_at, n.status = 'active'
    """
    try:
        await run_write(cypher, {"rows": rows})
        log.debug("write_scripts_batch: wrote %d scripts", len(rows))
    except Exception as exc:  # noqa: BLE001
        log.warning("write_scripts_batch skipped (%d): %s", len(rows), exc)


async def write_jobs_batch(jobs: list[dict], scan_run_id: str) -> None:
    """Write all Job nodes + PART_OF edges in one UNWIND round trip."""
    if not jobs:
        return
    now = _now()
    rows = [
        {
            "id": j["id"], "name": j["name"], "type": j.get("type", "batch"),
            "domain": j.get("domain", ""), "owner": j.get("owner", ""),
            "line_start": j.get("line_start", 0), "line_end": j.get("line_end", 0),
            "risk_tags": j.get("risk_tags", []), "confidence": VERIFIED,
            "script_id": j.get("script_id", ""),
            "scan_run_id": scan_run_id, "updated_at": now,
        }
        for j in jobs
    ]
    cypher = """
        UNWIND $rows AS row
        MERGE (n:Job {id: row.id})
        ON CREATE SET n.name = row.name, n.type = row.type, n.domain = row.domain,
                      n.path = row.domain,
                      n.owner = row.owner, n.line_start = row.line_start,
                      n.line_end = row.line_end, n.risk_tags = row.risk_tags,
                      n.confidence = row.confidence, n.scan_run_id = row.scan_run_id,
                      n.created_at = row.updated_at, n.status = 'active'
        ON MATCH  SET n.name = row.name, n.type = row.type, n.domain = row.domain,
                      n.path = row.domain,
                      n.line_start = row.line_start, n.line_end = row.line_end,
                      n.risk_tags = row.risk_tags, n.scan_run_id = row.scan_run_id,
                      n.updated_at = row.updated_at, n.status = 'active'
        WITH n, row WHERE row.script_id <> ''
        MATCH (s:Script {id: row.script_id})
        MERGE (s)-[:PART_OF]->(n)
    """
    try:
        await run_write(cypher, {"rows": rows})
        log.debug("write_jobs_batch: wrote %d jobs", len(rows))
    except Exception as exc:  # noqa: BLE001
        log.warning("write_jobs_batch skipped (%d): %s", len(rows), exc)


async def write_datasets_batch(datasets: list[dict], scan_run_id: str) -> None:
    """Write all Dataset nodes in one UNWIND round trip (dedup by id first)."""
    if not datasets:
        return
    now = _now()
    # Caller may pass duplicates — keep last-seen per id
    seen: dict[str, dict] = {}
    for d in datasets:
        seen[d["id"]] = d
    rows = [
        {
            "id": d["id"], "name": d["name"],
            "qualified_name": d.get("qualified_name", d["name"]),
            "datasource_id": d.get("datasource_id", ""),
            "format": d.get("format", "table"), "confidence": VERIFIED,
            "scan_run_id": scan_run_id, "updated_at": now,
        }
        for d in seen.values()
    ]
    cypher = """
        UNWIND $rows AS row
        MERGE (n:Dataset {id: row.id})
        ON CREATE SET n.name = row.name, n.qualified_name = row.qualified_name,
                      n.datasource_id = row.datasource_id, n.format = row.format,
                      n.confidence = row.confidence, n.scan_run_id = row.scan_run_id,
                      n.created_at = row.updated_at, n.status = 'active'
        ON MATCH  SET n.name = row.name, n.qualified_name = row.qualified_name,
                      n.scan_run_id = row.scan_run_id,
                      n.updated_at = row.updated_at, n.status = 'active'
    """
    try:
        await run_write(cypher, {"rows": rows})
        log.debug("write_datasets_batch: wrote %d datasets", len(rows))
    except Exception as exc:  # noqa: BLE001
        log.warning("write_datasets_batch skipped (%d): %s", len(rows), exc)


async def write_columns_batch(columns: list[dict], scan_run_id: str) -> None:
    """Write all Column nodes + HAS_COLUMN edges in one UNWIND round trip."""
    if not columns:
        return
    now = _now()
    seen: dict[str, dict] = {}
    for c in columns:
        seen[c["id"]] = c
    rows = [
        {
            "id": c["id"], "name": c["name"],
            "qualified_name": c.get("qualified_name", c["name"]),
            "dataset_id": c.get("dataset_id", ""),
            "data_type": c.get("data_type", "unknown"),
            "pii_flag": c.get("pii_flag", False),
            "sensitive_flag": c.get("sensitive_flag", False),
            "confidence": VERIFIED,
            "scan_run_id": scan_run_id, "updated_at": now,
        }
        for c in seen.values()
    ]
    cypher = """
        UNWIND $rows AS row
        MERGE (n:Column {id: row.id})
        ON CREATE SET n.name = row.name, n.qualified_name = row.qualified_name,
                      n.dataset_id = row.dataset_id, n.data_type = row.data_type,
                      n.pii_flag = row.pii_flag, n.sensitive_flag = row.sensitive_flag,
                      n.confidence = row.confidence, n.scan_run_id = row.scan_run_id,
                      n.created_at = row.updated_at, n.status = 'active'
        ON MATCH  SET n.name = row.name, n.qualified_name = row.qualified_name,
                      n.scan_run_id = row.scan_run_id,
                      n.updated_at = row.updated_at, n.status = 'active'
        WITH n, row WHERE row.dataset_id <> ''
        MATCH (d:Dataset {id: row.dataset_id})
        MERGE (d)-[:HAS_COLUMN]->(n)
    """
    try:
        await run_write(cypher, {"rows": rows})
        log.debug("write_columns_batch: wrote %d columns", len(rows))
    except Exception as exc:  # noqa: BLE001
        log.warning("write_columns_batch skipped (%d): %s", len(rows), exc)


async def write_rels_batch(
    from_label: str,
    rel_type: str,
    to_label: str,
    pairs: list[dict],
) -> None:
    """Write multiple relationships of the same type in one UNWIND round trip.

    Each dict in *pairs* must contain ``from_id`` and ``to_id``.
    An optional ``confidence`` key is forwarded to the relationship.
    Duplicate (from_id, to_id) pairs are deduplicated before writing.
    """
    if not pairs:
        return
    # Dedup by (from_id, to_id)
    seen_pairs: dict[tuple, dict] = {}
    for p in pairs:
        seen_pairs[(p["from_id"], p["to_id"])] = p
    deduped = list(seen_pairs.values())
    cypher = f"""
        UNWIND $pairs AS p
        MATCH (a:{from_label} {{id: p.from_id}})
        MATCH (b:{to_label}   {{id: p.to_id}})
        MERGE (a)-[r:{rel_type}]->(b)
        SET r.confidence = coalesce(p.confidence, 'verified')
    """
    try:
        await run_write(cypher, {"pairs": deduped})
        log.debug("write_rels_batch: wrote %d %s edges", len(deduped), rel_type)
    except Exception as exc:  # noqa: BLE001
        log.warning("write_rels_batch skipped (%s→%s, %d): %s", from_label, to_label, len(deduped), exc)


# ---------------------------------------------------------------------------
# Deprecation sweep — called at end of each scan run
# ---------------------------------------------------------------------------

async def deprecate_stale_nodes(scan_run_id: str, label: str) -> int:
    """Mark nodes of `label` that were NOT touched in this scan run as DEPRECATED."""
    cypher = f"""
        MATCH (n:{label})
        WHERE n.scan_run_id <> $scan_run_id AND n.status = 'active'
        SET n.status = 'deprecated', n.deprecated_at = $ts
        RETURN count(n) AS deprecated
    """
    try:
        rows = await run_write(cypher, {"scan_run_id": scan_run_id, "ts": _now()})
        return rows[0].get("deprecated", 0) if rows else 0
    except Exception as exc:  # noqa: BLE001
        log.warning("deprecate_stale_nodes skipped (%s): %s", label, exc)
        return 0


# ---------------------------------------------------------------------------
# Domain-specific writers — DataLineageMetaModel v3
# ---------------------------------------------------------------------------

async def write_data_source(data: dict, scan_run_id: str) -> dict:
    """Write a DataSource node (repository / database / file store)."""
    return await merge_node(
        NodeLabel.DATA_SOURCE,
        {"id": data["id"]},
        {
            Prop.NAME:        data["name"],
            Prop.PATH:        data.get("path", ""),
            Prop.TYPE:        data.get("type", "file_store"),
            Prop.ENVIRONMENT: data.get("environment", "dev"),
            Prop.CONFIDENCE:  VERIFIED,
        },
        scan_run_id,
    )


async def write_dataset(data: dict, scan_run_id: str) -> dict:
    """Write a Dataset node and create HAS_DATASET from its DataSource."""
    node = await merge_node(
        NodeLabel.DATASET,
        {"id": data["id"]},
        {
            Prop.NAME:           data["name"],
            Prop.QUALIFIED_NAME: data.get("qualified_name", data["name"]),
            Prop.DATASOURCE_ID:  data.get("datasource_id", ""),
            Prop.FORMAT:         data.get("format", "table"),
            Prop.CONFIDENCE:     VERIFIED,
        },
        scan_run_id,
    )
    if data.get("datasource_id"):
        await merge_rel(
            NodeLabel.DATA_SOURCE, "id", data["datasource_id"],
            RelType.HAS_DATASET,
            NodeLabel.DATASET, "id", data["id"],
        )
    return node


async def write_column(data: dict, scan_run_id: str) -> dict:
    """Write a Column node and create HAS_COLUMN from its Dataset."""
    node = await merge_node(
        NodeLabel.COLUMN,
        {"id": data["id"]},
        {
            Prop.NAME:          data["name"],
            Prop.QUALIFIED_NAME: data.get("qualified_name", data["name"]),
            Prop.DATASET_ID:    data.get("dataset_id", ""),
            Prop.DATA_TYPE:     data.get("data_type", "unknown"),
            Prop.PII_FLAG:      data.get("pii_flag", False),
            Prop.SENSITIVE_FLAG: data.get("sensitive_flag", False),
            Prop.CONFIDENCE:    VERIFIED,
        },
        scan_run_id,
    )
    if data.get("dataset_id"):
        await merge_rel(
            NodeLabel.DATASET, "id", data["dataset_id"],
            RelType.HAS_COLUMN,
            NodeLabel.COLUMN, "id", data["id"],
        )
    return node


async def write_script(data: dict, scan_run_id: str) -> dict:
    """Write a Script node (one per Python / SQL file)."""
    return await merge_node(
        NodeLabel.SCRIPT,
        {"id": data["id"]},
        {
            Prop.NAME:             data.get("name", ""),
            Prop.PATH:             data.get("path", ""),
            Prop.REPOSITORY:       data.get("repository", ""),
            Prop.LANGUAGE:         data.get("language", "Python"),
            Prop.HASH:             data.get("hash", ""),
            Prop.EXECUTION_ENGINE: data.get("execution_engine", "python"),
            Prop.RISK_TAGS:        data.get("risk_tags", []),
            Prop.CONFIDENCE:       VERIFIED,
        },
        scan_run_id,
    )


async def write_job(data: dict, scan_run_id: str) -> dict:
    """Write a Job node (pipeline entry-point / significant function)."""
    node = await merge_node(
        NodeLabel.JOB,
        {"id": data["id"]},
        {
            Prop.NAME:       data["name"],
            Prop.TYPE:       data.get("type", "batch"),
            Prop.DOMAIN:     data.get("domain", ""),
            Prop.OWNER:      data.get("owner", ""),
            Prop.LINE_START: data.get("line_start", 0),
            Prop.LINE_END:   data.get("line_end", 0),
            Prop.RISK_TAGS:  data.get("risk_tags", []),
            Prop.CONFIDENCE: VERIFIED,
        },
        scan_run_id,
    )
    # Script PART_OF Job  (Script is a component that implements the Job)
    if data.get("script_id"):
        await merge_rel(
            NodeLabel.SCRIPT, "id", data["script_id"],
            RelType.PART_OF,
            NodeLabel.JOB, "id", data["id"],
        )
    return node


async def write_transformation(data: dict, scan_run_id: str) -> dict:
    """Write a Transformation node, optionally linking it to a Script."""
    node = await merge_node(
        NodeLabel.TRANSFORMATION,
        {"id": data["id"]},
        {
            Prop.TYPE:           data.get("type", "expression"),
            Prop.LOGIC:          data.get("logic", ""),
            Prop.LANGUAGE:       data.get("language", "SQL"),
            Prop.IS_AGGREGATION: data.get("is_aggregation", False),
            Prop.IS_JOIN:        data.get("is_join", False),
            Prop.IS_FILTER:      data.get("is_filter", False),
            Prop.LINE_START:     data.get("line_start", 0),
            Prop.LINE_END:       data.get("line_end", 0),
            Prop.CONFIDENCE:     VERIFIED,
        },
        scan_run_id,
    )
    if data.get("script_id"):
        await merge_rel(
            NodeLabel.SCRIPT, "id", data["script_id"],
            RelType.CONTAINS,
            NodeLabel.TRANSFORMATION, "id", data["id"],
        )
    return node


async def write_derived_from_edge(
    src_col_id: str,
    tgt_col_id: str,
    expression: str,
    confidence: str,
    scan_run_id: str,
) -> None:
    """Write a DERIVED_FROM edge between two Column nodes."""
    try:
        await run_write("""
            MATCH (src:Column {id: $src_id})
            MATCH (tgt:Column {id: $tgt_id})
            MERGE (src)-[r:DERIVED_FROM]->(tgt)
            SET r.expression  = $expression,
                r.confidence  = $confidence,
                r.scan_run_id = $scan_run_id,
                r.updated_at  = $ts
        """, {
            "src_id":      src_col_id,
            "tgt_id":      tgt_col_id,
            "expression":  expression,
            "confidence":  confidence,
            "scan_run_id": scan_run_id,
            "ts":          _now(),
        })
    except Exception as exc:  # noqa: BLE001
        log.warning("write_derived_from_edge skipped: %s", exc)


# ---------------------------------------------------------------------------
# Governance writers (LLM / compliance agents)
# ---------------------------------------------------------------------------

async def write_llm_summary(data: dict, scan_run_id: str) -> dict:
    """Write an LLMSummary node and link it to a Script or Job."""
    node = await merge_node(
        NodeLabel.LLM_SUMMARY,
        {"id": data["id"]},
        {
            Prop.SUMMARY:    data.get("summary", ""),
            "model_id":      data.get("model_id", "unknown"),
            Prop.TIMESTAMP:  _now(),
            Prop.CONFIDENCE: INFERRED,
        },
        scan_run_id,
    )
    parent_id = data.get("function_id") or data.get("script_id") or data.get("job_id")
    if parent_id:
        try:
            await run_write("""
                MATCH (p {id: $pid})
                MATCH (s:LLMSummary {id: $sid})
                MERGE (p)-[:HAS_SUMMARY]->(s)
            """, {"pid": parent_id, "sid": data["id"]})
        except Exception as exc:  # noqa: BLE001
            log.warning("write_llm_summary rel skipped: %s", exc)
    return node


async def write_business_rule(data: dict, scan_run_id: str) -> dict:
    """Write a BusinessRule node and GOVERNED_BY edge from its Script / Job."""
    node = await merge_node(
        NodeLabel.BUSINESS_RULE,
        {"id": data["id"]},
        {
            Prop.NAME:        data["name"],
            Prop.DESCRIPTION: data.get("description", ""),
            "category":       data.get("category", ""),
            "severity":       data.get("severity", "medium"),
            Prop.CONFIDENCE:  data.get("confidence", INFERRED),
        },
        scan_run_id,
    )
    parent_id = data.get("function_id") or data.get("script_id") or data.get("job_id")
    if parent_id:
        try:
            await run_write("""
                MATCH (p {id: $pid})
                MATCH (b:BusinessRule {id: $bid})
                MERGE (p)-[:GOVERNED_BY]->(b)
            """, {"pid": parent_id, "bid": data["id"]})
        except Exception as exc:  # noqa: BLE001
            log.warning("write_business_rule rel skipped: %s", exc)
    return node
