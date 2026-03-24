"""
Canned Cypher queries -- DataLineageMetaModel v3.
Returns real Neo4j data only. Empty lists/dicts when the graph has no data yet.
Run Phase 1 pipeline first to populate the graph.
Labels: DataSource, Dataset, Column, Script, Job, Transformation, Dashboard, Alias
"""
import logging
from graph.neo4j_client import run_query, run_write

log = logging.getLogger(__name__)


def _safe_int(value) -> int:
    return int(value) if value is not None else 0


# ---------------------------------------------------------------------------
# Stats / Dashboard
# ---------------------------------------------------------------------------

async def get_graph_stats() -> dict:
    rows = await run_query("""
        MATCH (n)
        RETURN
          sum(CASE WHEN n:Job          THEN 1 ELSE 0 END) AS jobs,
          sum(CASE WHEN n:Dataset      THEN 1 ELSE 0 END) AS datasets,
          sum(CASE WHEN n:Column       THEN 1 ELSE 0 END) AS columns,
          sum(CASE WHEN n:Script       THEN 1 ELSE 0 END) AS scripts,
          sum(CASE WHEN n:DataSource   THEN 1 ELSE 0 END) AS datasources,
          sum(CASE WHEN n:BusinessRule THEN 1 ELSE 0 END) AS rules
    """)
    if not rows:
        return {"functions": 0, "schema_objects": 0, "files": 0,
                "repositories": 0, "business_rules": 0}
    r = rows[0]
    return {
        # Field names match what the Dashboard UI stat cards expect
        "functions":      _safe_int(r.get("jobs")),
        "schema_objects": _safe_int(r.get("datasets")) + _safe_int(r.get("columns")),
        "files":          _safe_int(r.get("scripts")),
        "repositories":   _safe_int(r.get("datasources")),
        "business_rules": _safe_int(r.get("rules")),
    }


async def get_node_label_counts() -> list:
    """Return per-label node counts."""
    return await run_query("""
        MATCH (n)
        UNWIND labels(n) AS label
        RETURN label, count(*) AS count
        ORDER BY count DESC
    """)


# ---------------------------------------------------------------------------
# Phase 2 -- Runtime field resolution (verified confidence only)
# ---------------------------------------------------------------------------

async def resolve_reads(job_id: str) -> list:
    return await run_query("""
        MATCH (j:Job {id: $jid})-[r:READS_FROM]->(d:Dataset)
        WHERE r.confidence = 'verified'
        RETURN d.id AS id, d.name AS name, d.qualified_name AS qualified_name,
               d.format AS format, d.status AS status
    """, {"jid": job_id})


async def resolve_writes(job_id: str) -> list:
    return await run_query("""
        MATCH (j:Job {id: $jid})-[r:WRITES_TO]->(d:Dataset)
        WHERE r.confidence = 'verified'
        RETURN d.id AS id, d.name AS name, d.qualified_name AS qualified_name,
               d.format AS format, d.status AS status
    """, {"jid": job_id})


async def resolve_dataflows(job_id: str) -> list:
    return await run_query("""
        MATCH (j:Job {id: $jid})-[:READS_FROM]->(src_ds:Dataset)
              -[:HAS_COLUMN]->(src:Column)
              -[df:DERIVED_FROM]->(tgt:Column)
              <-[:HAS_COLUMN]-(tgt_ds:Dataset)
        RETURN src_ds.name AS src_table, src.name AS src_col,
               df.expression AS expression, df.confidence AS confidence,
               tgt_ds.name AS tgt_table, tgt.name AS tgt_col
        ORDER BY src_table, src_col
    """, {"jid": job_id})


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------

async def get_function_lineage(job_id: str) -> dict:
    reads = await run_query("""
        MATCH (j:Job {id: $jid})-[r:READS_FROM]->(d:Dataset)
        RETURN j.id AS src, d.id AS tgt, 'READS_FROM' AS rel, r.confidence AS conf
    """, {"jid": job_id})
    writes = await run_query("""
        MATCH (j:Job {id: $jid})-[r:WRITES_TO]->(d:Dataset)
        RETURN j.id AS src, d.id AS tgt, 'WRITES_TO' AS rel, r.confidence AS conf
    """, {"jid": job_id})
    deps = await run_query("""
        MATCH (j:Job {id: $jid})-[r:DEPENDS_ON]->(g:Job)
        RETURN j.id AS src, g.id AS tgt, 'DEPENDS_ON' AS rel, 'verified' AS conf
    """, {"jid": job_id})
    rules = await run_query("""
        MATCH (j:Job {id: $jid})-[r:GOVERNED_BY]->(b:BusinessRule)
        RETURN j.id AS src, b.id AS tgt, 'GOVERNED_BY' AS rel, 'verified' AS conf
    """, {"jid": job_id})
    return {"edges": reads + writes + deps + rules}


def _merge_col_nodes(rows: list) -> dict:
    acc = {}
    for row in rows:
        cid = row.get("col_id")
        if cid and cid not in acc:
            acc[cid] = {
                "id":         cid,
                "name":       row.get("col_name"),
                "dataset":    row.get("ds_name"),
                "dataset_id": row.get("ds_id"),
                "dtype":      row.get("dtype"),
                "pii":        bool(row.get("pii")),
            }
    return acc


def _merge_col_edges(rows: list) -> dict:
    acc = {}
    for row in rows:
        src, tgt = row.get("src"), row.get("tgt")
        if src and tgt:
            acc[f"{src}:{tgt}"] = {
                "src":        src,
                "tgt":        tgt,
                "expression": row.get("expression") or "",
                "confidence": row.get("confidence") or "",
            }
    return acc


async def _run_col_lineage(params: dict):
    up_nodes = await run_query("""
        MATCH (focal:Column {name: $col})<-[:HAS_COLUMN]-(d:Dataset {name: $ds})
        WITH focal
        MATCH (focal)-[:DERIVED_FROM*0..6]->(up:Column)
        WITH DISTINCT up AS c
        OPTIONAL MATCH (ds:Dataset)-[:HAS_COLUMN]->(c)
        RETURN DISTINCT c.id AS col_id, c.name AS col_name,
               ds.id AS ds_id, ds.name AS ds_name,
               c.data_type AS dtype, c.pii_flag AS pii
    """, params)
    dn_nodes = await run_query("""
        MATCH (focal:Column {name: $col})<-[:HAS_COLUMN]-(d:Dataset {name: $ds})
        WITH focal
        MATCH (dn:Column)-[:DERIVED_FROM*1..6]->(focal)
        WITH DISTINCT dn AS c
        OPTIONAL MATCH (ds:Dataset)-[:HAS_COLUMN]->(c)
        RETURN DISTINCT c.id AS col_id, c.name AS col_name,
               ds.id AS ds_id, ds.name AS ds_name,
               c.data_type AS dtype, c.pii_flag AS pii
    """, params)
    up_edges = await run_query("""
        MATCH (focal:Column {name: $col})<-[:HAS_COLUMN]-(d:Dataset {name: $ds})
        WITH focal
        MATCH path = (focal)-[:DERIVED_FROM*1..6]->(:Column)
        UNWIND relationships(path) AS r
        RETURN DISTINCT startNode(r).id AS src, endNode(r).id AS tgt,
               r.expression AS expression, r.confidence AS confidence
    """, params)
    dn_edges = await run_query("""
        MATCH (focal:Column {name: $col})<-[:HAS_COLUMN]-(d:Dataset {name: $ds})
        WITH focal
        MATCH path = (:Column)-[:DERIVED_FROM*1..6]->(focal)
        UNWIND relationships(path) AS r
        RETURN DISTINCT startNode(r).id AS src, endNode(r).id AS tgt,
               r.expression AS expression, r.confidence AS confidence
    """, params)
    return up_nodes, dn_nodes, up_edges, dn_edges


async def get_column_lineage_graph(dataset_name: str, column_name: str) -> dict:
    params = {"ds": dataset_name, "col": column_name}
    up_nodes, dn_nodes, up_edges, dn_edges = await _run_col_lineage(params)
    nodes = _merge_col_nodes(up_nodes + dn_nodes)
    edges = _merge_col_edges(up_edges + dn_edges)
    return {
        "nodes": list(nodes.values()),
        "edges": list(edges.values()),
        "focus": {"dataset": dataset_name, "column": column_name},
    }


async def get_datasets_with_columns() -> list:
    return await run_query("""
        MATCH (d:Dataset)-[:HAS_COLUMN]->(c:Column)
        WITH d, collect({id: c.id, name: c.name,
                         dtype: c.data_type, pii: c.pii_flag}) AS columns
        RETURN d.id AS dataset_id, d.name AS dataset_name,
               d.qualified_name AS qualified_name, columns
        ORDER BY d.name
    """)


async def get_all_column_lineage_summary() -> list:
    """Return every DERIVED_FROM relationship for the full column lineage overview."""
    return await run_query("""
        MATCH (src:Column)-[r:DERIVED_FROM]->(tgt:Column)
        OPTIONAL MATCH (src_ds:Dataset)-[:HAS_COLUMN]->(src)
        OPTIONAL MATCH (tgt_ds:Dataset)-[:HAS_COLUMN]->(tgt)
        RETURN
          src_ds.name  AS src_dataset,
          src.name     AS src_column,
          src.pii_flag AS src_pii,
          tgt_ds.name  AS tgt_dataset,
          tgt.name     AS tgt_column,
          tgt.pii_flag AS tgt_pii,
          r.expression AS expression
        ORDER BY src_dataset, src_column, tgt_dataset, tgt_column
    """)


async def get_all_functional_lineage_summary() -> list:
    """Return every DEPENDS_ON relationship between Job nodes for functional lineage overview."""
    return await run_query("""
        MATCH (caller:Job)-[:DEPENDS_ON]->(callee:Job)
        RETURN
          caller.id                                        AS caller_id,
          caller.name                                      AS caller_name,
          coalesce(caller.domain, caller.path, '?')        AS caller_path,
          coalesce(caller.line_start, 0)                   AS caller_line,
          callee.id                                        AS callee_id,
          callee.name                                      AS callee_name,
          coalesce(callee.domain, callee.path, '?')        AS callee_path,
          coalesce(callee.line_start, 0)                   AS callee_line
        ORDER BY caller_path, caller_name, callee_name
    """)


# ---------------------------------------------------------------------------
# Compliance queries
# ---------------------------------------------------------------------------

async def pii_without_audit() -> list:
    return await run_query("""
        MATCH (j:Job)
        WHERE 'PII' IN j.risk_tags
          AND NOT 'audit_required' IN j.risk_tags
        OPTIONAL MATCH (j)-[:READS_FROM|WRITES_TO]->(d:Dataset)
        OPTIONAL MATCH (d)-[:HAS_COLUMN]->(c:Column {pii_flag: true})
        RETURN j.id AS job_id, j.name AS job_name, j.domain AS path,
               collect(DISTINCT d.name + '.' + c.name) AS pii_columns
        ORDER BY j.name
    """)


async def regulatory_report_lineage() -> list:
    return await run_query("""
        MATCH (j:Job)
        WHERE 'regulatory_report' IN j.risk_tags
        OPTIONAL MATCH (j)-[:READS_FROM]->(d:Dataset)
        RETURN j.id AS job_id, j.name AS name,
               collect(DISTINCT d.name) AS source_datasets
        ORDER BY j.name
    """)


async def deprecated_columns_in_use() -> list:
    return await run_query("""
        MATCH (j:Job)-[:READS_FROM]->(d:Dataset)-[:HAS_COLUMN]->(c:Column)
        WHERE c.name IN [
            'old_amount', 'spot_rate_old', 'legacy_customer_id',
            'ssn_plain',  'credit_score_raw', 'amount_local_old'
        ]
        RETURN j.name AS job_name, j.domain AS path,
               d.name + '.' + c.name AS deprecated_column
        ORDER BY deprecated_column
    """)


# ---------------------------------------------------------------------------
# Impact analysis
# ---------------------------------------------------------------------------

async def column_impact(table: str, column: str) -> list:
    return await run_query("""
        MATCH (d:Dataset {name: $tbl})-[:HAS_COLUMN]->(c:Column {name: $col})
        MATCH (j:Job)-[r:READS_FROM|WRITES_TO]->(d)
        OPTIONAL MATCH (caller:Job)-[:DEPENDS_ON]->(j)
        RETURN j.id AS job_id, j.name AS name, j.domain AS path,
               type(r) AS relationship,
               collect(DISTINCT caller.name) AS callers,
               j.risk_tags AS risk_tags
        ORDER BY name
    """, {"tbl": table, "col": column})


async def dataset_impact(dataset_name: str) -> list:
    return await run_query("""
        MATCH (d:Dataset {name: $ds})
        OPTIONAL MATCH (reader:Job)-[:READS_FROM]->(d)
        OPTIONAL MATCH (writer:Job)-[:WRITES_TO]->(d)
        RETURN d.id AS dataset_id, d.name AS dataset_name,
               collect(DISTINCT reader.name) AS read_by,
               collect(DISTINCT writer.name) AS written_by
    """, {"ds": dataset_name})


# ---------------------------------------------------------------------------
# Generic Cypher passthrough (Graph Explorer)
# ---------------------------------------------------------------------------

async def run_raw_cypher(cypher: str, params: dict = None) -> list:
    return await run_query(cypher, params or {})


# ---------------------------------------------------------------------------
# STM -- Source-to-Target Mapping
# ---------------------------------------------------------------------------

async def seed_stm(mappings: list) -> dict:
    created = 0
    for m in mappings:
        await run_write("""
            MERGE (stm:STM {id: $stm_id})
            ON CREATE SET
              stm.target_table  = $target_table,
              stm.target_column = $target_column,
              stm.target_system = $target_system,
              stm.owner         = $owner,
              stm.created_at    = timestamp()
            WITH stm
            MATCH (c:Column {id: $column_id})
            MERGE (c)-[r:MAPS_TO]->(stm)
            ON CREATE SET
              r.transform_expr = $transform_expr,
              r.confidence     = 'verified',
              r.created_at     = timestamp()
        """, {
            "stm_id":         m["stm_id"],
            "column_id":      m["column_id"],
            "target_table":   m["target_table"],
            "target_column":  m["target_column"],
            "target_system":  m.get("target_system", "data_warehouse"),
            "owner":          m.get("owner", "data_engineering"),
            "transform_expr": m.get("transform_expr", ""),
        })
        created += 1
    return {"seeded": created}


async def list_stm_mappings() -> list:
    return await run_query("""
        MATCH (c:Column)-[r:MAPS_TO]->(stm:STM)
        OPTIONAL MATCH (d:Dataset)-[:HAS_COLUMN]->(c)
        OPTIONAL MATCH (j:Job)-[:READS_FROM|WRITES_TO]->(d)
        RETURN
          c.id              AS source_column_id,
          d.name            AS source_table,
          c.name            AS source_column,
          c.data_type       AS source_dtype,
          stm.id            AS stm_id,
          stm.target_table  AS target_table,
          stm.target_column AS target_column,
          stm.target_system AS target_system,
          stm.owner         AS owner,
          r.transform_expr  AS transform_expr,
          r.confidence      AS confidence,
          collect(DISTINCT
            j.name + ' (' + coalesce(j.domain, '?') + ':L' + toString(coalesce(j.line_start, 0)) + ')'
          ) AS used_by_functions
        ORDER BY source_table, source_column
    """)


async def get_stm_bridge(job_id: str) -> list:
    return await run_query("""
        MATCH (j:Job {id: $jid})-[rel:READS_FROM|WRITES_TO]->(d:Dataset)
              -[:HAS_COLUMN]->(c:Column)-[m:MAPS_TO]->(stm:STM)
        RETURN
          j.id              AS job_id,
          j.name            AS job_name,
          type(rel)         AS interaction,
          d.name            AS source_table,
          c.name            AS source_column,
          c.data_type       AS source_dtype,
          m.transform_expr  AS transform_expr,
          stm.target_table  AS target_table,
          stm.target_column AS target_column,
          stm.target_system AS target_system,
          stm.owner         AS owner
        ORDER BY source_table, source_column
    """, {"jid": job_id})


async def get_stm_full_lineage() -> list:
    return await run_query("""
        MATCH (c:Column)-[m:MAPS_TO]->(stm:STM)
        OPTIONAL MATCH (d:Dataset)-[:HAS_COLUMN]->(c)
        OPTIONAL MATCH (j:Job)-[rel:READS_FROM|WRITES_TO]->(d)
        WITH
          d.name                                   AS source_table,
          c.name                                   AS source_column,
          c.id                                     AS column_id,
          m.transform_expr                         AS transform_expr,
          stm.id                                   AS stm_id,
          stm.target_table                         AS target_table,
          stm.target_column                        AS target_column,
          stm.target_system                        AS target_system,
          type(rel)                                AS interaction,
          collect(DISTINCT j.name)                 AS function_names,
          collect(DISTINCT coalesce(j.path, j.domain, '?')) AS function_paths
        RETURN
          source_table, source_column, column_id,
          transform_expr, stm_id,
          target_table, target_column, target_system,
          interaction, function_names, function_paths
        ORDER BY target_table, target_column
    """)


# ---------------------------------------------------------------------------
# All-at-once graph endpoints (no per-job selection needed)
# ---------------------------------------------------------------------------

async def get_all_job_graph() -> dict:
    """All Job/Dataset/BusinessRule nodes and every READS_FROM/WRITES_TO/DEPENDS_ON/GOVERNED_BY edge."""
    nodes = await run_query("""
        MATCH (n) WHERE n:Job OR n:Dataset OR n:BusinessRule
        RETURN n.id AS id, n.name AS name, labels(n)[0] AS type,
               coalesce(n.path, n.domain, '') AS path,
               coalesce(n.risk_tags, [])       AS risk_tags
    """)
    edges = await run_query("""
        MATCH (a:Job)-[r:READS_FROM|WRITES_TO|DEPENDS_ON|GOVERNED_BY]->(b)
        WHERE b:Job OR b:Dataset OR b:BusinessRule
        RETURN a.id AS src, b.id AS tgt, type(r) AS rel
    """)
    return {"nodes": nodes, "edges": edges}


async def get_all_column_graph() -> dict:
    """All Column nodes that participate in DERIVED_FROM edges, plus those edges."""
    edges = await run_query("""
        MATCH (src:Column)-[r:DERIVED_FROM]->(tgt:Column)
        RETURN src.id AS src_id, tgt.id AS tgt_id, r.expression AS expression
    """)
    if not edges:
        return {"nodes": [], "edges": []}
    col_ids = list({e["src_id"] for e in edges} | {e["tgt_id"] for e in edges})
    nodes = await run_query("""
        MATCH (c:Column) WHERE c.id IN $ids
        OPTIONAL MATCH (d:Dataset)-[:HAS_COLUMN]->(c)
        RETURN c.id AS id, c.name AS name, d.name AS dataset, c.pii_flag AS pii
    """, {"ids": col_ids})
    return {"nodes": nodes, "edges": edges}


# ---------------------------------------------------------------------------
# Phase 2 -- Pipeline / Job Registry
# ---------------------------------------------------------------------------

async def register_pipeline(pipeline_id: str, name: str, script_id: str,
                             description: str = "") -> dict:
    await run_write("""
        MERGE (j:Job {id: $pid})
        ON CREATE SET
          j.name           = $name,
          j.description    = $description,
          j.type           = 'pipeline',
          j.registered_at  = timestamp(),
          j.status         = 'active'
        WITH j
        MATCH (s:Script {id: $sid})
        MERGE (s)-[:PART_OF]->(j)
    """, {"pid": pipeline_id, "name": name, "sid": script_id,
          "description": description})
    return {"pipeline_id": pipeline_id, "script_id": script_id,
            "status": "registered"}


async def list_pipelines() -> list:
    return await run_query("""
        MATCH (s:Script)-[:PART_OF]->(j:Job)
        WHERE j.type = 'pipeline'
        RETURN j.id AS pipeline_id, j.name AS name,
               j.description AS description,
               j.status AS status, j.registered_at AS registered_at,
               s.id AS script_id, s.name AS script_name, s.path AS script_path
        ORDER BY j.name
    """)


async def _run_pipeline_fields_queries(pid: str) -> tuple:
    reads = await run_query("""
        MATCH (j:Job {id: $pid})-[r:READS_FROM]->(d:Dataset)
        WHERE r.confidence = 'verified'
        OPTIONAL MATCH (d)-[:HAS_COLUMN]->(c:Column)
        OPTIONAL MATCH (c)-[:MAPS_TO]->(stm:STM)
        RETURN d.id AS id, d.name AS table, c.name AS column,
               c.data_type AS dtype, d.status AS status,
               stm.target_table AS stm_target_table,
               stm.target_column AS stm_target_column,
               stm.target_system AS target_system
        ORDER BY d.name, c.name
    """, {"pid": pid})
    writes = await run_query("""
        MATCH (j:Job {id: $pid})-[r:WRITES_TO]->(d:Dataset)
        WHERE r.confidence = 'verified'
        OPTIONAL MATCH (d)-[:HAS_COLUMN]->(c:Column)
        OPTIONAL MATCH (c)-[:MAPS_TO]->(stm:STM)
        RETURN d.id AS id, d.name AS table, c.name AS column,
               c.data_type AS dtype, d.status AS status,
               stm.target_table AS stm_target_table,
               stm.target_column AS stm_target_column,
               stm.target_system AS target_system
        ORDER BY d.name, c.name
    """, {"pid": pid})
    stm_targets = await run_query("""
        MATCH (j:Job {id: $pid})-[:READS_FROM|WRITES_TO]->(d:Dataset)
              -[:HAS_COLUMN]->(c:Column)-[m:MAPS_TO]->(stm:STM)
        RETURN DISTINCT stm.id AS stm_id, stm.target_table AS target_table,
               stm.target_column AS target_column,
               stm.target_system AS target_system,
               stm.owner AS owner, m.transform_expr AS transform_expr
        ORDER BY target_table, target_column
    """, {"pid": pid})
    meta_rows = await run_query("""
        MATCH (s:Script)-[:PART_OF]->(j:Job {id: $pid})
        RETURN j.id AS pipeline_id, j.name AS name,
               j.description AS description,
               s.id AS script_id, s.name AS script_name, s.path AS script_path
        LIMIT 1
    """, {"pid": pid})
    return reads, writes, stm_targets, meta_rows


async def resolve_pipeline_fields(pipeline_id: str) -> dict:
    reads, writes, stm_targets, meta_rows = \
        await _run_pipeline_fields_queries(pipeline_id)
    meta = meta_rows[0] if meta_rows else {}
    return {
        "pipeline": meta,
        "resolution": {
            "reads":               reads,
            "writes":              writes,
            "stm_targets":         stm_targets,
            "total_source_fields": len(reads),
            "total_target_fields": len(writes),
            "total_stm_targets":   len(stm_targets),
        },
    }
