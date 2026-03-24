"""
STM — Source-to-Target Mapping routes.

Bridges the data-graph (Column nodes) to an explicit target schema
(STM nodes) via MAPS_TO edges.  v3 ontology: Column → MAPS_TO → STM.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from graph import queries
from graph.neo4j_client import run_query, run_write

router = APIRouter()


# ---------------------------------------------------------------------------
# Seed payload shape
# ---------------------------------------------------------------------------

class STMMappingItem(BaseModel):
    source_table:   str
    source_column:  str
    target_table:   str
    target_column:  str
    target_system:  str = "data_warehouse"
    transform_expr: str = ""
    owner:          str = "data_engineering"


class SeedRequest(BaseModel):
    mappings: list[STMMappingItem]


# ---------------------------------------------------------------------------
# Default seed data aligned with sample_repo tables
# ---------------------------------------------------------------------------

DEFAULT_MAPPINGS: list[dict] = [
    # customer_master → customer_dw
    {"source_table": "customer_master", "source_column": "customer_id",        "target_table": "customer_dw", "target_column": "cust_id",               "target_system": "data_warehouse",   "transform_expr": "CAST(customer_id AS VARCHAR(36))",              "owner": "data_engineering"},
    {"source_table": "customer_master", "source_column": "customer_name",      "target_table": "customer_dw", "target_column": "full_name",             "target_system": "data_warehouse",   "transform_expr": "UPPER(customer_name)",                          "owner": "data_engineering"},
    {"source_table": "customer_master", "source_column": "email",              "target_table": "customer_dw", "target_column": "email_address",         "target_system": "data_warehouse",   "transform_expr": "LOWER(email)",                                  "owner": "data_engineering"},
    {"source_table": "customer_master", "source_column": "ssn",               "target_table": "customer_dw", "target_column": "ssn_encrypted",         "target_system": "data_warehouse",   "transform_expr": "AES_ENCRYPT(ssn, vault_key)",                   "owner": "security_team"},
    {"source_table": "customer_master", "source_column": "phone",             "target_table": "customer_dw", "target_column": "phone_masked",          "target_system": "data_warehouse",   "transform_expr": "CONCAT('XXX-XXX-', RIGHT(phone,4))",             "owner": "data_engineering"},
    {"source_table": "customer_master", "source_column": "kyc_status",        "target_table": "customer_dw", "target_column": "kyc_status_cd",         "target_system": "data_warehouse",   "transform_expr": "UPPER(kyc_status)",                             "owner": "compliance_team"},
    # risk_scores → risk_dw
    {"source_table": "risk_scores", "source_column": "credit_score",          "target_table": "risk_dw", "target_column": "credit_score_band",         "target_system": "risk_warehouse",   "transform_expr": "CASE WHEN credit_score>=750 THEN 'A' WHEN credit_score>=650 THEN 'B' ELSE 'C' END", "owner": "risk_team"},
    {"source_table": "risk_scores", "source_column": "pd_estimate",           "target_table": "risk_dw", "target_column": "probability_default",       "target_system": "risk_warehouse",   "transform_expr": "ROUND(pd_estimate, 6)",                         "owner": "risk_team"},
    {"source_table": "risk_scores", "source_column": "lgd_estimate",          "target_table": "risk_dw", "target_column": "loss_given_default",        "target_system": "risk_warehouse",   "transform_expr": "ROUND(lgd_estimate, 6)",                        "owner": "risk_team"},
    {"source_table": "risk_scores", "source_column": "ead",                   "target_table": "risk_dw", "target_column": "exposure_at_default",       "target_system": "risk_warehouse",   "transform_expr": "ead * conversion_factor",                       "owner": "risk_team"},
    # transactions → transaction_dw
    {"source_table": "transactions", "source_column": "amount",               "target_table": "transaction_dw", "target_column": "transaction_amount_usd", "target_system": "data_warehouse",  "transform_expr": "amount * fx_rate_usd",                        "owner": "data_engineering"},
    {"source_table": "transactions", "source_column": "currency",             "target_table": "transaction_dw", "target_column": "base_currency",         "target_system": "data_warehouse",  "transform_expr": "currency",                                    "owner": "data_engineering"},
    {"source_table": "transactions", "source_column": "account_id",           "target_table": "transaction_dw", "target_column": "account_key",           "target_system": "data_warehouse",  "transform_expr": "HASHBYTES(account_id)",                       "owner": "data_engineering"},
    # market_data → market_dw
    {"source_table": "market_data", "source_column": "spot_rate",             "target_table": "market_dw", "target_column": "spot_rate_close",          "target_system": "mds_warehouse",    "transform_expr": "ROUND(spot_rate, 6)",                           "owner": "quant_team"},
    {"source_table": "market_data", "source_column": "volatility",            "target_table": "market_dw", "target_column": "vol_30d",                  "target_system": "mds_warehouse",    "transform_expr": "ANNUALISE(volatility, 30)",                     "owner": "quant_team"},
    {"source_table": "market_data", "source_column": "var_95",                "target_table": "market_dw", "target_column": "value_at_risk_95",         "target_system": "mds_warehouse",    "transform_expr": "var_95",                                        "owner": "quant_team"},
    # capital_requirements → regulatory_dw
    {"source_table": "capital_requirements", "source_column": "rwa_credit",   "target_table": "report_dw", "target_column": "risk_weighted_assets",     "target_system": "regulatory_reporting", "transform_expr": "SUM(rwa_credit) OVER(PARTITION BY entity)",  "owner": "risk_team"},
    {"source_table": "capital_requirements", "source_column": "rwa_market",   "target_table": "report_dw", "target_column": "market_rwa",               "target_system": "regulatory_reporting", "transform_expr": "rwa_market",                                "owner": "risk_team"},
    {"source_table": "capital_requirements", "source_column": "cet1_ratio",   "target_table": "report_dw", "target_column": "cet1_ratio",               "target_system": "regulatory_reporting", "transform_expr": "tier1_capital / rwa",                        "owner": "risk_team"},
    # var_results → mds regulatory
    {"source_table": "var_results", "source_column": "var_95",                "target_table": "market_dw", "target_column": "var_95_daily",             "target_system": "mds_warehouse",    "transform_expr": "var_95",                                        "owner": "quant_team"},
    {"source_table": "var_results", "source_column": "stressed_var",          "target_table": "market_dw", "target_column": "stressed_var_10d",         "target_system": "mds_warehouse",    "transform_expr": "stressed_var * SQRT(10)",                       "owner": "quant_team"},
]


def _stm_node_id(target_system: str, target_table: str, target_column: str) -> str:
    return f"stm::{target_system}::{target_table}::{target_column}"


async def _seed_from_list(mappings_data: list[dict]) -> dict:
    """
    For each mapping, find the Column node (via parent Dataset) and create
    the STM node and MAPS_TO edge.  Mappings with no matching Column are
    still created as orphan STM nodes (useful as targets even before
    Phase-1 runs).
    """
    seeded = 0
    orphaned = 0
    for m in mappings_data:
        stm_id = _stm_node_id(m["target_system"], m["target_table"], m["target_column"])
        # Upsert STM node
        await run_write("""
            MERGE (stm:STM {id: $stm_id})
            ON CREATE SET
              stm.target_table  = $target_table,
              stm.target_column = $target_column,
              stm.target_system = $target_system,
              stm.owner         = $owner,
              stm.created_at    = timestamp()
            ON MATCH SET
              stm.target_table  = $target_table,
              stm.target_column = $target_column
        """, {
            "stm_id":         stm_id,
            "target_table":   m["target_table"],
            "target_column":  m["target_column"],
            "target_system":  m["target_system"],
            "owner":          m.get("owner", "data_engineering"),
        })

        # Try to link existing Column nodes (match via parent Dataset → Column)
        rows = await run_query("""
            MATCH (d:Dataset {name: $src_table})-[:HAS_COLUMN]->(c:Column {name: $src_col})
            RETURN c.id AS cid
        """, {"src_table": m["source_table"], "src_col": m["source_column"]})

        if rows:
            for row in rows:
                await run_write("""
                    MATCH (c:Column {id: $cid})
                    MATCH (stm:STM {id: $stm_id})
                    MERGE (c)-[r:MAPS_TO]->(stm)
                    ON CREATE SET
                      r.transform_expr = $transform_expr,
                      r.confidence     = 'verified',
                      r.created_at     = timestamp()
                """, {
                    "cid":            row["cid"],
                    "stm_id":         stm_id,
                    "transform_expr": m.get("transform_expr", ""),
                })
            seeded += 1
        else:
            orphaned += 1

    return {
        "status": "ok",
        "stm_nodes_created_or_updated": seeded + orphaned,
        "edges_linked_to_schema": seeded,
        "orphaned_stm_nodes": orphaned,
        "note": "Orphaned STM nodes exist as targets even without matching Column nodes."
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/seed")
async def seed_stm_default():
    """
    Seed the default STM mappings derived from sample_repo tables.
    Idempotent — safe to call multiple times.
    """
    result = await _seed_from_list(DEFAULT_MAPPINGS)
    return result


@router.post("/seed/custom")
async def seed_stm_custom(req: SeedRequest):
    """Seed custom STM mappings provided in the request body."""
    data = [m.model_dump() for m in req.mappings]
    result = await _seed_from_list(data)
    return result


@router.get("/mappings")
async def get_stm_mappings():
    """Return all Column → STM MAPS_TO edges."""
    rows = await queries.list_stm_mappings()
    return {"mappings": rows, "total": len(rows)}


@router.get("/lineage")
async def get_stm_full_lineage():
    """Full code→STM lineage: Job → READS_FROM/WRITES_TO → Dataset → Column → MAPS_TO → STM."""
    rows = await queries.get_stm_full_lineage()
    # Group by target system for easier rendering
    systems: dict = {}
    for r in rows:
        ts = r.get("target_system", "unknown")
        systems.setdefault(ts, []).append(r)
    return {"lineage": rows, "by_system": systems, "total": len(rows)}


@router.get("/bridge/{job_id}")
async def get_job_stm_bridge(job_id: str):
    """Trace one Job through its reads/writes → Dataset → Column → STM target."""
    rows = await queries.get_stm_bridge(job_id)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No STM bridge found for job '{job_id}'. "
                   "Run POST /api/stm/seed first, then trigger Phase 1 hydration."
        )
    return {"job_id": job_id, "bridge": rows, "total": len(rows)}


@router.get("/stats")
async def get_stm_stats():
    """Count of STM nodes, MAPS_TO edges, and mapped Column nodes."""
    try:
        rows = await run_query("""
            MATCH (stm:STM)
            WITH count(stm) AS stm_nodes
            OPTIONAL MATCH ()-[r:MAPS_TO]->(:STM)
            WITH stm_nodes, count(r) AS maps_to_edges
            OPTIONAL MATCH (c:Column)-[:MAPS_TO]->(:STM)
            RETURN stm_nodes, maps_to_edges, count(DISTINCT c) AS mapped_schema_objects
        """)
        if rows:
            return rows[0]
        return {"stm_nodes": 0, "maps_to_edges": 0, "mapped_schema_objects": 0}
    except Exception:
        # Neo4j may not be running
        return {"stm_nodes": 0, "maps_to_edges": 0, "mapped_schema_objects": 0,
                "error": "Neo4j unreachable — start Neo4j and re-run Phase 1 pipeline"}
