"""
Phase 2 — Runtime consumption routes.

Registered pipelines query the knowledge graph at startup to resolve
source fields, transforms, and target columns dynamically — no hardcoded
column names in pipeline code.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from graph import queries
from graph.neo4j_client import run_query

router = APIRouter()


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class RegisterPipelineRequest(BaseModel):
    pipeline_id:  str
    name:         str
    script_id:    str
    description:  str = ""


# ---------------------------------------------------------------------------
# Demo seed data — representative pipelines from sample_repo
# ---------------------------------------------------------------------------

DEMO_PIPELINES = [
    {
        "pipeline_id":  "pipe::etl::customer_ingest",
        "name":         "Customer Ingest ETL",
        "description":  "Ingests customers from source CRM into staging tables. "
                        "Reads customer profile fields, writes to customer_staging.",
        "function_name": "run_customer_ingest",
    },
    {
        "pipeline_id":  "pipe::risk::credit_risk",
        "name":         "Credit Risk Calculator",
        "description":  "Calculates PD/LGD/EAD per customer. "
                        "Reads from risk_staging, writes capital_calc.",
        "function_name": "run_daily_risk_pipeline",
    },
    {
        "pipeline_id":  "pipe::reporting::basel3",
        "name":         "Basel III Capital Report",
        "description":  "Aggregates RWA across portfolios and computes CET1 ratio "
                        "for regulatory submission.",
        "function_name": "generate_pillar1_report",
    },
    {
        "pipeline_id":  "pipe::compliance::aml",
        "name":         "AML Screening",
        "description":  "Screens all transactions against AML watchlists. "
                        "PII-tagged function requiring audit trail.",
        "function_name": "run_daily_aml_screening",
    },
    {
        "pipeline_id":  "pipe::risk::market_risk",
        "name":         "Market Risk Engine",
        "description":  "Computes VaR and volatility from market data feed.",
        "function_name": "run_historical_var",
    },
]


async def _register_demo_pipelines() -> dict:
    """Find Scripts by function name and register them as Phase 2 pipelines."""
    registered = []
    skipped = []
    for p in DEMO_PIPELINES:
        rows = await run_query("""
            MATCH (s:Script) WHERE s.name = $name RETURN s.id AS sid LIMIT 1
        """, {"name": p["function_name"]})

        if not rows:
            skipped.append(p["function_name"])
            continue

        sid = rows[0]["sid"]
        await queries.register_pipeline(
            pipeline_id=p["pipeline_id"],
            name=p["name"],
            script_id=sid,
            description=p["description"],
        )
        registered.append({"pipeline_id": p["pipeline_id"], "script_id": sid})

    return {
        "status": "ok",
        "registered": registered,
        "skipped_scripts_not_found": skipped,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/seed")
async def seed_demo_pipelines():
    """
    Register the 5 representative pipelines from sample_repo.
    Links each Job node to the matching Script in the graph via PART_OF.
    Idempotent — safe to call multiple times.
    """
    result = await _register_demo_pipelines()
    return result


@router.post("/register")
async def register_pipeline(req: RegisterPipelineRequest):
    """Register a single pipeline manually."""
    # Check Script exists
    rows = await run_query(
        "MATCH (s:Script {id: $sid}) RETURN s.name AS name LIMIT 1",
        {"sid": req.script_id}
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Script '{req.script_id}' not found in the graph. "
                   "Run Phase 1 hydration first."
        )
    result = await queries.register_pipeline(
        pipeline_id=req.pipeline_id,
        name=req.name,
        script_id=req.script_id,
        description=req.description,
    )
    return result


@router.get("/pipelines")
async def list_pipelines():
    """List all registered pipelines."""
    rows = await queries.list_pipelines()
    return {"pipelines": rows, "total": len(rows)}


@router.get("/{pipeline_id}/resolve")
async def resolve_pipeline(pipeline_id: str):
    """
    Phase 2 field resolution — the core value proposition.

    Returns the complete field manifest for a pipeline:
      • source_reads  — tables/columns the function reads
      • source_writes — tables/columns the function writes
      • stm_targets   — downstream DW columns (via MAPS_TO edges)

    All filtered to confidence = 'verified' so inferred/uncertain edges
    never drive production pipelines.
    """
    rows = await run_query(
        "MATCH (j:Job {id: $pid}) WHERE j.type = 'pipeline' RETURN 1 AS exists LIMIT 1",
        {"pid": pipeline_id}
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Pipeline '{pipeline_id}' not registered. "
                   "Call POST /api/phase2/register or POST /api/phase2/seed first."
        )
    result = await queries.resolve_pipeline_fields(pipeline_id)
    return result


@router.get("/stats")
async def phase2_stats():
    """Count of registered pipelines and resolved fields."""
    try:
        rows = await run_query("""
            MATCH (p:Job) WHERE p.type = 'pipeline'
            WITH count(p) AS pipelines
            OPTIONAL MATCH (:Script)-[:PART_OF]->(j:Job) WHERE j.type = 'pipeline'
            RETURN pipelines, count(*) AS linked_scripts
        """)
        return rows[0] if rows else {"pipelines": 0, "linked_scripts": 0}
    except Exception:
        return {"pipelines": 0, "linked_scripts": 0,
                "error": "Neo4j unreachable — start Neo4j and re-run Phase 1"}
