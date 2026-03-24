from fastapi import APIRouter
from graph import queries

router = APIRouter()

QUERY_REGISTRY = {
    "pii_without_audit":         queries.pii_without_audit,
    "regulatory_report_lineage": queries.regulatory_report_lineage,
    "deprecated_columns_in_use": queries.deprecated_columns_in_use,
}

LABEL_MAP = {
    "pii_without_audit":         "PII Without Audit",
    "regulatory_report_lineage": "Regulatory Report Lineage",
    "deprecated_columns_in_use": "Deprecated Columns In Use",
}


@router.get("/queries")
def list_queries():
    return [
        {"id": k, "label": LABEL_MAP.get(k, k.replace("_", " ").title())}
        for k in QUERY_REGISTRY
    ]


@router.get("/run/{query_id}")
async def run_compliance_query(query_id: str):
    if query_id not in QUERY_REGISTRY:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Unknown compliance query: {query_id}")
    rows = await QUERY_REGISTRY[query_id]()
    return {"query": query_id, "rows": rows, "count": len(rows)}
