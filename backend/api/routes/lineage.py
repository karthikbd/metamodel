from fastapi import APIRouter
from graph import queries

router = APIRouter()


@router.get("/job/{job_id}")
async def get_job_lineage(job_id: str):
    return await queries.get_function_lineage(job_id)


@router.get("/column")
async def get_column_lineage(table: str, column: str):
    rows = await queries.get_column_lineage(table, column)
    return rows


@router.get("/column-graph")
async def get_column_lineage_graph(dataset: str, column: str):
    """Graph-friendly column lineage: upstream sources + downstream derived columns."""
    return await queries.get_column_lineage_graph(dataset, column)


@router.get("/datasets-columns")
async def get_datasets_with_columns():
    """All datasets with their columns — used to populate the column lineage selector."""
    return await queries.get_datasets_with_columns()


@router.get("/all-column-lineage")
async def get_all_column_lineage():
    """All DERIVED_FROM relationships — full column lineage overview without needing a selector."""
    rows = await queries.get_all_column_lineage_summary()
    return {"chains": rows, "total": len(rows)}


@router.get("/all-functional-lineage")
async def get_all_functional_lineage():
    """All DEPENDS_ON relationships between Job nodes — full functional call-chain overview."""
    rows = await queries.get_all_functional_lineage_summary()
    return {"chains": rows, "total": len(rows)}


@router.get("/all-job-graph")
async def get_all_job_graph():
    """All Job/Dataset/BusinessRule nodes + all edges — render the full process & data graph at once."""
    return await queries.get_all_job_graph()


@router.get("/all-column-graph")
async def get_all_column_graph():
    """All Column nodes + DERIVED_FROM edges — render the full column lineage graph at once."""
    return await queries.get_all_column_graph()
