from fastapi import APIRouter
from graph import queries

router = APIRouter()


@router.get("/column")
async def column_impact(table: str, column: str):
    rows = await queries.column_impact(table, column)
    return {"table": table, "column": column, "affected": rows, "count": len(rows)}


@router.get("/dataset")
async def dataset_impact(name: str):
    rows = await queries.dataset_impact(name)
    return {"dataset": name, "result": rows}
