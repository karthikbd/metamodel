from fastapi import APIRouter
from graph import queries
from graph.neo4j_client import ping

router = APIRouter()


@router.get("/")
async def get_stats():
    stats = await queries.get_graph_stats()
    db_online = await ping()
    return {"graph": stats, "neo4j_online": db_online}


@router.get("/neo4j")
async def neo4j_health():
    ok = await ping()
    return {"online": ok}
