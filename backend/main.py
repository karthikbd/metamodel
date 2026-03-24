import logging
import os
import signal
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import pipelines, runs, graph, lineage, compliance, impact, stats, stm, phase2
from config import settings
from graph import schema_init
from graph.neo4j_client import active_uri, close_driver

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────────────────
    try:
        result = await schema_init.apply_schema()
        log.info("Neo4j schema ready (active: %s) — %s", active_uri(), result)
    except Exception as exc:
        log.warning("Neo4j schema init skipped (no DB available): %s", exc)
    yield
    # ── Shutdown ──────────────────────────────────────────────────────────
    await close_driver()


app = FastAPI(
    title="Meta Model Engine API",
    description="Living graph database of code, data, and business rules",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pipelines.router, prefix="/api/pipeline", tags=["Pipeline"])
app.include_router(runs.router,      prefix="/api/runs",     tags=["Runs"])
app.include_router(graph.router,     prefix="/api/graph",    tags=["Graph"])
app.include_router(lineage.router,   prefix="/api/lineage",  tags=["Lineage"])
app.include_router(compliance.router,prefix="/api/compliance",tags=["Compliance"])
app.include_router(impact.router,    prefix="/api/impact",   tags=["Impact"])
app.include_router(stats.router,     prefix="/api/stats",    tags=["Stats"])
app.include_router(stm.router,       prefix="/api/stm",      tags=["STM"])
app.include_router(phase2.router,    prefix="/api/phase2",   tags=["Phase2"])


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "meta-model-engine",
        "neo4j": active_uri() or "not connected yet",
    }


@app.post("/api/shutdown")
def shutdown():
    os.kill(os.getpid(), signal.SIGTERM)
    return {"status": "shutting down"}
