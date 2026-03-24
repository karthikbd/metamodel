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


@app.get("/api/config")
def get_config():
    """Return safe (non-secret) runtime configuration for the frontend."""
    return {
        "neo4j_uri":       active_uri() or settings.neo4j_uri,
        "repo_scan_root":  settings.repo_scan_root,
        "llm_enabled":     bool(settings.openai_api_key),
        "neo4j_connected": bool(active_uri()),
    }


@app.get("/api/neo4j-creds")
def get_neo4j_creds():
    """
    Return Neo4j connection credentials for NeoVis.js (browser Bolt connection).
    Intended for internal/dev use — guard with auth middleware in production.
    """
    return {
        "uri":      active_uri() or settings.neo4j_uri,
        "user":     settings.effective_user,
        "password": settings.neo4j_password,
        "database": settings.neo4j_database or None,
    }


@app.post("/api/shutdown")
def shutdown():
    os.kill(os.getpid(), signal.SIGTERM)
    return {"status": "shutting down"}
