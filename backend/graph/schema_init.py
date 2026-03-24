"""
Apply DataLineageMetaModel v3 constraints and indexes to Neo4j.
Safe to run multiple times — all statements use IF NOT EXISTS.
Called automatically on backend startup.
"""
import logging

from graph.neo4j_client import run_write

log = logging.getLogger(__name__)

# ── UNIQUE constraints (from meta_modelv3.json) ─────────────────────────────
CONSTRAINTS = [
    ("DataSource",     "id"),
    ("Dataset",        "id"),
    ("Column",         "id"),
    ("Script",         "id"),
    ("Job",            "id"),
    ("Transformation", "id"),
    ("Dashboard",      "id"),
    ("Alias",          "id"),
    ("LLMSummary",     "id"),
    ("BusinessRule",   "id"),
    ("PipelineRun",    "id"),
]

# ── Indexes (from meta_modelv3.json) ─────────────────────────────────────────
INDEXES = [
    ("Dataset",        "name"),
    ("Column",         "name"),
    ("Job",            "name"),
    ("Script",         "name"),
    ("Transformation", "type"),
    ("Dashboard",      "name"),
    ("DataSource",     "name"),
    ("Alias",          "name"),
    ("Job",            "status"),
    ("Dataset",        "status"),
    ("Column",         "pii_flag"),
]


async def apply_schema() -> dict:
    """
    Create all v3 constraints and indexes if they don't already exist.
    Returns a summary dict with counts of applied statements.
    """
    applied = 0
    skipped = 0

    for label, prop in CONSTRAINTS:
        name = f"constraint_{label.lower()}_{prop}"
        cypher = (
            f"CREATE CONSTRAINT {name} IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE"
        )
        try:
            await run_write(cypher)
            applied += 1
        except Exception as exc:
            log.debug("Constraint %s skipped: %s", name, exc)
            skipped += 1

    for label, prop in INDEXES:
        name = f"idx_{label.lower()}_{prop}"
        cypher = (
            f"CREATE INDEX {name} IF NOT EXISTS "
            f"FOR (n:{label}) ON (n.{prop})"
        )
        try:
            await run_write(cypher)
            applied += 1
        except Exception as exc:
            log.debug("Index %s skipped: %s", name, exc)
            skipped += 1

    log.info("Schema init complete: %d applied, %d already existed", applied, skipped)
    return {"applied": applied, "skipped": skipped}
