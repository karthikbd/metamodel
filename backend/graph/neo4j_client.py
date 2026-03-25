import logging

from neo4j import AsyncGraphDatabase, AsyncDriver
from neo4j.exceptions import ServiceUnavailable, SessionExpired
from config import settings

log = logging.getLogger(__name__)

_driver: AsyncDriver | None = None
_active_uri: str | None = None

# AuraDB drops idle connections after ~120 s.
# Keep max_connection_lifetime well below that so the pool never hands out a
# dead socket.  liveness_check_timeout=0 means "always verify before use".
_DRIVER_KWARGS = {
    "max_connection_lifetime":   120,   # seconds — retire connections before AuraDB kills them
    "max_connection_pool_size":  10,
    "connection_timeout":        15,    # seconds to establish a new connection
    "liveness_check_timeout":    0,     # always ping the connection before use (0 = always)
}


async def _make_driver(uri: str, user: str, password: str, database: str = "") -> AsyncDriver:
    """Create and verify an async Neo4j driver."""
    driver = AsyncGraphDatabase.driver(uri, auth=(user, password), **_DRIVER_KWARGS)
    async with driver.session(**({"database": database} if database else {})) as s:
        await s.run("RETURN 1")
    return driver


async def _reset_driver() -> None:
    """Close and discard the cached driver so the next call recreates it."""
    global _driver
    if _driver is not None:
        try:
            await _driver.close()
        except Exception:
            pass
        _driver = None


async def get_driver() -> AsyncDriver:
    global _driver, _active_uri
    if _driver is not None:
        return _driver

    # ── Primary: AuraDB ───────────────────────────────────────────────────
    aura_exc: Exception | None = None
    try:
        _driver = await _make_driver(
            settings.neo4j_uri,
            settings.effective_user,
            settings.neo4j_password,
            settings.neo4j_database,
        )
        _active_uri = settings.neo4j_uri
        log.info("Neo4j connected → AuraDB (%s)", settings.neo4j_uri)
        return _driver
    except Exception as exc:
        aura_exc = exc
        log.warning("AuraDB unavailable (%s) — trying local Docker Neo4j …", exc)

    # ── Fallback: local Docker Neo4j ──────────────────────────────────────
    try:
        _driver = await _make_driver(
            settings.neo4j_local_uri,
            settings.neo4j_local_user,
            settings.neo4j_local_password,
        )
        _active_uri = settings.neo4j_local_uri
        log.info("Neo4j connected → local Docker (%s)", settings.neo4j_local_uri)
        return _driver
    except Exception as local_exc:
        log.error("Local Neo4j also unavailable: %s", local_exc)
        raise RuntimeError(
            f"No Neo4j available. AuraDB error: {aura_exc}  |  Local error: {local_exc}"
        ) from local_exc


async def close_driver():
    global _driver
    if _driver:
        await _driver.close()
        _driver = None


def _session_kwargs() -> dict:
    """Return database kwarg when a specific database name is configured (AuraDB)."""
    # Only pass 'database' for AuraDB; local Docker uses the default db.
    if _active_uri and _active_uri == settings.neo4j_uri and settings.neo4j_database:
        return {"database": settings.neo4j_database}
    return {}


async def run_query(cypher: str, params: dict | None = None) -> list[dict]:
    """Execute a read query and return rows as plain dicts.

    Automatically retries once if the driver returns a defunct/stale connection
    (ServiceUnavailable / SessionExpired).  This transparently handles the case
    where AuraDB has dropped an idle connection from the pool.
    """
    for attempt in range(2):
        try:
            driver = await get_driver()
            async with driver.session(**_session_kwargs()) as session:
                result = await session.run(cypher, params or {})
                return await result.data()
        except (ServiceUnavailable, SessionExpired) as exc:
            log.warning("Neo4j connection defunct (attempt %d): %s", attempt + 1, exc)
            await _reset_driver()
            if attempt == 1:
                raise
    return []  # unreachable — keeps type-checker happy


async def run_write(cypher: str, params: dict | None = None) -> list[dict]:
    """Execute a write query inside an explicit write transaction.

    Same auto-reconnect retry semantics as run_query.
    """
    for attempt in range(2):
        try:
            driver = await get_driver()
            async with driver.session(**_session_kwargs()) as session:
                result = await session.run(cypher, params or {})
                return await result.data()
        except (ServiceUnavailable, SessionExpired) as exc:
            log.warning("Neo4j connection defunct (attempt %d): %s", attempt + 1, exc)
            await _reset_driver()
            if attempt == 1:
                raise
    return []  # unreachable — keeps type-checker happy


async def ping() -> bool:
    """Return True if Neo4j is reachable."""
    try:
        await run_query("RETURN 1 AS ok")
        return True
    except Exception:
        return False


def active_uri() -> str | None:
    """Return the URI of the currently connected Neo4j instance."""
    return _active_uri

