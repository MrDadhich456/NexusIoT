"""
Async Database Connection Pool
===============================
Manages a pool of async connections to TimescaleDB using asyncpg.

Why asyncpg directly (not SQLAlchemy)?
  - All API queries are read-only SELECTs against the existing Step 7 schema
  - No ORM models, migrations, or relationships needed
  - asyncpg is ~3x faster than SQLAlchemy async for raw parameterised queries
  - Keeps the API container lightweight (~10MB vs ~50MB with SQLAlchemy)

Connection Pool Architecture:
  - min_size=2:  Always 2 connections ready → first request never waits
  - max_size=10: Handles burst traffic (e.g., dashboard loading 5 panels at once)
  - Pool auto-creates connections on demand between min and max
  - Connections are returned to pool after each request (via `async with`)

Data Flow:
  main.py lifespan startup → create_pool() → pool ready
  route handler → get_pool() → pool.fetch() → results
  main.py lifespan shutdown → close_pool() → connections drained
"""

import os
import logging
import asyncio

import asyncpg
import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()

# ─── Configuration ───────────────────────────────────────────────────
# 12-Factor: all config from environment variables.
# Default DSN points to the Docker Compose TimescaleDB service.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://nexusiot:nexusiot@localhost:5432/nexusiot",
)

# ─── Global Pool ─────────────────────────────────────────────────────
# Module-level variable holding the connection pool.
# Created once during FastAPI lifespan startup, used by all route handlers.
_pool: asyncpg.Pool | None = None


async def create_pool(
    dsn: str | None = None,
    min_size: int = 2,
    max_size: int = 10,
    max_retries: int = 10,
    retry_delay: float = 3.0,
) -> asyncpg.Pool:
    """
    Create and store the global asyncpg connection pool.

    Retries on failure because TimescaleDB may still be starting up
    when the API container boots (Docker Compose race condition).

    Parameters
    ----------
    dsn : str
        PostgreSQL connection string. Defaults to DATABASE_URL env var.
    min_size : int
        Minimum connections kept open (default: 2).
    max_size : int
        Maximum connections allowed (default: 10).
    max_retries : int
        Number of connection attempts before giving up (default: 10).
    retry_delay : float
        Seconds between retry attempts (default: 3.0).

    Returns
    -------
    asyncpg.Pool
        The created connection pool.

    Raises
    ------
    ConnectionError
        If all retry attempts fail.
    """
    global _pool

    dsn = dsn or DATABASE_URL

    for attempt in range(1, max_retries + 1):
        try:
            _pool = await asyncpg.create_pool(
                dsn=dsn,
                min_size=min_size,
                max_size=max_size,
                # Statement cache: asyncpg caches prepared statements per
                # connection. Our ~6 distinct queries benefit from this —
                # the query plan is compiled once, reused on every call.
                # Default is 1024, which is fine for our small query set.
            )
            log.info(
                "database_pool_created",
                min_size=min_size,
                max_size=max_size,
                dsn=_sanitize_dsn(dsn),
            )
            return _pool

        except (
            asyncpg.CannotConnectNowError,
            asyncpg.ConnectionDoesNotExistError,
            OSError,
            ConnectionRefusedError,
        ) as e:
            log.warning(
                "database_connection_retry",
                attempt=attempt,
                max_retries=max_retries,
                error=str(e),
            )
            if attempt < max_retries:
                await asyncio.sleep(retry_delay)

    raise ConnectionError(
        f"Could not connect to TimescaleDB after {max_retries} attempts"
    )


def get_pool() -> asyncpg.Pool:
    """
    Return the global connection pool.

    Called by route handlers to execute queries:
        pool = get_pool()
        rows = await pool.fetch("SELECT ...")

    Raises
    ------
    RuntimeError
        If called before create_pool() (lifespan not started).
    """
    if _pool is None:
        raise RuntimeError(
            "Database pool not initialised. "
            "Ensure create_pool() is called during lifespan startup."
        )
    return _pool


async def close_pool() -> None:
    """
    Close the connection pool and release all database connections.

    Called during FastAPI lifespan shutdown. Waits for active queries
    to complete before closing connections (graceful drain).
    """
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        log.info("database_pool_closed")


def _sanitize_dsn(dsn: str) -> str:
    """
    Remove the password from the DSN for safe logging.

    Input:  postgresql://nexusiot:secretpass@host:5432/nexusiot
    Output: postgresql://nexusiot:***@host:5432/nexusiot
    """
    if "://" in dsn and "@" in dsn:
        prefix = dsn.split("://")[0] + "://"
        after_scheme = dsn.split("://")[1]
        if "@" in after_scheme:
            user_pass = after_scheme.split("@")[0]
            rest = after_scheme.split("@")[1]
            if ":" in user_pass:
                user = user_pass.split(":")[0]
                return f"{prefix}{user}:***@{rest}"
    return dsn
