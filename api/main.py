"""
FastAPI Application — NexusIoT API Gateway
=============================================
The public-facing API for the NexusIoT platform. Provides:
  - REST endpoints for historical queries (TimescaleDB)
  - WebSocket endpoints for real-time live streaming (Kafka)
  - Prometheus metrics endpoint for observability
  - Health check for Kubernetes liveness/readiness probes

Lifespan Flow:
  Startup:
    1. Connect to TimescaleDB (asyncpg connection pool)
    2. Start Kafka WebSocket fan-out consumer (background task)
    3. Record start time for uptime tracking

  Shutdown:
    1. Stop Kafka fan-out consumer
    2. Close database connection pool
    3. Log final stats

Entry Point:
  uvicorn api.main:app --host 0.0.0.0 --port 8000
"""

import time
import logging
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from api import database
from api.routes import devices, anomalies, ws
from api.models import HealthStatus

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()

# ─── Startup timestamp (for uptime tracking) ────────────────────────
_start_time: float = 0.0


# ─── Lifespan Manager ───────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle — startup and shutdown.

    FastAPI's lifespan replaces the deprecated @app.on_event() pattern.
    Everything before `yield` runs on startup; after `yield` on shutdown.
    """
    global _start_time

    # ── Startup ──────────────────────────────────────────────────
    log.info("api_starting")

    # 1. Connect to TimescaleDB
    await database.create_pool()

    # 2. Start Kafka → WebSocket fan-out consumer
    await ws.start_kafka_consumer()

    # 3. Record start time
    _start_time = time.time()
    log.info("api_ready", port=8000)

    yield

    # ── Shutdown ─────────────────────────────────────────────────
    log.info("api_shutting_down")

    # 1. Stop Kafka consumer
    await ws.stop_kafka_consumer()

    # 2. Close database pool
    await database.close_pool()

    log.info("api_stopped")


# ─── FastAPI App ─────────────────────────────────────────────────────
app = FastAPI(
    title="NexusIoT API",
    description=(
        "Production-grade Industrial IoT API with real-time WebSocket streaming, "
        "SHAP-powered explainable anomaly detection, and time-series queries."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ─── CORS Middleware ─────────────────────────────────────────────────
# Allow all origins for development. In production, restrict to
# specific dashboard domains.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Mount Route Groups ─────────────────────────────────────────────
# REST endpoints under /api prefix
app.include_router(devices.router, prefix="/api")
app.include_router(anomalies.router, prefix="/api")

# WebSocket endpoints at root (/ws/telemetry/{id}, /ws/anomalies)
app.include_router(ws.router)

# Prometheus metrics endpoint (ASGI sub-app)
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


# ─── Health Check ────────────────────────────────────────────────────
@app.get(
    "/health", response_model=HealthStatus, tags=["system"], summary="Health check"
)
async def health_check():
    """
    Liveness/readiness probe for Kubernetes.

    Checks:
      1. Database: Can we execute a simple query?
      2. Kafka: Is the fan-out consumer task still running?

    Returns 200 with status details. Kubernetes uses this to
    determine if the pod should receive traffic (readiness) and
    if it should be restarted (liveness).
    """
    # Check database
    db_status = "disconnected"
    try:
        pool = database.get_pool()
        await pool.fetchval("SELECT 1")
        db_status = "connected"
    except Exception:
        db_status = "disconnected"

    # Check Kafka consumer task
    kafka_status = "connected"
    if ws._consumer_task is None or ws._consumer_task.done():
        kafka_status = "disconnected"

    # Overall status
    status = "healthy" if db_status == "connected" else "degraded"

    return HealthStatus(
        status=status,
        database=db_status,
        kafka=kafka_status,
        uptime_seconds=round(time.time() - _start_time, 1),
    )
