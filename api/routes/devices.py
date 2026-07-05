"""
Device Telemetry REST Endpoints
=================================
Queries the TimescaleDB `telemetry` hypertable to serve device data.

Endpoints:
  GET /api/devices              — List all devices with last-seen timestamp
  GET /api/devices/{id}/latest  — Most recent reading from a device
  GET /api/devices/{id}/history — Paginated time-range query

Why these queries are fast:
  The telemetry table has two indexes (from schema.sql):
    idx_telemetry_device_time  → (device_id, time DESC)
    idx_telemetry_type_time    → (device_type, time DESC)
  All queries filter by device_id + ORDER BY time DESC → index-only scans.
  TimescaleDB's hypertable chunking means "last 1 hour" only scans ~1 chunk.

Data Flow:
  Client → FastAPI → asyncpg pool → TimescaleDB → asyncpg → Pydantic → JSON
"""

import time
import json
import logging
from datetime import datetime, timezone, timedelta

import structlog
from fastapi import APIRouter, Query, HTTPException

from api.database import get_pool
from api.models import DeviceInfo, TelemetryReading, TelemetryHistoryResponse
from api.metrics import db_query_duration, db_query_errors

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()

router = APIRouter(tags=["devices"])


# ─── GET /api/devices ────────────────────────────────────────────────
@router.get(
    "/devices",
    response_model=list[DeviceInfo],
    summary="List all devices",
    description=(
        "Returns all devices that have sent at least one telemetry reading, "
        "with their type and the timestamp of their most recent reading."
    ),
)
async def list_devices():
    """
    List all registered devices with their last-seen timestamp.

    Query logic:
      1. SELECT DISTINCT device_id, device_type from telemetry
      2. For each device, find MAX(time) as last_seen
      3. GROUP BY device_id, device_type
      4. ORDER BY last_seen DESC (most active devices first)

    Why GROUP BY and not a devices table?
      We don't have a separate devices table — devices are "discovered"
      from telemetry data. This is intentional: new device types can
      start publishing without any schema changes or registration.
    """
    pool = get_pool()
    start = time.monotonic()

    try:
        rows = await pool.fetch(
            """
            SELECT device_id, device_type, MAX(time) AS last_seen
            FROM telemetry
            GROUP BY device_id, device_type
            ORDER BY last_seen DESC
            """
        )
        duration = time.monotonic() - start
        db_query_duration.labels(query="list_devices").observe(duration)

        return [
            DeviceInfo(
                device_id=row["device_id"],
                device_type=row["device_type"],
                last_seen=row["last_seen"],
            )
            for row in rows
        ]

    except Exception as e:
        db_query_errors.labels(query="list_devices").inc()
        log.error("list_devices_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Database query failed")


# ─── GET /api/devices/{device_id}/latest ─────────────────────────────
@router.get(
    "/devices/{device_id}/latest",
    response_model=TelemetryReading,
    summary="Latest device reading",
    description=(
        "Returns the most recent telemetry reading from a specific device, "
        "including all sensor metrics as a JSON object."
    ),
)
async def get_latest_reading(device_id: str):
    """
    Get the most recent reading from a specific device.

    Query logic:
      SELECT * FROM telemetry
      WHERE device_id = $1
      ORDER BY time DESC
      LIMIT 1

    Uses idx_telemetry_device_time index → single index lookup.
    With TimescaleDB chunking, only scans the latest chunk (~1 day).
    """
    pool = get_pool()
    start = time.monotonic()

    try:
        row = await pool.fetchrow(
            """
            SELECT time, device_id, device_type, metrics
            FROM telemetry
            WHERE device_id = $1
            ORDER BY time DESC
            LIMIT 1
            """,
            device_id,
        )
        duration = time.monotonic() - start
        db_query_duration.labels(query="latest_reading").observe(duration)

        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No telemetry found for device '{device_id}'",
            )

        # metrics is stored as JSONB in TimescaleDB → asyncpg returns it as str
        metrics = row["metrics"]
        if isinstance(metrics, str):
            metrics = json.loads(metrics)

        return TelemetryReading(
            time=row["time"],
            device_id=row["device_id"],
            device_type=row["device_type"],
            metrics=metrics,
        )

    except HTTPException:
        raise  # Re-raise 404, don't catch it as a DB error
    except Exception as e:
        db_query_errors.labels(query="latest_reading").inc()
        log.error("latest_reading_failed", device_id=device_id, error=str(e))
        raise HTTPException(status_code=500, detail="Database query failed")


# ─── GET /api/devices/{device_id}/history ────────────────────────────
@router.get(
    "/devices/{device_id}/history",
    response_model=TelemetryHistoryResponse,
    summary="Device telemetry history",
    description=(
        "Returns paginated telemetry history for a device within a time range. "
        "Defaults to the last 1 hour with 100 results per page."
    ),
)
async def get_device_history(
    device_id: str,
    start: datetime | None = Query(
        default=None,
        description="Start time (ISO 8601). Defaults to 1 hour ago.",
        alias="start",
    ),
    end: datetime | None = Query(
        default=None,
        description="End time (ISO 8601). Defaults to now.",
        alias="end",
    ),
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
        description="Number of readings to return (max 1000).",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="Offset for pagination.",
    ),
):
    """
    Get paginated telemetry history for a device within a time range.

    Query logic:
      1. Count total matching rows (for pagination metadata)
      2. SELECT with LIMIT/OFFSET for the current page
      3. Return readings + pagination info

    Default time range: last 1 hour.
    This matches the most common dashboard use case: "show me recent data".

    Pagination uses LIMIT/OFFSET because:
      - Simple to implement and understand
      - Sufficient for our scale (130K rows/day max)
      - Cursor-based pagination would be needed at >10M rows/day

    Parameters
    ----------
    device_id : str
        Device identifier, e.g. "cnc-001".
    start : datetime
        Start of time range (default: 1 hour ago).
    end : datetime
        End of time range (default: now).
    limit : int
        Page size, 1–1000 (default: 100).
    offset : int
        Pagination offset (default: 0).
    """
    pool = get_pool()

    # Default time range: last 1 hour
    now = datetime.now(timezone.utc)
    if end is None:
        end = now
    if start is None:
        start = now - timedelta(hours=1)

    # Ensure timezone-aware (asyncpg requires it for TIMESTAMPTZ)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    query_start = time.monotonic()

    try:
        # Step 1: Count total matching rows
        total_row = await pool.fetchrow(
            """
            SELECT COUNT(*) AS total
            FROM telemetry
            WHERE device_id = $1 AND time >= $2 AND time <= $3
            """,
            device_id,
            start,
            end,
        )
        total = total_row["total"] if total_row else 0

        # Step 2: Fetch the current page
        rows = await pool.fetch(
            """
            SELECT time, device_id, device_type, metrics
            FROM telemetry
            WHERE device_id = $1 AND time >= $2 AND time <= $3
            ORDER BY time DESC
            LIMIT $4 OFFSET $5
            """,
            device_id,
            start,
            end,
            limit,
            offset,
        )

        duration = time.monotonic() - query_start
        db_query_duration.labels(query="device_history").observe(duration)

        readings = []
        for row in rows:
            metrics = row["metrics"]
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            readings.append(
                TelemetryReading(
                    time=row["time"],
                    device_id=row["device_id"],
                    device_type=row["device_type"],
                    metrics=metrics,
                )
            )

        return TelemetryHistoryResponse(
            device_id=device_id,
            readings=readings,
            total=total,
            limit=limit,
            offset=offset,
        )

    except Exception as e:
        db_query_errors.labels(query="device_history").inc()
        log.error("device_history_failed", device_id=device_id, error=str(e))
        raise HTTPException(status_code=500, detail="Database query failed")
