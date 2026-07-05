"""
Anomaly Event REST Endpoints
==============================
Queries the TimescaleDB `anomaly_events` hypertable.

Endpoints:
  GET /api/anomalies         — List anomalies with filters
  GET /api/anomalies/explain — SHAP explanation for a specific anomaly
"""

import time
import json
import logging
from datetime import datetime, timezone, timedelta

import structlog
from fastapi import APIRouter, Query, HTTPException

from api.database import get_pool
from api.models import AnomalyEvent, AnomalyListResponse, SHAPExplanation
from api.metrics import db_query_duration, db_query_errors

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()

router = APIRouter(tags=["anomalies"])


def _parse_shap(raw):
    """Parse SHAP contributions from JSONB (may be None or str)."""
    if raw is None:
        return None
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


@router.get(
    "/anomalies", response_model=AnomalyListResponse, summary="List anomaly events"
)
async def list_anomalies(
    device_id: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    field: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """List anomaly events with optional filters (device, severity, field, time)."""
    pool = get_pool()

    now = datetime.now(timezone.utc)
    if end is None:
        end = now
    if start is None:
        start = now - timedelta(hours=24)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    query_start = time.monotonic()

    try:
        # Build dynamic WHERE clause with parameterised queries
        conditions = ["time >= $1", "time <= $2"]
        params: list = [start, end]
        idx = 3

        if device_id is not None:
            conditions.append(f"device_id = ${idx}")
            params.append(device_id)
            idx += 1
        if severity is not None:
            conditions.append(f"severity = ${idx}")
            params.append(severity)
            idx += 1
        if field is not None:
            conditions.append(f"field = ${idx}")
            params.append(field)
            idx += 1

        where = " AND ".join(conditions)

        # Count total
        total_row = await pool.fetchrow(
            f"SELECT COUNT(*) AS total FROM anomaly_events WHERE {where}",
            *params,
        )
        total = total_row["total"] if total_row else 0

        # Fetch page
        data_query = (
            f"SELECT time, detected_at, device_id, device_type, "
            f"field, value, mean, std, z_score, severity, shap_contributions "
            f"FROM anomaly_events WHERE {where} "
            f"ORDER BY time DESC LIMIT ${idx} OFFSET ${idx + 1}"
        )
        params.extend([limit, offset])
        rows = await pool.fetch(data_query, *params)

        db_query_duration.labels(query="list_anomalies").observe(
            time.monotonic() - query_start
        )

        anomalies = [
            AnomalyEvent(
                time=r["time"],
                detected_at=r["detected_at"],
                device_id=r["device_id"],
                device_type=r["device_type"],
                field=r["field"],
                value=r["value"],
                mean=r["mean"],
                std=r["std"],
                z_score=r["z_score"],
                severity=r["severity"],
                shap_contributions=_parse_shap(r["shap_contributions"]),
            )
            for r in rows
        ]

        return AnomalyListResponse(
            anomalies=anomalies,
            total=total,
            limit=limit,
            offset=offset,
        )

    except Exception as e:
        db_query_errors.labels(query="list_anomalies").inc()
        log.error("list_anomalies_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Database query failed")


@router.get(
    "/anomalies/explain",
    response_model=SHAPExplanation,
    summary="SHAP explanation for an anomaly",
)
async def explain_anomaly(
    device_id: str = Query(description="Device ID, e.g. 'cnc-001'"),
    event_time: datetime = Query(
        alias="time", description="Anomaly timestamp (ISO 8601)"
    ),
    field: str = Query(description="Metric field, e.g. 'vibration_g'"),
):
    """
    Get SHAP feature contribution percentages for a specific anomaly.

    Identifies the anomaly by (device_id, time, field) — the natural
    composite key. Returns per-feature percentages explaining WHY
    the anomaly was flagged.
    """
    pool = get_pool()
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)

    query_start = time.monotonic()
    try:
        row = await pool.fetchrow(
            """
            SELECT device_id, time, field, severity, shap_contributions
            FROM anomaly_events
            WHERE device_id = $1 AND time = $2 AND field = $3
            LIMIT 1
            """,
            device_id,
            event_time,
            field,
        )
        db_query_duration.labels(query="explain_anomaly").observe(
            time.monotonic() - query_start
        )

        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No anomaly found for device '{device_id}' "
                f"at {event_time.isoformat()} on field '{field}'",
            )

        return SHAPExplanation(
            device_id=row["device_id"],
            time=row["time"],
            field=row["field"],
            severity=row["severity"],
            shap_contributions=_parse_shap(row["shap_contributions"]),
        )

    except HTTPException:
        raise
    except Exception as e:
        db_query_errors.labels(query="explain_anomaly").inc()
        log.error("explain_anomaly_failed", device_id=device_id, error=str(e))
        raise HTTPException(status_code=500, detail="Database query failed")
