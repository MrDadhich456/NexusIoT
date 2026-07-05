"""
API Response Models (Pydantic v2)
==================================
Type-safe response schemas for all REST endpoints.

Why Pydantic models for responses?
  - FastAPI uses these for automatic OpenAPI documentation
  - Guarantees consistent JSON structure for frontend consumers
  - Type validation catches serialisation bugs at the API boundary
  - response_model= parameter handles null-safety and field exclusion

These models mirror the TimescaleDB schema defined in schema.sql (Step 7)
but add API-specific metadata (e.g., pagination info, ISO timestamps).
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ─── Device Endpoints ────────────────────────────────────────────────


class DeviceInfo(BaseModel):
    """
    Summary info for a device — returned by GET /api/devices.

    Fields are derived from:
      SELECT DISTINCT device_id, device_type, MAX(time) as last_seen
      FROM telemetry GROUP BY device_id, device_type

    Example response:
      {
        "device_id": "cnc-001",
        "device_type": "cnc_machine",
        "last_seen": "2026-06-23T09:00:00Z"
      }
    """

    device_id: str = Field(description="Unique device identifier, e.g. 'cnc-001'")
    device_type: str = Field(description="Device type string, e.g. 'cnc_machine'")
    last_seen: datetime = Field(description="Timestamp of the most recent reading")


class TelemetryReading(BaseModel):
    """
    Single telemetry reading — returned by GET /api/devices/{id}/latest
    and GET /api/devices/{id}/history.

    Maps directly to one row in the telemetry hypertable.

    Example response:
      {
        "time": "2026-06-23T09:00:00Z",
        "device_id": "cnc-001",
        "device_type": "cnc_machine",
        "metrics": {
          "spindle_rpm": 3002.5,
          "vibration_g": 0.81,
          "tool_wear_pct": 12.3,
          "feed_rate_mmpm": 498.7,
          "cycle_count": 1542,
          "cutting_temp_c": 285.2
        }
      }
    """

    time: datetime = Field(description="When the device recorded this reading")
    device_id: str = Field(description="Device identifier")
    device_type: str = Field(description="Device type")
    metrics: dict[str, Any] = Field(
        description="All sensor values as key-value pairs (JSONB from TimescaleDB)"
    )


class TelemetryHistoryResponse(BaseModel):
    """
    Paginated telemetry history — returned by GET /api/devices/{id}/history.

    Includes pagination metadata so the frontend knows if there are
    more pages to fetch.

    Example response:
      {
        "device_id": "cnc-001",
        "readings": [...],
        "total": 1542,
        "limit": 100,
        "offset": 0
      }
    """

    device_id: str
    readings: list[TelemetryReading]
    total: int = Field(description="Total number of readings matching the query")
    limit: int = Field(description="Page size used for this query")
    offset: int = Field(description="Offset used for this query")


# ─── Anomaly Endpoints ───────────────────────────────────────────────


class AnomalyEvent(BaseModel):
    """
    Single anomaly event — returned by GET /api/anomalies.

    Maps directly to one row in the anomaly_events hypertable.

    Example response:
      {
        "time": "2026-06-23T09:00:00Z",
        "detected_at": "2026-06-23T09:00:00.15Z",
        "device_id": "cnc-001",
        "device_type": "cnc_machine",
        "field": "vibration_g",
        "value": 6.48,
        "mean": 0.82,
        "std": 0.04,
        "z_score": 141.5,
        "severity": "critical",
        "shap_contributions": {
          "spindle_rpm": 0.68,
          "vibration_g": 0.22,
          "cutting_temp_c": 0.07,
          "tool_wear_pct": 0.02,
          "feed_rate_mmpm": 0.01
        }
      }
    """

    time: datetime = Field(description="When the device recorded the anomalous reading")
    detected_at: datetime = Field(description="When the processor flagged it")
    device_id: str = Field(description="Device identifier")
    device_type: str = Field(description="Device type")
    field: str = Field(description="Which metric triggered, e.g. 'vibration_g'")
    value: float = Field(description="The actual anomalous reading")
    mean: float = Field(description="Sliding window mean at time of detection")
    std: float = Field(description="Sliding window std deviation")
    z_score: float = Field(description="How many σ from normal")
    severity: str = Field(description="'warning' (z>3.5) or 'critical' (z>5.0)")
    shap_contributions: dict[str, float] | None = Field(
        default=None,
        description="Per-feature SHAP contribution percentages (null during warmup)",
    )


class AnomalyListResponse(BaseModel):
    """
    Paginated anomaly list — returned by GET /api/anomalies.
    """

    anomalies: list[AnomalyEvent]
    total: int = Field(description="Total matching anomaly events")
    limit: int
    offset: int


class SHAPExplanation(BaseModel):
    """
    SHAP explanation for a single anomaly — returned by
    GET /api/anomalies/explain.

    This is the differentiator endpoint: it tells the operator
    exactly WHY an anomaly was flagged, with per-feature percentages.

    Example response:
      {
        "device_id": "cnc-001",
        "time": "2026-06-23T09:00:00Z",
        "field": "vibration_g",
        "severity": "critical",
        "shap_contributions": {
          "spindle_rpm": 0.68,
          "vibration_g": 0.22,
          "cutting_temp_c": 0.07,
          "tool_wear_pct": 0.02,
          "feed_rate_mmpm": 0.01
        }
      }
    """

    device_id: str
    time: datetime
    field: str
    severity: str
    shap_contributions: dict[str, float] | None = Field(
        default=None,
        description="Per-feature contribution percentages (null if model wasn't trained yet)",
    )


# ─── Health Endpoint ─────────────────────────────────────────────────


class HealthStatus(BaseModel):
    """
    Health check response — returned by GET /health.

    Used by Kubernetes liveness/readiness probes to determine
    if the API pod should receive traffic.

    Example response:
      {
        "status": "healthy",
        "database": "connected",
        "kafka": "connected",
        "uptime_seconds": 3642.5
      }
    """

    status: str = Field(description="'healthy' or 'degraded'")
    database: str = Field(description="'connected' or 'disconnected'")
    kafka: str = Field(description="'connected' or 'disconnected'")
    uptime_seconds: float = Field(description="Seconds since API started")
