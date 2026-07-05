"""
Telemetry Schema Validator
===========================
Strict Pydantic models for every device type's telemetry payload.

Why validate?
  IoT devices are unreliable — firmware bugs, network corruption, sensor
  malfunction. Without validation, the anomaly detector trains on garbage
  data and produces meaningless alerts.

How it works:
  1. Every message MUST have: device_id, device_type, timestamp, bridge_received_at
  2. Each device_type has its own schema with physical range constraints
  3. validate_telemetry() looks up the schema and parses — returns None on failure
"""

import logging

import structlog
from pydantic import BaseModel, Field, ValidationError

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()


# ─── Base Schema (shared by ALL device types) ────────────────────────
class BaseTelemetry(BaseModel):
    """
    Every message from every device MUST have these four fields.

    - device_id:          Set by BaseDevice.publish()  e.g. "cnc-001"
    - device_type:        Set by BaseDevice.publish()  e.g. "cnc_machine"
    - timestamp:          Set by BaseDevice.publish()  Unix epoch seconds
    - bridge_received_at: Set by the bridge when it forwarded to Kafka
    """

    device_id: str
    device_type: str
    timestamp: float
    bridge_received_at: float


# ─── CNC Machine Schema ─────────────────────────────────────────────
class CNCTelemetry(BaseTelemetry):
    """
    CNC machine telemetry with physical range constraints.

    Field ranges are based on realistic industrial CNC specs:
    - spindle_rpm:    0–50,000 (high-speed CNC spindles top out ~40k)
    - vibration_g:    0–100    (>10g is already severe; anomaly spikes ~8x baseline)
    - tool_wear_pct:  0–100    (percentage, capped by BaseDevice)
    - feed_rate_mmpm: 0+       (mm per minute, always positive)
    - cycle_count:    0+       (monotonically increasing integer)
    - cutting_temp_c: -50–2000 (includes extreme anomaly spikes)
    """

    spindle_rpm: float = Field(ge=0, le=50000)
    vibration_g: float = Field(ge=0, le=100)
    tool_wear_pct: float = Field(ge=0, le=100)
    feed_rate_mmpm: float = Field(ge=0)
    cycle_count: int = Field(ge=0)
    cutting_temp_c: float = Field(ge=-50, le=2000)


# ─── Robotic Arm Schema ─────────────────────────────────────────────
class RoboticArmTelemetry(BaseTelemetry):
    """
    Robotic arm telemetry with physical range constraints.

    - joint torques:     0–5000 Nm (industrial arms can exert high torque)
    - joint_temp_c:      -50–500   (includes overheating scenarios)
    - position_error_mm: 0–500     (anomaly spike_factor=20 on base ~0.12mm)
    - cycles_completed:  0+        (monotonically increasing)
    - servo_current_a:   0–200     (industrial servo amperage)
    """

    joint1_torque_nm: float = Field(ge=0, le=5000)
    joint2_torque_nm: float = Field(ge=0, le=5000)
    joint_temp_c: float = Field(ge=-50, le=500)
    position_error_mm: float = Field(ge=0, le=500)
    cycles_completed: int = Field(ge=0)
    servo_current_a: float = Field(ge=0, le=200)


# ─── Conveyor Belt Schema ───────────────────────────────────────────
class ConveyorBeltTelemetry(BaseTelemetry):
    """
    Conveyor belt telemetry with physical range constraints.

    - belt_speed_mps:  0–50    (meters per second, includes anomaly spikes)
    - motor_current_a: 0–200   (industrial motor amperage)
    - belt_tension_n:  0–5000  (Newtons, spike_factor=4 on base ~350N)
    - roller_temp_c:   -50–500 (includes overheating scenarios)
    - items_per_min:   0–500   (production throughput)
    - runtime_hours:   0+      (cumulative, always increasing)
    """

    belt_speed_mps: float = Field(ge=0, le=50)
    motor_current_a: float = Field(ge=0, le=200)
    belt_tension_n: float = Field(ge=0, le=5000)
    roller_temp_c: float = Field(ge=-50, le=500)
    items_per_min: float = Field(ge=0, le=500)
    runtime_hours: float = Field(ge=0)


# ─── Schema Registry ────────────────────────────────────────────────
# Maps device_type string → Pydantic model class.
# When a new device type is added, just add a row here.
DEVICE_SCHEMAS: dict[str, type[BaseTelemetry]] = {
    "cnc_machine": CNCTelemetry,
    "robotic_arm": RoboticArmTelemetry,
    "conveyor_belt": ConveyorBeltTelemetry,
}


# ─── Validation Function ────────────────────────────────────────────
def validate_telemetry(raw: dict) -> BaseTelemetry | None:
    """
    Validate a raw telemetry dict against the appropriate device schema.

    Steps:
      1. Extract device_type from the raw dict
      2. Look up the matching Pydantic schema in DEVICE_SCHEMAS
      3. Parse through the schema — Pydantic checks types and ranges
      4. Return the validated model instance, or None if invalid

    Parameters
    ----------
    raw : dict
        The deserialized JSON payload from Kafka.

    Returns
    -------
    BaseTelemetry | None
        Validated telemetry object, or None if validation failed.
    """
    device_type = raw.get("device_type")
    schema = DEVICE_SCHEMAS.get(device_type)

    if schema is None:
        log.warning(
            "unknown_device_type",
            device_type=device_type,
            device_id=raw.get("device_id"),
        )
        return None

    try:
        return schema(**raw)
    except ValidationError as e:
        log.warning(
            "validation_failed",
            errors=e.error_count(),
            device_id=raw.get("device_id"),
            device_type=device_type,
            details=str(e.errors()[0]) if e.errors() else "unknown",
        )
        return None
