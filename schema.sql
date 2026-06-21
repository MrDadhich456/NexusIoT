-- ============================================================================
-- NexusIoT — TimescaleDB Schema (Step 7)
-- ============================================================================
-- This file is auto-executed by the TimescaleDB Docker container on FIRST boot
-- (mounted into /docker-entrypoint-initdb.d/).
--
-- Two hypertables:
--   1. telemetry       — Every validated sensor reading from every device
--   2. anomaly_events  — Every anomaly detected (Z-score + SHAP contributions)
--
-- Why hypertables?
--   TimescaleDB auto-partitions data into time-based "chunks". Queries like
--   "give me the last hour of CNC data" only scan ~1 chunk instead of the
--   entire table. This gives us 10-100x query speedup over plain PostgreSQL.
-- ============================================================================

-- Enable the TimescaleDB extension (must be done ONCE per database)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table 1: telemetry
-- ─────────────────────────────────────────────────────────────────────────────
-- Stores EVERY validated sensor reading that passes Pydantic validation.
-- At 3 devices × 1 reading/2 seconds = ~130K rows/day.
--
-- Column design decisions:
--   time        → TIMESTAMPTZ (required by TimescaleDB for hypertable PK)
--   device_id   → TEXT (e.g. "cnc-001", used for per-device filtering)
--   device_type → TEXT (e.g. "cnc_machine", used for GROUP BY device type)
--   metrics     → JSONB (flexible: each device type has different sensor fields)
--
-- Why JSONB for metrics?
--   CNC has: spindle_rpm, vibration_g, cutting_temp_c, tool_wear_pct, feed_rate_mmpm, cycle_count
--   Robotic arm has: joint1_torque_nm, joint2_torque_nm, joint_temp_c, position_error_mm, ...
--   Conveyor has: belt_speed_mps, motor_current_a, belt_tension_n, ...
--   Using JSONB avoids 18+ nullable columns and makes adding new device types zero-migration.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telemetry (
    time        TIMESTAMPTZ     NOT NULL,
    device_id   TEXT            NOT NULL,
    device_type TEXT            NOT NULL,
    metrics     JSONB           NOT NULL
);

-- Convert to hypertable: auto-partition by time in 1-day chunks.
-- 1-day chunks at ~130K rows/day keeps each chunk small (~5MB) for fast queries.
-- if_not_exists prevents errors on container restart.
SELECT create_hypertable('telemetry', 'time', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '1 day');

-- Index: fast lookups for "show me the last N readings for device X"
-- (device_id, time DESC) is the most common query pattern for dashboards.
CREATE INDEX IF NOT EXISTS idx_telemetry_device_time
    ON telemetry (device_id, time DESC);

-- Index: fast lookups for "show me all CNC readings in the last hour"
CREATE INDEX IF NOT EXISTS idx_telemetry_type_time
    ON telemetry (device_type, time DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- Table 2: anomaly_events
-- ─────────────────────────────────────────────────────────────────────────────
-- Stores every anomaly detected by the Z-score detector, enriched with
-- SHAP explanations. Much sparser than telemetry (~5% anomaly rate).
--
-- Column design decisions:
--   time               → When the anomaly occurred (device timestamp)
--   detected_at        → When the processor flagged it (processing timestamp)
--   field              → Which metric triggered (e.g. "vibration_g")
--   value/mean/std     → The actual reading vs. expected baseline
--   z_score            → How many standard deviations away
--   severity           → "warning" (z>3.5) or "critical" (z>5.0)
--   shap_contributions → JSONB with per-feature % contributions (nullable during warmup)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_events (
    time                TIMESTAMPTZ         NOT NULL,
    detected_at         TIMESTAMPTZ         NOT NULL,
    device_id           TEXT                NOT NULL,
    device_type         TEXT                NOT NULL,
    field               TEXT                NOT NULL,
    value               DOUBLE PRECISION    NOT NULL,
    mean                DOUBLE PRECISION    NOT NULL,
    std                 DOUBLE PRECISION    NOT NULL,
    z_score             DOUBLE PRECISION    NOT NULL,
    severity            TEXT                NOT NULL,
    shap_contributions  JSONB
);

-- Convert to hypertable: 7-day chunks (anomalies are sparse, larger chunks are fine)
SELECT create_hypertable('anomaly_events', 'time', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '7 days');

-- Index: fast lookups for "show me anomalies for device X"
CREATE INDEX IF NOT EXISTS idx_anomaly_device_time
    ON anomaly_events (device_id, time DESC);

-- Index: fast lookups for "show me all critical alerts"
CREATE INDEX IF NOT EXISTS idx_anomaly_severity_time
    ON anomaly_events (severity, time DESC);

-- Index: fast lookups for "show me all vibration anomalies"
CREATE INDEX IF NOT EXISTS idx_anomaly_field_time
    ON anomaly_events (field, time DESC);
