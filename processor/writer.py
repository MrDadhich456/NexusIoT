"""
TimescaleDB Writer
===================
Persists telemetry readings and anomaly events into TimescaleDB for
permanent storage, historical queries, and dashboard visualisation.

Architecture:
  - Uses psycopg (v3) for modern, type-safe PostgreSQL access
  - Connection pool (min=2, max=5) avoids reconnection overhead
  - Synchronous writes — matches the synchronous Kafka consumer loop
  - Parameterised queries prevent SQL injection and enable prepared stmts
  - Retry logic with exponential backoff for transient DB failures

Why sync and not async?
  The Kafka consumer loop in consumer.py is synchronous (blocking poll()).
  Mixing async DB writes into a sync loop would require an event loop and
  add complexity for zero benefit. At our throughput (~1.5 rows/sec), a
  sync INSERT takes <1ms — negligible compared to the 1s poll timeout.

Data Flow:
  consumer.py → writer.write_telemetry()  → INSERT into telemetry table
  consumer.py → writer.write_anomaly()    → INSERT into anomaly_events table
"""

import json
import time
import logging
from datetime import datetime, timezone
from typing import LiteralString

import structlog
from psycopg_pool import ConnectionPool

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()


class TimescaleWriter:
    """
    Writes telemetry data and anomaly events to TimescaleDB.

    Lifecycle:
      1. __init__(dsn)           → creates a connection pool to TimescaleDB
      2. write_telemetry(...)    → INSERT one validated reading
      3. write_anomaly(...)      → INSERT one anomaly event with SHAP data
      4. close()                 → drain pool and release connections

    Parameters
    ----------
    dsn : str
        PostgreSQL connection string. Format:
        postgresql://user:password@host:port/dbname
        Example: postgresql://nexusiot:nexusiot@timescaledb:5432/nexusiot

    min_pool : int
        Minimum number of connections to keep open. 2 is enough for our
        throughput (~1.5 writes/sec) but ensures one is always ready.

    max_pool : int
        Maximum connections. 5 handles burst writes (e.g., multiple
        anomalies detected simultaneously) without exhausting DB slots.
    """

    # ─── SQL Statements ──────────────────────────────────────────────
    # Using %s placeholders for psycopg parameterised queries.
    # Server-side prepared statements cache the query plan after the
    # first execution, making subsequent INSERTs ~30% faster.

    INSERT_TELEMETRY: LiteralString = """
        INSERT INTO telemetry (time, device_id, device_type, metrics)
        VALUES (%s, %s, %s, %s)
    """
    # Explanation:
    #   %s 1 → time:        TIMESTAMPTZ — when the device recorded this reading
    #   %s 2 → device_id:   TEXT        — e.g. "cnc-001"
    #   %s 3 → device_type: TEXT        — e.g. "cnc_machine"
    #   %s 4 → metrics:     JSONB       — all sensor values as a JSON string

    INSERT_ANOMALY: LiteralString = """
        INSERT INTO anomaly_events
            (time, detected_at, device_id, device_type,
             field, value, mean, std, z_score,
             severity, shap_contributions)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Explanation:
    #   %s 1  → time:               TIMESTAMPTZ — device timestamp of anomaly
    #   %s 2  → detected_at:        TIMESTAMPTZ — when processor detected it
    #   %s 3  → device_id:          TEXT        — e.g. "cnc-001"
    #   %s 4  → device_type:        TEXT        — e.g. "cnc_machine"
    #   %s 5  → field:              TEXT        — e.g. "vibration_g"
    #   %s 6  → value:              FLOAT       — the actual anomalous reading
    #   %s 7  → mean:               FLOAT       — sliding window mean
    #   %s 8  → std:                FLOAT       — sliding window std dev
    #   %s 9  → z_score:            FLOAT       — how many σ from normal
    #   %s 10 → severity:           TEXT        — "warning" or "critical"
    #   %s 11 → shap_contributions: JSONB       — per-feature % (nullable)

    def __init__(self, dsn: str, min_pool: int = 2, max_pool: int = 5):
        """
        Initialize the TimescaleDB writer with a connection pool.

        The pool opens min_pool connections immediately. Additional
        connections (up to max_pool) are created on-demand when all
        existing connections are busy.

        Parameters
        ----------
        dsn : str
            PostgreSQL connection string.
        min_pool : int
            Minimum connections kept open (default: 2).
        max_pool : int
            Maximum connections allowed (default: 5).
        """
        self.dsn = dsn

        # ─── Connection Pool ─────────────────────────────────────
        # ConnectionPool manages a set of reusable database connections.
        #
        # Why pooling?
        #   Opening a PostgreSQL connection takes ~5-20ms (TCP handshake +
        #   auth + TLS). At 1.5 writes/sec, opening a new connection per
        #   write would waste ~30ms/sec on connection overhead. A pool
        #   opens connections once and reuses them indefinitely.
        #
        # min_size=2: Always have 2 connections ready. Even if one is
        #   mid-transaction, the other is available immediately.
        #
        # max_size=5: Safety valve. If a burst of 5+ simultaneous writes
        #   hits (e.g., 5 anomalies detected in one reading), excess
        #   callers wait briefly rather than overwhelming the DB.
        #
        # timeout=10: If all connections are busy and max is reached,
        #   wait up to 10 seconds before raising an error.
        self._pool = ConnectionPool(
            conninfo=dsn,
            min_size=min_pool,
            max_size=max_pool,
            timeout=10,
        )

        log.info("timescale_writer_initialized",
                 dsn=self._sanitize_dsn(dsn),
                 min_pool=min_pool,
                 max_pool=max_pool)

    @staticmethod
    def _sanitize_dsn(dsn: str) -> str:
        """
        Remove the password from the DSN for safe logging.

        Input:  postgresql://nexusiot:secretpass@host:5432/nexusiot
        Output: postgresql://nexusiot:***@host:5432/nexusiot
        """
        # Find the password between :// and @
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

    def write_telemetry(self, device_id: str, device_type: str,
                        timestamp: float, metrics: dict) -> bool:
        """
        Write one validated telemetry reading to the telemetry table.

        Parameters
        ----------
        device_id : str
            Unique device identifier, e.g. "cnc-001".
        device_type : str
            Device type string, e.g. "cnc_machine".
        timestamp : float
            Unix epoch seconds from the device's reading.
        metrics : dict
            All sensor values as a dict (will be stored as JSONB).
            Example: {"spindle_rpm": 3002.5, "vibration_g": 0.81, ...}

        Returns
        -------
        bool
            True if the write succeeded, False if it failed after retries.
        """
        # Convert Unix epoch → Python datetime with UTC timezone.
        # TimescaleDB stores TIMESTAMPTZ, which requires timezone info.
        # fromtimestamp() with tz=UTC ensures consistent storage regardless
        # of the server's local timezone setting.
        ts = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        # Convert metrics dict → JSON string for JSONB column.
        # psycopg would handle dict→JSON automatically with Json adapter,
        # but explicit json.dumps() gives us control over formatting.
        metrics_json = json.dumps(metrics)

        return self._execute_with_retry(
            self.INSERT_TELEMETRY,
            (ts, device_id, device_type, metrics_json),
            operation="write_telemetry",
            device_id=device_id,
        )

    def write_anomaly(self, anomaly: dict) -> bool:
        """
        Write one anomaly event to the anomaly_events table.

        Parameters
        ----------
        anomaly : dict
            The anomaly dict produced by AnomalyDetector.check() and
            enriched with SHAP contributions by consumer.py.
            Expected keys: device_id, device_type, field, value, mean,
            std, z_score, severity, timestamp, detected_at,
            shap_contributions (optional, may be None).

        Returns
        -------
        bool
            True if the write succeeded, False if it failed after retries.
        """
        # Convert both timestamps to timezone-aware datetimes.
        # timestamp    = when the device recorded the reading
        # detected_at  = when the processor flagged it as anomalous
        ts = datetime.fromtimestamp(anomaly["timestamp"], tz=timezone.utc)
        detected = datetime.fromtimestamp(anomaly["detected_at"], tz=timezone.utc)

        # Convert SHAP contributions dict → JSON string (or None).
        # If SHAP model wasn't trained yet, shap_contributions is None,
        # which maps to SQL NULL in the JSONB column.
        shap_json = (
            json.dumps(anomaly["shap_contributions"])
            if anomaly.get("shap_contributions") is not None
            else None
        )

        return self._execute_with_retry(
            self.INSERT_ANOMALY,
            (
                ts,                          # time
                detected,                    # detected_at
                anomaly["device_id"],        # device_id
                anomaly["device_type"],       # device_type
                anomaly["field"],            # field
                anomaly["value"],            # value
                anomaly["mean"],             # mean
                anomaly["std"],              # std
                anomaly["z_score"],          # z_score
                anomaly["severity"],         # severity
                shap_json,                   # shap_contributions
            ),
            operation="write_anomaly",
            device_id=anomaly["device_id"],
        )

    def _execute_with_retry(self, query: LiteralString, params: tuple,
                            operation: str, device_id: str,
                            max_retries: int = 3) -> bool:
        """
        Execute a parameterised SQL query with retry + exponential backoff.

        Why retry?
          TimescaleDB might be temporarily unreachable (container restart,
          network blip, connection pool exhaustion). Retrying 3 times with
          exponential backoff (1s, 2s, 4s) handles most transient failures
          without blocking the Kafka consumer for too long.

        Why not retry forever?
          The Kafka consumer must keep polling to maintain its consumer
          group membership. If we block for >30 seconds, Kafka thinks
          we're dead and reassigns our partitions. 3 retries × 4s max
          = 7 seconds worst case, well under the limit.

        Parameters
        ----------
        query : str
            SQL INSERT statement with %s placeholders.
        params : tuple
            Values to substitute into the query.
        operation : str
            Name of the operation for logging (e.g. "write_telemetry").
        device_id : str
            Device ID for log context.
        max_retries : int
            Number of retry attempts (default: 3).

        Returns
        -------
        bool
            True if execution succeeded, False if all retries failed.
        """
        for attempt in range(max_retries):
            try:
                # ─── Get a connection from the pool ──────────────
                # The `with` block:
                #   1. Borrows a connection from the pool
                #   2. Executes the query inside a transaction
                #   3. Auto-commits on success (conn.__exit__ commits)
                #   4. Auto-rollbacks on exception
                #   5. Returns the connection to the pool
                with self._pool.connection() as conn:
                    # conn.execute() runs the parameterised query.
                    # psycopg substitutes %s placeholders with the params
                    # tuple, properly escaping values to prevent SQL injection.
                    conn.execute(query, params)

                return True

            except Exception as e:
                # Calculate backoff: 1s, 2s, 4s (exponential)
                wait = 2 ** attempt
                log.warning("db_write_retry",
                            operation=operation,
                            device_id=device_id,
                            attempt=attempt + 1,
                            max_retries=max_retries,
                            wait_seconds=wait,
                            error=str(e))
                time.sleep(wait)

        # All retries exhausted — log error but DON'T crash.
        # The Kafka consumer will continue processing; we just lose
        # this one DB write. The data still exists in Kafka and will
        # be in the anomaly-events topic if it was an anomaly.
        log.error("db_write_failed",
                  operation=operation,
                  device_id=device_id,
                  max_retries=max_retries)
        return False

    def close(self):
        """
        Close the connection pool and release all database connections.

        Called during graceful shutdown (SIGTERM/SIGINT) from consumer.py.
        ConnectionPool.close() waits for active connections to finish their
        current operations before closing them.
        """
        log.info("timescale_writer_closing")
        self._pool.close()
        log.info("timescale_writer_closed")
