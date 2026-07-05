"""
Z-Score Anomaly Detector
=========================
Maintains a sliding window of recent readings per device, per metric.
For each new reading, computes a Z-score (how many standard deviations
the value is from the running mean). Flags readings above a threshold.

Why Z-score?
  - Simple, effective, no training data needed
  - Adapts to gradual drift (e.g., tool wear increasing temperature)
  - The sliding window "forgets" old baselines as conditions change

How the math works:
  mean  = average of last N readings
  std   = standard deviation (spread of normal values)
  z     = |current_value - mean| / std
  If z > threshold → anomaly (value is unusually far from recent normal)

Our devices inject spikes at 3.5-8x normal → Z-scores will be >> threshold.
"""

import time
import logging
from collections import defaultdict, deque

import structlog

from .validator import BaseTelemetry

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()


class AnomalyDetector:
    """
    Per-device, per-metric sliding window anomaly detector using Z-scores.

    Parameters
    ----------
    window_size : int
        Number of recent readings to keep per metric. Larger = more stable
        baseline but slower to adapt. 100 ≈ 3.3 minutes at 2s intervals.
    z_threshold : float
        Z-score above which a reading is flagged. 3.5 catches clear spikes
        while avoiding false positives from normal sensor noise (±2%).
    min_samples : int
        Minimum readings before computing Z-scores. Too few samples gives
        unstable statistics. 20 = 40 seconds of warmup at 2s intervals.
    """

    # ─── Which fields to monitor per device type ─────────────────────
    # We only monitor fields where sudden spikes/drops are meaningful.
    # Fields like cycle_count always increase → would always be "anomalous".
    MONITORED_FIELDS: dict[str, list[str]] = {
        "cnc_machine": ["spindle_rpm", "vibration_g", "cutting_temp_c"],
        "robotic_arm": ["joint1_torque_nm", "joint2_torque_nm", "position_error_mm"],
        "conveyor_belt": ["belt_speed_mps", "motor_current_a", "belt_tension_n"],
    }

    def __init__(
        self, window_size: int = 100, z_threshold: float = 3.5, min_samples: int = 20
    ):
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.min_samples = min_samples

        # Nested structure: windows["cnc-001"]["vibration_g"] = deque([0.8, 0.81, ...])
        # defaultdict auto-creates missing keys, deque(maxlen=N) auto-evicts oldest
        self.windows: dict[str, dict[str, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=self.window_size))
        )

    def check(self, telemetry: BaseTelemetry) -> list[dict]:
        """
        Check one telemetry reading for anomalies.

        Steps:
          1. Get the list of monitored fields for this device type
          2. For each field, append the value to the sliding window
          3. If enough samples, compute Z-score
          4. If Z-score exceeds threshold, create an anomaly record

        Parameters
        ----------
        telemetry : BaseTelemetry
            A validated telemetry reading from the validator.

        Returns
        -------
        list[dict]
            List of anomaly dicts (empty if no anomalies detected).
            Each dict contains: device_id, device_type, field, value,
            mean, std, z_score, severity, timestamp.
        """
        anomalies = []
        device_id = telemetry.device_id
        device_type = telemetry.device_type
        fields = self.MONITORED_FIELDS.get(device_type, [])

        for field in fields:
            value = getattr(telemetry, field)
            window = self.windows[device_id][field]

            # Always add to window first (even before we have enough for stats)
            window.append(value)

            # Need minimum data for meaningful statistics
            if len(window) < self.min_samples:
                continue

            # ─── Z-score computation ─────────────────────────────────
            # mean: center of the "normal" distribution
            mean = sum(window) / len(window)

            # variance: average squared distance from the mean
            variance = sum((x - mean) ** 2 for x in window) / len(window)

            # std: square root of variance (same units as the original data)
            std = variance**0.5

            # If all values are identical, std=0, z-score is undefined → skip
            if std == 0:
                continue

            # z_score: how many standard deviations this value is from the mean
            # We use abs() because both spikes (high) and drops (low) matter
            z_score = abs(value - mean) / std

            if z_score > self.z_threshold:
                # Determine severity:
                #   warning:  3.5 < z < 5.0 → unusual, should investigate
                #   critical: z >= 5.0      → definitely broken, alert now
                severity = "critical" if z_score > 5.0 else "warning"

                anomaly = {
                    "device_id": device_id,
                    "device_type": device_type,
                    "field": field,
                    "value": round(value, 4),
                    "mean": round(mean, 4),
                    "std": round(std, 4),
                    "z_score": round(z_score, 2),
                    "severity": severity,
                    "timestamp": telemetry.timestamp,
                    "detected_at": time.time(),
                }
                anomalies.append(anomaly)

                log.info(
                    "anomaly_detected",
                    device_id=device_id,
                    field=field,
                    value=round(value, 2),
                    z_score=round(z_score, 2),
                    severity=severity,
                )

        return anomalies
