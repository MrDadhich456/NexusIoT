"""
SHAP Anomaly Explainer
=======================
Adds explainability to anomaly detection using SHAP (SHapley Additive
exPlanations) and scikit-learn's IsolationForest.

When the Z-score detector says "anomaly!", this module answers "WHY?"
by computing the percentage contribution of each sensor metric.

Architecture:
  - One IsolationForest model per device (trained on recent normal data)
  - SHAP TreeExplainer decomposes the model's anomaly score per feature
  - Results are normalized to contribution percentages

Why IsolationForest + SHAP TreeExplainer?
  - IsolationForest is unsupervised (no labeled data needed — perfect for IoT)
  - TreeExplainer is optimized for tree models: O(TLD) complexity
    where T=trees, L=leaves, D=depth
  - Together: lightweight enough for real-time explanations in a Kafka loop

Example output for a CNC anomaly:
  {
    "spindle_rpm": 68.2,    ← Primary driver (RPM dropped)
    "vibration_g": 22.1,    ← Secondary (vibration spiked)
    "cutting_temp_c": 6.8,  ← Minor contributor
    "tool_wear_pct": 2.1,   ← Negligible
    "feed_rate_mmpm": 0.8   ← Negligible
  }
"""

import logging
from collections import defaultdict, deque
from typing import Optional

import numpy as np
import pandas as pd
import shap
import structlog
from sklearn.ensemble import IsolationForest

from .validator import BaseTelemetry

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()


# ─── Feature Maps ────────────────────────────────────────────────────
# Which sensor metrics to use as ML features per device type.
#
# Design decisions:
#   1. We include MORE features than the Z-score detector monitors
#      (e.g., tool_wear_pct, feed_rate_mmpm) because SHAP can reveal
#      that a non-monitored metric was actually the root cause.
#   2. We EXCLUDE monotonically increasing counters (cycle_count,
#      cycles_completed, runtime_hours) because they always increase
#      and would dominate the model with false importance.
#   3. Feature order matters for DataFrame column alignment — the same
#      order is used during training AND explanation.
EXPLAINER_FEATURES: dict[str, list[str]] = {
    "cnc_machine": [
        "spindle_rpm",  # RPM of the cutting spindle (base ~3000)
        "vibration_g",  # Vibration in g-force (base ~0.8)
        "cutting_temp_c",  # Cutting temperature in °C (base ~280)
        "tool_wear_pct",  # Tool wear percentage (0-100, drifts up)
        "feed_rate_mmpm",  # Feed rate in mm/min (base ~500)
    ],
    "robotic_arm": [
        "joint1_torque_nm",  # Joint 1 torque in Nm (base ~45)
        "joint2_torque_nm",  # Joint 2 torque in Nm (base ~32)
        "joint_temp_c",  # Joint temperature in °C (base ~68)
        "position_error_mm",  # Position error in mm (base ~0.12)
        "servo_current_a",  # Servo current in Amps (base ~8.4)
    ],
    "conveyor_belt": [
        "belt_speed_mps",  # Belt speed in m/s
        "motor_current_a",  # Motor current in Amps
        "belt_tension_n",  # Belt tension in Newtons (base ~350)
        "roller_temp_c",  # Roller temperature in °C
        "items_per_min",  # Production throughput
    ],
}


class SHAPExplainer:
    """
    Per-device SHAP explainer using IsolationForest + TreeExplainer.

    Lifecycle per device:
      1. Z-score says "no anomaly" → update_normal() adds reading to buffer
      2. After min_samples readings  → _train() builds the IsolationForest
      3. Every retrain_every samples → _train() rebuilds (adapts to drift)
      4. Z-score says "anomaly!"    → explain() returns contribution %

    Parameters
    ----------
    min_samples : int
        Minimum normal readings before first training. At 2s intervals,
        100 samples = ~3.3 minutes of warmup. Training on fewer gives
        unstable baselines.
    retrain_every : int
        Retrain model every N new normal readings. 200 samples = ~6.6
        minutes. This keeps the model fresh as sensor baselines drift
        (e.g., tool wear gradually increases cutting temperature).
    """

    def __init__(self, min_samples: int = 100, retrain_every: int = 200):
        self.min_samples = min_samples
        self.retrain_every = retrain_every

        # ─── Per-device training buffers ─────────────────────────────
        # deque(maxlen=500) stores the last 500 normal readings per device.
        # At 2-second intervals, this is ~16.6 minutes of data.
        # Old readings are automatically evicted, so the model always
        # reflects CURRENT operating conditions, not conditions from
        # hours ago. This is critical for drifting sensors (tool wear).
        self._normal_data: dict[str, deque] = defaultdict(lambda: deque(maxlen=500))

        # ─── Per-device trained models ───────────────────────────────
        # Each device gets its own IsolationForest because sensor baselines
        # differ across machines (cnc-001 might run at 3000 RPM, cnc-002
        # at 4500 RPM). Sharing a model would cause false positives.
        self._models: dict[str, IsolationForest] = {}

        # ─── Per-device SHAP explainers ──────────────────────────────
        # TreeExplainer is bound to a specific model instance. When we
        # retrain the model, we must also recreate the explainer.
        self._explainers: dict[str, shap.TreeExplainer] = {}

        # ─── Sample counter for retrain scheduling ───────────────────
        # Tracks total normal readings per device since startup.
        # Used with modulo to trigger periodic retraining.
        self._sample_counts: dict[str, int] = defaultdict(int)

    def update_normal(self, device_id: str, device_type: str, telemetry: BaseTelemetry):
        """
        Feed a NON-anomalous reading into the training buffer.

        Called for every telemetry message where the Z-score detector
        found ZERO anomalies. This builds up the "what is normal?"
        dataset that the IsolationForest trains on.

        Parameters
        ----------
        device_id : str
            Unique device identifier, e.g. "cnc-001".
        device_type : str
            Device type string, e.g. "cnc_machine".
        telemetry : BaseTelemetry
            The validated Pydantic telemetry object (has all sensor fields).
        """
        # Look up which features this device type uses
        features = EXPLAINER_FEATURES.get(device_type)
        if features is None:
            return  # Unknown device type — skip silently

        # Extract only the feature columns from the telemetry object.
        # getattr() pulls the value from the Pydantic model (e.g.,
        # telemetry.spindle_rpm → 3000.5). Default 0.0 handles edge cases.
        row = {f: getattr(telemetry, f, 0.0) for f in features}

        # Append to the rolling buffer. If buffer is full (500 items),
        # the oldest reading is automatically evicted by deque.
        self._normal_data[device_id].append(row)

        # Increment the total sample counter for this device
        self._sample_counts[device_id] += 1

        # ─── Retrain check ───────────────────────────────────────
        # Trigger retraining when:
        #   1. We have at least min_samples (100) readings, AND
        #   2. The count is a multiple of retrain_every (200)
        #
        # The modulo check (n % 200 == 0) ensures we retrain periodically
        # without checking on every single reading. First training happens
        # at sample 200 (since 100 < 200, and 200 % 200 == 0).
        # Subsequent retrains at 400, 600, 800, ...
        n = self._sample_counts[device_id]
        if n >= self.min_samples and n % self.retrain_every == 0:
            self._train(device_id, device_type)

    def _train(self, device_id: str, device_type: str):
        """
        Train an IsolationForest on the device's recent normal data.

        This creates/replaces:
          1. An IsolationForest model trained on up to 500 recent readings
          2. A SHAP TreeExplainer bound to that model

        Called automatically by update_normal() at scheduled intervals.

        Parameters
        ----------
        device_id : str
            The device to train a model for.
        device_type : str
            Device type string to look up feature columns.
        """
        # Get the feature column names for this device type
        features = EXPLAINER_FEATURES[device_type]

        # Convert deque of dicts → list of dicts → pandas DataFrame.
        # DataFrame columns are explicitly set to match the feature order
        # used during training. This ensures explain() uses the same order.
        data = list(self._normal_data[device_id])
        df = pd.DataFrame(data, columns=features).fillna(0)

        # ─── IsolationForest configuration ───────────────────────
        # n_estimators=100:
        #   Number of decision trees in the forest. Each tree randomly
        #   selects a feature and a split value, then recursively
        #   partitions the data. Points that are easy to isolate (few
        #   splits needed) are more likely to be anomalies.
        #   100 trees is the sweet spot: more = more stable SHAP values
        #   but slower training. Fewer = noisy explanations.
        #
        # contamination=0.03:
        #   Tells IsolationForest that ~3% of training data might be
        #   anomalous (false negatives from the Z-score filter that
        #   slipped into our "normal" buffer). This makes the model
        #   robust — it won't learn these outliers as "normal".
        #
        # random_state=42:
        #   Fixed seed for reproducibility. Same data → same model →
        #   same SHAP values. Essential for debugging and testing.
        model = IsolationForest(
            n_estimators=100,
            contamination=0.03,
            random_state=42,
        )

        # Fit the model on the DataFrame of normal readings.
        # IsolationForest builds 100 random trees. For each tree:
        #   1. Randomly select a feature (e.g., spindle_rpm)
        #   2. Randomly select a split value within the feature's range
        #   3. Partition data into left/right subtrees
        #   4. Repeat until each point is isolated
        # Points requiring fewer splits = more anomalous.
        model.fit(df)

        # Store the trained model for this device
        self._models[device_id] = model

        # Create a SHAP TreeExplainer bound to this model.
        # TreeExplainer uses an efficient tree-traversal algorithm
        # (O(TLD) where T=trees, L=max leaves, D=max depth) to compute
        # exact SHAP values. This is much faster than KernelExplainer
        # (which uses sampling) and gives exact results for tree models.
        self._explainers[device_id] = shap.TreeExplainer(model)

        log.info(
            "shap_model_trained",
            device_id=device_id,
            samples=len(data),
            features=len(features),
        )

    def explain(
        self, device_id: str, device_type: str, telemetry: BaseTelemetry
    ) -> Optional[dict[str, float]]:
        """
        Compute SHAP contribution percentages for an anomalous reading.

        Takes the full telemetry reading (all metrics, not just the
        anomalous one) and computes how much each feature contributed
        to the IsolationForest's anomaly score.

        Parameters
        ----------
        device_id : str
            The device that produced the anomalous reading.
        device_type : str
            Device type for feature lookup.
        telemetry : BaseTelemetry
            The full validated telemetry reading (includes all metrics).

        Returns
        -------
        dict[str, float] | None
            Contribution percentages sorted by magnitude (highest first).
            Example: {"spindle_rpm": 68.2, "vibration_g": 22.1, ...}
            Returns None if no model is trained yet (still warming up).
        """
        # ─── Guard: model not ready ──────────────────────────────
        # If we haven't received enough normal readings to train a
        # model for this device, we can't explain anything yet.
        # The consumer will produce the anomaly event WITHOUT
        # shap_contributions (set to null in JSON).
        if device_id not in self._explainers:
            return None

        # ─── Guard: unknown device type ──────────────────────────
        features = EXPLAINER_FEATURES.get(device_type)
        if features is None:
            return None

        # ─── Build input DataFrame ───────────────────────────────
        # Create a single-row DataFrame with the EXACT same columns
        # and order that the model was trained on. Column mismatch
        # would cause SHAP to produce garbage values.
        row = {f: getattr(telemetry, f, 0.0) for f in features}
        df = pd.DataFrame([row], columns=features)

        # ─── Compute SHAP values ─────────────────────────────────
        # shap_values() returns a 2D numpy array of shape (1, n_features).
        # Each element is the SHAP value for that feature:
        #   - Positive value = feature pushes toward "anomaly"
        #   - Negative value = feature pushes toward "normal"
        #
        # For example, if spindle_rpm dropped significantly:
        #   shap_values = [[-0.15, 0.08, 0.02, 0.01, 0.005]]
        #   spindle_rpm has the largest magnitude → primary driver
        shap_values = self._explainers[device_id].shap_values(df)

        # ─── Take absolute values ────────────────────────────────
        # We care about MAGNITUDE of contribution, not direction.
        # A feature strongly pushing toward "normal" is also informative
        # (e.g., "temperature was fine, so it's NOT the cause").
        # abs() ensures both spikes and drops contribute positively.
        abs_shap = np.abs(shap_values[0])

        # ─── Convert to percentages ──────────────────────────────
        # Normalize so all contributions sum to 100%.
        # The epsilon (1e-9) prevents division by zero if all SHAP
        # values are 0 (extremely unlikely but defensive programming).
        total = abs_shap.sum() + 1e-9
        contributions = {
            features[i]: round(float(abs_shap[i] / total) * 100, 1)
            for i in range(len(features))
        }

        # ─── Sort by contribution (highest first) ────────────────
        # Engineers want to see the most impactful feature first.
        # Example: {"spindle_rpm": 68.2, "vibration_g": 22.1, ...}
        contributions = dict(sorted(contributions.items(), key=lambda x: -x[1]))

        log.info(
            "shap_explanation_computed",
            device_id=device_id,
            top_feature=list(contributions.keys())[0],
            top_contribution=list(contributions.values())[0],
        )

        return contributions
