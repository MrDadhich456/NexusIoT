import pytest
from processor.detector import AnomalyDetector
from processor.validator import CNCTelemetry

def create_cnc_telemetry(spindle_rpm=3000.0, vibration_g=0.8, cutting_temp_c=280.0, timestamp=100.0):
    return CNCTelemetry(
        device_id="cnc-001",
        device_type="cnc_machine",
        timestamp=timestamp,
        bridge_received_at=timestamp + 0.1,
        spindle_rpm=spindle_rpm,
        vibration_g=vibration_g,
        tool_wear_pct=12.5,
        feed_rate_mmpm=500.0,
        cycle_count=42,
        cutting_temp_c=cutting_temp_c
    )

def test_detector_warmup():
    detector = AnomalyDetector(window_size=50, min_samples=20, z_threshold=3.0)
    
    # Send 19 normal readings (under min_samples = 20)
    for i in range(19):
        t = create_cnc_telemetry(vibration_g=0.8, timestamp=100.0 + i)
        anomalies = detector.check(t)
        assert len(anomalies) == 0

def test_detector_normal_operation():
    detector = AnomalyDetector(window_size=50, min_samples=20, z_threshold=3.0)
    
    # Establish a baseline with some noise (alternating 0.79 and 0.81)
    for i in range(20):
        val = 0.79 if i % 2 == 0 else 0.81
        t = create_cnc_telemetry(vibration_g=val, timestamp=100.0 + i)
        anomalies = detector.check(t)
        assert len(anomalies) == 0

    # Add a normal reading within the noise range, should not trigger anomaly
    t_normal = create_cnc_telemetry(vibration_g=0.8, timestamp=120.0)
    anomalies = detector.check(t_normal)
    assert len(anomalies) == 0

def test_detector_anomaly_detection():
    detector = AnomalyDetector(window_size=50, min_samples=20, z_threshold=3.0)
    
    # Baseline with noise (alternating 0.79 and 0.81)
    for i in range(40):
        val = 0.79 if i % 2 == 0 else 0.81
        t = create_cnc_telemetry(vibration_g=val, timestamp=100.0 + i)
        detector.check(t)

    # Spike vibration_g (normal: ~0.8, spiked: 5.0)
    t_spiked = create_cnc_telemetry(vibration_g=5.0, timestamp=140.0)
    anomalies = detector.check(t_spiked)
    
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly["device_id"] == "cnc-001"
    assert anomaly["field"] == "vibration_g"
    assert anomaly["value"] == 5.0
    assert anomaly["z_score"] > 3.0
    assert anomaly["severity"] in ["warning", "critical"]
