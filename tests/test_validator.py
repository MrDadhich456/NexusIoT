from processor.validator import (
    validate_telemetry,
    CNCTelemetry,
    RoboticArmTelemetry,
    ConveyorBeltTelemetry,
)


def test_validate_cnc_valid():
    raw_data = {
        "device_id": "cnc-001",
        "device_type": "cnc_machine",
        "timestamp": 1625097600.0,
        "bridge_received_at": 1625097601.0,
        "spindle_rpm": 3000.0,
        "vibration_g": 0.8,
        "tool_wear_pct": 12.5,
        "feed_rate_mmpm": 500.0,
        "cycle_count": 42,
        "cutting_temp_c": 280.0,
    }
    result = validate_telemetry(raw_data)
    assert result is not None
    assert isinstance(result, CNCTelemetry)
    assert result.device_id == "cnc-001"
    assert result.spindle_rpm == 3000.0


def test_validate_robotic_arm_valid():
    raw_data = {
        "device_id": "arm-001",
        "device_type": "robotic_arm",
        "timestamp": 1625097600.0,
        "bridge_received_at": 1625097601.0,
        "joint1_torque_nm": 45.0,
        "joint2_torque_nm": 32.0,
        "joint_temp_c": 68.0,
        "position_error_mm": 0.12,
        "cycles_completed": 150,
        "servo_current_a": 8.4,
    }
    result = validate_telemetry(raw_data)
    assert result is not None
    assert isinstance(result, RoboticArmTelemetry)
    assert result.joint_temp_c == 68.0


def test_validate_conveyor_belt_valid():
    raw_data = {
        "device_id": "belt-001",
        "device_type": "conveyor_belt",
        "timestamp": 1625097600.0,
        "bridge_received_at": 1625097601.0,
        "belt_speed_mps": 1.5,
        "motor_current_a": 12.0,
        "belt_tension_n": 350.0,
        "roller_temp_c": 45.0,
        "items_per_min": 60.0,
        "runtime_hours": 120.5,
    }
    result = validate_telemetry(raw_data)
    assert result is not None
    assert isinstance(result, ConveyorBeltTelemetry)
    assert result.runtime_hours == 120.5


def test_validate_invalid_device_type():
    raw_data = {
        "device_id": "unknown-001",
        "device_type": "unknown_type",
        "timestamp": 1625097600.0,
        "bridge_received_at": 1625097601.0,
    }
    result = validate_telemetry(raw_data)
    assert result is None


def test_validate_cnc_missing_field():
    raw_data = {
        "device_id": "cnc-001",
        "device_type": "cnc_machine",
        "timestamp": 1625097600.0,
        "bridge_received_at": 1625097601.0,
        # missing spindle_rpm
        "vibration_g": 0.8,
        "tool_wear_pct": 12.5,
        "feed_rate_mmpm": 500.0,
        "cycle_count": 42,
        "cutting_temp_c": 280.0,
    }
    result = validate_telemetry(raw_data)
    assert result is None


def test_validate_cnc_out_of_range():
    raw_data = {
        "device_id": "cnc-001",
        "device_type": "cnc_machine",
        "timestamp": 1625097600.0,
        "bridge_received_at": 1625097601.0,
        "spindle_rpm": -10.0,  # out of range (ge=0)
        "vibration_g": 0.8,
        "tool_wear_pct": 12.5,
        "feed_rate_mmpm": 500.0,
        "cycle_count": 42,
        "cutting_temp_c": 280.0,
    }
    result = validate_telemetry(raw_data)
    assert result is None
