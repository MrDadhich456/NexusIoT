# ============================================================================
# NexusIoT — Kubernetes Multi-Device Runner
# ============================================================================
# Runs all 3 device simulators (CNC Machine, Robotic Arm, Conveyor Belt)
# concurrently in separate threads within a single container.
#
# Why one container for all devices?
#   - Simpler K8s Job management (1 pod instead of 3)
#   - All devices share the same MQTT connection config
#   - Thread-level parallelism is sufficient for 3 simulators
#
# The MQTT broker address is read from environment variables
# (set by the K8s ConfigMap via envFrom in the Job manifest).
# ============================================================================

import threading
import logging
import os
import signal
import sys

# Import all device simulator classes
from devices.cnc_machine import CNCMachine
from devices.robotic_arm import RoboticArm
from devices.conveyor_belt import ConveyorBelt

# Configure structured logging (timestamps + thread names for debugging)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


def run_device(device_cls, device_id: str, broker: str, port: int):
    """
    Instantiate and run a single device simulator.

    Args:
        device_cls: The device class (CNCMachine, RoboticArm, or ConveyorBelt)
        device_id:  Unique ID for this device instance (e.g. "cnc-001")
        broker:     MQTT broker hostname (from K8s ConfigMap)
        port:       MQTT broker port (from K8s ConfigMap)
    """
    try:
        log.info(
            f"Starting {device_cls.__name__} (id={device_id}, broker={broker}:{port})"
        )
        # Create device instance — BaseDevice.__init__ connects to MQTT
        device = device_cls(device_id=device_id)
        # Override the MQTT connection to use the K8s service address
        device.client.disconnect()
        device.client.connect(broker, port, keepalive=60)
        device.client.loop_start()
        # Publish readings every 2 seconds (infinite loop)
        device.run(interval=2.0)
    except Exception as e:
        log.error(f"{device_cls.__name__} crashed: {e}", exc_info=True)
        raise


def main():
    """
    Entry point: reads MQTT config from environment, launches all devices in threads.
    """
    # Read MQTT broker address from environment variables (set by K8s ConfigMap)
    # Falls back to "localhost" for local development outside K8s
    broker = os.getenv("MQTT_BROKER", "localhost")
    port = int(os.getenv("MQTT_PORT", "1883"))

    log.info(f"MQTT Broker: {broker}:{port}")
    log.info("Launching 3 device simulators...")

    # Define all devices to run: (DeviceClass, device_id)
    devices = [
        (CNCMachine, "cnc-001"),
        (RoboticArm, "arm-001"),
        (ConveyorBelt, "conv-001"),
    ]

    # Launch each device in its own daemon thread
    # daemon=True means threads die when the main thread exits (clean shutdown)
    threads = []
    for device_cls, device_id in devices:
        t = threading.Thread(
            target=run_device,
            args=(device_cls, device_id, broker, port),
            name=f"Device-{device_id}",  # Thread name appears in log output
            daemon=True,  # Die with main thread
        )
        t.start()
        threads.append(t)
        log.info(f"  ✓ {device_cls.__name__} ({device_id}) thread started")

    log.info("All device simulators running. Press Ctrl+C to stop.")

    # Handle graceful shutdown (SIGTERM from K8s pod termination)
    def shutdown(signum, frame):
        log.info("Shutdown signal received. Stopping all devices...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Block main thread — join all device threads (they run forever)
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
