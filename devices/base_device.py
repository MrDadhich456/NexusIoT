import json
import logging
import os
import random
import time

import paho.mqtt.client as mqtt


class BaseDevice:
    def __init__(
        self, device_id: str, device_type: str, broker: str = None, port: int = None
    ):
        # Read MQTT broker from environment variables (set by K8s ConfigMap)
        # Falls back to localhost:1883 for local development
        broker = broker or os.getenv("MQTT_BROKER", "localhost")
        port = port or int(os.getenv("MQTT_PORT", "1883"))
        self.device_id = device_id
        self.device_type = device_type
        self.topic = f"sensors/{device_id}/data"
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=device_id)
        self.client.on_connect = self._on_connect
        self.client.connect(broker, port, keepalive=60)
        self.client.loop_start()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        logging.info(f"[{self.device_id}] Connected to broker rc={reason_code}")

    def publish(self, payload: dict):
        payload["device_id"] = self.device_id
        payload["device_type"] = self.device_type
        payload["timestamp"] = time.time()

        # Print to terminal so we can see the data!
        print(f"[{self.device_id}] Publishing: {json.dumps(payload)}")

        self.client.publish(
            self.topic,
            json.dumps(payload),
            qos=1,  # at-least-once delivery
        )

    def add_noise(self, value: float, pct: float = 0.02) -> float:
        """Add ±2% Gaussian noise to a reading."""
        return value * (1 + random.gauss(0, pct))

    def inject_anomaly(
        self, value: float, probability: float = 0.03, spike_factor: float = 3.5
    ) -> float:
        """3% chance of injecting a spike anomaly."""
        if random.random() < probability:
            return value * spike_factor
        return value

    def run(self, interval: float = 2.0):
        while True:
            self.publish(self.read())
            time.sleep(interval)

    def read(self) -> dict:
        raise NotImplementedError
