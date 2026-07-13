"""
MQTT → Kafka Bridge
====================
Standalone microservice that subscribes to ALL sensor MQTT topics
and forwards every message into Kafka's durable, partitioned log.

Data Flow:
  Device → MQTT (lightweight pub/sub) → Bridge → Kafka (durable, ordered, replayable)

Why this exists:
  MQTT is great for constrained IoT devices but doesn't durably store messages.
  Kafka acts as a persistent buffer so downstream consumers (anomaly detector,
  SHAP explainer, DB writer) can replay, reprocess, and scale independently.
"""

import json
import time
import os
import sys
import signal
import logging

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ─── Structured Logging ──────────────────────────────────────────────
# structlog emits JSON-formatted log lines instead of plain text.
# In production, logs feed into ELK / Loki / CloudWatch — JSON is
# machine-parseable and lets us filter by fields like device_id.
import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()


# ─── Configuration (12-Factor: env vars) ─────────────────────────────
# All config comes from environment variables so the SAME code runs
# in dev, staging, and production — only the env vars change.
# Defaults point to localhost for running outside Docker.
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-telemetry")
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))


# ─── Kafka Topic Bootstrap ───────────────────────────────────────────
def ensure_topic_exists(
    bootstrap_servers: str,
    topic: str,
    num_partitions: int = 6,
    replication_factor: int = 3,
):
    """
    Create the Kafka topic if it doesn't already exist.

    Parameters
    ----------
    num_partitions : int
        Each partition is an independent ordered log. More partitions
        means more parallel consumers. 6 is a solid starting point
        for 3 device types × multiple instances.

    replication_factor : int
        How many brokers store a copy of each partition. With 3 brokers
        and factor=3, every message exists on all brokers — if one dies,
        zero data is lost.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        log.info("topic_already_exists", topic=topic)
        return

    new_topic = NewTopic(
        topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    futures = admin.create_topics([new_topic])
    for topic_name, future in futures.items():
        future.result()  # blocks until creation completes; raises on error
        log.info(
            "topic_created",
            topic=topic_name,
            partitions=num_partitions,
            replication=replication_factor,
        )


# ─── Kafka Producer ──────────────────────────────────────────────────
# Configured for RELIABILITY over raw throughput.
producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKERS,
        # Identifies this producer in Kafka broker logs & metrics
        "client.id": "mqtt-kafka-bridge",
        # acks=all → wait for ALL in-sync replicas to confirm the write.
        # This is the STRONGEST durability guarantee Kafka offers.
        # acks=1 (leader-only) is faster but risks data loss if leader crashes
        # before replicating.
        "acks": "all",
        # Automatic retry on transient failures (network blip, broker restart)
        "retries": 5,
        "retry.backoff.ms": 100,
        # Idempotent producer: even if a retry sends the same message twice,
        # Kafka deduplicates it server-side. Combined with acks=all, this
        # gives us exactly-once semantics from producer → broker.
        "enable.idempotence": True,
    }
)


def delivery_callback(err, msg):
    """
    Asynchronous callback invoked by the producer once Kafka confirms
    (or rejects) a message. This is our observability hook — we know
    exactly which messages succeeded and which failed.
    """
    if err:
        log.error("kafka_delivery_failed", error=str(err), topic=msg.topic())
    else:
        log.debug(
            "kafka_delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


# ─── MQTT Callbacks ──────────────────────────────────────────────────
def on_connect(client, userdata, flags, reason_code, properties):
    """
    Fires when the MQTT client connects (or reconnects) to Mosquitto.

    We subscribe to 'sensors/#' — a wildcard that matches:
      sensors/cnc-001/data  ✓
      sensors/arm-001/data  ✓
      sensors/belt-001/data ✓
      (any future device)   ✓

    QoS=1 (at-least-once): Mosquitto retries delivery if we don't ACK.
    """
    log.info("mqtt_connected", rc=str(reason_code))
    client.subscribe("sensors/#", qos=1)


def on_message(client, userdata, msg):
    """
    Hot path — called for EVERY telemetry message from EVERY device.

    Steps:
      1. Parse JSON payload from the device
      2. Stamp 'bridge_received_at' for end-to-end latency measurement
      3. Produce to Kafka, keyed by device_id

    Why key by device_id?
      Kafka guarantees message ordering WITHIN a partition. By using
      device_id as the partition key, all readings from cnc-001 land
      in the SAME partition, in order. The stream processor (Step 5)
      sees a chronologically ordered stream per device — critical for
      time-series anomaly detection and SHAP explanations.
    """
    try:
        payload = json.loads(msg.payload)

        # Enrich with bridge-level timestamp (epoch seconds)
        payload["bridge_received_at"] = time.time()

        # Partition key: device_id → consistent hashing → same partition
        key = payload.get("device_id", "unknown").encode()

        producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=json.dumps(payload).encode(),
            callback=delivery_callback,
        )

        # Trigger delivery callbacks for already-sent messages.
        # Without poll(), callbacks accumulate silently until flush().
        producer.poll(0)

    except json.JSONDecodeError as e:
        log.error("invalid_json", error=str(e), topic=msg.topic)
    except Exception as e:
        log.error("bridge_error", error=str(e), topic=msg.topic)


# ─── Graceful Shutdown ────────────────────────────────────────────────
def shutdown(signum, frame):
    """
    Handle SIGTERM (Docker stop) and SIGINT (Ctrl+C).

    producer.flush() blocks until ALL buffered messages are confirmed
    by Kafka. Without this, we'd lose any messages still sitting in
    the producer's internal buffer when the process exits.
    """
    log.info("shutting_down", signal=signum)
    producer.flush(timeout=10)
    mqtt_client.disconnect()
    sys.exit(0)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


# ─── Main Entry Point ────────────────────────────────────────────────
if __name__ == "__main__":
    log.info(
        "bridge_starting",
        mqtt=f"{MQTT_BROKER}:{MQTT_PORT}",
        kafka=KAFKA_BROKERS,
        topic=KAFKA_TOPIC,
    )

    # Step 1: Ensure the target Kafka topic exists
    # Retry in case Kafka brokers aren't fully up yet (common in Docker)
    for attempt in range(10):
        try:
            ensure_topic_exists(
                KAFKA_BROKERS,
                KAFKA_TOPIC,
                replication_factor=KAFKA_REPLICATION_FACTOR,
            )
            break
        except Exception as e:
            log.warning("kafka_not_ready", attempt=attempt + 1, error=str(e))
            time.sleep(3)
    else:
        log.error("kafka_unavailable_after_retries")
        sys.exit(1)

    # Step 2: Connect to MQTT broker with retry logic
    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="mqtt-kafka-bridge",
    )
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    for attempt in range(10):
        try:
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            log.info("mqtt_connection_initiated")
            break
        except ConnectionRefusedError:
            log.warning("mqtt_not_ready", attempt=attempt + 1, wait=3)
            time.sleep(3)
    else:
        log.error("mqtt_unavailable_after_retries")
        sys.exit(1)

    # loop_forever() blocks, dispatching on_connect / on_message callbacks.
    # It also handles automatic reconnection if the connection drops.
    log.info("bridge_running")
    mqtt_client.loop_forever()
