"""
Stream Processor — Kafka Consumer Main Loop
=============================================
The beating heart of the stream processor. Runs forever, pulling messages
from Kafka's raw-telemetry topic, validating them, running anomaly detection,
explaining anomalies with SHAP, persisting to TimescaleDB, and producing
enriched alerts.

Data Flow (Step 5 + Step 6 + Step 7):
  Kafka [raw-telemetry]
    → Consume
    → Validate (Pydantic)
    → Detect (Z-score)
    → IF anomaly: Explain (SHAP) → Write to TimescaleDB → Produce [anomaly-events]
    → IF normal: Feed into SHAP training buffer
    → Write ALL valid readings to TimescaleDB

Why this architecture?
  - Consumer groups enable horizontal scaling (run N instances, Kafka auto-splits)
  - Manual offset commit = at-least-once processing (never skip a message)
  - Idempotent producer = safe retries (no duplicate anomaly alerts)
  - SHAP explainer adds per-feature contribution % to every anomaly alert
  - TimescaleDB gives permanent, queryable storage for historical analysis
  - Prometheus metrics = observable in Grafana dashboards (Step 9)
"""

import json
import time
import os
import sys
import signal
import logging

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import Counter, Gauge, start_http_server

import structlog

from .validator import validate_telemetry
from .detector import AnomalyDetector
from .explainer import SHAPExplainer
from .writer import TimescaleWriter

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()


# ─── Configuration (12-Factor: env vars) ─────────────────────────────
# All config comes from environment variables so the SAME code runs
# in dev, staging, and production — only the env vars change.
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19094")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "raw-telemetry")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "anomaly-events")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "stream-processor")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))
TIMESCALE_DSN = os.getenv(
    "TIMESCALE_DSN", "postgresql://nexusiot:nexusiot@localhost:5432/nexusiot"
)


# ─── Prometheus Metrics ──────────────────────────────────────────────
# These counters live in memory and are scraped by Prometheus on :8001/metrics.
# Labels let us filter by device_type and status in Grafana queries.
messages_processed = Counter(
    "processor_messages_total",
    "Total messages consumed from Kafka",
    ["device_type", "status"],  # status: "valid", "invalid", "error"
)

anomalies_detected = Counter(
    "processor_anomalies_total",
    "Anomalies detected by the Z-score detector",
    ["device_type", "severity"],  # severity: "warning", "critical"
)

processing_lag = Gauge(
    "processor_lag_seconds",
    "Time from device timestamp to processor processing (seconds)",
)

# SHAP Explainer metrics (Step 6)
# Track how many anomalies got SHAP explanations vs. how many were
# skipped because the model was still warming up (not enough normal data).
shap_explanations = Counter(
    "processor_shap_explanations_total",
    "Total SHAP explanations successfully computed",
    ["device_type"],
)

shap_model_not_ready = Counter(
    "processor_shap_model_not_ready_total",
    "Times SHAP explanation was skipped (model not yet trained)",
    ["device_type"],
)

# TimescaleDB metrics (Step 7)
# Track successful and failed database writes for alerting.
db_writes = Counter(
    "processor_db_writes_total",
    "Total successful database writes",
    ["table"],  # table: "telemetry" or "anomaly_events"
)

db_errors = Counter(
    "processor_db_errors_total",
    "Total failed database writes (after all retries)",
    ["table"],
)


# ─── Kafka Topic Bootstrap ──────────────────────────────────────────
def ensure_topic_exists(
    bootstrap_servers: str,
    topic: str,
    num_partitions: int = 6,
    replication_factor: int = 3,
):
    """
    Create a Kafka topic if it doesn't already exist.

    We need the anomaly-events topic to exist before producing to it.
    Same logic as the bridge's ensure_topic_exists — DRY violation is
    intentional: each microservice should be self-contained and not
    import from sibling packages.
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
        future.result()
        log.info(
            "topic_created",
            topic=topic_name,
            partitions=num_partitions,
            replication=replication_factor,
        )


# ─── Kafka Consumer ─────────────────────────────────────────────────
consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BROKERS,
        # Consumer group: Kafka tracks which messages each group has processed.
        # If we run 3 instances with the same group.id, Kafka splits 6 partitions
        # across them (2 each) — automatic horizontal scaling.
        "group.id": CONSUMER_GROUP,
        # Where to start if this consumer group has never consumed before:
        # "earliest" = read from the very beginning of the topic
        # "latest"   = only read new messages (would miss historical data)
        "auto.offset.reset": "earliest",
        # CRITICAL: We commit offsets MANUALLY after processing each message.
        # Auto-commit says "I processed this" on a timer, even if we crashed
        # mid-processing. Manual commit = we only acknowledge AFTER validation
        # and anomaly detection are complete.
        "enable.auto.commit": False,
    }
)


# ─── Kafka Producer (for anomaly events) ────────────────────────────
# Same reliability settings as the bridge: acks=all + idempotent.
producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKERS,
        "client.id": "stream-processor",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 100,
        "enable.idempotence": True,
    }
)


def delivery_callback(err, msg):
    """Called when Kafka confirms (or rejects) an anomaly event."""
    if err:
        log.error("anomaly_delivery_failed", error=str(err), topic=msg.topic())
    else:
        log.debug(
            "anomaly_delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


# ─── Graceful Shutdown ───────────────────────────────────────────────
running = True


def shutdown(signum, frame):
    """
    Handle SIGTERM (Docker stop) and SIGINT (Ctrl+C).

    1. Set running=False to break the main loop
    2. Flush producer to ensure all anomaly events are confirmed
    3. Close consumer to leave the consumer group cleanly
       (Kafka immediately reassigns partitions to other instances)
    """
    global running
    log.info("shutting_down", signal=signum)
    running = False


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


# ─── Main Processing Loop ───────────────────────────────────────────
def run():
    """
    The main event loop. Steps per message:

    1. consumer.poll()        — Block up to 1s waiting for a message
    2. json.loads()           — Deserialize the raw bytes into a dict
    3. validate_telemetry()   — Parse through the Pydantic schema
    4. detector.check()       — Compute Z-scores, flag anomalies
    5. producer.produce()     — Send anomaly alerts to anomaly-events topic
    6. consumer.commit()      — Tell Kafka "I'm done with this message"
    7. Update Prometheus metrics

    Why commit AFTER processing?
      If we crash between steps 3-5, Kafka re-delivers the message.
      This gives us at-least-once processing: we might process a message
      twice, but never skip one. The idempotent producer handles dupes.
    """
    log.info(
        "processor_starting",
        kafka=KAFKA_BROKERS,
        input_topic=INPUT_TOPIC,
        output_topic=OUTPUT_TOPIC,
        consumer_group=CONSUMER_GROUP,
        timescale_dsn="***",
    )

    # Start Prometheus metrics HTTP server on port 8001
    start_http_server(METRICS_PORT)
    log.info("metrics_server_started", port=METRICS_PORT)

    # Ensure the output topic exists (retry if Kafka isn't ready yet)
    for attempt in range(10):
        try:
            ensure_topic_exists(
                KAFKA_BROKERS,
                OUTPUT_TOPIC,
                replication_factor=KAFKA_REPLICATION_FACTOR,
            )
            break
        except Exception as e:
            log.warning("kafka_not_ready", attempt=attempt + 1, error=str(e))
            time.sleep(3)
    else:
        log.error("kafka_unavailable_after_retries")
        sys.exit(1)

    # Subscribe to the input topic
    consumer.subscribe([INPUT_TOPIC])
    log.info("subscribed_to_topic", topic=INPUT_TOPIC)

    # Initialize the anomaly detector (sliding windows start empty)
    detector = AnomalyDetector(window_size=100, z_threshold=3.5, min_samples=20)

    # Initialize the SHAP explainer (Step 6)
    # min_samples=100: ~3.3 min warmup before first model training
    # retrain_every=200: retrain every ~6.6 min to adapt to drift
    explainer = SHAPExplainer(min_samples=100, retrain_every=200)

    # Initialize the TimescaleDB writer (Step 7)
    # Connection pool opens 2 connections immediately, scales to 5 on burst.
    # The writer handles retries internally — a failed write won't crash the loop.
    writer = TimescaleWriter(dsn=TIMESCALE_DSN)

    total_processed = 0
    total_anomalies = 0

    while running:
        # ─── Step 1: Poll for a message ──────────────────────────
        # Blocks for up to 1 second. Returns None if no message available.
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue  # No message, loop back and poll again

        if msg.error():
            # Partition EOF is normal (we've caught up to the end)
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log.debug(
                    "partition_eof", partition=msg.partition(), offset=msg.offset()
                )
            else:
                log.error("consumer_error", error=str(msg.error()))
            continue

        # ─── Step 2: Deserialize ─────────────────────────────────
        try:
            raw = json.loads(msg.value())
        except json.JSONDecodeError as e:
            log.error(
                "json_decode_error",
                error=str(e),
                partition=msg.partition(),
                offset=msg.offset(),
            )
            messages_processed.labels(device_type="unknown", status="error").inc()
            consumer.commit(message=msg)
            continue

        device_type = raw.get("device_type", "unknown")

        # ─── Step 3: Validate ────────────────────────────────────
        telemetry = validate_telemetry(raw)
        if telemetry is None:
            messages_processed.labels(device_type=device_type, status="invalid").inc()
            consumer.commit(message=msg)  # Skip bad data, don't reprocess
            continue

        # ─── Step 4: Detect anomalies ────────────────────────────
        anomalies = detector.check(telemetry)

        # ─── Step 4b: SHAP Explainer (Step 6) ────────────────────
        # Two paths:
        #   A) Anomaly detected → explain WHY with SHAP contributions
        #   B) No anomaly → feed this normal reading into SHAP buffer
        if anomalies:
            # Path A: Compute SHAP explanation for the full reading.
            # We pass ALL metrics (not just the anomalous field) so SHAP
            # can reveal cross-metric correlations — e.g., "vibration
            # spiked BUT the primary driver was spindle RPM dropping".
            shap_result = explainer.explain(
                device_id=telemetry.device_id,
                device_type=device_type,
                telemetry=telemetry,
            )

            # Attach SHAP contributions to EVERY anomaly event from
            # this reading. If model isn't trained yet, shap_result
            # is None → anomaly event has shap_contributions: null.
            for anomaly in anomalies:
                anomaly["shap_contributions"] = shap_result

            # Update Prometheus metrics for SHAP observability
            if shap_result:
                shap_explanations.labels(device_type=device_type).inc()
            else:
                shap_model_not_ready.labels(device_type=device_type).inc()
        else:
            # Path B: Normal reading — feed into SHAP training buffer.
            # The IsolationForest only trains on non-anomalous data so
            # it learns what "normal" looks like for this device.
            explainer.update_normal(
                device_id=telemetry.device_id,
                device_type=device_type,
                telemetry=telemetry,
            )

        # ─── Step 5: Produce anomaly alerts ──────────────────────
        for anomaly in anomalies:
            producer.produce(
                topic=OUTPUT_TOPIC,
                key=anomaly["device_id"].encode(),
                value=json.dumps(anomaly).encode(),
                callback=delivery_callback,
            )
            anomalies_detected.labels(
                device_type=anomaly["device_type"],
                severity=anomaly["severity"],
            ).inc()

        # Trigger delivery callbacks for queued messages
        producer.poll(0)

        # ─── Step 6: Write to TimescaleDB ────────────────────────
        # Write EVERY valid reading to the telemetry table.
        # We extract all sensor-specific fields (everything except
        # the base fields) into a metrics dict for JSONB storage.
        base_fields = {"device_id", "device_type", "timestamp", "bridge_received_at"}
        metrics = {
            k: v for k, v in telemetry.model_dump().items() if k not in base_fields
        }
        if writer.write_telemetry(
            device_id=telemetry.device_id,
            device_type=device_type,
            timestamp=telemetry.timestamp,
            metrics=metrics,
        ):
            db_writes.labels(table="telemetry").inc()
        else:
            db_errors.labels(table="telemetry").inc()

        # Write each anomaly event to the anomaly_events table.
        for anomaly in anomalies:
            if writer.write_anomaly(anomaly):
                db_writes.labels(table="anomaly_events").inc()
            else:
                db_errors.labels(table="anomaly_events").inc()

        # ─── Step 7: Commit offset ───────────────────────────────
        # "I'm done processing this message" — Kafka records the offset
        # so if we restart, we pick up from where we left off.
        consumer.commit(message=msg)

        # ─── Step 8: Update metrics ──────────────────────────────
        messages_processed.labels(device_type=device_type, status="valid").inc()

        # Processing lag: how long from device → processor
        lag = time.time() - telemetry.timestamp
        processing_lag.set(lag)

        total_processed += 1
        total_anomalies += len(anomalies)

        # Periodic progress log (every 50 messages)
        if total_processed % 50 == 0:
            log.info(
                "processing_progress",
                total_processed=total_processed,
                total_anomalies=total_anomalies,
            )

    # ─── Cleanup on shutdown ─────────────────────────────────────
    log.info("flushing_producer")
    producer.flush(timeout=10)
    consumer.close()
    writer.close()  # Drain the TimescaleDB connection pool
    log.info(
        "processor_stopped",
        total_processed=total_processed,
        total_anomalies=total_anomalies,
    )


# ─── Entry Point ─────────────────────────────────────────────────────
if __name__ == "__main__":
    run()
