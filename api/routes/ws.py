"""
WebSocket Live Streaming + Kafka Fan-out
==========================================
Real-time data push from Kafka topics to browser clients.

Architecture:
  Kafka Consumer (background asyncio task, group: ws-fanout)
    ├── raw-telemetry    → broadcast_telemetry(device_id, data)
    │                       → sends to WebSockets subscribed to that device
    └── anomaly-events   → broadcast_anomaly(data)
                            → sends to ALL anomaly WebSocket subscribers

WebSocket Endpoints:
  WS /ws/telemetry/{device_id}  — per-device live sensor stream
  WS /ws/anomalies              — global anomaly alert broadcast

Why a separate Kafka consumer group (ws-fanout)?
  The stream processor uses consumer group "stream-processor".
  The API uses "ws-fanout". Kafka delivers messages to BOTH groups
  independently — the API doesn't interfere with processing.

Why asyncio.to_thread for Kafka poll?
  confluent_kafka.Consumer.poll() is a blocking C call.
  Wrapping it in asyncio.to_thread() runs it in a thread pool,
  keeping the asyncio event loop free for WebSocket I/O.
"""

import os
import json
import asyncio
import logging
from typing import Dict, Set

import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from confluent_kafka import Consumer, KafkaError

from api.metrics import ws_connections_active, ws_messages_sent, kafka_messages_consumed

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()

# ─── Configuration ───────────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19094")
TELEMETRY_TOPIC = os.getenv("INPUT_TOPIC", "raw-telemetry")
ANOMALY_TOPIC = os.getenv("OUTPUT_TOPIC", "anomaly-events")

router = APIRouter(tags=["websocket"])

# ─── Background task handle ─────────────────────────────────────────
_consumer_task: asyncio.Task | None = None


class ConnectionManager:
    """
    Manages WebSocket connections for live streaming.

    Two types of subscriptions:
      1. device_sockets[device_id] → Set of WebSockets watching one device
      2. anomaly_sockets → Set of WebSockets watching ALL anomaly alerts

    Dead connection cleanup:
      When send_json() raises an exception (client disconnected without
      a proper close frame), the socket is added to a 'dead' set and
      removed after iteration. This prevents memory leaks.
    """

    def __init__(self):
        self.device_sockets: Dict[str, Set[WebSocket]] = {}
        self.anomaly_sockets: Set[WebSocket] = set()

    async def connect_device(self, ws: WebSocket, device_id: str):
        """Accept a WebSocket and subscribe it to a device's telemetry."""
        await ws.accept()
        self.device_sockets.setdefault(device_id, set()).add(ws)
        ws_connections_active.labels(type="telemetry").inc()
        log.info("ws_device_connected", device_id=device_id)

    async def connect_anomaly(self, ws: WebSocket):
        """Accept a WebSocket and subscribe it to all anomaly alerts."""
        await ws.accept()
        self.anomaly_sockets.add(ws)
        ws_connections_active.labels(type="anomaly").inc()
        log.info("ws_anomaly_connected")

    def disconnect_device(self, ws: WebSocket, device_id: str):
        """Remove a device WebSocket on disconnect."""
        sockets = self.device_sockets.get(device_id)
        if sockets:
            sockets.discard(ws)
            if not sockets:
                del self.device_sockets[device_id]
        ws_connections_active.labels(type="telemetry").dec()
        log.info("ws_device_disconnected", device_id=device_id)

    def disconnect_anomaly(self, ws: WebSocket):
        """Remove an anomaly WebSocket on disconnect."""
        self.anomaly_sockets.discard(ws)
        ws_connections_active.labels(type="anomaly").dec()
        log.info("ws_anomaly_disconnected")

    async def broadcast_telemetry(self, device_id: str, data: dict):
        """Send telemetry data to all WebSockets watching this device."""
        sockets = self.device_sockets.get(device_id, set())
        if not sockets:
            return

        dead: Set[WebSocket] = set()
        for ws in sockets:
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)

        # Clean up dead connections
        for ws in dead:
            sockets.discard(ws)
            ws_connections_active.labels(type="telemetry").dec()

        if dead:
            log.debug("ws_dead_cleaned", device_id=device_id, count=len(dead))

        ws_messages_sent.labels(type="telemetry").inc(len(sockets))

    async def broadcast_anomaly(self, data: dict):
        """Send anomaly alert to ALL anomaly subscribers."""
        if not self.anomaly_sockets:
            return

        dead: Set[WebSocket] = set()
        for ws in self.anomaly_sockets:
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)

        self.anomaly_sockets -= dead
        for _ in dead:
            ws_connections_active.labels(type="anomaly").dec()

        ws_messages_sent.labels(type="anomaly").inc(len(self.anomaly_sockets))


# Global connection manager instance
manager = ConnectionManager()


# ─── WebSocket Endpoints ─────────────────────────────────────────────


@router.websocket("/ws/telemetry/{device_id}")
async def telemetry_ws(ws: WebSocket, device_id: str):
    """
    Per-device live telemetry stream.

    Client connects → receives every reading from this device as JSON.
    The client sends keep-alive pings; we just wait for disconnect.
    """
    await manager.connect_device(ws, device_id)
    try:
        while True:
            # Wait for client messages (keep-alive pings)
            # This blocks until the client disconnects
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_device(ws, device_id)


@router.websocket("/ws/anomalies")
async def anomaly_ws(ws: WebSocket):
    """
    Global anomaly alert broadcast.

    Client connects → receives ALL anomaly events from ALL devices.
    """
    await manager.connect_anomaly(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_anomaly(ws)


# ─── Kafka → WebSocket Fan-out (Background Task) ────────────────────


async def start_kafka_consumer():
    """
    Start the background Kafka consumer that fans out messages
    to WebSocket clients. Called during FastAPI lifespan startup.
    """
    global _consumer_task
    _consumer_task = asyncio.create_task(_kafka_fanout_loop())
    log.info("kafka_ws_fanout_started", topics=[TELEMETRY_TOPIC, ANOMALY_TOPIC])


async def stop_kafka_consumer():
    """Cancel the background Kafka consumer. Called during shutdown."""
    global _consumer_task
    if _consumer_task is not None:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
        _consumer_task = None
        log.info("kafka_ws_fanout_stopped")


async def _kafka_fanout_loop():
    """
    Background loop: consume Kafka → fan-out to WebSocket clients.

    Uses asyncio.to_thread() to run the blocking Kafka poll in a
    thread pool worker, keeping the event loop free for WS I/O.

    Consumer group "ws-fanout" is separate from "stream-processor"
    so the API gets its own independent copy of all messages.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "ws-fanout",
            "auto.offset.reset": "latest",  # Only stream NEW messages
            "enable.auto.commit": True,  # Auto-commit is fine for fan-out
        }
    )
    consumer.subscribe([TELEMETRY_TOPIC, ANOMALY_TOPIC])
    log.info("kafka_consumer_subscribed", topics=[TELEMETRY_TOPIC, ANOMALY_TOPIC])

    try:
        while True:
            # Run blocking poll() in thread pool (50ms timeout)
            msg = await asyncio.to_thread(consumer.poll, 0.05)

            if msg is None:
                await asyncio.sleep(0)  # Yield to event loop
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # Normal: caught up to end of partition
                log.error("kafka_fanout_error", error=str(msg.error()))
                continue

            # Deserialize message
            try:
                data = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                continue

            topic = msg.topic()
            kafka_messages_consumed.labels(topic=topic).inc()

            # Fan-out based on topic
            if topic == TELEMETRY_TOPIC:
                device_id = data.get("device_id")
                if device_id:
                    await manager.broadcast_telemetry(device_id, data)
            elif topic == ANOMALY_TOPIC:
                await manager.broadcast_anomaly(data)

    except asyncio.CancelledError:
        log.info("kafka_fanout_cancelled")
    finally:
        consumer.close()
        log.info("kafka_fanout_consumer_closed")
