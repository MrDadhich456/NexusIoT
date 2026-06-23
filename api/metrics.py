"""
API Prometheus Metrics
=======================
Custom metrics for monitoring the FastAPI application.
Scraped by Prometheus on GET /metrics (mounted as ASGI sub-app in main.py).

Metric naming convention:
  nexusiot_api_<noun>_<unit>
  Labels: [method, endpoint, status_code] for request metrics

These metrics feed into the Grafana "Platform Health" dashboard (Step 9):
  - Request rate: how many API calls per second
  - Request latency: p50/p95/p99 response times
  - WebSocket connections: how many live dashboard clients
  - DB query latency: time spent waiting for TimescaleDB
"""

from prometheus_client import Counter, Histogram, Gauge


# ─── HTTP Request Metrics ────────────────────────────────────────────

# Total API requests — labelled by method, endpoint, status_code.
# Example Prometheus query: rate(nexusiot_api_requests_total[5m])
api_requests_total = Counter(
    "nexusiot_api_requests_total",
    "Total HTTP requests received by the API",
    ["method", "endpoint", "status_code"],
)

# Request duration histogram — measures end-to-end response time.
# Buckets chosen for a typical API: most responses <50ms, alerting >500ms.
# Example: histogram_quantile(0.95, nexusiot_api_request_duration_seconds)
api_request_duration = Histogram(
    "nexusiot_api_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)


# ─── WebSocket Metrics ───────────────────────────────────────────────

# Active WebSocket connections — gauge (goes up and down).
# Separate labels for telemetry vs anomaly connections.
ws_connections_active = Gauge(
    "nexusiot_ws_connections_active",
    "Currently active WebSocket connections",
    ["type"],  # type: "telemetry" or "anomaly"
)

# Total WebSocket messages sent to clients.
ws_messages_sent = Counter(
    "nexusiot_ws_messages_sent_total",
    "Total WebSocket messages pushed to clients",
    ["type"],  # type: "telemetry" or "anomaly"
)


# ─── Database Metrics ────────────────────────────────────────────────

# TimescaleDB query duration — measures time spent waiting for DB response.
# Helps identify slow queries or connection pool exhaustion.
db_query_duration = Histogram(
    "nexusiot_api_db_query_seconds",
    "TimescaleDB query execution time in seconds",
    ["query"],  # query: "list_devices", "latest_reading", "history", etc.
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
)

# Database query errors (after the query itself fails, not connection issues).
db_query_errors = Counter(
    "nexusiot_api_db_errors_total",
    "Total failed database queries",
    ["query"],
)


# ─── Kafka Consumer Metrics ──────────────────────────────────────────

# Messages consumed from Kafka for WebSocket fan-out.
kafka_messages_consumed = Counter(
    "nexusiot_api_kafka_consumed_total",
    "Total Kafka messages consumed for WebSocket fan-out",
    ["topic"],
)
