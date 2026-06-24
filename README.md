# 🏭 NexusIoT: Production-Grade Industrial IoT Platform

NexusIoT is a comprehensive, production-grade telemetry platform designed for industrial environments. It provides real-time streaming, explainable anomaly detection, and robust data persistence for heavy machinery like CNC machines, robotic arms, and conveyor belts.

> **Note:** This project implements an enterprise-grade architecture using 100% self-hosted, free-tier equivalents of expensive AWS managed services (running via Kubernetes/Minikube instead of EKS, MSK, RDS, etc.).

---

## 🚀 Architecture & Tech Stack

The platform is designed to handle high-throughput, bursty sensor telemetry with absolute data integrity and sub-50ms latency from device to dashboard.

- **Messaging & Stream Buffer**: 📡 `Mosquitto (MQTT)` → ⚡ `Apache Kafka`
- **Data Persistence**: 🗄️ `TimescaleDB` (PostgreSQL with time-series partitioning)
- **Machine Learning**: 🧠 `SHAP` Explainable AI (XAI) + `scikit-learn` for Z-score anomaly detection
- **Backend API**: 🔌 `FastAPI` + `WebSockets` (Live telemetry streaming)
- **Infrastructure & Deployment**: ☸️ `Kubernetes` (K8s) + `Terraform` + `GitHub Actions` (CI/CD)
- **Observability**: 📊 `Prometheus` + `Grafana`

---

## 📁 Project Structure

```text
nexusiot/
├── devices/                 # IoT sensor simulators (CNC, Robotic Arm, Conveyor Belt)
├── bridge/                  # MQTT → Kafka bridge microservice
├── processor/               # Stream processing worker (Kafka consumer, Z-score detector)
├── api/                     # FastAPI application (REST + WebSocket endpoints)
├── mosquitto/               # Mosquitto broker configuration
├── k8s/                     # Kubernetes manifests for all microservices
├── terraform/               # AWS Infrastructure as Code
└── .github/workflows/       # CI/CD pipelines
```

---

## 🏗️ Build Progress & Roadmap

We are building this platform layer by layer:

- [x] **Step 1: Environment Setup** — Project scaffolding, virtual environments, and dependency management.
- [x] **Step 2: Device Simulators** — Python classes that generate realistic, drifting, and noisy data for industrial sensors.
- [x] **Step 3: MQTT Broker** — Setting up Mosquitto via Docker Compose to receive device telemetry over port 1883.
- [x] **Step 4: Kafka Pipeline** — 3-broker Kafka cluster (KRaft mode, no ZooKeeper) with an MQTT-to-Kafka bridge microservice that forwards all sensor data into the `raw-telemetry` topic with device-level partitioning for ordered, durable streaming.
- [x] **Step 5: Stream Processor** — Kafka consumer microservice with Pydantic schema validation, sliding-window Z-score anomaly detection, anomaly event production to the `anomaly-events` topic, and Prometheus metrics on port 8001.
- [x] **Step 6: SHAP Explainer** — Explainable AI layer using IsolationForest + SHAP TreeExplainer. Per-device models train on normal readings, then compute per-feature contribution percentages (e.g., `spindle_rpm: 68%, vibration_g: 22%`) for every anomaly alert. Enriched events on the `anomaly-events` topic now include `shap_contributions`.
- [x] **Step 7: TimescaleDB** — High-performance time-series data storage with auto-partitioned hypertables (`telemetry` for all readings, `anomaly_events` for alerts + SHAP). Connection-pooled writer with retry logic. JSONB metrics storage for zero-migration device extensibility.
- [x] **Step 8: FastAPI + WebSocket** — Production API gateway with REST endpoints for historical telemetry/anomaly queries (TimescaleDB), real-time WebSocket streaming via Kafka fan-out consumer, SHAP anomaly explanation endpoint, Prometheus metrics, and Kubernetes-ready health checks. Interactive API docs at `/docs`.
- [x] **Step 9: Observability** — Metrics, logging, and dashboards (Prometheus & Grafana).
- [ ] **Step 10: Kubernetes** — Container orchestration for all services.
- [ ] **Step 11: Terraform** — Provisioning free-tier AWS infrastructure.
- [ ] **Step 12: CI/CD Pipeline** — Automated testing and deployment.

---

## 💻 Getting Started (Local Development)

### Prerequisites

- Python 3.13+
- Docker & Docker Compose

### 1. Clone & Setup

```bash
git clone https://github.com/MrDadhich456/NexusIoT.git
cd NexusIoT
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Infrastructure

```bash
# Start the full stack: Mosquitto + Kafka cluster (3 brokers) + Kafka UI + Bridge
docker compose up -d
```

### 3. Verify Services

```bash
# Check all containers are running
docker compose ps

# Open Kafka UI in browser
# http://localhost:8080
```

### 4. Run a Device Simulator

```bash
# Start a CNC machine simulator (publishes to MQTT → Bridge → Kafka)
python -m devices.cnc_machine
```

### 5. Verify End-to-End Data Flow

```bash
# Consume messages from Kafka to confirm data is flowing
docker compose exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic raw-telemetry \
  --from-beginning
```

---

## 🔧 Services & Ports

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| Mosquitto | `nexusiot-mosquitto` | `1883` | MQTT broker for device telemetry |
| Kafka Broker 1 | `nexusiot-kafka-1` | `19094` | Kafka (external listener) |
| Kafka Broker 2 | `nexusiot-kafka-2` | `29094` | Kafka (external listener) |
| Kafka Broker 3 | `nexusiot-kafka-3` | `39094` | Kafka (external listener) |
| Kafka UI | `nexusiot-kafka-ui` | `8080` | Web dashboard for Kafka inspection |
| Bridge | `nexusiot-bridge` | — | MQTT → Kafka forwarder (no external port) |
| Processor | `nexusiot-processor` | `8001` | Stream processor (anomaly detection + Prometheus metrics) |
| TimescaleDB | `nexusiot-timescaledb` | `5432` | Time-series database (PostgreSQL + hypertables) |
| API Gateway | `nexusiot-api` | `8000` | FastAPI REST + WebSocket (docs at http://localhost:8000/docs) |
| Prometheus | `nexusiot-prometheus` | `9090` | Metrics scraper (http://localhost:9090) |
| Grafana | `nexusiot-grafana` | `3000` | Dashboards (http://localhost:3000) |
