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
├── processor/               # Stream processing worker (Kafka consumer, SHAP explainer)
├── api/                     # FastAPI application (REST + WebSocket endpoints)
├── k8s/                     # Kubernetes manifests for all microservices
├── terraform/               # AWS Infrastructure as Code
└── .github/workflows/       # CI/CD pipelines
```

---

## 🏗️ Build Progress & Roadmap

We are building this platform layer by layer:

- [x] **Step 1: Environment Setup** — Project scaffolding, virtual environments, and dependency management.
- [x] **Step 2: Device Simulators** — Python classes that generate realistic, drifting, and noisy data for industrial sensors.
- [ ] **Step 3: MQTT Broker** — Setting up Mosquitto to receive device telemetry.
- [ ] **Step 4: Kafka Pipeline** — Bridging MQTT to a durable Kafka stream.
- [ ] **Step 5: Stream Processor** — Consuming data, validating schemas, and detecting anomalies.
- [ ] **Step 6: SHAP Explainer** — Explaining *why* an anomaly was flagged.
- [ ] **Step 7: TimescaleDB** — High-performance time-series data storage.
- [ ] **Step 8: FastAPI + WebSocket** — Live streaming data to the frontend.
- [ ] **Step 9: Observability** — Metrics, logging, and dashboards.
- [ ] **Step 10: Kubernetes** — Container orchestration for all services.
- [ ] **Step 11: Terraform** — Provisioning free-tier AWS infrastructure.
- [ ] **Step 12: CI/CD Pipeline** — Automated testing and deployment.

---

## 💻 Getting Started (Local Development)

*(Instructions will be added here as we progress through the infrastructure steps.)*

1. Ensure you have Python 3.13+ installed.
2. Create and activate the virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run a device simulator to test data generation:
   ```bash
   python -m devices.cnc_machine
   ```
