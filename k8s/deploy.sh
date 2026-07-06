#!/usr/bin/env bash
# ============================================================================
# NexusIoT — Kubernetes Master Deployment Script
# ============================================================================
# One-command deployment of the entire NexusIoT platform to Minikube.
#
# Prerequisites:
#   1. Minikube running:        minikube start --cpus 4 --memory 8192
#   2. Ingress addon enabled:   minikube addons enable ingress
#   3. Docker CLI installed:    docker --version
#   4. kubectl installed:       kubectl version --client
#
# Usage:
#   chmod +x k8s/deploy.sh
#   ./k8s/deploy.sh
#
# What this script does:
#   1. Points Docker CLI to Minikube's Docker daemon (no registry needed)
#   2. Builds all 4 custom Docker images locally
#   3. Creates the nexusiot namespace and base resources
#   4. Creates ConfigMaps from external config files
#   5. Deploys infrastructure tier (Kafka, Mosquitto, TimescaleDB)
#   6. Waits for infrastructure readiness
#   7. Deploys application tier (Bridge, Processor, API)
#   8. Deploys monitoring tier (Prometheus, Grafana)
#   9. Deploys Ingress rules
#   10. Launches device simulators
#   11. Prints access URLs
# ============================================================================

set -euo pipefail    # Exit on error (-e), undefined vars (-u), pipe failures (-o pipefail)

# Color codes for pretty output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'         # No Color

# Navigate to project root (script is in k8s/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo -e "${BLUE}🏭 NexusIoT Kubernetes Deployment${NC}"
echo "================================================="
echo ""

# ──────────────────────────────────────────────────────────────────────────
# Step 1: Configure Minikube Docker environment
# ──────────────────────────────────────────────────────────────────────────
# "eval $(minikube docker-env)" tells the Docker CLI to talk to Minikube's
# internal Docker daemon instead of the host's Docker. This means any image
# we build is immediately available to Kubernetes pods — no registry push needed.
echo -e "${YELLOW}📦 Step 1: Configuring Minikube Docker environment...${NC}"
eval $(minikube docker-env)
echo -e "${GREEN}   ✓ Docker CLI now points to Minikube's Docker daemon${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Step 2: Build Docker images
# ──────────────────────────────────────────────────────────────────────────
# Build all 4 custom images. Since we're using Minikube's Docker,
# these images are available to K8s immediately (imagePullPolicy: Never).
echo -e "${YELLOW}🔨 Step 2: Building Docker images...${NC}"
docker build -t nexusiot/bridge:latest    -f Dockerfile.bridge    . && echo -e "${GREEN}   ✓ nexusiot/bridge${NC}"
docker build -t nexusiot/processor:latest -f Dockerfile.processor . && echo -e "${GREEN}   ✓ nexusiot/processor${NC}"
docker build -t nexusiot/api:latest       -f Dockerfile.api       . && echo -e "${GREEN}   ✓ nexusiot/api${NC}"
docker build -t nexusiot/devices:latest   -f Dockerfile.devices   . && echo -e "${GREEN}   ✓ nexusiot/devices${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Step 3: Create namespace and base resources
# ──────────────────────────────────────────────────────────────────────────
# Apply foundation manifests: Namespace, ConfigMap, and Secrets.
# These must exist BEFORE any workload pods reference them.
echo -e "${YELLOW}📁 Step 3: Creating namespace and base resources...${NC}"
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secrets.yaml
echo -e "${GREEN}   ✓ Namespace 'nexusiot' + ConfigMap + Secrets created${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Step 4: Create ConfigMaps from external files
# ──────────────────────────────────────────────────────────────────────────
# Some configs are too large or complex to embed in YAML (SQL, JSON).
# We create ConfigMaps directly from the source files.
# The "--dry-run=client -o yaml | kubectl apply" pattern is idempotent —
# it creates or updates the ConfigMap without errors on re-runs.
echo -e "${YELLOW}📄 Step 4: Creating ConfigMaps from config files...${NC}"

# Mosquitto config: listener 1883, allow_anonymous true
kubectl create configmap mosquitto-config \
  --from-file=mosquitto.conf=mosquitto/mosquitto.conf \
  -n nexusiot --dry-run=client -o yaml | kubectl apply -f -

# Prometheus config: scrape targets for processor and API
kubectl create configmap prometheus-config \
  --from-file=prometheus.yml=prometheus/prometheus.yml \
  -n nexusiot --dry-run=client -o yaml | kubectl apply -f -

# TimescaleDB init schema: creates hypertables on first boot
kubectl create configmap timescaledb-init \
  --from-file=schema.sql=schema.sql \
  -n nexusiot --dry-run=client -o yaml | kubectl apply -f -

# Grafana datasource provisioning: points to Prometheus
kubectl create configmap grafana-datasources \
  --from-file=prometheus.yml=grafana/provisioning/datasources/prometheus.yml \
  -n nexusiot --dry-run=client -o yaml | kubectl apply -f -

# Grafana dashboard provider: tells Grafana where to find dashboard JSON files
kubectl create configmap grafana-dashboard-provider \
  --from-file=dashboards.yml=grafana/provisioning/dashboards/dashboards.yml \
  -n nexusiot --dry-run=client -o yaml | kubectl apply -f -

# Grafana dashboard JSON: the actual NexusIoT dashboard
kubectl create configmap grafana-dashboards \
  --from-file=nexusiot.json=grafana/dashboards/nexusiot.json \
  -n nexusiot --dry-run=client -o yaml | kubectl apply -f -

echo -e "${GREEN}   ✓ All ConfigMaps created from source files${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Step 5: Deploy infrastructure tier
# ──────────────────────────────────────────────────────────────────────────
# Infrastructure must be up before application services can start.
# Order: Mosquitto (MQTT) → Kafka (streaming) → TimescaleDB (storage)
echo -e "${YELLOW}🏗️  Step 5: Deploying infrastructure tier...${NC}"
kubectl apply -f k8s/mosquitto/
kubectl apply -f k8s/kafka/
kubectl apply -f k8s/timescaledb/
echo -e "${GREEN}   ✓ Mosquitto, Kafka, TimescaleDB manifests applied${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Step 6: Wait for infrastructure readiness
# ──────────────────────────────────────────────────────────────────────────
# Block until all infrastructure pods are healthy.
# If a pod doesn't become ready within the timeout, the script exits with an error.
echo -e "${YELLOW}⏳ Step 6: Waiting for infrastructure readiness...${NC}"

echo "   Waiting for Kafka StatefulSet (3 replicas)..."
kubectl rollout status statefulset/kafka -n nexusiot --timeout=300s

echo "   Waiting for TimescaleDB StatefulSet..."
kubectl rollout status statefulset/timescaledb -n nexusiot --timeout=120s

echo "   Waiting for Mosquitto Deployment..."
kubectl rollout status deployment/mosquitto -n nexusiot --timeout=60s

echo -e "${GREEN}   ✓ All infrastructure pods are ready${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Step 7: Deploy application tier
# ──────────────────────────────────────────────────────────────────────────
# Application services depend on infrastructure (Kafka brokers, DB, MQTT).
echo -e "${YELLOW}🚀 Step 7: Deploying application tier...${NC}"
kubectl apply -f k8s/bridge/
kubectl apply -f k8s/processor/
kubectl apply -f k8s/api/
echo -e "${GREEN}   ✓ Bridge, Processor, API manifests applied${NC}"

# Wait for applications
echo -e "${YELLOW}⏳ Waiting for application readiness...${NC}"
kubectl rollout status deployment/bridge -n nexusiot --timeout=120s
kubectl rollout status deployment/processor -n nexusiot --timeout=120s
kubectl rollout status deployment/api -n nexusiot --timeout=120s
echo -e "${GREEN}   ✓ All application pods are ready${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Step 8: Deploy monitoring tier
# ──────────────────────────────────────────────────────────────────────────
echo -e "${YELLOW}📊 Step 8: Deploying monitoring tier...${NC}"
kubectl apply -f k8s/monitoring/
kubectl rollout status deployment/prometheus -n nexusiot --timeout=60s
kubectl rollout status deployment/grafana -n nexusiot --timeout=60s
echo -e "${GREEN}   ✓ Prometheus + Grafana are ready${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Step 9: Deploy Ingress
# ──────────────────────────────────────────────────────────────────────────
echo -e "${YELLOW}🌐 Step 9: Deploying Ingress rules...${NC}"
kubectl apply -f k8s/ingress.yaml
echo -e "${GREEN}   ✓ Ingress rules applied${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Step 10: Launch device simulators
# ──────────────────────────────────────────────────────────────────────────
echo -e "${YELLOW}🤖 Step 10: Launching device simulators...${NC}"
# Delete existing job if re-running (Jobs are immutable)
kubectl delete job device-simulators -n nexusiot --ignore-not-found=true
kubectl apply -f k8s/devices/
echo -e "${GREEN}   ✓ Device simulator job launched${NC}"

# ──────────────────────────────────────────────────────────────────────────
# Done! Print access information
# ──────────────────────────────────────────────────────────────────────────
MINIKUBE_IP=$(minikube ip)

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ NexusIoT deployed successfully to Kubernetes!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BLUE}📡 Access URLs:${NC}"
echo "   Add to /etc/hosts:"
echo "   ${MINIKUBE_IP}  api.nexusiot.local grafana.nexusiot.local"
echo ""
echo "   API Docs:           http://api.nexusiot.local/docs"
echo "   Grafana Dashboards: http://grafana.nexusiot.local"
echo ""
echo -e "${BLUE}🔧 Quick access via port-forward (no /etc/hosts needed):${NC}"
echo "   kubectl port-forward svc/api 8000:8000 -n nexusiot"
echo "   kubectl port-forward svc/grafana 3000:3000 -n nexusiot"
echo "   kubectl port-forward svc/prometheus 9090:9090 -n nexusiot"
echo ""
echo -e "${BLUE}📋 Useful commands:${NC}"
echo "   kubectl get pods -n nexusiot          # Check pod status"
echo "   kubectl logs -f deploy/processor -n nexusiot  # Stream processor logs"
echo "   kubectl logs -f job/device-simulators -n nexusiot  # Device logs"
echo ""
