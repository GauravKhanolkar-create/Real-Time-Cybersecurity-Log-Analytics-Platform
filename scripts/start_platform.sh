#!/bin/bash
set -e

echo "=============================================="
echo "  CyberSec Log Analytics Platform"
echo "  Starting all services (staged bring-up)..."
echo "=============================================="

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if [ ! -f .env ]; then
    echo "No .env found — copying .env.example to .env."
    cp .env.example .env
fi

# Step 0: build all images once up front.
echo "[0/8] Building images (producer, spark, airflow, superset)..."
docker compose build

# Step 1: Infrastructure
echo "[1/8] Starting Infrastructure (Postgres, Redis)..."
docker compose up -d postgres redis
sleep 10
docker compose ps postgres redis

# Step 2: Kafka
echo "[2/8] Starting Kafka (Zookeeper -> Broker -> UI)..."
docker compose up -d zookeeper
sleep 15
docker compose up -d kafka-broker
sleep 20
docker compose up -d kafka-ui
echo "  Kafka UI: http://localhost:8090"

# Step 3: Storage
echo "[3/8] Starting Storage (MinIO, Elasticsearch, Kibana)..."
docker compose up -d minio
sleep 10
docker compose up -d minio-init
docker compose up -d elasticsearch
sleep 30
docker compose up -d kibana
bash "$PROJECT_DIR/scripts/init-elasticsearch.sh" || echo "  (Elasticsearch mapping will be retried later if this failed — it may still be starting.)"
echo "  MinIO Console: http://localhost:9001"
echo "  Kibana:        http://localhost:5601"

# Step 4: Spark
echo "[4/8] Starting Spark Cluster..."
docker compose up -d spark-master
sleep 15
docker compose up -d spark-worker-1 spark-worker-2
echo "  Spark UI: http://localhost:8080"

# Step 5: Orchestration
echo "[5/8] Initializing Airflow..."
docker compose up airflow-init
echo "  Airflow DB initialized."
docker compose up -d airflow-webserver airflow-scheduler airflow-worker
sleep 20
echo "  Airflow UI: http://localhost:8089 (login: admin / admin, or your .env values)"

# Step 6: Visualization
echo "[6/8] Starting Apache Superset..."
docker compose up -d superset
sleep 15
echo "  Superset UI: http://localhost:8088"

# Step 7: Application layer
echo "[7/8] Starting log-producer (idle — waits for /start) and alert-service..."
docker compose up -d log-producer alert-service

# Step 8: Health check
echo "[8/8] Running health checks..."
bash "$PROJECT_DIR/scripts/health_check.sh"

echo ""
echo "=============================================="
echo "  All services started."
echo "=============================================="
echo ""
echo "  Service URLs:"
echo "  - Airflow:        http://localhost:8089"
echo "  - Kafka UI:       http://localhost:8090"
echo "  - Spark UI:       http://localhost:8080"
echo "  - MinIO:          http://localhost:9001"
echo "  - Kibana:         http://localhost:5601"
echo "  - Superset:       http://localhost:8088"
echo "  - Elasticsearch:  http://localhost:9200"
echo "  - Producer API:   http://localhost:5050"
echo ""
echo "  Next step: trigger the pipeline DAG:"
echo "      bash scripts/run-pipeline.sh"
echo "  or open http://localhost:8089 and trigger 'cybersec_log_pipeline' from the UI."
