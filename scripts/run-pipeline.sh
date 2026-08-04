#!/bin/bash
# Triggers the main orchestration DAG (health checks -> topics -> train
# model -> start streaming -> start producer). Run this after
# start_platform.sh has finished and every service is healthy.
set -e

echo "Triggering DAG: cybersec_log_pipeline ..."
docker exec airflow-webserver airflow dags trigger cybersec_log_pipeline

echo ""
echo "Track progress at: http://localhost:8089  (DAGs -> cybersec_log_pipeline -> Graph view)"
echo "Or tail it from the CLI with:"
echo "    docker exec airflow-webserver airflow dags list-runs -d cybersec_log_pipeline"
