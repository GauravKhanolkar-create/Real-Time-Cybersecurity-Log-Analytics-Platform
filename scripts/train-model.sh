#!/bin/bash
# Manually (re)trains the RandomForest model, without going through Airflow.
# Useful right after `docker compose up` before you've configured Airflow,
# or any time you want to retrain on an updated data/logs.csv.
set -e

echo "Training ML model on spark-master (this can take a minute)..."
docker exec spark-master /opt/bitnami/spark/bin/spark-submit /opt/spark-apps/ml_model.py

echo ""
echo "Model + label mapping written to ./spark/models/"
ls -la "$(dirname "$0")/../spark/models/"
