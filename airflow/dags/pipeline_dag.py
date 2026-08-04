"""
pipeline_dag.py

Main orchestration DAG for the CyberSec Log Analytics Platform.

Brings the pipeline up in dependency order:

    health checks (Kafka/Spark/MinIO/Elasticsearch)
        -> create Kafka topics
        -> train the ML model (offline, on the full CSV)
        -> launch the Spark Structured Streaming job (detached, long-running)
        -> trigger the log-producer's Flask API to start streaming rows
        -> a light monitoring/sanity task
"""

from datetime import timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "cybersec-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

PRODUCER_URL = "http://log-producer:5000"

def trigger_producer(**context):  # noqa: ANN001, ARG001
    resp = requests.post(f"{PRODUCER_URL}/start", json={"loop": False}, timeout=10)
    
    # Accept 200 (OK), 201 (Created), and 409 (Conflict/Already Running) as success
    if resp.status_code in (200, 201, 409):
        print(f"[Airflow] Producer is active. Response: {resp.status_code} {resp.text}")
        return
        
    # If it's any other error (like 500), crash the task
    resp.raise_for_status()

with DAG(
    dag_id="cybersec_log_pipeline",
    default_args=default_args,
    description="Real-Time Cybersecurity Log Analytics Pipeline — bring-up DAG",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["cybersec", "streaming", "kafka", "spark"],
) as dag:

    # ── Health checks ───────────────────────────────────────────
    check_kafka = BashOperator(
        task_id="check_kafka_health",
        bash_command=(
            "docker exec kafka-broker kafka-broker-api-versions "
            "--bootstrap-server localhost:9092 > /dev/null && echo 'Kafka OK'"
        ),
    )

    check_spark = BashOperator(
        task_id="check_spark_health",
        bash_command='curl -sf http://spark-master:8080 > /dev/null && echo "Spark OK"',
    )

    check_minio = BashOperator(
        task_id="check_minio_health",
        bash_command='curl -sf http://minio:9000/minio/health/live > /dev/null && echo "MinIO OK"',
    )

    check_elasticsearch = BashOperator(
        task_id="check_elasticsearch_health",
        bash_command='curl -sf http://elasticsearch:9200/_cluster/health > /dev/null && echo "ES OK"',
    )

    # ── Kafka topics ────────────────────────────────────────────
    create_topics = BashOperator(
        task_id="create_kafka_topics",
        bash_command=(
            "docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 "
            "--create --if-not-exists --topic raw-logs --partitions 3 --replication-factor 1 && "
            "echo 'Kafka topic raw-logs ready.'"
        ),
    )

    # ── Train ML model (offline batch job on the full CSV) ─────
    train_ml_model = BashOperator(
        task_id="train_ml_model",
        bash_command=(
            "docker exec spark-master /opt/bitnami/spark/bin/spark-submit "
            "/opt/spark-apps/ml_model.py"
        ),
    )

    # ── Launch the streaming job, detached (it runs forever) ────
    start_streaming = BashOperator(
        task_id="start_spark_streaming",
        bash_command=(
            "docker exec -d spark-master /opt/bitnami/spark/bin/spark-submit "
            "/opt/spark-apps/streaming_job.py "
            "> /opt/airflow/logs/streaming_job_launch.log 2>&1 ; "
            "sleep 5 && echo 'Streaming job launched in background on spark-master.'"
        ),
    )

    # ── Trigger the producer's control API ──────────────────────
    start_producer = PythonOperator(
        task_id="start_log_producer",
        python_callable=trigger_producer,
    )

    # ── Lightweight monitoring/sanity task ──────────────────────
    monitor_pipeline = BashOperator(
        task_id="monitor_pipeline",
        bash_command=(
            "echo '=== Kafka Topic List ===' && "
            "docker exec kafka-broker kafka-topics --bootstrap-server kafka-broker:29092 --list && "
            "echo '=== Elasticsearch harmful-logs count ===' && "
            "curl -s http://elasticsearch:9200/harmful-logs/_count || echo 'Index not created yet (no harmful traffic seen yet).'"
        ),
    )

    [check_kafka, check_spark, check_minio, check_elasticsearch] >> create_topics
    create_topics >> train_ml_model >> start_streaming >> start_producer >> monitor_pipeline