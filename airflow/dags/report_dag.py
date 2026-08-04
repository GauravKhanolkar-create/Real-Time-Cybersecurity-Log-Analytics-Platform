"""
report_dag.py

Runs hourly. Triggers spark/rollup_job.py (via docker exec, same pattern
as pipeline_dag.py) to refresh the `reporting.hourly_traffic_rollup`
Postgres table that feeds Superset, then queries it for a quick
human-readable threat summary in the task logs.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "cybersec-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def print_daily_summary(**context):  # noqa: ANN001, ARG001
    import os

    import psycopg2

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname=os.getenv("REPORTING_DB", "reporting"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT threat_label, SUM(total_events) AS events
                FROM hourly_traffic_rollup
                WHERE rollup_hour >= NOW() - INTERVAL '24 hours'
                GROUP BY threat_label
                ORDER BY events DESC;
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    print("=== THREAT SUMMARY (last 24h) ===")
    if not rows:
        print("  No rollup data yet.")
    for threat_label, events in rows:
        print(f"  {threat_label}: {events} events")


with DAG(
    dag_id="daily_threat_report",
    default_args=default_args,
    description="Hourly Postgres rollup for Superset + a text threat summary",
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["cybersec", "report"],
) as dag:

    run_rollup = BashOperator(
        task_id="run_hourly_rollup",
        bash_command=(
            "docker exec spark-master /opt/bitnami/spark/bin/spark-submit "
            "/opt/spark-apps/rollup_job.py"
        ),
    )

    generate_summary = PythonOperator(
        task_id="print_daily_threat_summary",
        python_callable=print_daily_summary,
    )

    run_rollup >> generate_summary
