"""
rollup_job.py

Batch (not streaming) Spark job that builds the hourly traffic rollup used
by the Superset "Normal Traffic Overview" dashboard.

Superset does not read MinIO or Elasticsearch directly — running
Trino/Hive over MinIO just to give Superset a SQL view was judged not
worth an 18th container on a 16GB laptop. Instead, this job aggregates
both storage backends (MinIO Parquet for normal traffic, the
`harmful-logs` Elasticsearch index for harmful traffic) into a single
Postgres table, `reporting.hourly_traffic_rollup`, which Superset queries
directly.

Triggered hourly by airflow/dags/report_dag.py via
`docker exec spark-master spark-submit ...`, the same pattern used for
the other Spark jobs (see pipeline_dag.py for why).
"""

import os
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    date_trunc,
    lit,
    sum as spark_sum,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_INDEX_HARMFUL = os.getenv("ES_INDEX_HARMFUL", "harmful-logs")
NORMAL_INPUT_PATH = os.getenv("NORMAL_OUTPUT_PATH", "s3a://normal-logs/parquet/")

# REPORTING_DB_URL looks like postgresql://user:pass@host:5432/dbname
# (set in docker-compose.yml). Split it into a plain JDBC URL plus
# separate user/password options, which is what Spark's JDBC writer
# actually expects.
_raw_url = os.getenv(
    "REPORTING_DB_URL", "postgresql://airflow:airflow@postgres:5432/reporting"
)
_parsed = urlparse(_raw_url)
JDBC_URL = f"jdbc:postgresql://{_parsed.hostname}:{_parsed.port or 5432}{_parsed.path}"
JDBC_USER = _parsed.username or "airflow"
JDBC_PASSWORD = _parsed.password or "airflow"


def main() -> None:
    spark = (
        SparkSession.builder.appName("CybersecHourlyRollup")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Normal traffic from MinIO ───────────────────────────────
    try:
        normal_df = spark.read.parquet(NORMAL_INPUT_PATH)
        normal_rollup = (
            normal_df.withColumn("rollup_hour", date_trunc("hour", col("ingestion_time")))
            .groupBy("rollup_hour", "proto", "threat_label")
            .agg(
                spark_sum(lit(1)).alias("total_events"),
                spark_sum(col("sbytes")).alias("total_bytes_sent"),
                spark_sum(col("dbytes")).alias("total_bytes_recv"),
                avg(col("dur")).alias("avg_duration"),
            )
            .withColumnRenamed("proto", "protocol")
        )
        normal_count = normal_rollup.count()
    except Exception as exc:  # noqa: BLE001
        print(f"[ROLLUP][WARN] No normal-traffic Parquet data yet ({exc}); skipping.")
        normal_rollup = None
        normal_count = 0

    # ── Harmful traffic from Elasticsearch ──────────────────────
    # ── Harmful traffic from Elasticsearch ──────────────────────
    try:
        harmful_df = (
            spark.read.format("org.elasticsearch.spark.sql")
            .option("es.nodes", ES_HOST)
            .option("es.port", ES_PORT)
            .option("es.resource", ES_INDEX_HARMFUL)
            .option("es.nodes.wan.only", "true")
            .load()
        )
        harmful_rollup = (
            harmful_df.withColumn("rollup_hour", date_trunc("hour", col("ingestion_time")))
            .groupBy("rollup_hour", "proto", "threat_label")
            .agg(
                spark_sum(lit(1)).alias("total_events"),
                spark_sum(col("sbytes")).alias("total_bytes_sent"),
                spark_sum(col("dbytes")).alias("total_bytes_recv"),
                avg(col("dur")).alias("avg_duration"),
            )
            .withColumnRenamed("proto", "protocol")
        )
        harmful_count = harmful_rollup.count()
    except Exception as exc:  # noqa: BLE001
        print(f"[ROLLUP][WARN] No harmful-logs data yet in Elasticsearch ({exc}); skipping.")
        harmful_rollup = None
        harmful_count = 0

    if normal_count == 0 and harmful_count == 0:
        print("[ROLLUP] Nothing to roll up yet — both sources are empty. Exiting.")
        spark.stop()
        return

    if normal_rollup is not None and harmful_rollup is not None:
        combined = normal_rollup.unionByName(harmful_rollup)
    else:
        combined = normal_rollup if normal_rollup is not None else harmful_rollup

    print(f"[ROLLUP] Writing {combined.count()} rollup rows to Postgres 'reporting' DB")

    # Full overwrite keeps the table idempotent across re-runs of the
    # same hour without needing per-row upsert logic.
    (
        combined.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("dbtable", "hourly_traffic_rollup")
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    print("[ROLLUP] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
