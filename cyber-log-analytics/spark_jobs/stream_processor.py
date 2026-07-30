"""
Real-time threat detection pipeline using Spark Structured Streaming.

Pipeline:
  Kafka(security-logs) -> parse JSON -> windowed feature aggregation
    -> rule-based detection -> ML anomaly scoring
    -> write enriched events to Elasticsearch + MySQL
    -> write high/critical severity events to Kafka(security-alerts)

Run (inside the spark container, or spark-submit locally):
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4,\
com.mysql:mysql-connector-j:8.4.0 \
      spark_jobs/stream_processor.py
"""
import os
import sys

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from spark_jobs.threat_rules import apply_all_rules  # noqa: E402
from ml.anomaly_detector import get_anomaly_score_udf  # noqa: E402


def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


LOG_SCHEMA = StructType([
    StructField("log_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("src_ip", StringType()),
    StructField("dst_ip", StringType()),
    StructField("src_port", IntegerType()),
    StructField("dst_port", IntegerType()),
    StructField("protocol", StringType()),
    StructField("event_type", StringType()),
    StructField("action", StringType()),
    StructField("user", StringType()),
    StructField("bytes_sent", LongType()),
    StructField("bytes_received", LongType()),
    StructField("duration_ms", IntegerType()),
    StructField("failed_login_count_5m", IntegerType()),
    StructField("unique_ports_contacted_1m", IntegerType()),
    StructField("label", StringType()),
])


def build_spark_session(cfg):
    return (
        SparkSession.builder
        .appName("CyberLogThreatDetection")
        .master(cfg["spark"]["master"])
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_kafka_stream(spark, cfg):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", cfg["kafka"]["log_topic"])
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_logs(raw_df):
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .select(F.from_json(F.col("json_str"), LOG_SCHEMA).alias("data"), "kafka_ts")
        .select("data.*", "kafka_ts")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("event_time", F.coalesce(F.col("event_time"), F.col("kafka_ts")))
    )
    return parsed


def enrich_with_rolling_features(df, cfg):
    """
    The producer already ships pre-computed rolling counters
    (failed_login_count_5m, unique_ports_contacted_1m) for simplicity in
    this reference implementation. In a production deployment these would
    instead be computed here via stateful windowed aggregations, e.g.:

        df.groupBy(F.window("event_time", "5 minutes"), "src_ip", "user") \\
          .agg(F.sum(F.when(F.col("action") == "login_failed", 1)).alias("failed_login_count_5m"))

    and then joined back onto the raw event stream.
    """
    return df


def apply_ml_scoring(df, cfg):
    udf = get_anomaly_score_udf(cfg["model"]["path"], cfg["model"]["scaler_path"])
    df = df.withColumn(
        "ml_anomaly_score",
        udf(
            F.col("bytes_sent").cast("double"),
            F.col("bytes_received").cast("double"),
            F.col("duration_ms").cast("double"),
            F.col("dst_port").cast("double"),
            F.col("failed_login_count_5m").cast("double"),
            F.col("unique_ports_contacted_1m").cast("double"),
        ),
    )
    threshold = cfg["detection"]["ml_anomaly_score_threshold"]
    df = df.withColumn("ml_flagged", F.col("ml_anomaly_score") >= F.lit(threshold))
    return df


def finalize_severity(df):
    return df.withColumn(
        "final_severity",
        F.when(F.col("rule_severity") == "critical", F.lit("critical"))
        .when((F.col("rule_severity") == "high") | (F.col("ml_flagged")), F.lit("high"))
        .when(F.col("rule_severity") == "medium", F.lit("medium"))
        .when(F.col("ml_anomaly_score") >= 0.3, F.lit("low"))
        .otherwise(F.lit("info")),
    )


def write_to_console(df, name):
    return (
        df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .queryName(name)
        .start()
    )


def write_to_elasticsearch(df, cfg, index, checkpoint_suffix):
    return (
        df.writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", cfg["elasticsearch"]["host"].replace("http://", "").split(":")[0])
        .option("es.port", cfg["elasticsearch"]["host"].split(":")[-1])
        .option("es.resource", f"{index}/_doc")
        .option("checkpointLocation", f"{cfg['spark']['checkpoint_dir']}/{checkpoint_suffix}")
        .start()
    )


def write_to_mysql(df, cfg):
    """
    Writes each micro-batch of enriched events to MySQL via JDBC.

    JDBC sinks aren't a native Structured Streaming output format, so this
    uses foreachBatch: for every micro-batch, convert 'triggered_rules'
    (an array column) to a JSON string (MySQL has no native array type),
    then write with the standard batch DataFrameWriter JDBC path.
    """
    mysql_cfg = cfg["mysql"]
    jdbc_url = f"jdbc:mysql://{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['db']}"
    jdbc_props = {
        "user": mysql_cfg["user"],
        "password": mysql_cfg["password"],
        "driver": mysql_cfg["jdbc_driver"],
    }

    def _write_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        (
            batch_df
            .withColumn("triggered_rules", F.to_json(F.col("triggered_rules")))
            .withColumnRenamed("user", "app_user")
            .write
            .jdbc(
                url=jdbc_url,
                table=mysql_cfg["table_logs"],
                mode="append",
                properties=jdbc_props,
            )
        )

    return (
        df.writeStream
        .outputMode("append")
        .foreachBatch(_write_batch)
        .option("checkpointLocation", f"{cfg['spark']['checkpoint_dir']}/mysql_logs")
        .start()
    )


def write_alerts_to_kafka(df, cfg):
    alerts = df.filter(F.col("final_severity").isin("high", "critical"))
    payload = alerts.select(
        F.to_json(F.struct(
            "log_id", "timestamp", "src_ip", "dst_ip", "user",
            "triggered_rules", "final_severity", "ml_anomaly_score",
        )).alias("value")
    )
    return (
        payload.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("topic", cfg["kafka"]["alert_topic"])
        .option("checkpointLocation", f"{cfg['spark']['checkpoint_dir']}/alerts_kafka")
        .outputMode("append")
        .start()
    )


def main():
    cfg = load_config()
    spark = build_spark_session(cfg)
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kafka_stream(spark, cfg)
    parsed = parse_logs(raw)
    enriched = enrich_with_rolling_features(parsed, cfg)
    ruled = apply_all_rules(enriched, cfg["detection"])
    scored = apply_ml_scoring(ruled, cfg)
    final_df = finalize_severity(scored)

    queries = []
    queries.append(write_to_console(final_df, "console_debug"))
    queries.append(write_alerts_to_kafka(final_df, cfg))
    queries.append(write_to_mysql(final_df, cfg))

    # Elasticsearch sink requires its connector JAR to be on the Spark
    # classpath (see spark-submit --packages in the module docstring) and a
    # running Elasticsearch instance. Uncomment once wired up:
    #
    # queries.append(write_to_elasticsearch(final_df, cfg, cfg["elasticsearch"]["index_logs"], "es_logs"))

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
