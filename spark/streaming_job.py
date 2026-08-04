"""
streaming_job.py

Spark Structured Streaming job:

    Kafka (raw-logs)
        -> parse JSON (actual UNSW-NB15 45-column schema + log_id/producer_timestamp)
        -> clean
        -> RandomForest PipelineModel classification (per micro-batch)
        -> threat_label / severity derived from the model's prediction
        -> split:
             normal traffic   -> MinIO   (partitioned Parquet, via S3A)
             harmful traffic  -> Elasticsearch ("harmful-logs" index)

Run via:
    spark-submit /opt/spark-apps/streaming_job.py

(The Airflow DAG runs this with `docker exec spark-master spark-submit ...`
rather than SparkSubmitOperator, because the Airflow images do not ship a
spark-submit binary — see airflow/dags/pipeline_dag.py for why.)
"""

import json
import os

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, udf, when
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Config ────────────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "raw-logs")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_INDEX_HARMFUL = os.getenv("ES_INDEX_HARMFUL", "harmful-logs")
MODEL_DIR = os.getenv("MODEL_DIR", "/opt/spark-apps/models/rf_model")
LABEL_MAP_PATH = os.getenv("LABEL_MAP_PATH", "/opt/spark-apps/models/label_mapping.json")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "s3a://spark-checkpoints/streaming/")
NORMAL_OUTPUT_PATH = os.getenv("NORMAL_OUTPUT_PATH", "s3a://normal-logs/parquet/")

# attack_cat -> severity. Anything not listed here defaults to MEDIUM.
SEVERITY_MAP = {
    "Generic": "MEDIUM",
    "Exploits": "HIGH",
    "Fuzzers": "LOW",
    "DoS": "HIGH",
    "Reconnaissance": "MEDIUM",
    "Analysis": "MEDIUM",
    "Backdoor": "HIGH",
    "Shellcode": "HIGH",
    "Worms": "HIGH",
}

# ── SparkSession ──────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("CybersecLogStreaming")
    .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .config("spark.executor.memory", "1g")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Log schema — the real UNSW-NB15 columns actually present in
#    data/logs.csv, plus the two fields the producer adds. ──────
LOG_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("dur", DoubleType(), True),
        StructField("proto", StringType(), True),
        StructField("service", StringType(), True),
        StructField("state", StringType(), True),
        StructField("spkts", IntegerType(), True),
        StructField("dpkts", IntegerType(), True),
        StructField("sbytes", LongType(), True),
        StructField("dbytes", LongType(), True),
        StructField("rate", DoubleType(), True),
        StructField("sttl", IntegerType(), True),
        StructField("dttl", IntegerType(), True),
        StructField("sload", DoubleType(), True),
        StructField("dload", DoubleType(), True),
        StructField("sloss", IntegerType(), True),
        StructField("dloss", IntegerType(), True),
        StructField("sinpkt", DoubleType(), True),
        StructField("dinpkt", DoubleType(), True),
        StructField("sjit", DoubleType(), True),
        StructField("djit", DoubleType(), True),
        StructField("swin", IntegerType(), True),
        StructField("stcpb", LongType(), True),
        StructField("dtcpb", LongType(), True),
        StructField("dwin", IntegerType(), True),
        StructField("tcprtt", DoubleType(), True),
        StructField("synack", DoubleType(), True),
        StructField("ackdat", DoubleType(), True),
        StructField("smean", IntegerType(), True),
        StructField("dmean", IntegerType(), True),
        StructField("trans_depth", IntegerType(), True),
        StructField("response_body_len", IntegerType(), True),
        StructField("ct_srv_src", IntegerType(), True),
        StructField("ct_state_ttl", IntegerType(), True),
        StructField("ct_dst_ltm", IntegerType(), True),
        StructField("ct_src_dport_ltm", IntegerType(), True),
        StructField("ct_dst_sport_ltm", IntegerType(), True),
        StructField("ct_dst_src_ltm", IntegerType(), True),
        StructField("is_ftp_login", IntegerType(), True),
        StructField("ct_ftp_cmd", IntegerType(), True),
        StructField("ct_flw_http_mthd", IntegerType(), True),
        StructField("ct_src_ltm", IntegerType(), True),
        StructField("ct_srv_dst", IntegerType(), True),
        StructField("is_sm_ips_ports", IntegerType(), True),
        StructField("attack_cat", StringType(), True),
        StructField("label", IntegerType(), True),
        StructField("log_id", StringType(), True),
        StructField("producer_timestamp", StringType(), True),
    ]
)

NUMERIC_FILL_COLS = [
    "dur", "spkts", "dpkts", "sbytes", "dbytes", "rate", "sttl", "dttl",
    "sload", "dload", "sloss", "dloss", "sinpkt", "dinpkt", "sjit", "djit",
]

# ── Read from Kafka ───────────────────────────────────────────
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ── Parse JSON ────────────────────────────────────────────────
parsed = (
    raw_stream.select(from_json(col("value").cast("string"), LOG_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("ingestion_time", current_timestamp())
)

# ── Cleaning ──────────────────────────────────────────────────
# Create a dictionary to hold our numeric fill values
fill_values = {}
for c in NUMERIC_FILL_COLS:
    fill_values[c] = 0.0

# Clean the dataset: filter out null protos, fill numerics, then fill categoricals
cleaned = (
    parsed.filter(col("proto").isNotNull())
    .fillna(fill_values)
    .fillna({"service": "-", "state": "UNKNOWN", "attack_cat": "Normal"})
)

# ── Load ML model + label mapping once (broadcast implicitly via closure) ──
model = PipelineModel.load(MODEL_DIR)

with open(LABEL_MAP_PATH, "r", encoding="utf-8") as f:
    LABEL_MAPPING = json.load(f)  # {"0": "Normal", "1": "Generic", ...}

_label_mapping_bc = spark.sparkContext.broadcast(LABEL_MAPPING)


@udf(returnType=StringType())
def prediction_to_label(prediction: float) -> str:
    return _label_mapping_bc.value.get(str(int(prediction)), "UNKNOWN")


@udf(returnType=StringType())
def label_to_severity(threat_label: str) -> str:
    if threat_label == "Normal":
        return "NONE"
    return SEVERITY_MAP.get(threat_label, "MEDIUM")


# ── Columns to actually persist downstream. Earlier iterations of
#    this job wrote the model's raw output straight to Elasticsearch,
#    leaking five ML-internal columns (proto_idx, label_idx, features,
#    rawPrediction, probability) into every document. We select an
#    explicit, intentional output schema instead. ───────────────────
OUTPUT_COLUMNS = [
    "log_id", "id", "ingestion_time", "producer_timestamp",
    "dur", "proto", "service", "state", "spkts", "dpkts", "sbytes",
    "dbytes", "rate", "sttl", "dttl", "sload", "dload",
    "attack_cat", "label",
    "threat_label", "severity", "is_harmful", "alerted",
]


def apply_ml_and_route(batch_df, batch_id):  # noqa: ANN001
    if batch_df.rdd.isEmpty():
        return

    predictions = model.transform(batch_df)

    result = (
        predictions.withColumn("threat_label", prediction_to_label(col("prediction")))
        .withColumn("severity", label_to_severity(col("threat_label")))
        .withColumn(
            "is_harmful",
            when(col("threat_label") != lit("Normal"), lit(True)).otherwise(lit(False)),
        )
        .withColumn("alerted", lit(False))
        .select(*OUTPUT_COLUMNS)
    )

    normal_df = result.filter(~col("is_harmful"))
    harmful_df = result.filter(col("is_harmful"))

    normal_count = normal_df.count()
    harmful_count = harmful_df.count()
    print(f"[Batch {batch_id}] Normal: {normal_count} | Harmful: {harmful_count}")

    # ── Normal traffic -> MinIO (partitioned Parquet via S3A) ──────
    if normal_count > 0:
        normal_df.write.mode("append").partitionBy("proto").parquet(NORMAL_OUTPUT_PATH)

    # ── Harmful traffic -> Elasticsearch ────────────────────────────
    if harmful_count > 0:
        harmful_df.write.format("org.elasticsearch.spark.sql") \
            .option("es.nodes", ES_HOST) \
            .option("es.port", ES_PORT) \
            .option("es.resource", ES_INDEX_HARMFUL) \
            .option("es.nodes.wan.only", "true") \
            .option("es.write.operation", "index") \
            .option("es.mapping.id", "log_id") \
            .mode("append").save()


# ── Start streaming ───────────────────────────────────────────
query = (
    cleaned.writeStream.foreachBatch(apply_ml_and_route)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="10 seconds")
    .start()
)

print("[SPARK] Streaming job started. Waiting for data...")
query.awaitTermination()
