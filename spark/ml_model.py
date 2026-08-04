"""
ml_model.py

Offline training script for the threat-classification model.

Trains a multi-class RandomForestClassifier on the UNSW-NB15-style
network-flow dataset (data/logs.csv) to predict `attack_cat`
(Normal / Generic / Exploits / Fuzzers / DoS / Reconnaissance /
Analysis / Backdoor / Shellcode / Worms).

Run once before the streaming job starts (the Airflow pipeline_dag
does this automatically via `train_ml_model` before `start_spark_streaming`):

    spark-submit /opt/spark-apps/ml_model.py

Outputs:
    /opt/spark-apps/models/rf_model/          <- the fitted PipelineModel
    /opt/spark-apps/models/label_mapping.json <- {index: attack_cat} produced
                                                  by the fitted StringIndexer

IMPORTANT (fixes a real gap from earlier project iterations): StringIndexer
assigns indices by descending label frequency, which is NOT stable across
retraining runs if the class balance shifts. Earlier versions of this
project hardcoded "0.0 -> NORMAL, 1.0 -> ATTACK, ..." directly in the
streaming job, which silently breaks the moment the model is retrained on
a differently-balanced sample. Here we persist the indexer's actual
`labels` array to label_mapping.json and have streaming_job.py read it
back at runtime, so the mapping is always derived from the model that is
actually loaded, never assumed.
"""

import json
import os
from pyspark.sql.functions import rand
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession

DATA_PATH = os.getenv("TRAINING_DATA_PATH", "/opt/data/logs.csv")
MODEL_DIR = os.getenv("MODEL_DIR", "/opt/spark-apps/models/rf_model")
LABEL_MAP_PATH = os.getenv("LABEL_MAP_PATH", "/opt/spark-apps/models/label_mapping.json")

CATEGORICAL_COLS = ["proto", "service", "state"]

NUMERIC_COLS = [
    "dur", "spkts", "dpkts", "sbytes", "dbytes", "rate", "sttl", "dttl",
    "sload", "dload", "sloss", "dloss", "sinpkt", "dinpkt", "sjit", "djit",
    "swin", "stcpb", "dtcpb", "dwin", "tcprtt", "synack", "ackdat",
    "smean", "dmean", "trans_depth", "response_body_len", "ct_srv_src",
    "ct_state_ttl", "ct_dst_ltm", "ct_src_dport_ltm", "ct_dst_sport_ltm",
    "ct_dst_src_ltm", "is_ftp_login", "ct_ftp_cmd", "ct_flw_http_mthd",
    "ct_src_ltm", "ct_srv_dst", "is_sm_ips_ports",
]


def main() -> None:
    spark = (
        SparkSession.builder.appName("CybersecML-Training")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[ML] Reading training data from {DATA_PATH}")
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    df.printSchema()
    total_rows = df.count()
    print(f"[ML] Loaded {total_rows} rows")

    if total_rows < 20:
        print(
            "[ML][WARN] Very small training set — this is fine for a "
            "learning/demo pipeline, but expect an over-confident model."
        )

    # ── Categorical indexers ──────────────────────────────────
    cat_indexers = []
    for c in CATEGORICAL_COLS:
        indexer = StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        cat_indexers.append(indexer)

    label_indexer = StringIndexer(
        inputCol="attack_cat", outputCol="label_idx", handleInvalid="keep"
    )

    feature_cols = NUMERIC_COLS.copy()
    for c in CATEGORICAL_COLS:
        feature_cols.append(f"{c}_idx")
    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="skip"
    )

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label_idx",
        predictionCol="prediction",
        numTrees=100,
        maxDepth=10,
        maxBins=200,
        seed=42,
    )

    pipeline = Pipeline(stages=cat_indexers + [label_indexer, assembler, rf])

    # Shuffle the dataset so the model sees threats!
    df = df.orderBy(rand())

    # ── Train / test split ────────────────────────────────────
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"[ML] Train rows: {train_df.count()} | Test rows: {test_df.count()}")

    model = pipeline.fit(train_df)

    # ── Evaluate ───────────────────────────────────────────────
    predictions = model.transform(test_df)
    accuracy_eval = MulticlassClassificationEvaluator(
        labelCol="label_idx", predictionCol="prediction", metricName="accuracy"
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="label_idx", predictionCol="prediction", metricName="f1"
    )
    accuracy = accuracy_eval.evaluate(predictions)
    f1 = f1_eval.evaluate(predictions)
    print(f"[ML] Test accuracy: {accuracy:.4f}")
    print(f"[ML] Test F1 score: {f1:.4f}")

    # ── Persist model ─────────────────────────────────────────
    os.makedirs(os.path.dirname(MODEL_DIR), exist_ok=True)
    model.write().overwrite().save(MODEL_DIR)
    print(f"[ML] Model saved to {MODEL_DIR}")

    # ── Persist the label mapping the streaming job depends on ─
    fitted_label_indexer = model.stages[len(CATEGORICAL_COLS)]
    labels = fitted_label_indexer.labels  # index position == encoded label

    label_mapping = {}
    for i, label in enumerate(labels):
        label_mapping[str(i)] = label

    with open(LABEL_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(label_mapping, f, indent=2)
    print(f"[ML] Label mapping saved to {LABEL_MAP_PATH}: {label_mapping}")

    spark.stop()


if __name__ == "__main__":
    main()
