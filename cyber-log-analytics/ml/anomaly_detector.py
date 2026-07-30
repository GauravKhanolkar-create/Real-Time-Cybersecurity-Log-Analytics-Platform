"""
Loads the pre-trained Isolation Forest model + scaler and exposes:
  - score_batch(df): pandas DataFrame -> anomaly scores (0..1, higher = more anomalous)
  - get_anomaly_score_udf(): a Spark pandas_udf usable inside stream_processor.py
"""
import os

import joblib
import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

FEATURE_COLUMNS = [
    "bytes_sent",
    "bytes_received",
    "duration_ms",
    "dst_port",
    "failed_login_count_5m",
    "unique_ports_contacted_1m",
]

_model = None
_scaler = None


def _load(model_path, scaler_path):
    global _model, _scaler
    if _model is None or _scaler is None:
        if not os.path.exists(model_path) or not os.path.exists(scaler_path):
            raise FileNotFoundError(
                f"Model/scaler not found at {model_path} / {scaler_path}. "
                f"Run ml/train_model.py first."
            )
        _model = joblib.load(model_path)
        _scaler = joblib.load(scaler_path)
    return _model, _scaler


def score_batch(df: pd.DataFrame, model_path: str, scaler_path: str) -> pd.Series:
    model, scaler = _load(model_path, scaler_path)
    X = df[FEATURE_COLUMNS].fillna(0)
    X_scaled = scaler.transform(X)

    # decision_function: higher = more normal. Convert to a 0..1 "anomaly score"
    # where higher = more anomalous, via a sigmoid-style squashing of the
    # negative decision function.
    raw = -model.decision_function(X_scaled)
    score = 1.0 / (1.0 + np.exp(-raw * 5))  # squashing factor tuned empirically
    return pd.Series(score)


def get_anomaly_score_udf(model_path: str, scaler_path: str):
    @pandas_udf(DoubleType())
    def anomaly_score_udf(
        bytes_sent: pd.Series,
        bytes_received: pd.Series,
        duration_ms: pd.Series,
        dst_port: pd.Series,
        failed_login_count_5m: pd.Series,
        unique_ports_contacted_1m: pd.Series,
    ) -> pd.Series:
        df = pd.DataFrame(
            {
                "bytes_sent": bytes_sent,
                "bytes_received": bytes_received,
                "duration_ms": duration_ms,
                "dst_port": dst_port,
                "failed_login_count_5m": failed_login_count_5m,
                "unique_ports_contacted_1m": unique_ports_contacted_1m,
            }
        )
        return score_batch(df, model_path, scaler_path)

    return anomaly_score_udf
