"""
Trains an unsupervised anomaly-detection model (Isolation Forest) on the
historical security log dataset, and saves the fitted model + scaler for
use inside the Spark streaming job (via a pandas UDF, see
ml/anomaly_detector.py).

Run:
    python ml/train_model.py --csv data/sample_security_logs.csv
"""
import argparse
import os

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

FEATURE_COLUMNS = [
    "bytes_sent",
    "bytes_received",
    "duration_ms",
    "dst_port",
    "failed_login_count_5m",
    "unique_ports_contacted_1m",
]


def load_data(csv_path):
    df = pd.read_csv(csv_path)
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in dataset: {missing}")
    return df


def train(csv_path, model_out, scaler_out, contamination=0.03):
    df = load_data(csv_path)
    X = df[FEATURE_COLUMNS].fillna(0)

    X_train, X_val = train_test_split(X, test_size=0.2, random_state=42)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train_scaled)

    # Quick sanity check on validation split
    preds = model.predict(X_val_scaled)  # -1 = anomaly, 1 = normal
    anomaly_rate = (preds == -1).mean()
    print(f"Validation anomaly rate: {anomaly_rate:.3%} (target ~{contamination:.1%})")

    os.makedirs(os.path.dirname(model_out), exist_ok=True)
    joblib.dump(model, model_out)
    joblib.dump(scaler, scaler_out)
    print(f"Saved model -> {model_out}")
    print(f"Saved scaler -> {scaler_out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="data/sample_security_logs.csv")
    parser.add_argument("--model-out", default="ml/models/isolation_forest.joblib")
    parser.add_argument("--scaler-out", default="ml/models/scaler.joblib")
    parser.add_argument("--contamination", type=float, default=0.03)
    args = parser.parse_args()

    train(args.csv, args.model_out, args.scaler_out, args.contamination)
