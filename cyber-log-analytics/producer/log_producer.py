"""
Reads security log records (from the sample CSV, looping) and publishes
them as JSON messages to a Kafka topic, simulating a live log stream.
"""
import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "security-logs")
RATE_PER_SEC = float(os.environ.get("PRODUCER_RATE_PER_SEC", "10"))
CSV_PATH = os.environ.get("LOG_CSV_PATH", "/app/data/sample_security_logs.csv")


def build_producer(retries=10):
    last_err = None
    for attempt in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
            )
        except Exception as e:
            last_err = e
            wait = min(2 ** attempt, 15)
            print(f"Kafka not ready ({e}); retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts: {last_err}")


def load_rows(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def refresh_timestamp(row):
    row = dict(row)
    row["timestamp"] = datetime.now(timezone.utc).isoformat()
    row["log_id"] = str(uuid.uuid4())
    return row


def main():
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}, topic={TOPIC}")
    producer = build_producer()
    rows = load_rows(CSV_PATH)
    print(f"Loaded {len(rows)} template log rows from {CSV_PATH}")

    delay = 1.0 / RATE_PER_SEC if RATE_PER_SEC > 0 else 0
    idx = 0
    sent = 0
    try:
        while True:
            row = refresh_timestamp(rows[idx % len(rows)])
            producer.send(TOPIC, value=row, key=row["src_ip"].encode("utf-8"))
            sent += 1
            idx += 1
            if sent % 500 == 0:
                producer.flush()
                print(f"Sent {sent} messages so far...")
            time.sleep(delay + random.uniform(-0.02, 0.02) if delay else 0)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
