"""
trigger_server.py

Small Flask control-plane API around producer.py.

The log-producer container does NOT stream automatically on startup —
it waits idle until something (the Airflow DAG, or you, manually) calls
POST /start. This ensures Kafka/Spark/the ML model are confirmed ready
before any events start flowing, and lets Airflow own the "when did
ingestion begin" decision.

Endpoints:
    GET  /health  -> 200 OK once the Flask app itself is up
    GET  /status  -> current run status (sent / total / running / error)
    POST /start   -> begins streaming logs.csv into Kafka in a background
                     thread. Body (all optional JSON fields):
                       { "loop": false, "delay_ms": 200 }
    POST /stop    -> requests a graceful stop of the current run
"""

from __future__ import annotations

import os
import threading

from flask import Flask, jsonify, request

import producer

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-logs")
CSV_PATH = os.getenv("CSV_PATH", "/data/logs.csv")
DEFAULT_DELAY_MS = int(os.getenv("STREAM_DELAY_MS", "0"))

_status: dict = {"running": False, "sent": 0, "total": 0, "error": None}
_lock = threading.Lock()


@app.get("/health")
def health():
    return jsonify(status="ok"), 200


@app.get("/status")
def get_status():
    with _lock:
        return jsonify(_status), 200


@app.post("/start")
def start():
    with _lock:
        if _status.get("running"):
            return jsonify(message="A streaming run is already in progress.", status=_status), 409

        body = request.get_json(silent=True) or {}
        loop = bool(body.get("loop", False))
        delay_ms = int(body.get("delay_ms", DEFAULT_DELAY_MS))

        producer.shutdown.should_stop = False

        thread = threading.Thread(
            target=producer.stream_logs,
            kwargs=dict(
                csv_path=CSV_PATH,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                topic=KAFKA_TOPIC,
                delay_ms=delay_ms,
                loop=loop,
                status=_status,
            ),
            daemon=True,
        )
        thread.start()

    return jsonify(message="Streaming started.", csv_path=CSV_PATH, topic=KAFKA_TOPIC), 202


@app.post("/stop")
def stop():
    producer.shutdown.should_stop = True
    return jsonify(message="Stop requested — will halt after the current row."), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
