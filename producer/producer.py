"""
producer.py

Reads the UNSW-NB15-style network-flow CSV (data/logs.csv) row by row and
streams each row into Kafka as a JSON message, simulating real-time log
ingestion. Every row already carries the dataset's own 45 columns
(id, dur, proto, service, state, spkts, dpkts, sbytes, dbytes, rate, ...,
attack_cat, label) — this module does not invent fields the dataset does
not have (e.g. there are no source_ip / dest_ip columns in this sample).

Designed to be imported by trigger_server.py, which exposes it over a
small Flask control API. It can also be run standalone:

    python producer.py
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] producer: %(message)s",
)
logger = logging.getLogger("producer")


class GracefulShutdown:
    """Tracks a shutdown request from SIGTERM/SIGINT so a running
    streaming loop can exit cleanly between rows instead of mid-send."""

    def __init__(self) -> None:
        self.should_stop = False
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame) -> None:  # noqa: ANN001
        logger.warning("Received signal %s — will stop after current row.", signum)
        self.should_stop = True


shutdown = GracefulShutdown()


def _delivery_report(err, msg) -> None:  # noqa: ANN001
    if err is not None:
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered to %s [partition %s] @ offset %s",
            msg.topic(), msg.partition(), msg.offset(),
        )


def build_producer(bootstrap_servers: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "cybersec-log-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "queue.buffering.max.messages": 100000,
        "linger.ms": 20,
    }
    return Producer(conf)


def stream_logs(
    csv_path: str,
    bootstrap_servers: str,
    topic: str,
    delay_ms: int = 200,
    jitter_ms: int = 50,
    loop: bool = False,
    status: Optional[dict] = None,
) -> dict:
    """
    Streams every row of csv_path into `topic`, one message at a time.

    `status`, if given, is a shared dict that trigger_server.py polls for
    live progress (sent / total / running / error).
    """
    if status is None:
        status = {}

    status.update(running=True, sent=0, total=0, error=None, started_at=datetime.now(timezone.utc).isoformat())

    try:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV not found at {csv_path}")

        df = pd.read_csv(csv_path)
        total = len(df)
        status["total"] = total
        logger.info("Loaded %d rows from %s", total, csv_path)

        producer = build_producer(bootstrap_servers)

        sent = 0
        while True:
            for idx, row in df.iterrows():
                if shutdown.should_stop:
                    logger.warning("Graceful shutdown requested — stopping stream.")
                    status.update(running=False, stopped_early=True)
                    producer.flush(10)
                    return status

                record = row.where(pd.notnull(row), None).to_dict()
                record["log_id"] = int(idx) if loop is False else f"{idx}-{int(time.time())}"
                record["producer_timestamp"] = datetime.now(timezone.utc).isoformat()

                producer.produce(
                    topic=topic,
                    key=str(record["log_id"]),
                    value=json.dumps(record, default=str),
                    callback=_delivery_report,
                )
                producer.poll(0)

                sent += 1
                status["sent"] = sent

                if sent % 5000 == 0 or sent == total:
                    logger.info("Streamed %d/%d rows...", sent, total)
                    # Use a short timeout for flush so it doesn't block heavily
                    producer.flush(1)

                # Only apply sleep and jitter if a delay is actually requested
                if delay_ms > 0:
                    delay = (delay_ms + random.randint(-jitter_ms, jitter_ms)) / 1000.0
                    time.sleep(max(delay, 0))

            producer.flush(10)
            logger.info("Finished one full pass over the CSV (%d rows).", total)

            if not loop or shutdown.should_stop:
                break

        status.update(running=False, finished_at=datetime.now(timezone.utc).isoformat())
        return status

    except Exception as exc:  # noqa: BLE001
        logger.exception("Producer failed: %s", exc)
        status.update(running=False, error=str(exc))
        return status


if __name__ == "__main__":
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-logs")
    CSV_PATH = os.getenv("CSV_PATH", "/data/logs.csv")
    STREAM_DELAY_MS = int(os.getenv("STREAM_DELAY_MS", "0"))
    LOOP = os.getenv("PRODUCER_LOOP", "false").lower() == "true"

    stream_logs(
        csv_path=CSV_PATH,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
        delay_ms=STREAM_DELAY_MS,
        loop=LOOP,
    )
