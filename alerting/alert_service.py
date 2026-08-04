"""
alert_service.py

Continuously polls the Elasticsearch `harmful-logs` index for documents
matching:

    severity == ALERT_SEVERITY_THRESHOLD (default: "HIGH")
    AND alerted == false

For every match it sends an email via SMTP, then marks the document
`alerted: true` via the Elasticsearch Update API — this is what prevents
duplicate emails on the next poll (Elasticsearch's own Watcher would do
the same job, but it is a paid X-Pack feature and this cluster runs with
xpack.security.enabled=false / no license, so polling is the honest
match for what's actually available here).
"""

from __future__ import annotations

import logging
import os
import smtplib
import time
from email.mime.text import MIMEText

from elasticsearch import Elasticsearch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] alert-service: %(message)s",
)
logger = logging.getLogger("alert-service")

ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_INDEX_HARMFUL = os.getenv("ES_INDEX_HARMFUL", "harmful-logs")

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", SMTP_USERNAME)
ALERT_TO_EMAIL = os.getenv("ALERT_TO_EMAIL", "")
ALERT_SEVERITY_THRESHOLD = os.getenv("ALERT_SEVERITY_THRESHOLD", "HIGH")
POLL_INTERVAL_SECONDS = int(os.getenv("ALERT_POLL_INTERVAL_SECONDS", "30"))

QUERY = {
    "size": 50,
    "query": {
        "bool": {
            "must": [
                {"term": {"severity": ALERT_SEVERITY_THRESHOLD}},
                {"term": {"alerted": False}},
            ]
        }
    },
    "sort": [{"ingestion_time": "asc"}],
}


def build_client() -> Elasticsearch:
    return Elasticsearch(hosts=[f"http://{ES_HOST}:{ES_PORT}"], request_timeout=15)


def wait_for_elasticsearch(es: Elasticsearch, max_wait_seconds: int = 300) -> None:
    waited = 0
    while waited < max_wait_seconds:
        try:
            if es.ping():
                logger.info("Connected to Elasticsearch.")
                return
        except Exception:  # noqa: BLE001
            pass
        logger.info("Waiting for Elasticsearch to become reachable...")
        time.sleep(5)
        waited += 5
    raise RuntimeError("Elasticsearch never became reachable.")


def send_email_alert(doc: dict) -> None:
    if not SMTP_USERNAME or not ALERT_TO_EMAIL:
        logger.warning(
            "SMTP_USERNAME / ALERT_TO_EMAIL not configured — logging alert instead of emailing:\n%s",
            doc,
        )
        return

    body_lines = [
        "THREAT DETECTED",
        "",
        f"log_id:        {doc.get('log_id')}",
        f"threat_label:  {doc.get('threat_label')}",
        f"severity:      {doc.get('severity')}",
        f"protocol:      {doc.get('proto')}",
        f"service:       {doc.get('service')}",
        f"state:         {doc.get('state')}",
        f"attack_cat:    {doc.get('attack_cat')}",
        f"ingestion_time:{doc.get('ingestion_time')}",
    ]
    body = "\n".join(body_lines)

    msg = MIMEText(body)
    msg["Subject"] = f"[ALERT] {doc.get('threat_label', 'THREAT')} detected — severity {doc.get('severity')}"
    msg["From"] = ALERT_FROM_EMAIL
    msg["To"] = ALERT_TO_EMAIL

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(msg["From"], [ALERT_TO_EMAIL], msg.as_string())

    logger.info("Emailed alert for log_id=%s to %s", doc.get("log_id"), ALERT_TO_EMAIL)


def poll_once(es: Elasticsearch) -> int:
    if not es.indices.exists(index=ES_INDEX_HARMFUL):
        logger.info("Index '%s' does not exist yet — no harmful traffic seen so far.", ES_INDEX_HARMFUL)
        return 0

    resp = es.search(index=ES_INDEX_HARMFUL, body=QUERY)
    hits = resp.get("hits", {}).get("hits", [])

    for hit in hits:
        doc_id = hit["_id"]
        source = hit["_source"]
        try:
            send_email_alert(source)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to send alert for %s: %s — will retry next poll.", doc_id, exc)
            continue

        # Mark as alerted so we never email this document twice.
        es.update(index=ES_INDEX_HARMFUL, id=doc_id, doc={"alerted": True})

    return len(hits)


def main() -> None:
    es = build_client()
    wait_for_elasticsearch(es)

    logger.info(
        "Polling '%s' every %ds for severity=%s AND alerted=false",
        ES_INDEX_HARMFUL, POLL_INTERVAL_SECONDS, ALERT_SEVERITY_THRESHOLD,
    )

    while True:
        try:
            n = poll_once(es)
            if n:
                logger.info("Processed %d new alert(s).", n)
        except Exception as exc:  # noqa: BLE001
            logger.error("Poll cycle failed: %s", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
