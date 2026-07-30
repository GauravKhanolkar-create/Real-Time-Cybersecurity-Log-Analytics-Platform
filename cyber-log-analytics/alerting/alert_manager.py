"""
Consumes the 'security-alerts' Kafka topic (populated by the Spark job for
any event scored high/critical severity) and dispatches notifications via
email (SMTP) and/or Slack incoming webhook.

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS
    KAFKA_ALERT_TOPIC
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL_TO
    SLACK_WEBHOOK_URL
"""
import json
import os
import smtplib
import time
from email.mime.text import MIMEText

import requests
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
ALERT_TOPIC = os.environ.get("KAFKA_ALERT_TOPIC", "security-alerts")

SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
ALERT_EMAIL_TO = [a for a in os.environ.get("ALERT_EMAIL_TO", "").split(",") if a]

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


def build_consumer(retries=10):
    last_err = None
    for attempt in range(retries):
        try:
            return KafkaConsumer(
                ALERT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="alert-manager",
            )
        except Exception as e:
            last_err = e
            wait = min(2 ** attempt, 15)
            print(f"Kafka not ready ({e}); retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts: {last_err}")


def format_message(alert: dict) -> str:
    return (
        f"[{alert.get('final_severity', 'unknown').upper()}] Security alert\n"
        f"  time: {alert.get('timestamp')}\n"
        f"  src_ip: {alert.get('src_ip')}\n"
        f"  user: {alert.get('user')}\n"
        f"  rules: {alert.get('triggered_rules')}\n"
        f"  ml_anomaly_score: {alert.get('ml_anomaly_score')}\n"
    )


def send_email(subject: str, body: str):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASSWORD and ALERT_EMAIL_TO):
        return
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(ALERT_EMAIL_TO)
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, ALERT_EMAIL_TO, msg.as_string())
    except Exception as e:
        print(f"Failed to send email alert: {e}")


def send_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=5)
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")


def main():
    print(f"Alert manager listening on topic '{ALERT_TOPIC}' via {BOOTSTRAP_SERVERS}")
    consumer = build_consumer()

    for message in consumer:
        alert = message.value
        text = format_message(alert)
        print("ALERT RECEIVED:\n" + text)

        severity = alert.get("final_severity", "info")
        if severity in ("high", "critical"):
            send_email(subject=f"[{severity.upper()}] Security Alert Triggered", body=text)
            send_slack(text=f":rotating_light: {text}")


if __name__ == "__main__":
    main()
