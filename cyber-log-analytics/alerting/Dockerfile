FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2 requests==2.32.3

COPY alerting/alert_manager.py /app/alert_manager.py

CMD ["python", "alert_manager.py"]
