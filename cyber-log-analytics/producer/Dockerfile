FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir kafka-python==2.0.2

COPY producer/log_producer.py /app/log_producer.py
COPY data/sample_security_logs.csv /app/data/sample_security_logs.csv

ENV LOG_CSV_PATH=/app/data/sample_security_logs.csv
ENV PRODUCER_RATE_PER_SEC=10

CMD ["python", "log_producer.py"]
