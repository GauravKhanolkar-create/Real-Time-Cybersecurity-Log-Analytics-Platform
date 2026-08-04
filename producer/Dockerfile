FROM python:3.11-slim

WORKDIR /app

# librdkafka build deps for confluent-kafka
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc build-essential librdkafka-dev curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py trigger_server.py ./

ENV PYTHONUNBUFFERED=1
EXPOSE 5000

HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
    CMD curl -f http://localhost:5000/health || exit 1

CMD ["python", "trigger_server.py"]
