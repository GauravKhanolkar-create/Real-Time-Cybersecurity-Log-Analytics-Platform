FROM apache/airflow:2.8.0-python3.11

USER root

# Docker CLI (client only — no daemon) so airflow-worker can run
# `docker exec spark-master spark-submit ...` against the host's Docker
# Engine via the mounted /var/run/docker.sock. This replaces relying on
# SparkSubmitOperator + a spark-submit binary baked into this image,
# which is not available on apache/airflow's base image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg lsb-release && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
      $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/
