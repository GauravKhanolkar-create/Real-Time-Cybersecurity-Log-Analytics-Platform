FROM apache/spark:3.5.1

USER root

# The official apache/spark image ships Python + PySpark, but not the
# extra libraries our streaming job needs (config parsing + ML scoring).
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/*

COPY spark/requirements-spark.txt /tmp/requirements-spark.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /tmp/requirements-spark.txt || \
    pip3 install --no-cache-dir -r /tmp/requirements-spark.txt

USER spark
