FROM bitnamilegacy/spark:3.5.0

USER root

RUN install_packages curl

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ── JARs directory ──────────────────────────────────────────
# All versions are picked to match Spark 3.5.0 / Scala 2.12.
ARG SPARK_JARS=/opt/bitnami/spark/jars

# Kafka connector (spark-sql-kafka + its transitive deps)
RUN curl -fSL -o ${SPARK_JARS}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fSL -o ${SPARK_JARS}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fSL -o ${SPARK_JARS}/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -fSL -o ${SPARK_JARS}/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# MinIO / S3A support
RUN curl -fSL -o ${SPARK_JARS}/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fSL -o ${SPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Elasticsearch-Hadoop connector (elasticsearch-spark-30, Scala 2.12)
RUN curl -fSL -o ${SPARK_JARS}/elasticsearch-spark-30_2.12-8.11.0.jar \
      https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar

# PostgreSQL JDBC driver (for the hourly reporting rollup job)
RUN curl -fSL -o ${SPARK_JARS}/postgresql-42.7.1.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar

COPY . /opt/spark-apps/
RUN mkdir -p /opt/spark-apps/models && chown -R 1001:1001 /opt/spark-apps

USER 1001
