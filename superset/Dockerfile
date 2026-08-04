FROM apache/superset:latest

USER root

RUN pip install --no-cache-dir psycopg2-binary==2.9.9 elasticsearch-dbapi==0.2.13

COPY superset_config.py /app/pythonpath/superset_config.py
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

USER superset

ENTRYPOINT ["/app/docker-entrypoint.sh"]
