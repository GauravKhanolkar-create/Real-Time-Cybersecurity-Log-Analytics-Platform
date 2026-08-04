#!/bin/bash
# Runs automatically on first Postgres container boot
# (mounted into /docker-entrypoint-initdb.d/).
# Creates the second "reporting" database used by the Spark
# hourly-rollup job and by Superset, alongside the "airflow"
# metadata database created by the official Postgres image.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE ${REPORTING_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${REPORTING_DB}')\gexec
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$REPORTING_DB" <<-EOSQL
    CREATE TABLE IF NOT EXISTS hourly_traffic_rollup (
        rollup_hour       TIMESTAMP NOT NULL,
        protocol          VARCHAR(32) NOT NULL,
        threat_label      VARCHAR(32) NOT NULL,
        total_events      BIGINT NOT NULL,
        total_bytes_sent   DOUBLE PRECISION,
        total_bytes_recv   DOUBLE PRECISION,
        avg_duration       DOUBLE PRECISION,
        PRIMARY KEY (rollup_hour, protocol, threat_label)
    );
EOSQL

echo "Reporting database and hourly_traffic_rollup table are ready."
