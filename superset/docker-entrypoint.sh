#!/bin/bash
set -e

superset db upgrade

superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER:-admin}" \
    --firstname Admin \
    --lastname User \
    --email admin@cybersec.local \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || echo "Admin user already exists — continuing."

superset init

exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
