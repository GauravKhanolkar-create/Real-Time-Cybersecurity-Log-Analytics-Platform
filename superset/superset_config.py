"""
superset_config.py

Minimal Superset configuration for the CyberSec platform. Superset's own
metadata store stays on the image's default (SQLite) — it doesn't need
to be production-grade for a single-user learning/portfolio deployment.
The "reporting" Postgres database (built by scripts/postgres-init.sh and
populated hourly by spark/rollup_job.py) is added as a *data source*
manually from the Superset UI (Settings -> Database Connections), not
configured here.
"""

import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-this-superset-secret-key")

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}
