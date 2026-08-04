#!/bin/bash
# Stops all containers. Named volumes (Postgres, MinIO, Elasticsearch)
# are kept, so your data survives a stop/start cycle.
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

echo "Stopping all CyberSec platform services..."
docker compose down

echo "Done. Data volumes were preserved. Use scripts/reset-project.sh for a full wipe."
