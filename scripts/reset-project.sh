#!/bin/bash
# WARNING: this deletes ALL data — Postgres, MinIO, Elasticsearch volumes,
# and every image built for this project. Use this when you want a truly
# clean slate (e.g. after changing the dataset schema).
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

read -r -p "This will DELETE all containers, volumes, and built images for this project. Continue? [y/N] " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

echo "Bringing everything down and removing volumes..."
docker compose down -v --remove-orphans

echo "Removing project-built images..."
docker compose down --rmi local || true

echo "Full reset complete. Run scripts/start_platform.sh to bring it back up from scratch."
