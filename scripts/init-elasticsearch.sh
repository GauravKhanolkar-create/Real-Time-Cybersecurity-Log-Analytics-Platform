#!/bin/bash
# Applies the harmful-logs index mapping to Elasticsearch.
# Safe to re-run: skips creation if the index already exists.
set -e

ES_URL="${ES_URL:-http://localhost:9200}"
INDEX="harmful-logs"

echo "Waiting for Elasticsearch to become ready at ${ES_URL}..."
# This loop pings ES every 5 seconds until it answers
until curl -sf "${ES_URL}/_cluster/health" > /dev/null; do
    echo "  Elasticsearch not ready yet, waiting 5 seconds..."
    sleep 5
done
echo "Elasticsearch is up!"

if curl -sf "${ES_URL}/${INDEX}" > /dev/null 2>&1; then
    echo "Index '${INDEX}' already exists — nothing to do."
    exit 0
fi

echo "Creating index '${INDEX}' with its mapping..."
curl -sf -X PUT "${ES_URL}/${INDEX}" \
     -H "Content-Type: application/json" \
     -d @"$(dirname "$0")/../elasticsearch/mappings/harmful_logs_mapping.json"

echo ""
echo "Done."
