#!/bin/bash
# Health-checks every service.
#
# NOTE ON THE KAFKA CHECK: an earlier version of this script curl'd
# http://localhost:9092 directly. Kafka speaks a binary wire protocol on
# that port, not HTTP, so curl always reported it "unhealthy" even when
# the broker was completely fine. The fix is to speak Kafka's own
# protocol via `kafka-broker-api-versions` instead of HTTP.

check_http() {
    local name=$1
    local url=$2
    if curl -sf "$url" > /dev/null 2>&1; then
        echo "  OK   - $name"
    else
        echo "  FAIL - $name (not reachable at $url)"
    fi
}

check_kafka() {
    if docker exec kafka-broker kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        echo "  OK   - Kafka Broker"
    else
        echo "  FAIL - Kafka Broker (kafka-broker-api-versions did not respond)"
    fi
}

echo ""
echo "=== Service Health Checks ==="
check_kafka
check_http "Kafka UI"          "http://localhost:8090"
check_http "Spark Master"      "http://localhost:8080"
check_http "MinIO"             "http://localhost:9001"
check_http "Elasticsearch"     "http://localhost:9200/_cluster/health"
check_http "Kibana"            "http://localhost:5601/api/status"
check_http "Airflow"           "http://localhost:8089/health"
check_http "Superset"          "http://localhost:8088/health"
check_http "Log Producer API"  "http://localhost:5050/health"
echo ""
