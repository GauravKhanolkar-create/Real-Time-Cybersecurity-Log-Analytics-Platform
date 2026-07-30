"""
Utility for creating the Elasticsearch indices (with explicit mappings)
used by the pipeline, and a small helper for bulk-indexing documents
(useful for backfills or testing outside of the Spark ES sink).
"""
import os

from elasticsearch import Elasticsearch, helpers

ES_HOST = os.environ.get("ELASTICSEARCH_HOST", "http://localhost:9200")

LOGS_MAPPING = {
    "mappings": {
        "properties": {
            "log_id": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "src_ip": {"type": "ip"},
            "dst_ip": {"type": "ip"},
            "src_port": {"type": "integer"},
            "dst_port": {"type": "integer"},
            "protocol": {"type": "keyword"},
            "event_type": {"type": "keyword"},
            "action": {"type": "keyword"},
            "user": {"type": "keyword"},
            "bytes_sent": {"type": "long"},
            "bytes_received": {"type": "long"},
            "duration_ms": {"type": "integer"},
            "triggered_rules": {"type": "keyword"},
            "rule_severity": {"type": "keyword"},
            "ml_anomaly_score": {"type": "float"},
            "ml_flagged": {"type": "boolean"},
            "final_severity": {"type": "keyword"},
        }
    }
}

ALERTS_MAPPING = {
    "mappings": {
        "properties": {
            "log_id": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "src_ip": {"type": "ip"},
            "user": {"type": "keyword"},
            "triggered_rules": {"type": "keyword"},
            "final_severity": {"type": "keyword"},
            "ml_anomaly_score": {"type": "float"},
        }
    }
}


def get_client():
    return Elasticsearch(ES_HOST)


def ensure_indices(client=None, logs_index="security-logs", alerts_index="security-alerts"):
    client = client or get_client()
    for index, mapping in [(logs_index, LOGS_MAPPING), (alerts_index, ALERTS_MAPPING)]:
        if not client.indices.exists(index=index):
            client.indices.create(index=index, body=mapping)
            print(f"Created index '{index}'")
        else:
            print(f"Index '{index}' already exists")


def bulk_index(client, index, documents, id_field="log_id"):
    """documents: iterable of dicts"""
    actions = (
        {"_index": index, "_id": doc.get(id_field), "_source": doc}
        for doc in documents
    )
    success, errors = helpers.bulk(client, actions, stats_only=False, raise_on_error=False)
    return success, errors


if __name__ == "__main__":
    ensure_indices()
