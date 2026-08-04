# Real-Time Cybersecurity Log Analytics Platform

A runnable, 19-container Big Data pipeline that streams network-flow
logs through Kafka -> Spark Structured Streaming -> ML threat
classification -> split storage (MinIO for normal traffic,
Elasticsearch for harmful traffic) -> dashboards (Kibana + Superset) ->
email alerting -> all orchestrated by Airflow.

Built against a real UNSW-NB15-style sample (`data/logs.csv`, 125 rows,
45 columns) — the schema throughout the codebase (Spark job, ML
training, Elasticsearch mapping) matches this dataset's actual columns,
not an invented `source_ip`/`dest_ip` schema.

See `docs/Architecture.md` for the full data-flow diagram and technology
rationale, `docs/Pipeline.md` for a single-row walkthrough, and
`docs/ProjectWorkflow.md` for day-to-day operating commands.

## Requirements

- Windows 11 + WSL2 (Ubuntu), or native Linux/macOS
- Docker Desktop with the WSL2 backend, or Docker Engine + Compose v2
- 16 GB RAM recommended (containers are capped to stay under ~12 GB combined)
- ~20 GB free disk

## Quick start

```bash
cd cybersec-log-platform
chmod +x scripts/*.sh
bash scripts/start_platform.sh      # builds images, brings everything up in order
bash scripts/run-pipeline.sh        # triggers the Airflow DAG that trains the model
                                     # and starts streaming data/logs.csv
```

Then open:

- Airflow — http://localhost:8089 (admin / admin)
- Kafka UI — http://localhost:8090
- Spark Master — http://localhost:8080
- MinIO Console — http://localhost:9001 (minioadmin / minioadmin123)
- Elasticsearch — http://localhost:9200
- Kibana — http://localhost:5601
- Superset — http://localhost:8088 (admin / admin)
- Producer control API — http://localhost:5050 (`/health`, `/status`, `POST /start`, `POST /stop`)

## Folder structure

```
cybersec-log-platform/
├── docker-compose.yml            # all 19 services
├── .env.example                  # copy to .env
├── data/logs.csv                 # your UNSW-NB15-style sample dataset
├── producer/                     # Kafka producer + Flask control API
├── spark/                        # streaming job, ML training, hourly rollup, Dockerfile+JARs
├── airflow/                      # Dockerfile (+ Docker CLI), DAGs
├── alerting/                     # email alert service (polls Elasticsearch)
├── elasticsearch/mappings/       # harmful-logs index mapping
├── superset/                     # Dockerfile, config, entrypoint
├── scripts/                      # start/stop/reset/health-check/train/trigger scripts
└── docs/                         # Architecture.md, Pipeline.md, ProjectWorkflow.md
```

## What's actually implemented (no placeholders)

- **Producer**: reads the CSV, streams one JSON message per row into
  Kafka with retry/backoff, graceful shutdown on SIGTERM, and a Flask
  API (`/start`, `/stop`, `/status`, `/health`) so ingestion only begins
  when Airflow (or you) says so.
- **Spark ML training** (`spark/ml_model.py`): indexes `proto`/`service`/
  `state`, trains a multi-class `RandomForestClassifier` against
  `attack_cat`, evaluates accuracy/F1, and persists both the model and a
  `label_mapping.json` so the streaming job never has to hardcode
  index-to-label strings that could drift after retraining.
- **Spark Structured Streaming** (`spark/streaming_job.py`): parses the
  real 45-column schema, cleans it, classifies every micro-batch,
  derives `threat_label`/`severity`/`is_harmful`, and writes an
  explicit, intentional output schema — the model's internal columns
  (`features`, `rawPrediction`, `probability`, `*_idx`) never leak into
  MinIO or Elasticsearch.
- **Hourly rollup** (`spark/rollup_job.py`): aggregates both MinIO
  (normal) and Elasticsearch (harmful) into a Postgres table that
  Superset queries directly — no Trino/Hive-over-MinIO container needed.
- **Airflow** (`airflow/dags/`): `pipeline_dag.py` brings the pipeline
  up end-to-end; `report_dag.py` runs the rollup hourly. Both launch
  Spark jobs via `docker exec spark-master spark-submit ...` because the
  Airflow images have no `spark-submit` binary of their own —
  `airflow-worker` gets the Docker CLI installed and the host's
  `docker.sock` mounted specifically to make this work.
- **Alerting** (`alerting/alert_service.py`): polls `harmful-logs` for
  `severity=HIGH AND alerted=false`, emails via SMTP, marks `alerted:
  true` — a deliberate substitute for Elasticsearch Watcher, which is a
  paid X-Pack feature unavailable on this cluster's free license.
- **Elasticsearch mapping**: matches the streaming job's actual output
  columns exactly (no drift between what's written and what's mapped).
- **Scripts**: `start_platform.sh` (staged bring-up),
  `health_check.sh` (fixed — an earlier version curl'd Kafka's binary
  port and always reported it unhealthy), `train-model.sh`,
  `run-pipeline.sh`, `stop-project.sh`, `reset-project.sh`,
  `init-elasticsearch.sh`, `postgres-init.sh`.

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `docker exec spark-master ...` fails from Airflow | `airflow-worker` can't reach the Docker socket | Confirm Docker Desktop's WSL2 integration is enabled for your distro; the compose file already mounts `/var/run/docker.sock` and runs `airflow-worker` as root specifically for this |
| Kafka shows unhealthy in `health_check.sh` | (Historical bug, already fixed here) | `health_check.sh` uses `kafka-broker-api-versions`, not `curl`, against Kafka |
| Streaming job can't load the model | Model hasn't been trained yet | Run `bash scripts/train-model.sh` or trigger `cybersec_log_pipeline` in Airflow, which trains before streaming starts |
| No harmful-logs index yet | No harmful traffic has been classified yet | Normal on a fresh run; the index is created on first harmful write. `bash scripts/init-elasticsearch.sh` pre-creates it with the correct mapping if you want it to exist immediately |
| Alert emails never arrive | `SMTP_USERNAME` / `ALERT_TO_EMAIL` not set in `.env` | Edit `.env`, then `docker compose restart alert-service`. Until configured, alerts are logged to the container's stdout instead |
| Out of memory / containers OOM-killed | Docker Desktop RAM limit too low | Increase WSL2 memory in `.wslconfig` (see `docs/ProjectWorkflow.md`) |

## Dataset

`data/logs.csv` ships with a 125-row sample matching the real
UNSW-NB15 feature set (`proto`, `service`, `state`, 35 flow-statistics
columns, `attack_cat`, `label`). Swap in a larger UNSW-NB15 CSV any time
— see "Bringing your own dataset" in `docs/ProjectWorkflow.md`.
