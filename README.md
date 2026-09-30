# Fleet Operations — Lambda Architecture Mini Project

EC8203 Applied Big Data Engineering — Mini Project (Use Case 1: Ride-Hailing
Fleet Operations).

## Group members

| Member | Student ID |
|---|---|
| Hapuarachchi H.P.L | EG/2020/3953 |
| Chathumal W.S. | EG/2020/3867 |
| Kulathilaka W.A.S.P | EG/2020/4029 |

## Architecture

Lambda architecture with two independent processing paths converging on a
shared serving layer. See `docs/architecture.md` for the full justification
(Lambda vs Kappa) and diagram.

- **Speed layer**: `streaming_source.py` (simulator) → Kafka `trip_events`
  → Spark Structured Streaming (`streaming/streaming_job.py`) → Postgres
  `live_fleet_metrics` (1-minute zone windows) and per-vehicle daily earnings
- **Batch layer**: `batch_source.py` (simulator) → daily CSV file → Airflow
  DAG (`airflow/dags/daily_profitability_dag.py`) → Postgres
  `daily_profitability_report`
- **Serving layer**: FastAPI service (`serving/main.py`) reading both tables

## Status

Core pipeline complete and verified end to end from a clean start
(`docker compose down -v && docker compose up -d --build`):

- **Speed layer**: real Kafka → Spark Structured Streaming → Postgres writes
  for zone-level live metrics and per-vehicle daily earnings, plus heartbeat
  updates for observability.
- **Batch layer**: Airflow DAG joins streamed earnings with the daily expense
  file and upserts `daily_profitability_report` (one row per vehicle per
  report date).
- **Serving layer**: `/fleet/live`, `/fleet/profitability`, and `/health`.
- **Observability**: structured JSON logging in the streaming, batch and
  serving stages; `/health` implements the "no data received in N minutes"
  alert rule (120 s threshold for each speed-layer stream, 600 s for the
  batch layer).
- **Startup reliability**: Zookeeper healthcheck, Kafka waits for it, Airflow
  webserver/scheduler wait for `airflow-init` to complete, and long-running
  services use `restart: on-failure`.

Still open:

- Structured logging for the streaming source simulator (`streaming_source.py`)
- Dashboard UI (endpoints are JSON-only today)
- Automated tests (pytest for the serving API and profit calculation)
- Optional stretch goal: Prometheus/Grafana metrics export

## Running locally

```bash
docker compose up -d --build
```

The first start takes a few minutes (Spark downloads its Kafka and Postgres
connector jars). To simulate a fresh machine, wipe all volumes first:

```bash
docker compose down -v
docker compose up -d --build
```

Services once running:

| Service | URL / access |
|---|---|
| Airflow UI | http://localhost:8080 (admin/admin) |
| Serving API (Swagger) | http://localhost:8000/docs |
| Live zone metrics | http://localhost:8000/fleet/live |
| Daily profitability | http://localhost:8000/fleet/profitability |
| Health / alert rule | http://localhost:8000/health |
| Postgres (app data) | localhost:5432 (fleet/fleet) |
| Kafka | localhost:9092 |

Make sure the `daily_profitability_report` DAG is unpaused in the Airflow UI.
The batch section of `/health` reports data only after the first DAG run
(up to one simulated day, i.e. about 5 minutes after start).

## Verifying the pipeline

```bash
# Container status (all should be Up / healthy)
docker compose ps

# Speed layer progress
docker compose logs -f spark-streaming-job

# Sample raw events from Kafka
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic trip_events --from-beginning --max-messages 5

# Rows written by the speed layer
docker compose exec postgres-app psql -U fleet -d fleet \
  -c "select count(*) from live_fleet_metrics;"
```

## Simulated clock

One simulated "day" = `SIMULATED_DAY_SECONDS` (default 300s / 5 minutes),
set as an environment variable on `batch-source-sim` and `spark-streaming-job`
in `docker-compose.yml`. The Airflow DAG is scheduled at the same 5-minute
interval. State this compression in the final report.

## Repo layout

```
producers/     streaming_source.py + batch_source.py (simulated sources)
streaming/     Spark Structured Streaming job (speed layer)
airflow/dags/  Airflow DAG (batch layer)
serving/       FastAPI serving layer
postgres/      init.sql — schema for both layers + observability heartbeat
docs/          architecture notes, diagrams, decision justification
```

## Known behaviours (worth stating in the report)

- **Partial latest window**: the newest 1-minute window in `/fleet/live` is
  still filling and is upserted as more events arrive, so its values grow
  between calls.
- **Recovery after a stall**: if Docker/WSL2 pauses (e.g. laptop sleep), Spark
  logs "Current batch is falling behind" and catches up on resume; the
  checkpoint/offset mechanism prevents data loss. Windows for the paused
  period may be missing or late.
- **Idempotent batch load**: re-running the DAG for the same report date
  upserts rather than duplicating rows.