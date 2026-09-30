# How to Run: Fleet Operations Lambda Pipeline (A to Z)

EC8202 Big Data and Analytics, Mini Project (Ride-Hailing Fleet Operations).

This guide takes you from a fresh machine to a fully running, verified pipeline, and covers restarting after a reboot, demoing the alert rule, stopping, and troubleshooting.

---

## 1. What you are running

| Layer | Flow | Output |
|---|---|---|
| Speed layer | `streaming_source.py` (simulator) -> Kafka topic `trip_events` -> Spark Structured Streaming (`streaming/streaming_job.py`) | Postgres tables `live_fleet_metrics`, `vehicle_daily_earnings` |
| Batch layer | `batch_source.py` (simulator) -> daily CSV in `airflow/data` -> Airflow DAG `daily_profitability_report` | Postgres table `daily_profitability_report` |
| Serving layer | FastAPI (`serving/main.py`) | `/fleet/live`, `/fleet/profitability`, `/health` |
| Observability | Every stage writes a heartbeat to `pipeline_heartbeat`; `/health` checks them | Alert rule: "no data in N seconds" |

All of it runs in Docker containers, started with one command.

**Simulated clock:** one simulated "day" = `SIMULATED_DAY_SECONDS` = 300 seconds (5 minutes), set in `docker-compose.yml`. State this compression in your report.

---

## 2. Prerequisites (one-time)

1. **Windows 10/11** with **Docker Desktop** installed and using the WSL 2 backend.
2. **Resources for Docker Desktop:** at least 4 CPUs and 6-8 GB RAM (Docker Desktop -> Settings -> Resources). The stack runs Kafka, Zookeeper, Spark, Airflow and two Postgres instances, so it is heavy.
3. **Internet access** on the first run (image pulls are several GB, and Spark downloads its Maven JARs).
4. **Free ports:** `5432` (Postgres), `8000` (API), `8080` (Airflow), `9092` (Kafka). If you run a local Postgres, stop it or change the host port (see Troubleshooting).
5. **Optional:** VS Code (`code .` opens the project).

Check Docker is working:

```bat
docker --version
docker compose version
docker info
```

If `docker info` errors, Docker Desktop is not running. Start it and wait until it says "Engine running".

---

## 3. Project layout

```
fleet-lambda-pipeline/
  docker-compose.yml
  README.md
  HOW_TO_RUN.md
  producers/        streaming_source.py + batch_source.py (simulated sources)
  streaming/        Dockerfile, requirements.txt, streaming_job.py (speed layer)
  airflow/dags/     daily_profitability_dag.py (batch layer)
  airflow/data/     daily expense CSVs written by the batch simulator
  serving/          main.py (FastAPI serving layer)
  postgres/         init.sql (schema for all tables)
  docs/             architecture notes
```

---

## 4. Required configuration (already applied, verify once)

These three settings were needed to make the project work. Confirm they are in your files.

**a) `streaming/Dockerfile` first line**

```dockerfile
FROM bitnamilegacy/spark:3.5
```

`bitnami/spark:3.5` no longer exists on Docker Hub. Legacy images no longer receive security updates, which is acceptable for a coursework project (mention it in the report).

**b) `streaming/requirements.txt`**

```
psycopg2-binary==2.9.9
kafka-python-ng==2.2.3
```

The old `kafka-python==2.0.2` crashes on the Python 3.12 that ships in the Spark image (`No module named 'kafka.vendor.six.moves'`). Do not install both `kafka-python` and `kafka-python-ng`.

**c) `serving/main.py`**

Use the corrected version: closes DB connections, checks a fixed list of expected stages, per-stage stale thresholds (speed layer 120 s, batch layer 600 s), and returns HTTP 503 on query failure.

**Optional but recommended:** delete the line `version: "3.8"` at the top of `docker-compose.yml` to remove the "attribute `version` is obsolete" warning. It is harmless either way.

---

## 5. First-time start

Open **Command Prompt** or **PowerShell** in the project folder:

```bat
cd "C:\Users\hapup\OneDrive\Desktop\8th sem\EC8202 Big Data and Analytics\MiniProject\fleet-lambda-pipeline"
```

Start everything **in the background** (`-d`) so closing the terminal does not stop it:

```bat
docker compose up -d --build
```

What happens, in order:

1. Images are pulled (Kafka, Zookeeper, Airflow, Postgres). This can take 10-20 minutes on the first run.
2. `streaming`, `producers` and `serving` images are built.
3. Postgres and Kafka become healthy. `kafka-init` creates the `trip_events` topic and exits with code 0 (this is normal).
4. `airflow-init` migrates the Airflow DB and creates the admin user, then exits with code 0 (also normal).
5. Airflow webserver and scheduler, the Spark job, both simulators and the API start.
6. **Spark downloads its JARs (about 58 MB) on every container start.** Allow 2-3 minutes before it begins writing data.

Watch progress:

```bat
docker compose ps
docker compose logs -f spark-streaming-job
```

Press `Ctrl+C` to stop following logs (this does **not** stop the containers when started with `-d`).

---

## 6. Access points

| Service | URL / access | Login |
|---|---|---|
| Serving API (docs) | http://localhost:8000/docs | none |
| Live metrics | http://localhost:8000/fleet/live | none |
| Profitability report | http://localhost:8000/fleet/profitability | none |
| Health / alerts | http://localhost:8000/health | none |
| Airflow UI | http://localhost:8080 | admin / admin |
| Postgres (app data) | localhost:5432, db `fleet` | fleet / fleet |
| Kafka (from host) | localhost:9092 | none |

Useful API options:

- `/fleet/live?zone=central&limit=20` filters by zone.
- `/fleet/profitability?source_file=vehicle_expenses_day5968928.csv` selects a specific simulated day (defaults to the latest).

---

## 7. Verify that everything works (checklist)

Wait about 3-5 minutes after start, then check each item.

### 7.1 Containers

```bat
docker compose ps
```

Expected **Up**: `zookeeper`, `kafka`, `postgres-app`, `postgres-airflow`, `airflow-webserver`, `airflow-scheduler`, `spark-streaming-job`, `streaming-source-sim`, `batch-source-sim`, `serving-api`.
Expected **Exited (0)** (normal one-shot jobs): `kafka-init`, `airflow-init`.

### 7.2 Kafka receives events

```bat
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trip_events --from-beginning --max-messages 5
```

You should see 5 JSON trip events.

### 7.3 Speed layer writes rows

Open http://localhost:8000/fleet/live. Expected: one row per zone per one-minute window (`central`, `east`, `north`, `south`, `west`).

Or via SQL:

```bat
docker compose exec postgres-app psql -P pager=off -U fleet -d fleet -c "select count(*) from live_fleet_metrics;"
```

### 7.4 Batch layer runs

1. Open http://localhost:8080 and log in with `admin` / `admin`.
2. Find the DAG `daily_profitability_report`. If it is paused, toggle it **on**.
3. It runs every 5 minutes (one simulated day). Wait for green successful runs.
4. Open http://localhost:8000/fleet/profitability. Expected: 20 vehicles from one daily file, sorted by `net_profit`.

Note: the first one or two runs after a cold start can show `total_earnings = 0.00`, because the batch job ran before Spark had written any earnings. Later runs show real values (about 1,700-2,000 per vehicle per day).

### 7.5 Health

Open http://localhost:8000/health. Expected once all stages have reported:

```json
{"status":"ok", "checks":[
  {"stage":"speed_layer_zone_metrics", "status":"ok", ...},
  {"stage":"speed_layer_vehicle_earnings", "status":"ok", ...},
  {"stage":"batch_layer", "status":"ok", ...}
]}
```

Right after a cold start you may briefly see `missing` or `stale` while Spark downloads its JARs. That is the alert rule working. It should clear within a few minutes.

---

## 8. Restarting after a reboot or a Docker stop

If you restart the laptop, or Docker Desktop stops, containers exit (status `Exited (137)`, meaning they were killed). Your **data is kept** because Postgres uses named volumes.

Restart procedure:

1. **Start Docker Desktop** and wait until the engine is running.
2. Go to the project folder.
3. Run:

```bat
docker compose up -d
```

You do not need `--build` unless you changed code in `streaming/`, `producers/` or `serving/`.

4. Wait 3-5 minutes for Spark to re-download its JARs and resume, then re-check `/health`.

Note: the simulated day number continues from the wall clock, so after a long downtime there will be gaps in the day sequence and in the live windows. That is expected.

If some services stay stuck, restart them individually:

```bat
docker compose restart spark-streaming-job
docker compose restart airflow-webserver airflow-scheduler
```

---

## 9. Demo: prove the alert rule works (for the report)

The rule is: a stage is stale if it has not reported within its threshold (speed layer 120 s, batch layer 600 s). A stage with no heartbeat at all shows as `missing`.

**Demo 1: kill the speed layer**

1. Confirm `/health` is fully `ok`.
2. Stop the streaming job:

```bat
docker compose stop spark-streaming-job
```

3. Wait a little over 2 minutes and reload `/health`. Expected: both `speed_layer_*` stages `stale`, `batch_layer` still `ok`, overall status `degraded`. Take a screenshot.
4. Recover:

```bat
docker compose start spark-streaming-job
```

Allow 2-3 minutes for JAR downloads. Health returns to `ok`.

**Demo 2: a missing stage**

```bat
docker compose exec postgres-app psql -U fleet -d fleet -c "delete from pipeline_heartbeat where stage='batch_layer';"
```

`/health` shows `batch_layer` as `missing` until the next DAG run rewrites its heartbeat.

---

## 10. Screenshots to collect for the report

1. `docker compose ps` with all services up.
2. Airflow UI with several green DAG runs.
3. `/fleet/live` showing complete one-minute windows.
4. `/fleet/profitability` showing one full day of 20 vehicles.
5. `/health` in the `ok` state, and again in the `degraded` state (Demo 1).
6. Optionally: the architecture diagram from `docs/`.

---

## 11. Stopping and cleaning up

| Goal | Command |
|---|---|
| Pause everything, keep containers and data | `docker compose stop` |
| Resume | `docker compose start` |
| Remove containers, keep data | `docker compose down` |
| **Full reset: remove containers AND all data** (use for a clean demo run) | `docker compose down -v` |
| Rebuild one service after code changes | `docker compose up -d --build serving-api` |

After `down -v`, the next `up -d` starts from scratch: schema is recreated from `postgres/init.sql`, and the Airflow DB and admin user are recreated.

---

## 12. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `bitnami/spark:3.5: not found` | Image removed from Docker Hub | Use `FROM bitnamilegacy/spark:3.5` in `streaming/Dockerfile` |
| Spark log: `No module named 'kafka.vendor.six.moves'` | Old `kafka-python` on Python 3.12 | Use `kafka-python-ng==2.2.3` in `streaming/requirements.txt`, then `docker compose build spark-streaming-job` |
| `/fleet/live` returns `{"count":0,...}` | Spark not writing yet, or crashed | `docker compose logs --tail 60 spark-streaming-job`. Wait for JARs to finish downloading |
| `/health` shows only some stages | Old `main.py` running | Copy the corrected `serving/main.py`, rebuild `serving-api`. Build log must show `COPY . .` as **not** `CACHED` |
| `/health` shows `batch_layer` stale between runs | Threshold shorter than the 5-minute batch cadence | Use corrected `main.py` (batch threshold 600 s) |
| `total_earnings = 0.00` in early batch rows | Batch ran before Spark wrote earnings | Normal at cold start. Later runs are correct. For a clean demo: `docker compose down -v` then `up -d --build` |
| Everything `Exited (137)` | Docker Desktop or laptop stopped | Start Docker Desktop, then `docker compose up -d` |
| Port already in use (5432 or 8080) | Another program uses the port | Stop it, or change the host side, e.g. `"5433:5432"` in `docker-compose.yml` |
| Bind-mount or file lock errors | OneDrive syncing the project folder | Pause OneDrive sync, or move the project to a plain path such as `C:\dev\fleet-lambda-pipeline` |
| Airflow webserver shows DB errors on first start | It started before `airflow-init` finished | `docker compose restart airflow-webserver airflow-scheduler` |
| `topic not found` in Spark logs | Spark started before `kafka-init` finished | `docker compose restart spark-streaming-job` |
| Docker is very slow or containers get killed | Not enough memory | Give Docker Desktop 6-8 GB RAM (Settings -> Resources) |
| `docker compose ps` is empty | Nothing created yet, or you are in the wrong folder | `cd` to the project folder, then `docker compose up -d` |

Useful diagnostic commands:

```bat
docker compose ps -a
docker compose logs --tail 60 <service-name>
docker compose exec postgres-app psql -P pager=off -U fleet -d fleet -c "\dt"
docker compose exec postgres-app psql -P pager=off -U fleet -d fleet -c "select * from pipeline_heartbeat;"
```

Service names: `zookeeper`, `kafka`, `kafka-init`, `postgres-app`, `postgres-airflow`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`, `spark-streaming-job`, `streaming-source-sim`, `batch-source-sim`, `serving-api`.

---

## 13. Quick reference (cheat sheet)

```bat
:: start (first time or after reboot)
docker compose up -d --build

:: status
docker compose ps

:: logs for one service
docker compose logs --tail 60 spark-streaming-job

:: stop, keep data
docker compose stop

:: full reset
docker compose down -v
```

Health: http://localhost:8000/health
Airflow: http://localhost:8080 (admin/admin)
