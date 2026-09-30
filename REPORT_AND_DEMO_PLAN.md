# Report and demo plan

Group: Hapuarachchi H.P.L (EG/2020/3953), Chathumal W.S. (EG/2020/3867), Kulathilaka W.A.S.P (EG/2020/4029)

Target: 8-15 page report (aim for about 12) and a 5-10 minute demo (aim for 8).

## Report outline, mapped to the rubric

| # | Section | Pages | Rubric area (marks) | What to write |
|---|---|---|---|---|
| 1 | Introduction and use case | 0.5 | Report (15) | Ride-hailing fleet operations; the two questions the system answers: "where is the fleet idle right now?" and "how profitable was each vehicle yesterday?" Name the simulated clock (1 day = 300 s). |
| 2 | Architecture decision: Lambda vs Kappa | 2.5 | Architecture (20) | Why Lambda fits: the expense data is a daily file from a different system and is not an event stream, so it cannot simply be replayed from Kafka; reconciliation needs a join of streamed earnings with file costs. Kappa would force expenses into Kafka and a single reprocessing path. State the honest trade-off: two code paths to maintain. Include the diagram from `docs/architecture.md`. |
| 3 | Technology choices | 1.5 | Tech stack (10) | One short paragraph per tool (Kafka, Spark Structured Streaming, Airflow, PostgreSQL, FastAPI, Docker Compose) and one alternative you rejected with a reason (for example Storm vs Spark, Cassandra/HDFS vs Postgres at this scale). |
| 4 | Ingestion | 2 | Ingestion (15) | Event schema; topic `trip_events` with 3 partitions keyed by `vehicle_id`; producer `acks=all` and retries; connection retry on startup; the daily CSV drop for the batch path. |
| 5 | Processing | 2 | Processing (15) | Spark: 1-minute zone windows, per-vehicle daily earnings, 30 s trigger, definition of idle ratio, handling of event time vs processing time (and watermark, if used). Airflow: three tasks (extract, compute, load), retries, join logic, net profit formula. |
| 6 | Storage and serving | 1 | Storage/serving (10) | Tables and keys; upsert strategy; why Postgres; the three endpoints and the dashboard. State the batch key: one row per vehicle per daily expense file. |
| 7 | Observability | 1.5 | Observability (10) | Structured JSON logs in every stage; heartbeat table; `/health` rule with thresholds (120 s streaming, 600 s batch) and why batch differs; evidence of the fault tests (see checklist). |
| 8 | Testing and validation | 1 | Code quality (5), Report (15) | 45 automated tests; clean-start test with `down -v`; idempotency integration test. |
| 9 | Limitations and future work | 0.5 | Report (15) | Simulated data; single broker and single node; no authentication; Lambda's duplicated logic; ideas: Prometheus/Grafana, schema registry, exactly-once sinks. |
| 10 | Conclusion and contributions | 0.5 | Report (15) | What was achieved, plus a table of who did what. |

Before writing section 2, check that `docs/architecture.md` already holds a real Lambda vs Kappa justification. It carries the biggest single mark.

## Evidence to capture (screenshots or log excerpts)

1. Architecture diagram.
2. `docker compose ps` after a clean `down -v` and `up`, all healthy on the first try.
3. Sample Kafka events (`kafka-console-consumer --max-messages 5`).
4. Spark log lines showing `batch=N rows=... status=ok`.
5. Dashboard with live data, then the same page with a red health chip.
6. Airflow UI with the DAG's green runs, and the `/fleet/profitability` output.
7. Fault test A: `docker compose stop kafka`, simulator logs showing `flush failed` and `degraded`, then recovery.
8. Fault test B: the earlier Spark "falling behind by 468 s" log line and the catch-up. Real evidence of recovery.
9. `pytest` output showing 45 passed.
10. Health endpoint JSON while a stage is stale.

## Demo script (about 8 minutes)

Start with the stack already running (first start takes a few minutes), or record the startup and cut it.

| Time | Show | Say |
|---|---|---|
| 0:00 | Architecture diagram | The problem, and why Lambda (30 s). |
| 0:30 | `docker compose ps` | Everything healthy; one command starts the system. |
| 1:15 | Kafka consumer output | Raw events, keyed by vehicle, 3 partitions. |
| 2:00 | Spark logs | Windowed aggregation, batch status lines. |
| 3:00 | Dashboard (`localhost:8000`) | Live zones, idle share, rebalance tag, earnings per minute. |
| 4:00 | Airflow UI and profitability table | Daily batch reconciliation, net profit, service flags. |
| 5:00 | Failure test | Stop `spark-streaming-job`; `/health` turns degraded and the dashboard chip goes red; start it again and watch it recover. |
| 6:45 | Terminal: `pytest` | 45 tests, including the idempotent upsert. |
| 7:30 | Limitations slide or spoken list | Honest trade-offs. |

Demo tip: the `/health` threshold is 120 s, so a live failure test means waiting two minutes. For the recording, run the API with a shorter threshold by adding `HEALTH_STALE_SECONDS: "30"` under `serving-api` environment in `docker-compose.yml`, then remove it afterwards (or keep 120 and cut the wait when editing).

## Split the work

Decide who writes which sections and who presents which part of the demo before you start. One sensible split: one person for sections 2-3 (architecture and tech choices), one for 4-5 (ingestion and processing), one for 6-8 (storage, observability, testing). Everyone reviews the whole report for consistency.
