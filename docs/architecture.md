# Architecture Decision: Lambda vs Kappa

## Use case and requirements

The fleet operations use case has two structurally different correctness
requirements. Live fleet utilization (active vehicles, idle ratio, earnings
by zone) is a monitoring signal — approximate, low-latency results are
acceptable and expected to be superseded continuously. Per-vehicle
profitability, by contrast, is a financial calculation that must be correct
once the day's actual fuel and maintenance costs are known — an approximate
or provisional profitability figure would be actively misleading to a fleet
manager deciding whether to pull a vehicle from service.

## Why Lambda

This dual requirement — an immediate, best-effort view and a delayed,
authoritative view of the same underlying activity — is precisely the
problem Lambda architecture is designed to solve. The speed layer computes
windowed utilization metrics directly from the `trip_events` Kafka stream
via Spark Structured Streaming, prioritizing latency over completeness. The
batch layer, orchestrated by Airflow, runs once per (simulated) day, joining
the day's full trip history against the actual expense file to produce a
profitability figure that batch processing's characteristic advantage — the
ability to see the complete dataset before computing — makes strictly more
accurate than anything the streaming path could produce in real time. The
two layers are allowed to disagree; the serving layer simply presents both
for what they are: a live estimate and yesterday's reconciled truth.

## Why not Kappa (rejected alternative)

A Kappa architecture would process both concerns through a single
stream-processing backbone, treating the daily reconciliation as a replay
of the event log over a bounded window. This was rejected for three reasons
specific to this use case:

1. The expense data does not arrive as a stream at all — it is a single
   daily file, so there is no natural "topic" for it to join against
   without artificially wrapping it in Kafka, adding complexity without
   benefit.
2. Reprocessing a full day's trip volume through the same streaming engine
   used for sub-minute utilization metrics would require the stream job to
   hold or re-read a day's worth of state, undermining the low-latency
   design the speed layer was built for.
3. Kappa "hasn't seen widespread adoption due to streaming complexity and
   cost compared to traditional batch processing" — for a genuinely
   once-daily batch computation, that cost is unjustified when Airflow
   already gives us mature batch scheduling, retries, and dependency
   management for free.

## Trade-off acknowledged

Lambda's well-known cost — maintaining two codebases (the Spark streaming
job and the Airflow batch job) and reconciling their outputs — is real here
too. We mitigate it by keeping the transformation logic in each layer
intentionally simple and by having both layers write to the same Postgres
instance under clearly named tables (`live_fleet_metrics`,
`daily_profitability_report`), so the "reconciliation" is just two tables
the serving layer reads from, not a merge algorithm.

## Diagram

See the rendered architecture overview shared in chat (three tiers:
sources → speed/batch layers → serving layer). Recreate it in the final
report as a labelled figure — screenshot the widget or redraw in
draw.io/Mermaid using the same structure:

```
Streaming source ---> Speed layer (Kafka + Spark Streaming) ---\
                                                                  --> Serving layer (Postgres + API)
Batch source ------> Batch layer (Airflow daily DAG) -----------/
```

## Technology stack justification (summary — expand per rubric's 10 marks)

| Layer | Choice | Why (tied to this use case) |
|---|---|---|
| Ingestion | Kafka | Continuous GPS/telemetry events need a durable, replayable, partition-parallel ingestion point; partitioning by `vehicle_id`/`zone` avoids hotspotting on busy zones |
| Stream processing | Spark Structured Streaming (over Storm) | DataFrame API and native Kafka/JDBC connectors minimize code for a 2-week build; windowed aggregation (tumbling window) matches the "utilization over time" requirement directly |
| Orchestration | Airflow | Daily batch job with retries, dependency tracking (extract → compute → load), and a UI for demoing pipeline health — exactly what the assignment's "batch jobs or reporting pipelines" use case describes |
| Storage/serving | PostgreSQL | Both output tables are small, structured, and queried by point/range lookups (not multi-TB analytics) — a full data lake (HDFS/S3+Parquet) would be over-engineered for this scale and adds unnecessary Docker Compose complexity for a local-only deployment |

## Open items / assumptions to state in the report

- Simulated day compression: `SIMULATED_DAY_SECONDS` (see README)
- No cloud services used — deliberately local-only via Docker Compose
- Fleet size fixed at `NUM_VEHICLES` (see producers/) for demo purposes
- **Day-boundary alignment**: the batch source computes its day index as
  `floor(unix_time / SIMULATED_DAY_SECONDS)`, and the streaming job's
  per-vehicle earnings aggregate uses a Spark tumbling window of the same
  duration. Spark's `window()` function aligns tumbling windows to the Unix
  epoch by default, so the two independently-running containers land on
  matching day boundaries without explicit coordination (e.g. no shared
  "day started" signal between them). This is a deliberate simplification:
  it assumes both containers' clocks are in sync (true by default under
  Docker Compose on one host) and would need an explicit coordination
  mechanism (e.g. a shared config service or a control topic) in a
  multi-host production deployment — worth naming in the report's
  limitations section.
- On the batch layer's first run, some vehicles may have no streaming
  earnings yet (treated as 0 rather than failing the run) — logged, not
  silently swallowed; expected only on the very first simulated day.
