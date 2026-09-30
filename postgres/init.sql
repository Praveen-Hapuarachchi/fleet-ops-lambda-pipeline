-- Speed layer output: continuously overwritten/updated live utilization metrics
CREATE TABLE IF NOT EXISTS live_fleet_metrics (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    zone                TEXT NOT NULL,
    active_vehicles     INTEGER NOT NULL,
    idle_vehicles       INTEGER NOT NULL,
    idle_ratio          NUMERIC(5, 4) NOT NULL,
    trips_started       INTEGER NOT NULL,
    total_earnings      NUMERIC(12, 2) NOT NULL,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, zone)
);

-- Speed layer intermediate: per-vehicle earnings/trip-count for the CURRENT
-- simulated-day window. Written continuously by the streaming job; read by
-- the batch layer as the "actual earnings so far" side of the daily join.
-- window_start/window_end come from Spark's tumbling window sized to
-- SIMULATED_DAY_SECONDS, which (by Spark's epoch-aligned window default)
-- lines up with the batch source's day boundary without explicit
-- coordination between the two containers. See docs/architecture.md.
CREATE TABLE IF NOT EXISTS vehicle_daily_earnings (
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    vehicle_id     TEXT NOT NULL,
    total_earnings NUMERIC(12, 2) NOT NULL,
    trip_count     INTEGER NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, vehicle_id)
);

-- Batch layer output: one row per vehicle per simulated-day file processed.
-- Keyed by source_file (not report_date) because a compressed simulated
-- clock can produce several "days" within one real calendar day.
CREATE TABLE IF NOT EXISTS daily_profitability_report (
    source_file          TEXT NOT NULL,
    report_date          DATE NOT NULL,
    vehicle_id           TEXT NOT NULL,
    total_earnings        NUMERIC(12, 2) NOT NULL,
    fuel_cost            NUMERIC(12, 2) NOT NULL,
    maintenance_cost     NUMERIC(12, 2) NOT NULL,
    distance_covered_km  NUMERIC(10, 2) NOT NULL,
    net_profit           NUMERIC(12, 2) NOT NULL,
    service_flag         BOOLEAN NOT NULL DEFAULT false,
    generated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_file, vehicle_id)
);

-- Observability: minimal pipeline health table for the "no data received in N minutes" check
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    stage        TEXT PRIMARY KEY,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status       TEXT NOT NULL DEFAULT 'ok'
);
