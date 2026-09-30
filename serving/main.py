"""
Serving layer: the "combines results from both layers" component of the
Lambda architecture.

Endpoints:
    GET /                     -> operations dashboard (serving/dashboard.html)
    GET /fleet/live           -> latest windows from live_fleet_metrics (speed layer)
    GET /fleet/profitability  -> one day's rows from daily_profitability_report (batch layer)
    GET /health               -> observability: flags any pipeline stage that
                                  hasn't reported a heartbeat within HEALTH_STALE_SECONDS,
                                  or that has never reported at all (the assignment's
                                  required "no data received in N minutes" alert rule)
"""

import json
import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="Fleet Ops Serving Layer")

DASHBOARD_PATH = Path(__file__).parent / "dashboard.html"

DB_CONFIG = {
    "host": os.environ.get("PIPELINE_DB_HOST", "postgres-app"),
    "dbname": os.environ.get("PIPELINE_DB_NAME", "fleet"),
    "user": os.environ.get("PIPELINE_DB_USER", "fleet"),
    "password": os.environ.get("PIPELINE_DB_PASSWORD", "fleet"),
}

HEALTH_STALE_SECONDS = int(os.environ.get("HEALTH_STALE_SECONDS", "120"))

# The batch DAG only runs once per simulated day (300s by default), so its
# heartbeat is naturally ~300s apart. Its threshold must exceed that cadence
# (default: 2 simulated days) or /health raises false alarms between runs.
BATCH_STALE_SECONDS = int(os.environ.get("BATCH_STALE_SECONDS", "600"))
STAGE_THRESHOLDS = {"batch_layer": BATCH_STALE_SECONDS}

# Every stage that must be heartbeating. A stage in this list with no row in
# pipeline_heartbeat is reported as "missing" and marks the pipeline degraded.
EXPECTED_STAGES = [
    "speed_layer_zone_metrics",
    "speed_layer_vehicle_earnings",
    "batch_layer",
]


class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(
            {
                "ts": self.formatTime(record),
                "level": record.levelname,
                "stage": "serving_layer",
                "msg": record.getMessage(),
            }
        )


logger = logging.getLogger("serving_layer")
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(JsonFormatter())
logger.addHandler(_handler)
logger.setLevel(logging.INFO)


@contextmanager
def db_cursor():
    """Yield a dict cursor and ALWAYS close the connection afterwards.

    Note: `with conn:` in psycopg2 only commits/rolls back the transaction, it
    does not close the connection, so we close it explicitly in `finally`.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()


def _query_failed(endpoint: str, exc: Exception) -> JSONResponse:
    logger.error(f"{endpoint} query failed: {exc}")
    return JSONResponse(
        status_code=503,
        content={"error": "query failed", "detail": str(exc)},
    )


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    """Operations dashboard. Served from the same origin as the JSON
    endpoints, so the page needs no CORS configuration."""
    try:
        return HTMLResponse(DASHBOARD_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.error(f"dashboard file not found at {DASHBOARD_PATH}")
        return HTMLResponse(
            "<h1>Dashboard not found</h1>"
            "<p>Place dashboard.html next to main.py and rebuild serving-api.</p>",
            status_code=404,
        )


@app.get("/fleet/live")
def get_live_metrics(
    limit: int = Query(50, ge=1, le=1000),
    zone: Optional[str] = None,
):
    """Latest speed-layer windows, newest first. Optional ?zone=central filter."""
    try:
        with db_cursor() as cur:
            cur.execute(
                """
                SELECT window_start, window_end, zone, active_vehicles,
                       idle_vehicles, idle_ratio, trips_started, total_earnings
                FROM live_fleet_metrics
                WHERE (%s IS NULL OR zone = %s)
                ORDER BY window_start DESC, zone
                LIMIT %s
                """,
                (zone, zone, limit),
            )
            rows = cur.fetchall()
        return {"count": len(rows), "metrics": rows}
    except Exception as exc:
        return _query_failed("/fleet/live", exc)


@app.get("/fleet/profitability")
def get_profitability(
    source_file: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """One day's batch report. Defaults to the most recent day; pass
    ?source_file=vehicle_expenses_dayNNNN.csv to view another day."""
    try:
        with db_cursor() as cur:
            cur.execute(
                """
                SELECT source_file, report_date, vehicle_id, total_earnings,
                       fuel_cost, maintenance_cost, distance_covered_km,
                       net_profit, service_flag
                FROM daily_profitability_report
                WHERE source_file = COALESCE(
                    %s,
                    (SELECT source_file
                     FROM daily_profitability_report
                     ORDER BY generated_at DESC
                     LIMIT 1)
                )
                ORDER BY net_profit DESC, vehicle_id
                LIMIT %s
                """,
                (source_file, limit),
            )
            rows = cur.fetchall()
        return {"count": len(rows), "report": rows}
    except Exception as exc:
        return _query_failed("/fleet/profitability", exc)


@app.get("/health")
def health():
    """Required observability check: alert if any expected pipeline stage has
    gone quiet for longer than HEALTH_STALE_SECONDS, or never reported."""
    try:
        with db_cursor() as cur:
            cur.execute(
                """
                SELECT stage, last_seen_at,
                       EXTRACT(EPOCH FROM (now() - last_seen_at)) AS seconds_since_seen
                FROM pipeline_heartbeat
                """
            )
            seen = {r["stage"]: r for r in cur.fetchall()}
    except Exception as exc:
        logger.error(f"/health query failed: {exc}")
        return JSONResponse(
            status_code=503,
            content={"status": "unknown", "error": str(exc)},
        )

    checks = []
    overall_ok = True

    # Check every expected stage (present or not), then any extra stages found.
    for stage in EXPECTED_STAGES + sorted(set(seen) - set(EXPECTED_STAGES)):
        row = seen.get(stage)
        if row is None:
            overall_ok = False
            checks.append(
                {
                    "stage": stage,
                    "last_seen_at": None,
                    "seconds_since_seen": None,
                    "status": "missing",
                }
            )
            continue

        age = float(row["seconds_since_seen"])
        threshold = STAGE_THRESHOLDS.get(stage, HEALTH_STALE_SECONDS)
        stale = age > threshold
        overall_ok = overall_ok and not stale
        checks.append(
            {
                "stage": stage,
                "last_seen_at": row["last_seen_at"].isoformat(),
                "seconds_since_seen": round(age, 1),
                "threshold_seconds": threshold,
                "status": "stale" if stale else "ok",
            }
        )

    if not overall_ok:
        logger.error(f"health check FAILED: {checks}")

    return {
        "status": "ok" if overall_ok else "degraded",
        "threshold_seconds": HEALTH_STALE_SECONDS,
        "checks": checks,
    }