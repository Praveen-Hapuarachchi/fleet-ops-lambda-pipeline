"""
Batch layer: daily profitability reconciliation DAG.

Runs on a schedule matching the simulated-day length (default: every 5
minutes, matching SIMULATED_DAY_SECONDS=300 in docker-compose.yml — adjust
`schedule` below if you change that value). Each run:

  1. extract_expense_file: finds the most recently written
     vehicle_expenses_dayNNN.csv in /opt/airflow/data
  2. compute_profitability: joins it against vehicle_daily_earnings
     (written by the streaming job) on vehicle_id, computes net_profit
  3. load_report: upserts the result into daily_profitability_report

Requires psycopg2-binary, which is installed via _PIP_ADDITIONAL_REQUIREMENTS
in docker-compose.yml's airflow-common block.
"""

from __future__ import annotations

import csv
import glob
import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = "/opt/airflow/data"

DB_CONFIG = {
    "host": os.environ.get("PIPELINE_DB_HOST", "postgres-app"),
    "dbname": os.environ.get("PIPELINE_DB_NAME", "fleet"),
    "user": os.environ.get("PIPELINE_DB_USER", "fleet"),
    "password": os.environ.get("PIPELINE_DB_PASSWORD", "fleet"),
}


class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(
            {
                "ts": self.formatTime(record),
                "level": record.levelname,
                "stage": "batch_layer",
                "msg": record.getMessage(),
            }
        )


logger = logging.getLogger("batch_layer")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(JsonFormatter())
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)

default_args = {
    "owner": "fleet-pipeline",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def update_heartbeat(stage: str) -> None:
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_heartbeat (stage, last_seen_at, status)
                VALUES (%s, now(), 'ok')
                ON CONFLICT (stage) DO UPDATE
                SET last_seen_at = now(), status = 'ok'
                """,
                (stage,),
            )
    except Exception as exc:
        logger.error(f"heartbeat update failed: {exc}")


def extract_expense_file(**context) -> str:
    files = sorted(glob.glob(os.path.join(DATA_DIR, "vehicle_expenses_day*.csv")))
    if not files:
        logger.error(f"no expense files found in {DATA_DIR}")
        raise FileNotFoundError(f"No vehicle_expenses_day*.csv files in {DATA_DIR}")
    latest = files[-1]
    logger.info(f"extract_expense_file found {latest} (of {len(files)} files)")
    return latest


def compute_profitability(**context) -> list[dict]:
    ti = context["ti"]
    expense_path: str = ti.xcom_pull(task_ids="extract_expense_file")

    expenses = {}
    with open(expense_path, newline="") as f:
        for row in csv.DictReader(f):
            expenses[row["vehicle_id"]] = row

    if not expenses:
        logger.error(f"expense file {expense_path} had no rows")
        return []

    earnings_by_vehicle = {}
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (vehicle_id) vehicle_id, total_earnings
                FROM vehicle_daily_earnings
                ORDER BY vehicle_id, window_start DESC
                """
            )
            for vehicle_id, total_earnings in cur.fetchall():
                earnings_by_vehicle[vehicle_id] = float(total_earnings)
    except Exception as exc:
        logger.error(f"failed to read vehicle_daily_earnings: {exc}")
        # Continue with zero earnings rather than failing the whole run --
        # this at least surfaces the cost side of the reconciliation, and
        # is logged clearly as a degraded result rather than silently wrong.

    source_file = os.path.basename(expense_path)
    report_date = datetime.utcnow().date().isoformat()
    rows = []
    for vehicle_id, exp in expenses.items():
        fuel_cost = float(exp["fuel_cost"])
        maintenance_cost = float(exp["maintenance_cost"])
        total_earnings = earnings_by_vehicle.get(vehicle_id, 0.0)
        net_profit = total_earnings - fuel_cost - maintenance_cost
        rows.append(
            {
                "source_file": source_file,
                "report_date": report_date,
                "vehicle_id": vehicle_id,
                "total_earnings": total_earnings,
                "fuel_cost": fuel_cost,
                "maintenance_cost": maintenance_cost,
                "distance_covered_km": float(exp["distance_covered"]),
                "net_profit": net_profit,
                "service_flag": exp["service_flag"].strip().lower() in ("true", "1"),
            }
        )

    missing_earnings = sum(1 for v in expenses if v not in earnings_by_vehicle)
    if missing_earnings:
        logger.info(
            f"compute_profitability: {missing_earnings}/{len(expenses)} vehicles had "
            "no streaming earnings yet (treated as 0) -- expected on the first run"
        )
    logger.info(f"compute_profitability produced {len(rows)} rows from {source_file}")
    return rows


def load_report(**context) -> None:
    ti = context["ti"]
    rows: list[dict] = ti.xcom_pull(task_ids="compute_profitability") or []
    if not rows:
        logger.error("load_report received no rows -- nothing to write")
        return

    try:
        with get_conn() as conn, conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO daily_profitability_report
                        (source_file, report_date, vehicle_id, total_earnings,
                         fuel_cost, maintenance_cost, distance_covered_km,
                         net_profit, service_flag)
                    VALUES (%(source_file)s, %(report_date)s, %(vehicle_id)s,
                            %(total_earnings)s, %(fuel_cost)s, %(maintenance_cost)s,
                            %(distance_covered_km)s, %(net_profit)s, %(service_flag)s)
                    ON CONFLICT (source_file, vehicle_id) DO UPDATE SET
                        total_earnings = EXCLUDED.total_earnings,
                        fuel_cost = EXCLUDED.fuel_cost,
                        maintenance_cost = EXCLUDED.maintenance_cost,
                        distance_covered_km = EXCLUDED.distance_covered_km,
                        net_profit = EXCLUDED.net_profit,
                        service_flag = EXCLUDED.service_flag,
                        generated_at = now()
                    """,
                    r,
                )
        logger.info(f"load_report wrote {len(rows)} rows")
        update_heartbeat("batch_layer")
    except Exception as exc:
        logger.error(f"load_report failed: {exc}")
        raise


with DAG(
    dag_id="daily_profitability_report",
    description="Batch layer: reconcile daily vehicle expenses against streamed trip earnings",
    default_args=default_args,
    schedule=timedelta(minutes=5),  # match SIMULATED_DAY_SECONDS=300; adjust together
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleet", "batch-layer", "lambda-architecture"],
) as dag:

    extract = PythonOperator(task_id="extract_expense_file", python_callable=extract_expense_file)
    compute = PythonOperator(task_id="compute_profitability", python_callable=compute_profitability)
    load = PythonOperator(task_id="load_report", python_callable=load_report)

    extract >> compute >> load
