"""Integration test against the real Postgres from docker compose.

Skipped automatically when Postgres is not reachable on localhost:5432, so the
rest of the suite still runs anywhere. It uses its own source_file name and
cleans up after itself, and it does not touch the pipeline heartbeat.
"""

import psycopg2
import pytest

import daily_profitability_dag as dag

LOCAL_DB = dict(host="localhost", port=5432, dbname="fleet", user="fleet", password="fleet", connect_timeout=2)
TEST_SOURCE_FILE = "pytest_fixture.csv"


def _db_available() -> bool:
    try:
        psycopg2.connect(**LOCAL_DB).close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_available(), reason="Postgres not reachable on localhost:5432 (run docker compose up first)"
)


class FakeTI:
    def __init__(self, rows):
        self._rows = rows

    def xcom_pull(self, task_ids):
        return self._rows


def _row(vehicle_id, earnings):
    return {
        "source_file": TEST_SOURCE_FILE,
        "report_date": "2026-01-01",
        "vehicle_id": vehicle_id,
        "total_earnings": earnings,
        "fuel_cost": 10.0,
        "maintenance_cost": 5.0,
        "distance_covered_km": 42.0,
        "net_profit": earnings - 15.0,
        "service_flag": False,
    }


def _fetch_test_rows():
    with psycopg2.connect(**LOCAL_DB) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT vehicle_id, total_earnings FROM daily_profitability_report "
            "WHERE source_file = %s ORDER BY vehicle_id",
            (TEST_SOURCE_FILE,),
        )
        return cur.fetchall()


@pytest.fixture(autouse=True)
def cleanup(monkeypatch):
    monkeypatch.setattr(dag, "DB_CONFIG", LOCAL_DB)
    monkeypatch.setattr(dag, "update_heartbeat", lambda stage: None)
    yield
    with psycopg2.connect(**LOCAL_DB) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM daily_profitability_report WHERE source_file = %s", (TEST_SOURCE_FILE,))


def test_rerunning_the_load_updates_rows_instead_of_duplicating():
    dag.load_report(ti=FakeTI([_row("vehicle_a", 100.0), _row("vehicle_b", 200.0)]))
    assert len(_fetch_test_rows()) == 2

    # Same day, new earnings: the row count must stay at 2 and values must change.
    dag.load_report(ti=FakeTI([_row("vehicle_a", 150.0), _row("vehicle_b", 250.0)]))
    rows = _fetch_test_rows()
    assert len(rows) == 2
    assert float(rows[0][1]) == pytest.approx(150.0)
    assert float(rows[1][1]) == pytest.approx(250.0)