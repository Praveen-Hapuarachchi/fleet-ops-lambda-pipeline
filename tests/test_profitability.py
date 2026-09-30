"""Tests for the batch layer (airflow/dags/daily_profitability_dag.py).

Airflow itself is stubbed in conftest.py; the database is replaced by a fake,
so these run anywhere with no services.
"""

import csv
from decimal import Decimal

import pytest

import daily_profitability_dag as dag

CSV_HEADER = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"]


# ------------------------------------------------------------------- helpers
def write_expense_csv(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        writer.writerows(rows)
    return str(path)


class FakeTI:
    """Minimal stand-in for Airflow's task instance (XCom access)."""

    def __init__(self, values):
        self._values = values

    def xcom_pull(self, task_ids):
        return self._values.get(task_ids)


class Store:
    def __init__(self):
        self.executed = []
        self.fetch_rows = []
        self.fail = None


class FakeCursor:
    def __init__(self, store):
        self._store = store

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._store.executed.append((sql, params))

    def fetchall(self):
        return self._store.fetch_rows


class FakeConn:
    def __init__(self, store):
        self._store = store

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return FakeCursor(self._store)


@pytest.fixture
def store(monkeypatch):
    s = Store()

    def fake_get_conn():
        if s.fail is not None:
            raise s.fail
        return FakeConn(s)

    monkeypatch.setattr(dag, "get_conn", fake_get_conn)
    return s


def run_compute(tmp_path, csv_rows, store, earnings=()):
    store.fetch_rows = list(earnings)
    path = write_expense_csv(tmp_path / "vehicle_expenses_day0001.csv", csv_rows)
    return dag.compute_profitability(ti=FakeTI({"extract_expense_file": path}))


# ------------------------------------------------------- extract_expense_file
def test_extract_picks_the_latest_file(monkeypatch, tmp_path):
    monkeypatch.setattr(dag, "DATA_DIR", str(tmp_path))
    for name in ("vehicle_expenses_day0001.csv", "vehicle_expenses_day0003.csv", "vehicle_expenses_day0002.csv"):
        (tmp_path / name).write_text("x")
    latest = dag.extract_expense_file()
    assert latest.endswith("vehicle_expenses_day0003.csv")


def test_extract_ignores_unrelated_files(monkeypatch, tmp_path):
    monkeypatch.setattr(dag, "DATA_DIR", str(tmp_path))
    (tmp_path / "vehicle_expenses_day0001.csv").write_text("x")
    (tmp_path / "zzz_notes.csv").write_text("x")
    assert dag.extract_expense_file().endswith("vehicle_expenses_day0001.csv")


def test_extract_raises_when_no_expense_file_exists(monkeypatch, tmp_path):
    monkeypatch.setattr(dag, "DATA_DIR", str(tmp_path))
    with pytest.raises(FileNotFoundError):
        dag.extract_expense_file()


# ------------------------------------------------------ compute_profitability
def test_net_profit_is_earnings_minus_fuel_minus_maintenance(tmp_path, store):
    rows = run_compute(
        tmp_path,
        [["vehicle_000", "10.5", "4.25", "120.0", "false"]],
        store,
        earnings=[("vehicle_000", 100.0)],
    )
    assert len(rows) == 1
    assert rows[0]["total_earnings"] == pytest.approx(100.0)
    assert rows[0]["net_profit"] == pytest.approx(85.25)


def test_decimal_earnings_from_postgres_are_converted_to_float(tmp_path, store):
    rows = run_compute(
        tmp_path,
        [["vehicle_000", "10", "5", "50", "false"]],
        store,
        earnings=[("vehicle_000", Decimal("200.50"))],
    )
    assert isinstance(rows[0]["total_earnings"], float)
    assert rows[0]["net_profit"] == pytest.approx(185.5)


def test_vehicle_without_streamed_earnings_gets_zero_earnings(tmp_path, store):
    rows = run_compute(tmp_path, [["vehicle_009", "20", "30", "80", "false"]], store, earnings=[])
    assert rows[0]["total_earnings"] == 0.0
    assert rows[0]["net_profit"] == pytest.approx(-50.0)


def test_earnings_for_vehicles_not_in_expense_file_are_ignored(tmp_path, store):
    rows = run_compute(
        tmp_path,
        [["vehicle_000", "1", "1", "1", "false"]],
        store,
        earnings=[("vehicle_000", 10.0), ("vehicle_777", 999.0)],
    )
    assert [r["vehicle_id"] for r in rows] == ["vehicle_000"]


def test_one_output_row_per_expense_row(tmp_path, store):
    rows = run_compute(
        tmp_path,
        [
            ["vehicle_000", "1", "1", "10", "false"],
            ["vehicle_001", "2", "2", "20", "true"],
            ["vehicle_002", "3", "3", "30", "false"],
        ],
        store,
        earnings=[("vehicle_000", 50.0), ("vehicle_001", 60.0), ("vehicle_002", 70.0)],
    )
    assert len(rows) == 3


@pytest.mark.parametrize(
    "raw,expected",
    [("true", True), ("True", True), (" TRUE ", True), ("1", True), ("false", False), ("False", False), ("0", False), ("", False)],
)
def test_service_flag_parsing(tmp_path, store, raw, expected):
    rows = run_compute(tmp_path, [["vehicle_000", "1", "1", "1", raw]], store)
    assert rows[0]["service_flag"] is expected


def test_distance_column_is_mapped_to_distance_covered_km(tmp_path, store):
    rows = run_compute(tmp_path, [["vehicle_000", "1", "1", "98.2", "false"]], store)
    assert rows[0]["distance_covered_km"] == pytest.approx(98.2)


def test_source_file_is_the_basename_only(tmp_path, store):
    rows = run_compute(tmp_path, [["vehicle_000", "1", "1", "1", "false"]], store)
    assert rows[0]["source_file"] == "vehicle_expenses_day0001.csv"


def test_empty_expense_file_produces_no_rows(tmp_path, store):
    assert run_compute(tmp_path, [], store) == []


# ---------------------------------------------------------------- load_report
def sample_row(vehicle_id="vehicle_000", earnings=100.0):
    return {
        "source_file": "vehicle_expenses_day0001.csv",
        "report_date": "2026-09-30",
        "vehicle_id": vehicle_id,
        "total_earnings": earnings,
        "fuel_cost": 10.0,
        "maintenance_cost": 5.0,
        "distance_covered_km": 50.0,
        "net_profit": earnings - 15.0,
        "service_flag": False,
    }


@pytest.fixture
def heartbeats(monkeypatch):
    calls = []
    monkeypatch.setattr(dag, "update_heartbeat", lambda stage: calls.append(stage))
    return calls


def test_load_writes_one_upsert_per_row_and_updates_heartbeat(store, heartbeats):
    rows = [sample_row("vehicle_000"), sample_row("vehicle_001")]
    dag.load_report(ti=FakeTI({"compute_profitability": rows}))
    assert len(store.executed) == 2
    assert heartbeats == ["batch_layer"]


def test_load_is_an_upsert_keyed_on_source_file_and_vehicle(store, heartbeats):
    """Re-running the DAG for the same day updates rows instead of duplicating."""
    dag.load_report(ti=FakeTI({"compute_profitability": [sample_row()]}))
    sql, _params = store.executed[0]
    assert "ON CONFLICT (source_file, vehicle_id) DO UPDATE" in sql


def test_load_with_no_rows_writes_nothing_and_skips_heartbeat(store, heartbeats):
    dag.load_report(ti=FakeTI({"compute_profitability": []}))
    assert store.executed == []
    assert heartbeats == []


def test_load_failure_is_raised_and_heartbeat_is_not_updated(store, heartbeats):
    store.fail = RuntimeError("db down")
    with pytest.raises(RuntimeError):
        dag.load_report(ti=FakeTI({"compute_profitability": [sample_row()]}))
    assert heartbeats == []