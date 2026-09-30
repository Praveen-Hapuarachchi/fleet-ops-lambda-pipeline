"""Tests for the serving layer (serving/main.py). No database required."""

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

import main


@pytest.fixture
def client(fake_db, monkeypatch):
    monkeypatch.setattr(main, "db_cursor", fake_db.cursor)
    return TestClient(main.app)


# ---------------------------------------------------------------- /fleet/live
def test_live_returns_rows_and_count(client, fake_db):
    fake_db.rows = [
        {"zone": "central", "active_vehicles": 25, "idle_vehicles": 18},
        {"zone": "north", "active_vehicles": 27, "idle_vehicles": 10},
    ]
    resp = client.get("/fleet/live")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    assert body["metrics"][0]["zone"] == "central"


def test_live_passes_zone_and_limit_to_query(client, fake_db):
    client.get("/fleet/live?zone=central&limit=5")
    _sql, params = fake_db.calls[-1]
    assert params == ("central", "central", 5)


def test_live_defaults_to_no_zone_filter_and_limit_50(client, fake_db):
    client.get("/fleet/live")
    _sql, params = fake_db.calls[-1]
    assert params == (None, None, 50)


@pytest.mark.parametrize("limit", [0, 1001, -3])
def test_live_rejects_out_of_range_limit(client, limit):
    assert client.get(f"/fleet/live?limit={limit}").status_code == 422


def test_live_returns_503_when_database_is_down(client, fake_db):
    fake_db.error = RuntimeError("connection refused")
    resp = client.get("/fleet/live")
    assert resp.status_code == 503
    assert resp.json()["error"] == "query failed"


# ----------------------------------------------------- /fleet/profitability
def test_profitability_returns_rows(client, fake_db):
    fake_db.rows = [{"vehicle_id": "vehicle_005", "net_profit": 1688.9}]
    resp = client.get("/fleet/profitability")
    assert resp.status_code == 200
    assert resp.json() == {"count": 1, "report": fake_db.rows}


def test_profitability_defaults_to_latest_day(client, fake_db):
    client.get("/fleet/profitability")
    _sql, params = fake_db.calls[-1]
    assert params == (None, 100)


def test_profitability_can_select_a_specific_day(client, fake_db):
    client.get("/fleet/profitability?source_file=vehicle_expenses_day1.csv&limit=10")
    _sql, params = fake_db.calls[-1]
    assert params == ("vehicle_expenses_day1.csv", 10)


def test_profitability_returns_503_when_database_is_down(client, fake_db):
    fake_db.error = RuntimeError("connection refused")
    assert client.get("/fleet/profitability").status_code == 503


# -------------------------------------------------------------------- /health
def heartbeat(stage, age_seconds):
    return {
        "stage": stage,
        "last_seen_at": datetime(2026, 9, 30, 5, 0, tzinfo=timezone.utc),
        "seconds_since_seen": age_seconds,
    }


def checks_by_stage(resp):
    return {c["stage"]: c for c in resp.json()["checks"]}


def test_health_ok_when_all_stages_are_fresh(client, fake_db):
    fake_db.rows = [
        heartbeat("speed_layer_zone_metrics", 10),
        heartbeat("speed_layer_vehicle_earnings", 12),
        heartbeat("batch_layer", 50),
    ]
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert all(c["status"] == "ok" for c in resp.json()["checks"])


def test_health_degraded_when_a_speed_stage_goes_quiet(client, fake_db):
    """The assignment's 'no data received in N minutes' alert rule."""
    fake_db.rows = [
        heartbeat("speed_layer_zone_metrics", 300),  # > 120 s threshold
        heartbeat("speed_layer_vehicle_earnings", 12),
        heartbeat("batch_layer", 50),
    ]
    resp = client.get("/health")
    assert resp.json()["status"] == "degraded"
    assert checks_by_stage(resp)["speed_layer_zone_metrics"]["status"] == "stale"
    assert checks_by_stage(resp)["speed_layer_vehicle_earnings"]["status"] == "ok"


def test_health_batch_layer_uses_its_own_longer_threshold(client, fake_db):
    # 400 s is stale for a streaming stage but fine for the 5-minute batch DAG.
    fake_db.rows = [
        heartbeat("speed_layer_zone_metrics", 10),
        heartbeat("speed_layer_vehicle_earnings", 10),
        heartbeat("batch_layer", 400),
    ]
    resp = client.get("/health")
    assert resp.json()["status"] == "ok"
    assert checks_by_stage(resp)["batch_layer"]["threshold_seconds"] == main.BATCH_STALE_SECONDS


def test_health_batch_layer_goes_stale_past_its_threshold(client, fake_db):
    fake_db.rows = [
        heartbeat("speed_layer_zone_metrics", 10),
        heartbeat("speed_layer_vehicle_earnings", 10),
        heartbeat("batch_layer", main.BATCH_STALE_SECONDS + 100),
    ]
    resp = client.get("/health")
    assert resp.json()["status"] == "degraded"
    assert checks_by_stage(resp)["batch_layer"]["status"] == "stale"


def test_health_reports_never_seen_stage_as_missing(client, fake_db):
    fake_db.rows = [
        heartbeat("speed_layer_zone_metrics", 10),
        heartbeat("speed_layer_vehicle_earnings", 10),
        # batch_layer has never reported
    ]
    resp = client.get("/health")
    assert resp.json()["status"] == "degraded"
    batch = checks_by_stage(resp)["batch_layer"]
    assert batch["status"] == "missing"
    assert batch["last_seen_at"] is None


def test_health_all_missing_on_empty_heartbeat_table(client, fake_db):
    fake_db.rows = []
    resp = client.get("/health")
    assert resp.json()["status"] == "degraded"
    assert {c["status"] for c in resp.json()["checks"]} == {"missing"}


def test_health_includes_unexpected_extra_stages(client, fake_db):
    fake_db.rows = [
        heartbeat("speed_layer_zone_metrics", 10),
        heartbeat("speed_layer_vehicle_earnings", 10),
        heartbeat("batch_layer", 10),
        heartbeat("some_new_stage", 10),
    ]
    assert "some_new_stage" in checks_by_stage(client.get("/health"))


def test_health_returns_503_unknown_when_database_is_down(client, fake_db):
    fake_db.error = RuntimeError("connection refused")
    resp = client.get("/health")
    assert resp.status_code == 503
    assert resp.json()["status"] == "unknown"


# ------------------------------------------------------------------ dashboard
def test_dashboard_is_served_at_root(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    assert "Fleet operations" in resp.text


def test_dashboard_missing_file_returns_404(client, monkeypatch, tmp_path):
    monkeypatch.setattr(main, "DASHBOARD_PATH", tmp_path / "nope.html")
    assert client.get("/").status_code == 404