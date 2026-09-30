"""
Shared test setup.

1. Airflow stub: the DAG module imports `airflow` at the top and builds a DAG
   at import time. Installing Airflow just to unit-test the profit logic is
   heavy (and painful on Windows), so if Airflow is not installed we register
   a tiny stand-in. The functions under test are plain Python and do not need
   the real scheduler.

2. fake_db fixture: replaces serving/main.py's `db_cursor` so API tests run
   without Postgres.
"""

import sys
import types
from contextlib import contextmanager

import pytest


def _install_airflow_stub() -> None:
    class DAG:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class PythonOperator:
        def __init__(self, *args, **kwargs):
            pass

        def __rshift__(self, other):  # supports `extract >> compute >> load`
            return other

    airflow = types.ModuleType("airflow")
    airflow.DAG = DAG
    operators = types.ModuleType("airflow.operators")
    python_ops = types.ModuleType("airflow.operators.python")
    python_ops.PythonOperator = PythonOperator
    sys.modules["airflow"] = airflow
    sys.modules["airflow.operators"] = operators
    sys.modules["airflow.operators.python"] = python_ops


# Note: the repo has its own top-level `airflow/` folder, which Python can
# mistake for the real package. Importing DAG (not just `airflow`) tells the
# two apart.
try:
    from airflow import DAG  # noqa: F401
except ImportError:
    _install_airflow_stub()


class FakeCursor:
    def __init__(self, db):
        self._db = db

    def execute(self, sql, params=None):
        self._db.calls.append((sql, params))

    def fetchall(self):
        return self._db.rows


class FakeDB:
    """Stands in for serving.main.db_cursor. Set .rows or .error per test."""

    def __init__(self):
        self.rows = []
        self.calls = []
        self.error = None

    @contextmanager
    def cursor(self):
        if self.error is not None:
            raise self.error
        yield FakeCursor(self)


@pytest.fixture
def fake_db():
    return FakeDB()