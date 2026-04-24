# mlb/tests/unit/conftest.py
"""
Stub out airflow + pendulum modules so that plugins and DAG modules can be
imported without those packages installed (e.g. in a bare unit-test env).
Tests that need real Airflow behaviour (e.g. DagBag) import it inside the
test body and require a real Airflow install.
"""
import importlib.util
import sys
from unittest.mock import MagicMock


def _stub_airflow():
    """Insert minimal stubs for airflow into sys.modules."""
    if "airflow" in sys.modules:
        return  # real Airflow is installed — leave it alone

    sys.modules.setdefault("airflow", MagicMock())
    sys.modules.setdefault("airflow.models", MagicMock())
    sys.modules.setdefault("airflow.models.param", MagicMock())
    sys.modules.setdefault("airflow.operators", MagicMock())
    sys.modules.setdefault("airflow.operators.python", MagicMock())
    sys.modules.setdefault("airflow.sensors", MagicMock())
    sys.modules.setdefault("airflow.sensors.external_task", MagicMock())


def _stub_pendulum():
    """Stub pendulum so DAGs can be imported without it installed."""
    if "pendulum" in sys.modules or importlib.util.find_spec("pendulum") is not None:
        return
    pendulum_stub = MagicMock()
    pendulum_stub.datetime = MagicMock(return_value=MagicMock())
    sys.modules.setdefault("pendulum", pendulum_stub)


_stub_airflow()
_stub_pendulum()
