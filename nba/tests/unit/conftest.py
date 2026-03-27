# tests/unit/conftest.py
"""
Stub out airflow modules so that plugins can be imported without a full
Airflow installation in the local/CI environment.  Tests that need real
Airflow behaviour (e.g. DagBag) import it inside the test body and will
fail gracefully when Airflow is not present.
"""
import sys
from unittest.mock import MagicMock


def _stub_airflow():
    """Insert minimal stubs for airflow into sys.modules."""
    if "airflow" in sys.modules:
        return  # real Airflow is installed — leave it alone

    airflow_stub = MagicMock()
    airflow_models_stub = MagicMock()
    airflow_models_param_stub = MagicMock()
    airflow_sensors_stub = MagicMock()

    sys.modules.setdefault("airflow", airflow_stub)
    sys.modules.setdefault("airflow.models", airflow_models_stub)
    sys.modules.setdefault("airflow.models.param", airflow_models_param_stub)
    sys.modules.setdefault("airflow.operators", MagicMock())
    sys.modules.setdefault("airflow.operators.python", MagicMock())
    sys.modules.setdefault("airflow.sensors", airflow_sensors_stub)
    sys.modules.setdefault("airflow.sensors.external_task", MagicMock())


_stub_airflow()


def _stub_nba_api():
    """Stub nba_api so unit tests don't require the package to be installed."""
    if "nba_api" in sys.modules:
        return
    nba_api_stub = MagicMock()
    http_stub = MagicMock()
    http_stub.STATS_HEADERS = {}
    sys.modules.setdefault("nba_api", nba_api_stub)
    sys.modules.setdefault("nba_api.stats", nba_api_stub)
    sys.modules.setdefault("nba_api.stats.endpoints", nba_api_stub)
    sys.modules.setdefault("nba_api.stats.static", nba_api_stub)
    sys.modules.setdefault("nba_api.stats.library", nba_api_stub)
    sys.modules.setdefault("nba_api.stats.library.http", http_stub)


def _stub_pendulum():
    """Stub pendulum so DAGs can be imported without it being installed."""
    import importlib.util
    if "pendulum" in sys.modules or importlib.util.find_spec("pendulum") is not None:
        return
    pendulum_stub = MagicMock()
    # Mock the datetime function to return a MagicMock that behaves like a datetime
    pendulum_stub.datetime = MagicMock(return_value=MagicMock())
    sys.modules.setdefault("pendulum", pendulum_stub)


_stub_nba_api()
_stub_pendulum()
