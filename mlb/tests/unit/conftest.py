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


class _SmartDAGMock:
    """A mock DAG factory that captures kwargs and supports context manager protocol."""

    def __call__(self, **kwargs):
        """Create a DAG mock that stores the kwargs as attributes."""
        dag_instance = MagicMock()
        for key, value in kwargs.items():
            setattr(dag_instance, key, value)
        dag_instance.__enter__ = MagicMock(return_value=dag_instance)
        dag_instance.__exit__ = MagicMock(return_value=False)
        return dag_instance


def _stub_airflow():
    """Insert minimal stubs for airflow into sys.modules."""
    if "airflow" in sys.modules:
        return  # real Airflow is installed — leave it alone

    airflow_mock = MagicMock()
    airflow_mock.DAG = _SmartDAGMock()

    sys.modules["airflow"] = airflow_mock
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


def _stub_mlflow():
    """Stub mlflow + mlflow.sklearn so train/score modules can be imported."""
    if "mlflow" in sys.modules:
        return
    mlflow_stub = MagicMock()
    mlflow_sklearn_stub = MagicMock()
    mlflow_tracking_stub = MagicMock()
    sys.modules.setdefault("mlflow", mlflow_stub)
    sys.modules.setdefault("mlflow.sklearn", mlflow_sklearn_stub)
    sys.modules.setdefault("mlflow.tracking", mlflow_tracking_stub)


_stub_airflow()
_stub_pendulum()
_stub_mlflow()
