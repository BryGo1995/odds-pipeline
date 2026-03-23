# tests/integration/conftest.py
"""
Stub out airflow and other optional modules so that plugins can be imported
without a full Airflow installation in the local/CI environment.
"""
import sys
from unittest.mock import MagicMock


def _stub_airflow():
    if "airflow" in sys.modules:
        return

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
