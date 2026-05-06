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


class _SmartOperatorMock:
    """Captures task kwargs and registers the resulting mock with the active DAG."""

    def __call__(self, *args, **kwargs):
        op = MagicMock()
        for k, v in kwargs.items():
            setattr(op, k, v)
        op.upstream_list = []
        op.downstream_list = []

        def _rshift(other):
            others = other if isinstance(other, list) else [other]
            for o in others:
                op.downstream_list.append(o)
                o.upstream_list.append(op)
            return other

        op.__rshift__ = MagicMock(side_effect=_rshift)
        if _SmartDAGMock._current is not None:
            _SmartDAGMock._current.tasks.append(op)
        return op


class _SmartDAGMock:
    """A mock DAG factory that captures kwargs, supports context manager protocol,
    and tracks tasks registered inside the ``with dag:`` block."""

    _current = None  # class-level pointer to the currently active DAG

    def __call__(self, *args, **kwargs):
        """Create a DAG mock that stores the kwargs as attributes.

        Accepts dag_id as the first positional argument (mirroring real
        airflow.DAG) or as a keyword argument.
        """
        if args:
            kwargs.setdefault("dag_id", args[0])
        dag_instance = MagicMock()
        for key, value in kwargs.items():
            setattr(dag_instance, key, value)
        dag_instance.tasks = []
        dag_instance.get_task = lambda task_id: next(
            (t for t in dag_instance.tasks if t.task_id == task_id),
            MagicMock(task_id=task_id, upstream_list=[], downstream_list=[]),
        )

        def _enter(*_):
            _SmartDAGMock._current = dag_instance
            return dag_instance

        def _exit(*_, **__):
            _SmartDAGMock._current = None
            return False

        dag_instance.__enter__ = MagicMock(side_effect=_enter)
        dag_instance.__exit__ = MagicMock(side_effect=_exit)
        return dag_instance


def _stub_airflow():
    """Insert minimal stubs for airflow into sys.modules.

    If another conftest already stubbed airflow with a plain MagicMock,
    upgrade the .DAG attribute to _SmartDAGMock so MLB DAG tests can
    introspect dag_id, tags, schedule_interval, etc.

    If real Airflow is installed (not a MagicMock), leave it alone.
    """
    existing = sys.modules.get("airflow")

    if existing is not None and not isinstance(existing, MagicMock):
        return  # real Airflow is installed — leave it alone

    if existing is None:
        airflow_mock = MagicMock()
        sys.modules["airflow"] = airflow_mock
        sys.modules.setdefault("airflow.models", MagicMock())
        sys.modules.setdefault("airflow.models.param", MagicMock())
        sys.modules.setdefault("airflow.operators", MagicMock())
        sys.modules.setdefault("airflow.operators.python", MagicMock())
        sys.modules.setdefault("airflow.sensors", MagicMock())
        sys.modules.setdefault("airflow.sensors.external_task", MagicMock())
    else:
        airflow_mock = existing

    # Upgrade .DAG to _SmartDAGMock if not already set
    if not isinstance(airflow_mock.DAG, _SmartDAGMock):
        airflow_mock.DAG = _SmartDAGMock()

    # Install task-registering operator/sensor stubs so tasks are tracked
    sys.modules["airflow.operators.python"].PythonOperator = _SmartOperatorMock()
    sys.modules["airflow.sensors.external_task"].ExternalTaskSensor = _SmartOperatorMock()


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
