"""Unit tests for mlb_train_dag.py — structural assertions only.

DagBag tests are intentionally omitted: a known Python 3.14 / SQLAlchemy bug
breaks DagBag instantiation across all NBA + MLB DAGs. The structural tests
below import the module directly and assert on the DAG object.
"""
from unittest.mock import MagicMock, patch


def test_train_dag_is_registered_with_mlb_tags():
    import mlb.dags.mlb_train_dag as mod
    assert mod.dag.dag_id == "mlb_train_dag"
    assert "mlb" in mod.dag.tags
    assert "ml" in mod.dag.tags


def test_train_dag_schedule_is_monday_3am_mt():
    import mlb.dags.mlb_train_dag as mod
    assert mod.dag.schedule_interval == "0 10 * * 1"


def test_train_dag_uses_sport_aware_callbacks():
    import mlb.dags.mlb_train_dag as mod
    from shared.plugins.slack_notifier import notify_failure, notify_model_ready
    assert mod.dag.on_failure_callback is notify_failure
    assert mod.dag.on_success_callback is notify_model_ready


def test_run_train_model_pushes_run_ids_to_xcom():
    from mlb.dags.mlb_train_dag import run_train_model
    ctx = {"ti": MagicMock()}
    with patch("mlb.dags.mlb_train_dag.train_all_models", return_value={"batter_hits": "run-1"}):
        run_train_model(**ctx)
    ctx["ti"].xcom_push.assert_called_once_with(key="mlflow_run_ids", value={"batter_hits": "run-1"})
