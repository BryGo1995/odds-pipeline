import importlib
import pytest


@pytest.fixture(scope="module")
def dag():
    module = importlib.import_module("mlb.dags.mlb_odds_backfill_dag")
    return module.dag


def test_dag_id(dag):
    assert dag.dag_id == "mlb_odds_backfill"


def test_manual_schedule_only(dag):
    assert dag.schedule_interval is None


def test_tags(dag):
    assert "mlb" in dag.tags
    assert "backfill" in dag.tags


def test_params_shape(dag):
    assert "date_from" in dag.params
    assert "date_to" in dag.params


def test_single_task(dag):
    assert list(dag.task_dict.keys()) == ["run_backfill"]
