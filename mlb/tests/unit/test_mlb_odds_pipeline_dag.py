import importlib
import pytest


@pytest.fixture(scope="module")
def dag():
    module = importlib.import_module("mlb.dags.mlb_odds_pipeline_dag")
    return module.dag


def test_dag_id(dag):
    assert dag.dag_id == "mlb_odds_pipeline"


def test_dag_schedule_and_tags(dag):
    assert dag.schedule_interval == "0 15 * * *"  # 8am MT
    assert "mlb" in dag.tags
    assert "odds" in dag.tags


def test_expected_task_ids(dag):
    expected = {
        "fetch_events",
        "fetch_odds",
        "fetch_scores",
        "transform_events",
        "transform_odds",
        "transform_scores",
        "fetch_player_props",
        "transform_player_props",
    }
    assert set(dag.task_dict.keys()) == expected


def test_fetch_props_depends_on_transform_events(dag):
    fetch_props = dag.get_task("fetch_player_props")
    upstream_ids = {t.task_id for t in fetch_props.upstream_list}
    assert "transform_events" in upstream_ids


def test_transform_props_depends_on_fetch_props(dag):
    transform_props = dag.get_task("transform_player_props")
    upstream_ids = {t.task_id for t in transform_props.upstream_list}
    assert "fetch_player_props" in upstream_ids
