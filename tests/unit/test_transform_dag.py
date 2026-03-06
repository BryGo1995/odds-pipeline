# tests/unit/test_transform_dag.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


def test_transform_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_transform" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_transform_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_transform"]
    task_ids = [t.task_id for t in dag.tasks]
    assert "transform_events"       in task_ids
    assert "transform_odds"         in task_ids
    assert "transform_scores"       in task_ids
    assert "transform_player_props" in task_ids
