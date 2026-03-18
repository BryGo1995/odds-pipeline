# tests/unit/test_player_props_dag.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


def test_player_props_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_player_props" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_player_props_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_player_props"]
    task_ids = [t.task_id for t in dag.tasks]
    assert "fetch_player_props" in task_ids
    assert "transform_player_props" in task_ids


def test_player_props_dag_task_order():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_player_props"]
    fetch_task = dag.get_task("fetch_player_props")
    assert "transform_player_props" in [t.task_id for t in fetch_task.downstream_list]


def test_player_props_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from plugins.slack_notifier import notify_success, notify_failure
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_player_props"]
    assert dag.on_success_callback is notify_success
    assert dag.on_failure_callback is notify_failure
