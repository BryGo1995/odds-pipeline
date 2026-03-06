# tests/unit/test_ingest_dag.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


def test_ingest_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_ingest" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_ingest_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_ingest"]
    task_ids = [t.task_id for t in dag.tasks]
    assert "fetch_events" in task_ids
    assert "fetch_odds" in task_ids
    assert "fetch_scores" in task_ids
