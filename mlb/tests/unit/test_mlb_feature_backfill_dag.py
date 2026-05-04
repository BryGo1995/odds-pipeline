# mlb/tests/unit/test_mlb_feature_backfill_dag.py
"""Structural tests for mlb_feature_backfill DAG. Requires real Airflow."""


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="mlb/dags/", include_examples=False)
    assert "mlb_feature_backfill" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_feature_backfill"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"run_backfill"}


def test_dag_is_manual_trigger_only():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_feature_backfill"]
    assert dag.schedule_interval is None


def test_dag_has_required_params():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_feature_backfill"]
    assert "date_from" in dag.params
    assert "date_to" in dag.params
