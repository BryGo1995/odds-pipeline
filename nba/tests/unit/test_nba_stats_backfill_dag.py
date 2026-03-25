# tests/unit/test_nba_stats_backfill_dag.py


def test_stats_backfill_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="nba/dags/", include_examples=False)
    assert "nba_stats_backfill" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_stats_backfill_dag_has_no_schedule():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_stats_backfill"]
    assert dag.schedule_interval is None
