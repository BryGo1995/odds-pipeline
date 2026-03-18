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


def test_ingest_dag_has_success_and_failure_callbacks():
    from airflow.models import DagBag
    from plugins.slack_notifier import notify_success, notify_failure
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_ingest"]
    assert dag.on_success_callback is notify_success
    assert dag.on_failure_callback is notify_failure


def test_fetch_and_store_pushes_quota_to_xcom():
    from unittest.mock import MagicMock, patch
    from dags.ingest_dag import _fetch_and_store

    ti = MagicMock()
    mock_fetch = MagicMock(return_value=({"event": "data"}, 350))
    mock_conn = MagicMock()

    with patch("dags.ingest_dag.get_data_db_conn", return_value=mock_conn), \
         patch.dict("os.environ", {"ODDS_API_KEY": "test_key"}):
        _fetch_and_store("events", mock_fetch, {"sport": "basketball_nba"}, ti)

    ti.xcom_push.assert_called_once_with(key="api_remaining", value=350)
