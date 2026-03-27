# tests/unit/test_nba_odds_pipeline_dag.py


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="nba/dags/", include_examples=False)
    assert "nba_odds_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_odds_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "fetch_events", "fetch_odds", "fetch_scores",
        "transform_events", "transform_odds", "transform_scores",
        "fetch_player_props", "transform_player_props",
    }


def test_dag_task_chain():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_odds_pipeline"]

    # All three fetch tasks feed transform_events
    for task_id in ["fetch_events", "fetch_odds", "fetch_scores"]:
        downstream = [t.task_id for t in dag.get_task(task_id).downstream_list]
        assert "transform_events" in downstream, f"{task_id} must feed transform_events"

    # transform_events feeds transform_odds, transform_scores, and fetch_player_props
    xform_events_downstream = [t.task_id for t in dag.get_task("transform_events").downstream_list]
    assert "transform_odds" in xform_events_downstream
    assert "transform_scores" in xform_events_downstream
    assert "fetch_player_props" in xform_events_downstream

    # fetch_player_props feeds transform_player_props
    assert "transform_player_props" in [
        t.task_id for t in dag.get_task("fetch_player_props").downstream_list
    ]


def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_failure
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_odds_pipeline"]
    assert dag.on_success_callback is None
    assert dag.on_failure_callback is notify_failure


def test_fetch_player_props_skips_when_no_games():
    from unittest.mock import MagicMock, patch
    from nba.dags.nba_odds_pipeline_dag import fetch_player_props_task

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = []  # no games
    mock_conn.cursor.return_value = mock_cursor
    ti = MagicMock()

    with patch("nba.dags.nba_odds_pipeline_dag.get_data_db_conn", return_value=mock_conn), \
         patch.dict("os.environ", {"ODDS_API_KEY": "test_key"}):
        fetch_player_props_task(ti=ti)

    ti.xcom_push.assert_called_once_with(key="skipped", value=True)


def test_fetch_player_props_pushes_api_remaining_when_games_exist():
    from unittest.mock import MagicMock, patch, call
    from nba.dags.nba_odds_pipeline_dag import fetch_player_props_task

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = [("game-abc",)]
    mock_conn.cursor.return_value = mock_cursor
    ti = MagicMock()

    fake_props = {"markets": []}
    with patch("nba.dags.nba_odds_pipeline_dag.get_data_db_conn", return_value=mock_conn), \
         patch.dict("os.environ", {"ODDS_API_KEY": "test_key"}), \
         patch("nba.dags.nba_odds_pipeline_dag.fetch_player_props",
               return_value=(fake_props, 400)) as mock_fetch, \
         patch("nba.dags.nba_odds_pipeline_dag.store_raw_response"):
        fetch_player_props_task(ti=ti)

    ti.xcom_push.assert_called_once_with(key="api_remaining", value=400)


def test_transform_player_props_skips_when_fetch_skipped():
    from unittest.mock import MagicMock, patch
    from nba.dags.nba_odds_pipeline_dag import transform_player_props_task

    ti = MagicMock()
    ti.xcom_pull.return_value = True  # fetch was skipped

    mock_conn = MagicMock()
    with patch("nba.dags.nba_odds_pipeline_dag.get_data_db_conn", return_value=mock_conn):
        transform_player_props_task(ti=ti)

    # DB should not be touched when skipped
    mock_conn.cursor.assert_not_called()


def test_transform_player_props_runs_when_not_skipped():
    from unittest.mock import MagicMock, patch, call
    from nba.dags.nba_odds_pipeline_dag import transform_player_props_task

    ti = MagicMock()
    ti.xcom_pull.return_value = None  # not skipped

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = ([{"props": "data"}],)
    mock_conn.cursor.return_value = mock_cursor

    with patch("nba.dags.nba_odds_pipeline_dag.get_data_db_conn", return_value=mock_conn), \
         patch("nba.dags.nba_odds_pipeline_dag.transform_player_props") as mock_transform:
        transform_player_props_task(ti=ti)

    mock_transform.assert_called_once()
