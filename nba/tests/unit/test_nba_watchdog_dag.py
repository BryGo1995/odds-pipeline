from datetime import date
from unittest.mock import MagicMock, patch


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="nba/dags/", include_examples=False)
    assert "nba_watchdog_dag" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_no_sensors():
    # Must run even when the daily chain is broken or paused (#51)
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_watchdog_dag"]
    assert {t.task_id for t in dag.tasks} == {"check_recommendations"}


def _run_check(missed, last_rec_date=None):
    import pendulum
    from nba.dags.nba_watchdog_dag import check_recommendations
    ctx = {"data_interval_end": pendulum.datetime(2026, 11, 7, 0, 30, tz="UTC")}
    with patch("nba.dags.nba_watchdog_dag.get_data_db_conn", return_value=MagicMock()), \
         patch("nba.dags.nba_watchdog_dag.find_missed_slates",
               return_value=(missed, last_rec_date)) as mock_find, \
         patch("nba.dags.nba_watchdog_dag.notify_missed_recommendations") as mock_notify:
        check_recommendations(**ctx)
    return mock_find, mock_notify


def test_alerts_when_slates_missed():
    mock_find, mock_notify = _run_check([date(2026, 11, 6)], date(2026, 10, 27))
    # 00:30 UTC Nov 7 is still the Nov 6 slate in ET
    assert mock_find.call_args.args[1:] == ("basketball_nba", "NBA", "2026-11-06")
    mock_notify.assert_called_once()


def test_silent_when_current():
    _, mock_notify = _run_check([])
    mock_notify.assert_not_called()
