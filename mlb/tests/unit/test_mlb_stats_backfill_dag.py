# mlb/tests/unit/test_mlb_stats_backfill_dag.py
"""Structural + behavior tests for mlb_stats_backfill DAG.

The DagBag-based tests require real Airflow installed. The run_backfill
validation tests can run without it (function-level).
"""
from unittest.mock import patch

import pytest


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="mlb/dags/", include_examples=False)
    assert "mlb_stats_backfill" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_no_schedule():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_backfill"]
    assert dag.schedule_interval is None


def test_run_backfill_rejects_bad_season_start_format():
    from mlb.dags.mlb_stats_backfill_dag import run_backfill
    with pytest.raises(ValueError, match="season_start"):
        run_backfill(params={"season_start": "not-a-year", "season_end": "2025"})


def test_run_backfill_rejects_bad_season_end_format():
    from mlb.dags.mlb_stats_backfill_dag import run_backfill
    with pytest.raises(ValueError, match="season_end"):
        run_backfill(params={"season_start": "2023", "season_end": "25-26"})


def test_run_backfill_rejects_end_before_start():
    from mlb.dags.mlb_stats_backfill_dag import run_backfill
    with pytest.raises(ValueError, match="must be <="):
        run_backfill(params={"season_start": "2025", "season_end": "2023"})


def test_run_backfill_iterates_seasons_and_calls_fetches():
    """3 seasons → fetch_teams called once, fetch_players + fetch_batter_game_logs
    called 3 times each with correct year-stamped date windows."""
    from mlb.dags import mlb_stats_backfill_dag as mod

    with patch.object(mod, "get_data_db_conn") as mock_get_conn, \
         patch.object(mod, "store_raw_response"), \
         patch.object(mod, "fetch_teams", return_value=[]) as m_teams, \
         patch.object(mod, "fetch_players", return_value=[]) as m_players, \
         patch.object(mod, "fetch_batter_game_logs", return_value=[]) as m_pgl, \
         patch.object(mod, "transform_teams"), \
         patch.object(mod, "transform_players"), \
         patch.object(mod, "transform_player_game_logs"), \
         patch.object(mod, "resolve_player_ids"), \
         patch.object(mod, "time"):
        mock_get_conn.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = None
        mod.run_backfill(params={"season_start": "2023", "season_end": "2025"})

    assert m_teams.call_count == 1
    assert m_players.call_count == 3
    assert m_pgl.call_count == 3

    # Each fetch_batter_game_logs call pins its dates to the season year
    years_called = sorted([call.args[0][:4] for call in m_pgl.call_args_list])
    assert years_called == ["2023", "2024", "2025"]
    # Start/end month-day should be 03-15 → 11-15
    for call in m_pgl.call_args_list:
        start, end = call.args[0], call.args[1]
        assert start.endswith("-03-15")
        assert end.endswith("-11-15")
