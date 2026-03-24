# tests/unit/test_nba_api_client.py
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd


def _mock_endpoint(df):
    """Build a mock nba_api endpoint object that returns `df` from get_data_frames()[0]."""
    mock = MagicMock()
    mock.get_data_frames.return_value = [df]
    return mock


def test_fetch_players_returns_list_of_dicts():
    from plugins.nba_api_client import fetch_players
    df = pd.DataFrame([{
        "PERSON_ID": 2544, "DISPLAY_FIRST_LAST": "LeBron James",
        "TEAM_ID": 1610612747, "TEAM_ABBREVIATION": "LAL",
        "ROSTERSTATUS": 1, "POSITION": "F",
    }])
    with patch("plugins.nba_api_client.CommonAllPlayers") as mock_cls:
        mock_cls.return_value = _mock_endpoint(df)
        result = fetch_players(delay_seconds=0)
    assert len(result) == 1
    assert result[0]["player_id"] == 2544
    assert result[0]["full_name"] == "LeBron James"
    assert result[0]["is_active"] is True


def test_fetch_players_filters_inactive():
    from plugins.nba_api_client import fetch_players
    df = pd.DataFrame([
        {"PERSON_ID": 1, "DISPLAY_FIRST_LAST": "Active Player",
         "TEAM_ID": 1, "TEAM_ABBREVIATION": "LAL", "ROSTERSTATUS": 1, "POSITION": "G"},
        {"PERSON_ID": 2, "DISPLAY_FIRST_LAST": "Inactive Player",
         "TEAM_ID": 0, "TEAM_ABBREVIATION": "", "ROSTERSTATUS": 0, "POSITION": ""},
    ])
    with patch("plugins.nba_api_client.CommonAllPlayers") as mock_cls:
        mock_cls.return_value = _mock_endpoint(df)
        result = fetch_players(delay_seconds=0)
    assert len(result) == 1
    assert result[0]["player_id"] == 1


def test_fetch_player_game_logs_merges_base_and_advanced():
    from plugins.nba_api_client import fetch_player_game_logs
    base_df = pd.DataFrame([{
        "PLAYER_ID": 2544, "PLAYER_NAME": "LeBron James", "GAME_ID": "0022400001",
        "GAME_DATE": "2025-01-15", "MATCHUP": "LAL vs. DEN", "TEAM_ID": 1610612747,
        "WL": "W", "MIN": 34.5, "FGA": 14, "FTA": 6, "PTS": 28,
        "REB": 8, "AST": 10, "BLK": 1, "STL": 2, "PLUS_MINUS": 12,
    }])
    adv_df = pd.DataFrame([{
        "PLAYER_ID": 2544, "GAME_ID": "0022400001", "USG_PCT": 0.312,
    }])
    with patch("plugins.nba_api_client.PlayerGameLogs") as mock_cls:
        mock_cls.side_effect = [_mock_endpoint(base_df), _mock_endpoint(adv_df)]
        result = fetch_player_game_logs(season="2024-25", delay_seconds=0)
    assert len(result) == 1
    row = result[0]
    assert row["player_id"] == 2544
    assert row["nba_game_id"] == "0022400001"
    assert row["min"] == 34.5
    assert row["usg_pct"] == pytest.approx(0.312)


def test_fetch_team_game_logs_returns_list_of_dicts():
    from plugins.nba_api_client import fetch_team_game_logs
    df = pd.DataFrame([{
        "TEAM_ID": 1610612743, "TEAM_ABBREVIATION": "DEN", "GAME_ID": "0022400001",
        "GAME_DATE": "2025-01-15", "MATCHUP": "DEN @ LAL", "WL": "L",
        "PTS": 110, "PLUS_MINUS": -12,
    }])
    with patch("plugins.nba_api_client.LeagueGameLog") as mock_cls:
        mock_cls.return_value = _mock_endpoint(df)
        result = fetch_team_game_logs(season="2024-25", delay_seconds=0)
    assert len(result) == 1
    assert result[0]["team_abbreviation"] == "DEN"
    assert result[0]["plus_minus"] == -12


def test_fetch_team_season_stats_merges_advanced_and_opponent():
    from plugins.nba_api_client import fetch_team_season_stats
    adv_df = pd.DataFrame([{
        "TEAM_ID": 1610612743, "TEAM_NAME": "Denver Nuggets",
        "PACE": 98.5, "OFF_RATING": 115.2, "DEF_RATING": 110.8,
    }])
    opp_df = pd.DataFrame([{
        "TEAM_ID": 1610612743, "TEAM_NAME": "Denver Nuggets", "OPP_PTS": 42.1,
    }])
    with patch("plugins.nba_api_client.LeagueDashTeamStats") as mock_cls:
        mock_cls.side_effect = [_mock_endpoint(adv_df), _mock_endpoint(opp_df)]
        result = fetch_team_season_stats(season="2024-25", delay_seconds=0)
    assert len(result) == 1
    assert result[0]["pace"] == pytest.approx(98.5)
    assert result[0]["opp_pts_pg"] == pytest.approx(42.1)


def test_fetch_player_game_logs_retries_on_429():
    import requests
    from plugins.nba_api_client import fetch_player_game_logs
    base_df = pd.DataFrame([{
        "PLAYER_ID": 1, "PLAYER_NAME": "P", "GAME_ID": "G1",
        "GAME_DATE": "2025-01-01", "MATCHUP": "A vs. B", "TEAM_ID": 1,
        "WL": "W", "MIN": 30.0, "FGA": 10, "FTA": 3, "PTS": 20,
        "REB": 5, "AST": 5, "BLK": 0, "STL": 1, "PLUS_MINUS": 5,
    }])
    adv_df = pd.DataFrame([{"PLAYER_ID": 1, "GAME_ID": "G1", "USG_PCT": 0.25}])
    err = requests.exceptions.HTTPError(response=MagicMock(status_code=429))
    with patch("plugins.nba_api_client.PlayerGameLogs") as mock_cls, \
         patch("plugins.nba_api_client.time.sleep") as mock_sleep:
        mock_cls.side_effect = [err, _mock_endpoint(base_df), _mock_endpoint(adv_df)]
        result = fetch_player_game_logs(season="2024-25", delay_seconds=0)
    mock_sleep.assert_any_call(30)
    assert len(result) == 1


def test_fetch_player_game_logs_task_bootstrap_fetches_all_players():
    """When player_name_mappings and player_game_logs are both empty, all players are fetched."""
    import sys
    sys.path.insert(0, ".")
    from unittest.mock import MagicMock, patch

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    # Both queries return empty — bootstrap mode
    mock_cursor.fetchall.side_effect = [[], []]

    fake_logs = [
        {"player_id": 1, "nba_game_id": "G1", "season": "2024-25", "game_date": "2025-01-01",
         "matchup": "A vs. B", "team_id": 1, "wl": "W", "min": 30.0, "fga": 8, "fta": 2,
         "usg_pct": 0.22, "pts": 15, "reb": 4, "ast": 3, "blk": 0, "stl": 1, "plus_minus": 5},
    ]

    with patch("dags.nba_player_stats_ingest_dag.get_data_db_conn", return_value=mock_conn), \
         patch("dags.nba_player_stats_ingest_dag.fetch_player_game_logs", return_value=fake_logs) as mock_fetch, \
         patch("dags.nba_player_stats_ingest_dag.store_raw_response"):
        from dags.nba_player_stats_ingest_dag import fetch_player_game_logs_task
        fetch_player_game_logs_task(ti=MagicMock())

    # In bootstrap mode (player_ids is empty set), no filtering occurs — all rows are stored
    mock_fetch.assert_called_once()
