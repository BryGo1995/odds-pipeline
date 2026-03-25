from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_LOG = {
    "team_id": 1610612743,
    "team_abbreviation": "DEN",
    "nba_game_id": "0022400001",
    "season": "2024-25",
    "game_date": "2025-01-15",
    "matchup": "DEN @ LAL",
    "wl": "L",
    "pts": 110,
    "plus_minus": -12,
}


def test_transform_team_game_logs_inserts_one_row():
    from nba.plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [SAMPLE_LOG])
    assert mock_cursor.execute.call_count == 1
    args = mock_cursor.execute.call_args[0][1]
    assert args[0] == 1610612743   # team_id
    assert args[1] == "DEN"        # team_abbreviation
    assert args[8] == -12          # plus_minus


def test_transform_team_game_logs_skips_empty():
    from nba.plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [])
    mock_cursor.execute.assert_not_called()


def test_transform_team_game_logs_uses_on_conflict():
    from nba.plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [SAMPLE_LOG])
    sql = mock_cursor.execute.call_args[0][0]
    assert "ON CONFLICT" in sql.upper()
