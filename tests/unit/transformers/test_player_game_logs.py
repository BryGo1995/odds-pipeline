from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_LOG = {
    "player_id": 2544,
    "nba_game_id": "0022400001",
    "season": "2024-25",
    "game_date": "2025-01-15",
    "matchup": "LAL vs. DEN",
    "team_id": 1610612747,
    "wl": "W",
    "min": 34.5,
    "fga": 14,
    "fta": 6,
    "usg_pct": 0.312,
    "pts": 28,
    "reb": 8,
    "ast": 10,
    "blk": 1,
    "stl": 2,
    "plus_minus": 12,
}


def test_transform_player_game_logs_inserts_one_row():
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [SAMPLE_LOG])
    assert mock_cursor.execute.call_count == 1
    args = mock_cursor.execute.call_args[0][1]
    assert args[0] == 2544             # player_id
    assert args[1] == "0022400001"     # nba_game_id
    assert args[4] == "LAL vs. DEN"   # matchup
    assert args[8] == 34.5            # min
    assert args[10] == 0.312          # usg_pct


def test_transform_player_game_logs_skips_empty():
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [])
    mock_cursor.execute.assert_not_called()


def test_transform_player_game_logs_uses_on_conflict():
    """Append-only: ON CONFLICT DO NOTHING ensures idempotency."""
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [SAMPLE_LOG])
    sql = mock_cursor.execute.call_args[0][0]
    assert "ON CONFLICT" in sql.upper()
