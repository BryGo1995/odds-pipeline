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
    "fgm": 11,
    "fga": 14,
    "fg_pct": 0.786,
    "fg3m": 2,
    "fg3a": 5,
    "fg3_pct": 0.4,
    "ftm": 4,
    "fta": 6,
    "ft_pct": 0.667,
    "usg_pct": 0.312,
    "pts": 28,
    "reb": 8,
    "oreb": 1,
    "dreb": 7,
    "ast": 10,
    "blk": 1,
    "stl": 2,
    "tov": 3,
    "pf": 2,
    "plus_minus": 12,
}


def _insert_call(mock_cursor):
    """Return the execute call args for the actual INSERT (not SAVEPOINT/RELEASE)."""
    for call in mock_cursor.execute.call_args_list:
        sql = call[0][0]
        if "INSERT" in sql.upper():
            return call
    return None


def test_transform_player_game_logs_inserts_one_row():
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [SAMPLE_LOG])
    insert = _insert_call(mock_cursor)
    assert insert is not None
    args = insert[0][1]
    assert args[0] == 2544             # player_id
    assert args[1] == "0022400001"     # nba_game_id
    assert args[4] == "LAL vs. DEN"   # matchup
    assert args[7] == 34.5            # min
    assert args[17] == 0.312          # usg_pct


def test_transform_player_game_logs_skips_empty():
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [])
    mock_cursor.execute.assert_not_called()


def test_transform_player_game_logs_inserts_extended_stat_columns():
    """All new shooting/turnover/foul columns must appear in the INSERT statement."""
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [SAMPLE_LOG])
    insert = _insert_call(mock_cursor)
    assert insert is not None
    sql, args = insert[0][0], insert[0][1]
    for col in ("fgm", "fg_pct", "fg3m", "fg3a", "fg3_pct", "ftm", "ft_pct",
                "oreb", "dreb", "tov", "pf"):
        assert col in sql, f"column '{col}' missing from INSERT"
    assert 11 in args,   "fgm value missing from args"
    assert 2 in args,    "fg3m value missing from args"
    assert 3 in args,    "tov value missing from args"
    assert 2 in args,    "pf value missing from args"


def test_transform_player_game_logs_uses_on_conflict():
    """Append-only: ON CONFLICT DO NOTHING ensures idempotency."""
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [SAMPLE_LOG])
    insert = _insert_call(mock_cursor)
    assert insert is not None
    assert "ON CONFLICT" in insert[0][0].upper()
