from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_PLAYERS = [
    {
        "player_id": 2544,
        "full_name": "LeBron James",
        "position": "F",
        "team_id": 1610612747,
        "team_abbreviation": "LAL",
        "is_active": True,
    }
]


def test_transform_players_upserts_one_row():
    from nba.plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn()
    transform_players(mock_conn, SAMPLE_PLAYERS)
    assert mock_cursor.execute.call_count == 1
    args = mock_cursor.execute.call_args[0][1]
    assert args[0] == 2544         # player_id
    assert args[1] == "LeBron James"  # full_name
    assert args[2] == "F"          # position
    assert args[4] == "LAL"        # team_abbreviation
    assert args[5] is True         # is_active
    mock_conn.commit.assert_called_once()


def test_transform_players_skips_empty():
    from nba.plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn()
    transform_players(mock_conn, [])
    mock_cursor.execute.assert_not_called()


def test_transform_players_idempotent_on_conflict():
    """Verify ON CONFLICT clause is present in the SQL (upsert not insert)."""
    from nba.plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn()
    transform_players(mock_conn, SAMPLE_PLAYERS)
    sql = mock_cursor.execute.call_args[0][0]
    assert "ON CONFLICT" in sql.upper()
    assert "DO UPDATE" in sql.upper()
