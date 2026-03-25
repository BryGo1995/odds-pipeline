from unittest.mock import MagicMock, call


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 0
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_parse_matchup_home_game():
    from nba.plugins.transformers.game_id_linker import parse_matchup_teams
    home, away = parse_matchup_teams("DEN vs. LAL")
    assert home == "DEN"
    assert away == "LAL"


def test_parse_matchup_away_game():
    from nba.plugins.transformers.game_id_linker import parse_matchup_teams
    # "DEN @ LAL" means DEN is away, LAL is home
    home, away = parse_matchup_teams("DEN @ LAL")
    assert home == "LAL"
    assert away == "DEN"


def test_link_nba_game_ids_updates_games_table():
    from nba.plugins.transformers.game_id_linker import link_nba_game_ids
    mock_conn, mock_cursor = _make_mock_conn()

    # team_game_logs rows: one game
    mock_cursor.fetchall.return_value = [
        ("0022400001", "2025-01-15", "DEN vs. LAL"),
    ]

    link_nba_game_ids(mock_conn)

    update_calls = [
        c for c in mock_cursor.execute.call_args_list
        if "UPDATE games" in str(c)
    ]
    assert len(update_calls) >= 1
    mock_conn.commit.assert_called()


def test_link_nba_game_ids_no_rows_is_noop():
    from nba.plugins.transformers.game_id_linker import link_nba_game_ids
    mock_conn, mock_cursor = _make_mock_conn()
    mock_cursor.fetchall.return_value = []
    link_nba_game_ids(mock_conn)
    update_calls = [
        c for c in mock_cursor.execute.call_args_list
        if "UPDATE games" in str(c)
    ]
    assert len(update_calls) == 0
