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


BOX_SCORE = {
    "min": 240, "fgm": 41, "fga": 88, "fg3m": 12, "fg3a": 35, "ftm": 16, "fta": 20,
    "oreb": 9, "dreb": 33, "reb": 42, "ast": 27, "stl": 7, "blk": 5, "tov": 13, "pf": 19,
}


def test_transform_team_game_logs_passes_box_score_columns():
    from nba.plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [{**SAMPLE_LOG, **BOX_SCORE}])
    sql, args = mock_cursor.execute.call_args[0]
    insert_cols = [c.strip() for c in sql.split("(", 1)[1].split(")", 1)[0].split(",")]
    row = dict(zip(insert_cols, args))
    for col, value in BOX_SCORE.items():
        assert row[col] == value


def test_transform_team_game_logs_missing_box_score_is_null():
    from nba.plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [SAMPLE_LOG])
    args = mock_cursor.execute.call_args[0][1]
    assert args[9:] == (None,) * len(BOX_SCORE)


def test_transform_team_game_logs_upserts_box_score_on_conflict():
    """Re-fetching a season must fill box score columns on existing rows."""
    from nba.plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [SAMPLE_LOG])
    sql = " ".join(mock_cursor.execute.call_args[0][0].split())
    assert "ON CONFLICT (team_id, nba_game_id) DO UPDATE SET" in sql
    set_clause = sql.split("DO UPDATE SET", 1)[1]
    for col in BOX_SCORE:
        assert f"{col} = EXCLUDED.{col}" in set_clause
    assert "team_id =" not in set_clause
    assert "nba_game_id =" not in set_clause
    mock_conn.commit.assert_called_once()


def test_transform_team_game_logs_skips_unchanged_rows():
    """Daily full-season re-fetches must not rewrite identical rows (#41)."""
    from nba.plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [SAMPLE_LOG])
    sql = " ".join(mock_cursor.execute.call_args[0][0].split())
    where = sql.split("fetched_at = NOW() WHERE", 1)[1]
    current, excluded = where.split("IS DISTINCT FROM")
    for col in ["pts", "plus_minus", "wl", *BOX_SCORE]:
        assert f"team_game_logs.{col}" in current
        assert f"EXCLUDED.{col}" in excluded
    assert "team_id" not in where.replace("team_game_logs.", "")
