from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_STATS = {
    "team_id": 1610612743,
    "team_name": "Denver Nuggets",
    "season": "2024-25",
    "pace": 98.5,
    "off_rating": 115.2,
    "def_rating": 110.8,
    "opp_pts_pg": 42.1,
}


def test_transform_team_season_stats_upserts():
    from nba.plugins.transformers.team_season_stats import transform_team_season_stats
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_season_stats(mock_conn, [SAMPLE_STATS])
    assert mock_cursor.execute.call_count == 1
    args = mock_cursor.execute.call_args[0][1]
    assert args[0] == 1610612743
    assert args[2] == "2024-25"
    assert args[3] == 98.5    # pace
    assert args[6] == 42.1   # opp_pts_paint_pg
    sql = mock_cursor.execute.call_args[0][0]
    assert "ON CONFLICT" in sql.upper()
    assert "DO UPDATE" in sql.upper()


def test_transform_team_season_stats_skips_empty():
    from nba.plugins.transformers.team_season_stats import transform_team_season_stats
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_season_stats(mock_conn, [])
    mock_cursor.execute.assert_not_called()
