# tests/unit/transformers/test_scores.py
from unittest.mock import MagicMock

SAMPLE_SCORES = [
    {
        "id": "abc123",
        "completed": True,
        "last_update": "2024-01-15T03:00:00Z",
        "home_team": "Los Angeles Lakers",
        "away_team": "Boston Celtics",
        "scores": [
            {"name": "Los Angeles Lakers", "score": "110"},
            {"name": "Boston Celtics", "score": "105"},
        ],
    }
]


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_transform_scores_upserts_row():
    from nba.plugins.transformers.scores import transform_scores
    mock_conn, mock_cursor = _make_mock_conn()
    # The transformer first SELECTs known game_ids; mock it to include our sample.
    mock_cursor.fetchall.return_value = [("abc123",)]
    transform_scores(conn=mock_conn, raw_scores=SAMPLE_SCORES)
    # Two execute calls now: SELECT known game_ids, then INSERT ... ON CONFLICT.
    assert mock_cursor.execute.call_count == 2
    insert_sql = mock_cursor.execute.call_args_list[1][0][0]
    assert "INSERT INTO scores" in insert_sql
    assert "ON CONFLICT" in insert_sql
    mock_conn.commit.assert_called_once()


def test_transform_scores_handles_null_scores():
    from nba.plugins.transformers.scores import transform_scores
    mock_conn, mock_cursor = _make_mock_conn()
    mock_cursor.fetchall.return_value = [("abc",)]
    raw = [{"id": "abc", "completed": False, "last_update": None,
            "home_team": "Lakers", "away_team": "Celtics", "scores": None}]
    transform_scores(conn=mock_conn, raw_scores=raw)
    # call_args is the INSERT (last call); SELECT is call_args_list[0].
    insert_args = mock_cursor.execute.call_args[0][1]
    assert insert_args[1] is None  # home_score
    assert insert_args[2] is None  # away_score


def test_transform_scores_skips_unknown_game_ids():
    """Scores for game_ids not in the games table should be skipped with a warning."""
    from nba.plugins.transformers.scores import transform_scores
    mock_conn, mock_cursor = _make_mock_conn()
    mock_cursor.fetchall.return_value = []  # no known games → everything skipped
    transform_scores(conn=mock_conn, raw_scores=SAMPLE_SCORES)
    # Only the SELECT ran; no INSERT.
    assert mock_cursor.execute.call_count == 1
    assert "SELECT game_id FROM games" in mock_cursor.execute.call_args_list[0][0][0]


def test_transform_scores_skips_empty_list():
    from nba.plugins.transformers.scores import transform_scores
    mock_conn, mock_cursor = _make_mock_conn()
    transform_scores(conn=mock_conn, raw_scores=[])
    mock_cursor.execute.assert_not_called()
