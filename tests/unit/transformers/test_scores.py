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
    from plugins.transformers.scores import transform_scores
    mock_conn, mock_cursor = _make_mock_conn()
    transform_scores(conn=mock_conn, raw_scores=SAMPLE_SCORES)
    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO scores" in sql
    assert "ON CONFLICT" in sql
    mock_conn.commit.assert_called_once()


def test_transform_scores_handles_null_scores():
    from plugins.transformers.scores import transform_scores
    mock_conn, mock_cursor = _make_mock_conn()
    raw = [{"id": "abc", "completed": False, "last_update": None,
            "home_team": "Lakers", "away_team": "Celtics", "scores": None}]
    transform_scores(conn=mock_conn, raw_scores=raw)
    args = mock_cursor.execute.call_args[0][1]
    assert args[1] is None  # home_score
    assert args[2] is None  # away_score


def test_transform_scores_skips_empty_list():
    from plugins.transformers.scores import transform_scores
    mock_conn, mock_cursor = _make_mock_conn()
    transform_scores(conn=mock_conn, raw_scores=[])
    mock_cursor.execute.assert_not_called()
