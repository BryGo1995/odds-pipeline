# tests/unit/transformers/test_events.py
from unittest.mock import MagicMock

SAMPLE_EVENTS = [
    {
        "id": "abc123",
        "sport_key": "basketball_nba",
        "commence_time": "2024-01-15T00:00:00Z",
        "home_team": "Los Angeles Lakers",
        "away_team": "Boston Celtics",
    }
]


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_transform_events_upserts_game():
    from shared.plugins.transformers.events import transform_events
    mock_conn, mock_cursor = _make_mock_conn()
    transform_events(conn=mock_conn, raw_events=SAMPLE_EVENTS)
    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO games" in sql
    assert "ON CONFLICT" in sql
    mock_conn.commit.assert_called_once()


def test_transform_events_skips_empty_list():
    from shared.plugins.transformers.events import transform_events
    mock_conn, mock_cursor = _make_mock_conn()
    transform_events(conn=mock_conn, raw_events=[])
    mock_cursor.execute.assert_not_called()
