from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_TEAMS = [
    {
        "team_id": 108,
        "full_name": "Los Angeles Angels",
        "abbreviation": "LAA",
        "league": "American League",
        "division": "American League West",
    },
    {
        "team_id": 119,
        "full_name": "Los Angeles Dodgers",
        "abbreviation": "LAD",
        "league": "National League",
        "division": "National League West",
    },
]


def test_transform_teams_empty_is_noop():
    from mlb.plugins.transformers.teams import transform_teams
    mock_conn, mock_cursor = _make_mock_conn()
    transform_teams(mock_conn, [])
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()


def test_transform_teams_upserts_all_rows():
    from mlb.plugins.transformers.teams import transform_teams
    mock_conn, mock_cursor = _make_mock_conn()
    transform_teams(mock_conn, SAMPLE_TEAMS)
    assert mock_cursor.execute.call_count == 2
    mock_conn.commit.assert_called_once()


def test_transform_teams_bind_params_order():
    from mlb.plugins.transformers.teams import transform_teams
    mock_conn, mock_cursor = _make_mock_conn()
    transform_teams(mock_conn, [SAMPLE_TEAMS[0]])
    _, args = mock_cursor.execute.call_args.args
    assert args == (108, "Los Angeles Angels", "LAA",
                    "American League", "American League West")


def test_transform_teams_handles_missing_division():
    from mlb.plugins.transformers.teams import transform_teams
    mock_conn, mock_cursor = _make_mock_conn()
    minimal = {"team_id": 999, "full_name": "Some Team", "abbreviation": "XXX"}
    transform_teams(mock_conn, [minimal])
    _, args = mock_cursor.execute.call_args.args
    assert args == (999, "Some Team", "XXX", None, None)
