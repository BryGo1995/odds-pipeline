from unittest.mock import MagicMock


def _make_mock_conn(teams_fetchall=None):
    """Build a mock conn/cursor.

    teams_fetchall: what the `SELECT team_id, abbreviation FROM mlb_teams`
    fetchall() should return. Default: empty.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = teams_fetchall or []
    return mock_conn, mock_cursor


SAMPLE_PLAYERS = [
    {
        "player_id": 660271,
        "full_name": "Shohei Ohtani",
        "position": "DH",
        "bats": "L",
        "throws": "R",
        "team_id": 119,
        "team_abbreviation": None,
        "is_active": True,
    }
]


def test_transform_players_empty_is_noop():
    from mlb.plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn()
    transform_players(mock_conn, [])
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()


def test_transform_players_resolves_team_abbreviation_from_mlb_teams():
    from mlb.plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn(teams_fetchall=[(119, "LAD"), (108, "LAA")])
    transform_players(mock_conn, SAMPLE_PLAYERS)
    # execute calls: 1 SELECT on mlb_teams + 1 INSERT per player = 2 total
    assert mock_cursor.execute.call_count == 2
    insert_args = mock_cursor.execute.call_args.args[1]
    # bind-param order: player_id, full_name, position, bats, throws,
    #                   team_id, team_abbreviation, is_active
    assert insert_args == (660271, "Shohei Ohtani", "DH", "L", "R",
                           119, "LAD", True)
    mock_conn.commit.assert_called_once()


def test_transform_players_missing_team_leaves_abbreviation_none():
    from mlb.plugins.transformers.players import transform_players
    # mlb_teams has no entry for team_id=119 → abbreviation should be None
    mock_conn, mock_cursor = _make_mock_conn(teams_fetchall=[(108, "LAA")])
    transform_players(mock_conn, SAMPLE_PLAYERS)
    insert_args = mock_cursor.execute.call_args.args[1]
    assert insert_args[6] is None  # team_abbreviation slot


def test_transform_players_free_agent_null_team():
    from mlb.plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn(teams_fetchall=[(119, "LAD")])
    fa = dict(SAMPLE_PLAYERS[0], team_id=None)
    transform_players(mock_conn, [fa])
    insert_args = mock_cursor.execute.call_args.args[1]
    assert insert_args[5] is None  # team_id slot
    assert insert_args[6] is None  # team_abbreviation slot
