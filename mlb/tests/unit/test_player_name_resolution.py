# mlb/tests/unit/test_player_name_resolution.py
from unittest.mock import MagicMock, patch


def _make_conn_with_cursors(unresolved_names, known_players):
    """Build a mock conn whose two `with conn.cursor()` blocks share a cursor.

    The first cursor use calls fetchall() twice: once for unresolved names,
    once for known players. Subsequent fetches aren't expected.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.side_effect = [
        [(name,) for name in unresolved_names],
        known_players,
    ]
    return mock_conn, mock_cursor


def _get_insert_mapping_calls(mock_cursor):
    """Return bind params for INSERT INTO mlb_player_name_mappings calls."""
    return [
        call.args[1]
        for call in mock_cursor.execute.call_args_list
        if "INSERT INTO mlb_player_name_mappings" in call.args[0]
    ]


def _get_update_player_props_calls(mock_cursor):
    return [
        call for call in mock_cursor.execute.call_args_list
        if "UPDATE player_props" in call.args[0]
    ]


def _get_unresolved_query_calls(mock_cursor):
    return [
        call for call in mock_cursor.execute.call_args_list
        if "FROM player_props pp" in call.args[0]
        and "NOT EXISTS" in call.args[0]
    ]


def test_resolve_exact_match_inserts_mapping():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Shohei Ohtani"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    resolve_player_ids(mock_conn)
    inserts = _get_insert_mapping_calls(mock_cursor)
    assert len(inserts) == 1
    odds_name, mlb_player_id, confidence = inserts[0]
    assert odds_name == "Shohei Ohtani"
    assert mlb_player_id == 660271
    assert confidence == 100.0
    mock_conn.commit.assert_called_once()


def test_resolve_fuzzy_match_above_threshold_inserts():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    # 'Shohei Otani' vs 'Shohei Ohtani' — rapidfuzz scores ~96
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Shohei Otani"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    resolve_player_ids(mock_conn)
    inserts = _get_insert_mapping_calls(mock_cursor)
    assert len(inserts) == 1
    assert inserts[0][1] == 660271
    assert inserts[0][2] >= 95.0


def test_resolve_below_threshold_no_insert_but_slack_alert():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Completely Unknown Name"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    with patch("mlb.plugins.transformers.player_name_resolution.send_slack_message") as mock_slack:
        resolve_player_ids(mock_conn, slack_webhook_url="https://hooks.slack.example/x")

    assert _get_insert_mapping_calls(mock_cursor) == []
    mock_slack.assert_called_once()
    args, _ = mock_slack.call_args
    assert args[0] == "https://hooks.slack.example/x"
    assert "Completely Unknown Name" in args[1]
    assert "[MLB]" in args[1]


def test_resolve_no_slack_when_webhook_url_none():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Completely Unknown Name"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    with patch("mlb.plugins.transformers.player_name_resolution.send_slack_message") as mock_slack:
        resolve_player_ids(mock_conn, slack_webhook_url=None)
    mock_slack.assert_not_called()


def test_resolve_unresolved_query_filters_on_baseball_mlb():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=[], known_players=[])
    resolve_player_ids(mock_conn)
    # With no unresolved, the function returns before running the UPDATE —
    # but the unresolved-names SELECT must have run once, with the MLB sport
    # filter embedded in the SQL string.
    unresolved_calls = _get_unresolved_query_calls(mock_cursor)
    assert len(unresolved_calls) == 1
    assert "g.sport = 'baseball_mlb'" in unresolved_calls[0].args[0]


def test_resolve_backfills_player_props_mlb_player_id():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Shohei Ohtani"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    resolve_player_ids(mock_conn)
    update_calls = _get_update_player_props_calls(mock_cursor)
    assert len(update_calls) == 1
    sql = update_calls[0].args[0]
    assert "mlb_player_id = m.mlb_player_id" in sql
    assert "g.sport = 'baseball_mlb'" in sql
    assert "pp.mlb_player_id IS NULL" in sql
