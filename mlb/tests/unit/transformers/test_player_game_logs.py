from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_LOG = {
    "player_id": 660271,
    "mlb_game_pk": "745123",
    "season": "2026",
    "game_date": "2026-04-22",
    "matchup": "LAA @ SEA",
    "team_id": 108,
    "opponent_team_id": 136,
    "plate_appearances": 4,
    "at_bats": 3,
    "hits": 2,
    "doubles": 0,
    "triples": 0,
    "home_runs": 1,
    "rbi": 2,
    "runs": 1,
    "walks": 1,
    "strikeouts": 1,
    "stolen_bases": 0,
    "total_bases": 5,
}


def test_transform_pgl_empty_is_noop():
    from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [])
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()


def test_transform_pgl_bind_params_order():
    from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [SAMPLE_LOG])
    # execute calls: SAVEPOINT, INSERT, RELEASE → 3 per row
    assert mock_cursor.execute.call_count == 3
    # The middle call is the INSERT; its bind params are the game-log values
    insert_call = mock_cursor.execute.call_args_list[1]
    args = insert_call.args[1]
    assert args == (
        660271, "745123", "2026", "2026-04-22", "LAA @ SEA",
        108, 136,
        4, 3, 2, 0, 0,
        1, 2, 1, 1, 1,
        0, 5,
    )
    mock_conn.commit.assert_called_once()


def test_transform_pgl_savepoint_isolates_failed_row():
    """One row's INSERT raises — SAVEPOINT is rolled back, other rows still land."""
    from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Sequence of execute calls across 2 rows:
    #   row 1: SAVEPOINT, INSERT (RAISES), ROLLBACK TO, RELEASE
    #   row 2: SAVEPOINT, INSERT (ok),    RELEASE
    call_results = [
        None,                                # SAVEPOINT  (row 1)
        Exception("FK violation"),           # INSERT     (row 1) — raises
        None,                                # ROLLBACK TO (row 1)
        None,                                # RELEASE     (row 1)
        None,                                # SAVEPOINT  (row 2)
        None,                                # INSERT     (row 2) — ok
        None,                                # RELEASE     (row 2)
    ]
    def _execute(*a, **kw):
        result = call_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result
    mock_cursor.execute.side_effect = _execute

    row1 = dict(SAMPLE_LOG, player_id=999999)  # will be the failing one
    row2 = SAMPLE_LOG
    transform_player_game_logs(mock_conn, [row1, row2])

    # Row 1: SAVEPOINT, INSERT(raises), ROLLBACK TO, RELEASE = 4
    # Row 2: SAVEPOINT, INSERT(ok),                 RELEASE = 3
    # Total = 7
    assert mock_cursor.execute.call_count == 7
    mock_conn.commit.assert_called_once()


def test_transform_pgl_doubleheader_two_rows_same_date():
    """Two game-log rows for the same player on the same date but different
    mlb_game_pk (doubleheader) — both insert, no dedup."""
    from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    game1 = dict(SAMPLE_LOG, mlb_game_pk="745123")
    game2 = dict(SAMPLE_LOG, mlb_game_pk="745124")
    transform_player_game_logs(mock_conn, [game1, game2])
    # 3 execute calls per row × 2 rows = 6
    assert mock_cursor.execute.call_count == 6
    mock_conn.commit.assert_called_once()
