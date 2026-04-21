# nba/tests/unit/ml/test_settle.py
from unittest.mock import MagicMock, call, patch
from datetime import date


def _make_cursor(rows=None, stale_ids=None, unsettled_newly=None):
    """Return a mock cursor that returns different row sets on successive fetchall calls."""
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    side_effects = []
    if rows is not None:
        side_effects.append(rows)          # resolvable rows
    if stale_ids is not None:
        side_effects.append(stale_ids)     # stale ids
    if unsettled_newly is not None:
        side_effects.append(unsettled_newly)  # newly settled dates check
    cursor.fetchall.side_effect = side_effects
    return cursor


def test_settle_hits_correct_stat_column_for_points():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(
        rows=[
            # id, player_name, prop_type, line, game_date, pts, reb, ast, fg3m, fg3a
            (1, "LeBron James", "player_points", 24.5, date(2026, 4, 3), 27, 8, 7, 2, 5),
        ],
        stale_ids=[],
        unsettled_newly=[],
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    update_calls = [
        c for c in cursor.execute.call_args_list
        if "UPDATE" in str(c) and "actual_result" in str(c)
    ]
    assert len(update_calls) == 1
    args = update_calls[0].args[1]
    assert args[0] is True    # 27 > 24.5
    assert args[1] == 27      # actual_stat_value
    assert args[2] == 1       # rec id


def test_settle_records_miss():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(
        rows=[
            (2, "Jayson Tatum", "player_rebounds", 7.5, date(2026, 4, 3), 18, 6, 4, 1, 3),
        ],
        stale_ids=[],
        unsettled_newly=[],
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    update_calls = [
        c for c in cursor.execute.call_args_list
        if "UPDATE" in str(c) and "actual_result" in str(c)
    ]
    assert len(update_calls) == 1
    args = update_calls[0].args[1]
    assert args[0] is False   # 6 < 7.5


def test_settle_skips_unknown_prop_type():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(
        rows=[
            (3, "Some Player", "player_steals", 1.5, date(2026, 4, 3), 10, 5, 3, 1, 2),
        ],
        stale_ids=[],
        unsettled_newly=[],
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    update_calls = [
        c for c in cursor.execute.call_args_list
        if "UPDATE" in str(c) and "actual_result" in str(c)
    ]
    assert len(update_calls) == 0


def test_settle_marks_stale_recs_unresolvable():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(rows=[], stale_ids=[(99,), (100,)], unsettled_newly=[])
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    stale_calls = [
        c for c in cursor.execute.call_args_list
        if "UPDATE" in str(c) and "settled_at" in str(c) and "actual_result" not in str(c)
    ]
    assert len(stale_calls) == 1
    assert [99, 100] == list(stale_calls[0].args[1][0])


def test_settle_commits():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(rows=[], stale_ids=[], unsettled_newly=[])
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    conn.commit.assert_called_once()


def test_settle_queries_are_sport_scoped():
    """All SELECTs in settle_recommendations must filter to sport='NBA'."""
    from nba.plugins.ml.settle import settle_recommendations

    # Trigger every SELECT path: 1 resolvable row -> settle -> completed-dates check -> recap.
    recap_rows = [
        ("Player A", "player_points", 20.0, "Over", True, 25.0, 0.10),
    ]
    cursor = _make_cursor(
        rows=[
            (1, "Player A", "player_points", 20.0, date(2026, 4, 3), 25, 5, 3, 2, 4),
        ],
        stale_ids=[],
        unsettled_newly=[(date(2026, 4, 3),)],
    )
    cursor.fetchall.side_effect = list(cursor.fetchall.side_effect) + [recap_rows]
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    select_sqls = [
        c.args[0] for c in cursor.execute.call_args_list
        if c.args and "SELECT" in str(c.args[0])
    ]
    # Main settle SELECT (aliased as r)
    assert any("r.sport = 'NBA'" in sql for sql in select_sqls), \
        "Main settle SELECT must filter by r.sport = 'NBA'"
    # _notify_completed_dates SELECT + _send_recap SELECT (both unaliased)
    unaliased_sport_selects = [sql for sql in select_sqls if "sport = 'NBA'" in sql and "r.sport" not in sql]
    assert len(unaliased_sport_selects) >= 2, \
        f"Expected 2 unaliased sport-scoped SELECTs (completed-dates + recap), got {len(unaliased_sport_selects)}"


def test_settle_stale_recs_query_is_sport_scoped():
    """Stale-recs SELECT must filter by sport='NBA'."""
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(rows=[], stale_ids=[], unsettled_newly=[])
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    stale_sqls = [
        c.args[0] for c in cursor.execute.call_args_list
        if c.args and "SELECT id FROM recommendations" in str(c.args[0])
    ]
    assert len(stale_sqls) == 1
    assert "sport = 'NBA'" in stale_sqls[0]


def test_settle_sends_slack_when_top10_complete():
    from nba.plugins.ml.settle import settle_recommendations

    # One rec just settled; after settling, game_date 2026-04-03 has all top-10 done.
    # 4 fetchall calls:
    #   1. resolvable rows
    #   2. stale ids
    #   3. completed dates (HAVING query in _notify_completed_dates)
    #   4. recap rows (SELECT in _send_recap)
    recap_rows = [
        ("Player A", "player_points", 20.0, "Over", True, 25.0, 0.10),
    ]
    cursor = _make_cursor(
        rows=[
            (1, "Player A", "player_points", 20.0, date(2026, 4, 3), 25, 5, 3, 2, 4),
        ],
        stale_ids=[],
        unsettled_newly=[(date(2026, 4, 3),)],
    )
    # Append the 4th side_effect for _send_recap's fetchall
    cursor.fetchall.side_effect = list(cursor.fetchall.side_effect) + [recap_rows]
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled") as mock_notify:
        settle_recommendations(conn)

    mock_notify.assert_called_once()
    call_args = mock_notify.call_args
    assert call_args.args[0] == date(2026, 4, 3)
