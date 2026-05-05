# mlb/tests/unit/ml/test_settle.py
"""Unit tests for mlb/plugins/ml/settle.py."""
from datetime import date
from unittest.mock import MagicMock, patch

import pytest


def _make_conn(resolvable_rows, stale_ids=None, recap_rows=None, completed_dates=None):
    """Build a mock Postgres connection that returns canned fetchall results in order."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    fetchall_queue = [
        resolvable_rows,
        list(stale_ids or []),
        completed_dates or [],
    ]
    if recap_rows is not None:
        fetchall_queue.append(recap_rows)
    cur.fetchall.side_effect = fetchall_queue

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def test_settle_single_game_day_updates_actual_result():
    from mlb.plugins.ml.settle import settle_recommendations
    rows = [(101, "Aaron Judge", "batter_hits", 1.5, date(2026, 5, 3), 2, 4, 1)]
    conn, cur = _make_conn(rows)
    settle_recommendations(conn)
    update_calls = [c for c in cur.execute.call_args_list if "UPDATE recommendations" in c.args[0] and "actual_result" in c.args[0]]
    assert len(update_calls) == 1
    args = update_calls[0].args[1]
    assert args[0] is True   # actual_result: 2 hits > 1.5 line
    assert args[1] == 2      # actual_stat_value
    assert args[2] == 101    # rec id


def test_settle_doubleheader_aggregates_stats():
    """Day-aggregate via SQL — test asserts the SUM/GROUP BY join is in the SELECT."""
    from mlb.plugins.ml.settle import settle_recommendations
    rows = [(202, "Mookie Betts", "batter_total_bases", 2.5, date(2026, 5, 3), 3, 6, 0)]
    conn, cur = _make_conn(rows)
    settle_recommendations(conn)

    select_calls = [c for c in cur.execute.call_args_list
                    if "SELECT" in c.args[0] and "settled_at IS NULL" in c.args[0]]
    assert select_calls, "expected an unsettled-rec SELECT"
    sql = select_calls[0].args[0]
    assert "SUM(hits)" in sql
    assert "SUM(total_bases)" in sql
    assert "SUM(home_runs)" in sql
    assert "GROUP BY player_id, game_date" in sql

    update_calls = [c for c in cur.execute.call_args_list if "UPDATE recommendations" in c.args[0] and "actual_result" in c.args[0]]
    args = update_calls[0].args[1]
    assert args[0] is True   # 6 total_bases > 2.5 line
    assert args[1] == 6


def test_settle_uses_mlb_player_name_mappings_table():
    from mlb.plugins.ml.settle import settle_recommendations
    conn, cur = _make_conn([])
    settle_recommendations(conn)
    select_calls = [c for c in cur.execute.call_args_list if "SELECT" in c.args[0]]
    sql = select_calls[0].args[0]
    assert "mlb_player_name_mappings" in sql
    assert "mlb_player_id" in sql


def test_settle_filters_sport_mlb_in_all_queries():
    from mlb.plugins.ml.settle import settle_recommendations
    conn, cur = _make_conn([])
    settle_recommendations(conn)
    selects = [c.args[0] for c in cur.execute.call_args_list if "SELECT" in c.args[0]]
    assert all("sport = 'MLB'" in s for s in selects), \
        f"some SELECTs missing sport='MLB' filter: {[s for s in selects if 'MLB' not in s]}"


def test_settle_marks_stale_recs_after_seven_days(caplog):
    from mlb.plugins.ml.settle import settle_recommendations
    conn, cur = _make_conn(resolvable_rows=[], stale_ids=[(901,), (902,)])
    settle_recommendations(conn)
    update_calls = [c for c in cur.execute.call_args_list
                    if "UPDATE recommendations" in c.args[0] and "settled_at = NOW()" in c.args[0]
                    and "actual_result" not in c.args[0]]
    assert len(update_calls) == 1
    assert update_calls[0].args[1][0] == [901, 902]


def test_settle_calls_notify_picks_settled_with_sport_mlb():
    from mlb.plugins.ml.settle import settle_recommendations
    rows = [(101, "Aaron Judge", "batter_hits", 1.5, date(2026, 5, 3), 2, 4, 1)]
    completed_dates = [(date(2026, 5, 3),)]
    recap_rows = [
        ("Aaron Judge", "batter_hits", 1.5, "Over", True, 2.0, 0.10),
    ]
    conn, _ = _make_conn(rows, completed_dates=completed_dates, recap_rows=recap_rows)
    with patch("mlb.plugins.ml.settle.notify_picks_settled") as mock_notify:
        settle_recommendations(conn)
    mock_notify.assert_called_once()
    _, kwargs = mock_notify.call_args.args, mock_notify.call_args.kwargs
    assert kwargs.get("sport") == "mlb" or (len(mock_notify.call_args.args) >= 3 and mock_notify.call_args.args[-1] == "mlb")
