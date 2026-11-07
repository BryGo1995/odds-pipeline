from datetime import date
from unittest.mock import MagicMock

from shared.plugins.watchdog import consecutive_missed, find_missed_slates


def test_no_missed_when_newest_slate_has_recs():
    slates = [(date(2026, 11, 6), True), (date(2026, 11, 5), False)]
    assert consecutive_missed(slates) == []


def test_counts_missed_until_newest_slate_with_recs():
    slates = [
        (date(2026, 11, 6), False),
        (date(2026, 11, 5), False),
        (date(2026, 10, 27), True),
        (date(2026, 10, 26), False),  # older gap is not part of the streak
    ]
    assert consecutive_missed(slates) == [date(2026, 11, 6), date(2026, 11, 5)]


def test_all_missed_when_no_slate_has_recs():
    slates = [(date(2026, 11, 6), False), (date(2026, 11, 5), False)]
    assert len(consecutive_missed(slates)) == 2


def test_no_slates_means_nothing_missed():
    # Off-season: no games, so no alert
    assert consecutive_missed([]) == []


def test_find_missed_slates_queries_by_sport_and_window():
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchall.return_value = [(date(2026, 11, 6), False), (date(2026, 10, 27), True)]
    cur.fetchone.return_value = (date(2026, 10, 27),)

    missed, last = find_missed_slates(conn, "basketball_nba", "NBA", "2026-11-06")

    assert missed == [date(2026, 11, 6)]
    assert last == date(2026, 10, 27)
    slate_params = cur.execute.call_args_list[0].args[1]
    assert slate_params == ("NBA", "basketball_nba", "2026-10-07", "2026-11-06")
