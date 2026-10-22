import pendulum

from shared.plugins.slate import slate_date


def test_slate_date_is_day_the_run_fires_not_logical_date():
    # nba_score_dag run for logical date 10-20 fires 2026-10-21 16:00 MT (22:00 UTC)
    ctx = {
        "ds": "2026-10-20",
        "data_interval_end": pendulum.datetime(2026, 10, 21, 16, 0, tz="America/Denver"),
    }
    assert slate_date(ctx) == "2026-10-21"


def test_slate_date_uses_eastern_time():
    # 03:30 UTC on 10-22 is still 10-21 in Eastern time
    ctx = {"data_interval_end": pendulum.datetime(2026, 10, 22, 3, 30, tz="UTC")}
    assert slate_date(ctx) == "2026-10-21"
