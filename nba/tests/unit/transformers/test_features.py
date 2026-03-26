# nba/tests/unit/transformers/test_features.py
import pandas as pd
from unittest.mock import MagicMock


def _make_mock_conn(fetchall_return, col_names):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fetchall_return
    mock_cursor.description = [(c,) for c in col_names]
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_american_odds_to_implied_prob_negative():
    from nba.plugins.transformers.features import american_odds_to_implied_prob
    result = american_odds_to_implied_prob(-110)
    assert abs(result - 110 / 210) < 1e-6


def test_american_odds_to_implied_prob_positive():
    from nba.plugins.transformers.features import american_odds_to_implied_prob
    result = american_odds_to_implied_prob(200)
    assert abs(result - 100 / 300) < 1e-6


def test_compute_rolling_stats_returns_empty_for_no_players():
    from nba.plugins.transformers.features import _compute_rolling_stats
    mock_conn, _ = _make_mock_conn([], ["player_id", "game_date", "pts", "reb", "ast", "fg3m", "fg3a"])
    result = _compute_rolling_stats(mock_conn, [], "2026-03-26")
    assert result.empty


def test_compute_rolling_stats_computes_rolling_averages():
    from nba.plugins.transformers.features import _compute_rolling_stats
    rows = [
        (1, "2026-03-20", 20.0, 5.0, 4.0, 2.0, 3.0),
        (1, "2026-03-21", 30.0, 7.0, 6.0, 1.0, 2.0),
        (1, "2026-03-22", 25.0, 6.0, 5.0, 3.0, 4.0),
    ]
    mock_conn, _ = _make_mock_conn(rows, ["player_id", "game_date", "pts", "reb", "ast", "fg3m", "fg3a"])
    result = _compute_rolling_stats(mock_conn, [1], "2026-03-26")
    pts_rows = result[result["prop_type"] == "player_points"]
    assert len(pts_rows) == 1
    assert abs(pts_rows.iloc[0]["rolling_avg_5g"] - 25.0) < 1e-6  # mean of [20, 30, 25]


def test_compute_rest_days_calculates_correctly():
    from nba.plugins.transformers.features import _compute_rest_days
    import datetime
    mock_conn, mock_cursor = _make_mock_conn(
        [(1, datetime.date(2026, 3, 24))],
        ["player_id", "last_game_date"],
    )
    result = _compute_rest_days(mock_conn, [1], "2026-03-26")
    assert result.iloc[0]["rest_days"] == 2


def test_build_features_returns_empty_when_no_props():
    from nba.plugins.transformers.features import build_features
    mock_conn, mock_cursor = _make_mock_conn([], [
        "player_id", "player_name", "game_date", "prop_type", "bookmaker",
        "line", "price", "line_movement", "matchup",
        "pts", "reb", "ast", "fg3m", "fg3a",
    ])
    result = build_features(mock_conn, "2026-03-26")
    assert result.empty
