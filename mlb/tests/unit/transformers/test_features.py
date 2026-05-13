# mlb/tests/unit/transformers/test_features.py
import datetime

from unittest.mock import MagicMock


def _make_mock_conn(fetchall_return, col_names):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fetchall_return
    mock_cursor.description = [(c,) for c in col_names]
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def _make_multi_query_conn(query_results):
    """Mock a connection whose cursor returns different rows per execute() call.

    query_results: list of (rows, col_names) — one entry per execute call,
    consumed in order.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    state = {"i": 0}

    def _execute(*args, **kwargs):
        rows, cols = query_results[state["i"]]
        mock_cursor.fetchall.return_value = rows
        mock_cursor.description = [(c,) for c in cols]
        state["i"] += 1

    mock_cursor.execute.side_effect = _execute
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


# --- helpers --------------------------------------------------------------

def test_mlb_prop_stat_map_covers_two_batter_props():
    from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP
    assert MLB_PROP_STAT_MAP == {
        "batter_hits": "hits",
        "batter_total_bases": "total_bases",
    }


# --- _compute_rolling_stats ----------------------------------------------

def test_compute_rolling_stats_returns_empty_for_no_players():
    from mlb.plugins.transformers.features import _compute_rolling_stats
    mock_conn, _ = _make_mock_conn(
        [], ["player_id", "game_date", "hits", "total_bases", "home_runs"]
    )
    result = _compute_rolling_stats(mock_conn, [], "2026-04-30")
    assert result.empty


def test_compute_rolling_stats_computes_means():
    from mlb.plugins.transformers.features import _compute_rolling_stats
    rows = [
        (660271, datetime.date(2026, 4, 20), 1, 1, 0),
        (660271, datetime.date(2026, 4, 21), 2, 5, 1),
        (660271, datetime.date(2026, 4, 22), 0, 0, 0),
    ]
    mock_conn, _ = _make_mock_conn(
        rows, ["player_id", "game_date", "hits", "total_bases", "home_runs"]
    )
    result = _compute_rolling_stats(mock_conn, [660271], "2026-04-30")
    hits_rows = result[result["prop_type"] == "batter_hits"]
    assert len(hits_rows) == 1
    assert abs(hits_rows.iloc[0]["rolling_avg_5g"] - 1.0) < 1e-6  # mean(1,2,0)
    tb_rows = result[result["prop_type"] == "batter_total_bases"]
    assert abs(tb_rows.iloc[0]["rolling_avg_5g"] - 2.0) < 1e-6   # mean(1,5,0)


def test_compute_rolling_stats_std_none_for_single_game():
    from mlb.plugins.transformers.features import _compute_rolling_stats
    rows = [(660271, datetime.date(2026, 4, 20), 1, 1, 0)]
    mock_conn, _ = _make_mock_conn(
        rows, ["player_id", "game_date", "hits", "total_bases", "home_runs"]
    )
    result = _compute_rolling_stats(mock_conn, [660271], "2026-04-30")
    hits_rows = result[result["prop_type"] == "batter_hits"]
    # sample std requires >= 2 rows; should be None
    assert hits_rows.iloc[0]["rolling_std_10g"] is None or \
           __import__("pandas").isna(hits_rows.iloc[0]["rolling_std_10g"])


# --- _compute_rest_days ---------------------------------------------------

def test_compute_rest_days_calculates_correctly():
    from mlb.plugins.transformers.features import _compute_rest_days
    mock_conn, _ = _make_mock_conn(
        [(660271, datetime.date(2026, 4, 28))],
        ["player_id", "last_game_date"],
    )
    result = _compute_rest_days(mock_conn, [660271], "2026-04-30")
    assert result.iloc[0]["rest_days"] == 2


def test_compute_rest_days_none_when_no_prior_game():
    from mlb.plugins.transformers.features import _compute_rest_days
    mock_conn, _ = _make_mock_conn([], ["player_id", "last_game_date"])
    result = _compute_rest_days(mock_conn, [660271], "2026-04-30")
    assert len(result) == 1
    assert result.iloc[0]["rest_days"] is None


# --- build_features -------------------------------------------------------

def test_build_features_returns_empty_when_no_props():
    from mlb.plugins.transformers.features import build_features
    mock_conn, _ = _make_mock_conn(
        [],
        [
            "player_id", "player_name", "game_date", "prop_type", "bookmaker",
            "line", "price", "line_movement", "matchup", "team_abbreviation",
            "home_abbr", "hits", "total_bases", "home_runs",
        ],
    )
    result = build_features(mock_conn, "2026-04-30")
    assert result.empty


def test_build_features_query_filters_by_baseball_mlb_sport():
    """The main query must include `g.sport = 'baseball_mlb'` so NBA rows
    on the same player_props table can't leak into MLB feature output."""
    from mlb.plugins.transformers.features import build_features
    mock_conn, mock_cursor = _make_mock_conn(
        [],
        [
            "player_id", "player_name", "game_date", "prop_type", "bookmaker",
            "line", "price", "line_movement", "matchup", "team_abbreviation",
            "home_abbr", "hits", "total_bases", "home_runs",
        ],
    )
    build_features(mock_conn, "2026-04-30")
    # First execute is the props query
    first_call = mock_cursor.execute.call_args_list[0]
    sql_text = first_call.args[0]
    assert "baseball_mlb" in sql_text
    assert "mlb_player_id" in sql_text


def test_build_features_full_row_with_two_prop_types():
    """One Over row per (player, prop_type, bookmaker) with rolling stats and
    rest_days merged in. Verifies actual_result is computed against the line."""
    from mlb.plugins.transformers.features import build_features

    # Row tuples for the props query — column order matches the SQL SELECT.
    # Player 660271 had 3 hits, 4 total_bases, 0 home_runs in his game on 2026-04-30.
    props_rows = [
        # player_id, player_name, game_date, prop_type, bookmaker,
        # line, price, line_movement, matchup, team_abbreviation, home_abbr,
        # hits, total_bases, home_runs
        (660271, "Mike Trout", datetime.date(2026, 4, 30), "batter_hits", "fanduel",
         1.5, -110, 0.0, "LAA @ SEA", "LAA", "SEA",
         3, 4, 0),
        (660271, "Mike Trout", datetime.date(2026, 4, 30), "batter_total_bases", "fanduel",
         2.5, +120, 0.0, "LAA @ SEA", "LAA", "SEA",
         3, 4, 0),
    ]
    rolling_rows = [
        # player_id, game_date, hits, total_bases, home_runs (3 prior games)
        (660271, datetime.date(2026, 4, 20), 2, 4, 1),
        (660271, datetime.date(2026, 4, 21), 1, 1, 0),
        (660271, datetime.date(2026, 4, 22), 3, 6, 1),
    ]
    rest_rows = [(660271, datetime.date(2026, 4, 28))]

    mock_conn, _ = _make_multi_query_conn([
        (props_rows, [
            "player_id", "player_name", "game_date", "prop_type", "bookmaker",
            "line", "price", "line_movement", "matchup", "team_abbreviation",
            "home_abbr", "hits", "total_bases", "home_runs",
        ]),
        (rolling_rows, ["player_id", "game_date", "hits", "total_bases", "home_runs"]),
        (rest_rows, ["player_id", "last_game_date"]),
    ])
    df = build_features(mock_conn, "2026-04-30")

    assert len(df) == 2
    assert set(df["prop_type"]) == {"batter_hits", "batter_total_bases"}

    # actual_result: hits=3 vs line 1.5 -> 1; total_bases=4 vs 2.5 -> 1
    hits_row = df[df["prop_type"] == "batter_hits"].iloc[0]
    tb_row   = df[df["prop_type"] == "batter_total_bases"].iloc[0]
    assert hits_row["actual_result"] == 1
    assert tb_row["actual_result"] == 1
    assert hits_row["actual_stat_value"] == 3
    assert tb_row["actual_stat_value"] == 4

    # is_home: LAA player, matchup "LAA @ SEA" → away (home_abbr=SEA != team=LAA)
    assert hits_row["is_home"] is False or hits_row["is_home"] == False  # noqa: E712

    # rest_days: 2026-04-30 - 2026-04-28 = 2
    assert hits_row["rest_days"] == 2

    # implied_prob_over for -110 ≈ 0.524
    assert abs(hits_row["implied_prob_over"] - 110 / 210) < 1e-6


def test_build_features_is_home_true_for_home_team():
    """Player on home team (matchup HOME side) should get is_home=True."""
    from mlb.plugins.transformers.features import build_features
    props_rows = [
        # SEA player at home in "LAA @ SEA"
        (592450, "Cal Raleigh", datetime.date(2026, 4, 30), "batter_hits", "fanduel",
         1.5, -110, 0.0, "LAA @ SEA", "SEA", "SEA",
         2, 2, 0),
    ]
    mock_conn, _ = _make_multi_query_conn([
        (props_rows, [
            "player_id", "player_name", "game_date", "prop_type", "bookmaker",
            "line", "price", "line_movement", "matchup", "team_abbreviation",
            "home_abbr", "hits", "total_bases", "home_runs",
        ]),
        ([], ["player_id", "game_date", "hits", "total_bases", "home_runs"]),
        ([], ["player_id", "last_game_date"]),
    ])
    df = build_features(mock_conn, "2026-04-30")
    assert df.iloc[0]["is_home"] == True  # noqa: E712


def test_build_features_actual_result_null_when_no_game_log():
    """If LEFT JOIN finds no game log row (game not yet played),
    actual_result and actual_stat_value should be NULL."""
    from mlb.plugins.transformers.features import build_features
    import pandas as pd
    props_rows = [
        (660271, "Mike Trout", datetime.date(2026, 4, 30), "batter_hits", "fanduel",
         1.5, -110, 0.0, "LAA @ SEA", "LAA", "SEA",
         None, None, None),
    ]
    mock_conn, _ = _make_multi_query_conn([
        (props_rows, [
            "player_id", "player_name", "game_date", "prop_type", "bookmaker",
            "line", "price", "line_movement", "matchup", "team_abbreviation",
            "home_abbr", "hits", "total_bases", "home_runs",
        ]),
        ([], ["player_id", "game_date", "hits", "total_bases", "home_runs"]),
        ([], ["player_id", "last_game_date"]),
    ])
    df = build_features(mock_conn, "2026-04-30")
    assert pd.isna(df.iloc[0]["actual_result"])
    assert pd.isna(df.iloc[0]["actual_stat_value"])


def test_build_features_output_columns_match_expected_schema():
    """Output DataFrame must carry exactly the columns the trainer/scorer
    consume — no leftover SQL columns like price, matchup, hits, etc."""
    from mlb.plugins.transformers.features import build_features
    props_rows = [
        (660271, "Mike Trout", datetime.date(2026, 4, 30), "batter_hits", "fanduel",
         1.5, -110, 0.0, "LAA @ SEA", "LAA", "SEA",
         3, 4, 0),
    ]
    mock_conn, _ = _make_multi_query_conn([
        (props_rows, [
            "player_id", "player_name", "game_date", "prop_type", "bookmaker",
            "line", "price", "line_movement", "matchup", "team_abbreviation",
            "home_abbr", "hits", "total_bases", "home_runs",
        ]),
        ([], ["player_id", "game_date", "hits", "total_bases", "home_runs"]),
        ([], ["player_id", "last_game_date"]),
    ])
    df = build_features(mock_conn, "2026-04-30")
    expected = {
        "player_id", "player_name", "game_date", "prop_type", "bookmaker",
        "line", "implied_prob_over", "line_movement",
        "rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g", "rolling_std_10g",
        "is_home", "rest_days",
        "actual_result", "actual_stat_value",
    }
    assert set(df.columns) == expected
