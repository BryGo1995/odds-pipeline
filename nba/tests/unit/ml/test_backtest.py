# nba/tests/unit/ml/test_backtest.py
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch


def _make_backtest_df(n_dates=30, rows_per_date=30, seed=7):
    """Labeled feature rows spread over n_dates consecutive days, 3 prop types."""
    rng = np.random.default_rng(seed)
    dates = np.repeat(pd.date_range("2026-03-01", periods=n_dates, freq="D"), rows_per_date)
    n = len(dates)
    implied = rng.uniform(0.4, 0.6, n)
    return pd.DataFrame({
        "player_id":         rng.integers(1, 40, n),
        "player_name":       ["Player A"] * n,
        "game_date":         dates,
        "prop_type":         rng.choice(["player_points", "player_rebounds", "player_assists"], n),
        "bookmaker":         rng.choice(["draftkings", "fanduel"], n),
        "line":              rng.integers(5, 30, n) + 0.5,
        "implied_prob_over": implied,
        "line_movement":     rng.uniform(-1, 1, n),
        "rolling_avg_5g":    rng.uniform(5, 30, n),
        "rolling_avg_10g":   rng.uniform(5, 30, n),
        "rolling_avg_20g":   rng.uniform(5, 30, n),
        "rolling_std_10g":   rng.uniform(1, 6, n),
        "is_home":           rng.choice([True, False], n),
        "rest_days":         rng.integers(1, 4, n).astype(float),
        "actual_result":     rng.integers(0, 2, n),
        "actual_stat_value": rng.uniform(5, 30, n),
        "fair_prob_over":    implied - 0.02,
    })


def test_make_folds_starts_after_min_train_dates_and_skips_gaps():
    from nba.plugins.ml.backtest import make_folds
    dates = list(pd.date_range("2026-03-01", periods=20, freq="D")) + [pd.Timestamp("2026-10-20")]
    folds = make_folds(dates, fold_days=7, min_train_dates=14)

    assert folds[0] == (pd.Timestamp("2026-03-15"), pd.Timestamp("2026-03-22"))
    # Off-season gap: next fold starts on the next real date, not an empty week
    assert folds[-1][0] == pd.Timestamp("2026-10-20")
    assert len(folds) == 2


def test_make_folds_empty_when_not_enough_dates():
    from nba.plugins.ml.backtest import make_folds
    assert make_folds(pd.date_range("2026-03-01", periods=5), fold_days=7, min_train_dates=14) == []


def test_run_backtest_never_trains_on_test_fold():
    import nba.plugins.ml.backtest as bt
    seen = []
    real_fit = bt.fit_recipe

    def recording_fit(history, calibration_days):
        seen.append(history["game_date"].max())
        return real_fit(history, calibration_days)

    df = _make_backtest_df()
    with patch.object(bt, "fit_recipe", side_effect=recording_fit):
        preds = bt.run_backtest(df, fold_days=7, min_train_dates=14)

    assert not preds.empty
    assert preds["model_prob"].between(0, 1).all()
    fold_starts = sorted(preds["fold_start"].unique())
    # One fit per (fold, prop type), each on history strictly before the fold
    assert len(seen) == len(fold_starts) * 3
    for fold_start in fold_starts:
        assert (preds.loc[preds["fold_start"] == fold_start, "game_date"] >= fold_start).all()
    assert max(seen) < max(fold_starts)


def test_add_eval_columns_weights_each_player_prop_once():
    from nba.plugins.ml.backtest import add_eval_columns
    df = pd.DataFrame({
        "player_id":         [1, 1, 1, 2],
        "game_date":         ["2026-04-01"] * 4,
        "prop_type":         ["player_points"] * 3 + ["player_points"],
        "line":              [24.5, 24.5, 25.0, 10.5],
        "actual_stat_value": [27, 27, 25, 8],
    })
    result = add_eval_columns(df)
    assert result["weight"].sum() == 2  # two unique player/date/props
    assert result["push"].tolist() == [False, False, True, False]


def test_attach_market_probs_devigs_with_matching_under():
    import datetime
    from nba.plugins.ml.backtest import attach_market_probs

    mock_conn = MagicMock()
    cur = mock_conn.cursor.return_value.__enter__.return_value
    cur.description = [(c,) for c in ["player_id", "game_date", "prop_type", "bookmaker", "line", "under_price"]]
    cur.fetchall.return_value = [(1, datetime.date(2026, 4, 1), "player_points", "fanduel", 24.5, -110)]

    df = pd.DataFrame({
        "player_id":         [1, 1],
        "game_date":         [datetime.date(2026, 4, 1)] * 2,
        "prop_type":         ["player_points"] * 2,
        "bookmaker":         ["fanduel", "betmgm"],
        "line":              [24.5, 24.5],
        "implied_prob_over": [110 / 210] * 2,
    })
    result = attach_market_probs(mock_conn, df)

    assert abs(result["fair_prob_over"].iloc[0] - 0.5) < 1e-9  # -110 / -110 → 50/50
    assert np.isnan(result["fair_prob_over"].iloc[1])         # no Under at betmgm


def test_settle_pays_posted_price():
    from nba.plugins.ml.backtest import settle
    picks = pd.DataFrame({
        "implied_prob_over": [110 / 210, 110 / 210, 1 / 3],
        "actual_result":     [1, 0, 0],
        "push":              [False, False, True],
    })
    profit = settle(picks)["profit"].tolist()
    assert abs(profit[0] - 100 / 110) < 1e-9  # -110 winner
    assert profit[1:] == [-1.0, 0.0]


def test_replay_picks_allocates_top_n_across_prop_types():
    from nba.plugins.ml.backtest import replay_picks
    day = pd.DataFrame({
        "game_date":         [pd.Timestamp("2026-04-01")] * 6,
        "prop_type":         ["player_points"] * 4 + ["player_assists"] * 2,
        "model_prob":        [0.9, 0.8, 0.7, 0.6, 0.55, 0.65],
        "implied_prob_over": [0.5] * 6,
        "actual_result":     [1, 0, 1, 1, 0, 1],
        "push":              [False] * 6,
    })
    picks = replay_picks(day, top_n=4)
    assert picks.groupby("prop_type").size().to_dict() == {"player_assists": 2, "player_points": 2}
    assert sorted(picks["model_prob"]) == [0.55, 0.65, 0.8, 0.9]


def test_probability_metrics_zero_delta_when_model_matches_market():
    from nba.plugins.ml.backtest import add_eval_columns, probability_metrics
    df = add_eval_columns(_make_backtest_df(n_dates=3))
    df["model_prob"] = df["fair_prob_over"]
    metrics = probability_metrics(df, by=None)
    assert abs(metrics["log_loss_delta"].iloc[0]) < 1e-12
    assert metrics["n_props"].iloc[0] == round(df["weight"].sum())
