# nba/tests/unit/ml/test_backtest.py
import numpy as np
import pytest
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

    def recording_fit(history, features):
        seen.append(history["game_date"].max())
        return real_fit(history, features)

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


def _with_eval_columns(df):
    from nba.plugins.ml.backtest import add_eval_columns
    return add_eval_columns(df)


def test_run_backtest_uses_given_feature_set():
    import nba.plugins.ml.backtest as bt
    features_seen = []
    real_fit = bt.fit_recipe

    def recording_fit(history, features):
        features_seen.append(tuple(features))
        return real_fit(history, features)

    with patch.object(bt, "fit_recipe", side_effect=recording_fit):
        preds = bt.run_backtest(_make_backtest_df(),
                                features=["implied_prob_over", "rest_days"])
    assert not preds.empty
    assert set(features_seen) == {("implied_prob_over", "rest_days")}


def test_permutation_importance_flags_the_feature_the_label_depends_on():
    from nba.plugins.ml.backtest import importance_summary, permutation_importance
    df = _make_backtest_df(n_dates=30, rows_per_date=60)
    # Label driven entirely by rest_days; everything else is noise
    df["actual_result"] = (df["rest_days"] >= 2).astype(int)
    df["actual_stat_value"] = df["line"] + np.where(df["actual_result"] == 1, 1.0, -1.0)
    df = _with_eval_columns(df)

    # Tests the importance measurement, not the recipe: the production trees are
    # deliberately too small to learn much from this little synthetic data.
    from nba.plugins.ml.train import XGB_PARAMS
    learner = {"n_estimators": 50, "max_depth": 2, "learning_rate": 0.3, "min_child_weight": 1}
    with patch.dict(XGB_PARAMS, learner):
        records = permutation_importance(df, n_repeats=2)
    assert set(records.columns) == {"fold_start", "prop_type", "feature", "n_props", "delta_log_loss"}

    overall = importance_summary(records, by=None)
    assert overall.iloc[0]["feature"] == "rest_days"
    assert overall.iloc[0]["delta_log_loss"] > 0.1
    assert overall.iloc[0]["share_folds_hurt"] == 1.0
    noise = overall[overall["feature"] != "rest_days"]["delta_log_loss"]
    assert (noise.abs() < overall.iloc[0]["delta_log_loss"] / 10).all()


def test_permutation_importance_excludes_pushes():
    from nba.plugins.ml.backtest import permutation_importance
    df = _with_eval_columns(_make_backtest_df())
    df["push"] = True
    assert permutation_importance(df, n_repeats=1).empty


def test_importance_summary_weights_folds_by_props():
    from nba.plugins.ml.backtest import importance_summary
    records = pd.DataFrame({
        "fold_start":     pd.to_datetime(["2026-03-15", "2026-03-22"] * 2),
        "prop_type":      ["player_points"] * 4,
        "feature":        ["a", "a", "b", "b"],
        "n_props":        [300.0, 100.0, 300.0, 100.0],
        "delta_log_loss": [0.01, -0.01, 0.0, 0.0],
    })
    summary = importance_summary(records)
    a = summary[summary["feature"] == "a"].iloc[0]
    assert a["delta_log_loss"] == pytest.approx(0.005)   # (0.01*300 - 0.01*100) / 400
    assert a["share_folds_hurt"] == 0.5
    assert a["folds"] == 2
    assert list(summary["feature"]) == ["a", "b"]         # most important first


def test_ablation_variants_drop_each_feature_and_group():
    from nba.plugins.ml.backtest import ABLATION_GROUPS, ablation_variants
    from nba.plugins.ml.train import PER_PROP_FEATURES
    variants = ablation_variants()
    assert variants["full"] == PER_PROP_FEATURES
    for feature in PER_PROP_FEATURES:
        assert feature not in variants[f"-{feature}"]
        assert len(variants[f"-{feature}"]) == len(PER_PROP_FEATURES) - 1
    for name, group in ABLATION_GROUPS.items():
        assert not set(group) & set(variants[f"-{name}"])
    # Groups whose features aren't all in the set are skipped
    assert "-all_rolling" not in ablation_variants(["implied_prob_over", "rolling_avg_10g"])


def test_ablation_reports_deltas_against_full_set():
    from nba.plugins.ml.backtest import ablation
    df = _with_eval_columns(_make_backtest_df())
    result = ablation(df, features=["implied_prob_over", "rest_days", "is_home"])
    assert list(result["variant"]) == ["full", "-implied_prob_over", "-rest_days", "-is_home"]
    full = result.iloc[0]
    assert full["d_log_loss"] == 0 and full["d_auc"] == 0
    np.testing.assert_allclose(result["d_log_loss"], result["log_loss_model"] - full["log_loss_model"])
