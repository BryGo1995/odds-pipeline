# nba/plugins/ml/backtest.py
"""
Walk-forward backtest for the NBA per-prop-type models.

Replays the production training recipe (train.py) fold by fold: each fold
trains on every labeled date before it, calibrates on the last
calibration_days of that history, and predicts the fold. Nothing a fold is
scored on was seen during fitting or calibration.

Predictions are compared against the de-vigged market probability and run
through the production pick rule (top-N by edge, slots allocated per prop
type), settled at the posted Over price.

Feature rows are duplicated per bookmaker/line, so every metric weights rows
so that each (player, game_date, prop_type) counts once.

Run against live data with the Airflow image (see README "Backtesting the
model"), capped to a few cores. XGBoost grabs every core, so running this
inside the scheduler starves the live DAGs:
    python -m nba.plugins.ml.backtest [--fold-days 7] [--min-train-dates 14] [--calibration-days 2] [--out preds.csv]
"""
import argparse
import logging

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score

from nba.plugins.ml.score import TOP_N, _allocate_slots
from nba.plugins.ml.train import (
    FEATURES_DIR,
    PER_PROP_FEATURES,
    VALIDATION_DAYS,
    XGB_PARAMS,
    _CalibratedModel,
    load_training_data,
    prepare_features,
)
from shared.plugins.odds_math import american_odds_to_implied_prob

log = logging.getLogger(__name__)

GROUP_KEYS = ["player_id", "game_date", "prop_type"]
MIN_TRAIN_ROWS = 50  # matches train_model's per-prop minimum
_EPS = 1e-6


def attach_market_probs(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Add implied_prob_under and fair_prob_over (Over probability with the vig
    removed) from the latest Over/Under snapshot in player_props. Rows with
    no matching Under at the same line get NaN.
    """
    query = """
        SELECT DISTINCT ON (pp.nba_player_id, pp.game_id, pp.bookmaker, pp.prop_type)
            pp.nba_player_id                                       AS player_id,
            (g.commence_time AT TIME ZONE 'America/New_York')::date AS game_date,
            pp.prop_type,
            pp.bookmaker,
            pp.point                                               AS line,
            pp.price                                               AS under_price
        FROM player_props pp
        JOIN games g ON pp.game_id = g.game_id
        WHERE pp.nba_player_id IS NOT NULL
          AND pp.outcome = 'Under'
        ORDER BY pp.nba_player_id, pp.game_id, pp.bookmaker, pp.prop_type, pp.last_update DESC
    """
    with conn.cursor() as cur:
        cur.execute(query)
        unders = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])
    if unders.empty:
        df["implied_prob_under"] = np.nan
    else:
        unders["game_date"] = pd.to_datetime(unders["game_date"])
        unders["line"] = unders["line"].astype(float)
        unders["implied_prob_under"] = unders["under_price"].apply(american_odds_to_implied_prob)
        df["line"] = df["line"].astype(float)
        df = df.merge(
            unders.drop(columns="under_price"),
            on=["player_id", "game_date", "prop_type", "bookmaker", "line"],
            how="left",
        )
    df["fair_prob_over"] = df["implied_prob_over"] / (df["implied_prob_over"] + df["implied_prob_under"])
    return df


def add_eval_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add weight (each player/date/prop counts once) and push (stat landed on the line)."""
    df = df.copy()
    df["weight"] = 1.0 / df.groupby(GROUP_KEYS)["line"].transform("size")
    df["push"] = df["actual_stat_value"].astype(float) == df["line"].astype(float)
    return df


def make_folds(dates, fold_days: int, min_train_dates: int) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Split sorted unique game dates into [start, end) test folds of fold_days
    calendar days, starting once min_train_dates dates are available to train
    on. Calendar gaps (off-season) are skipped rather than producing empty folds.
    """
    dates = sorted(pd.to_datetime(pd.Series(dates)).unique())
    if len(dates) <= min_train_dates:
        return []
    folds = []
    start = dates[min_train_dates]
    while start is not None:
        end = start + pd.Timedelta(days=fold_days)
        folds.append((start, end))
        start = next((d for d in dates if d >= end), None)
    return folds


def fit_recipe(history: pd.DataFrame, calibration_days: int = VALIDATION_DAYS):
    """
    Fit the train_model recipe on history: XGBoost on all but the last
    calibration_days, isotonic calibration on those days. Returns None when
    there isn't enough data to train.
    """
    cutoff = history["game_date"].max() - pd.Timedelta(days=calibration_days)
    fit_df = history[history["game_date"] <= cutoff]
    cal_df = history[history["game_date"] > cutoff]
    if len(fit_df) < MIN_TRAIN_ROWS or fit_df["actual_result"].nunique() < 2:
        return None

    X_fit, y_fit, _ = prepare_features(fit_df, features=PER_PROP_FEATURES)
    model = xgb.XGBClassifier(**XGB_PARAMS)
    model.fit(X_fit, y_fit)

    if cal_df.empty:
        return model
    X_cal, y_cal, _ = prepare_features(cal_df, features=PER_PROP_FEATURES)
    classes, counts = np.unique(y_cal, return_counts=True)
    if len(classes) < 2 or counts.min() < 2:
        return model
    iso = IsotonicRegression(y_min=0, y_max=1, out_of_bounds="clip")
    iso.fit(model.predict_proba(X_cal)[:, 1], y_cal)
    return _CalibratedModel(model, iso)


def run_backtest(
    df: pd.DataFrame,
    fold_days: int = 7,
    min_train_dates: int = 14,
    calibration_days: int = VALIDATION_DAYS,
) -> pd.DataFrame:
    """
    Walk-forward predictions for every labeled row in a test fold.
    Returns the test rows with fold_start and model_prob columns added.
    """
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])
    folds = make_folds(df["game_date"], fold_days, min_train_dates)

    out = []
    for fold_start, fold_end in folds:
        for prop_type, prop_df in df.groupby("prop_type"):
            history = prop_df[prop_df["game_date"] < fold_start]
            test = prop_df[(prop_df["game_date"] >= fold_start) & (prop_df["game_date"] < fold_end)]
            if test.empty:
                continue
            model = fit_recipe(history, calibration_days)
            if model is None:
                log.info("Skipping %s fold %s: not enough history", prop_type, fold_start.date())
                continue
            X_test, _, _ = prepare_features(test, features=PER_PROP_FEATURES)
            test = test.assign(fold_start=fold_start, model_prob=model.predict_proba(X_test)[:, 1])
            out.append(test)

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def _weighted_log_loss(y, p, w) -> float:
    p = np.clip(np.asarray(p, dtype=float), _EPS, 1 - _EPS)
    y = np.asarray(y, dtype=float)
    return float(np.average(-(y * np.log(p) + (1 - y) * np.log(1 - p)), weights=w))


def _weighted_brier(y, p, w) -> float:
    return float(np.average((np.asarray(p, dtype=float) - np.asarray(y, dtype=float)) ** 2, weights=w))


def _weighted_auc(y, p, w) -> float:
    if len(np.unique(y)) < 2:
        return float("nan")
    return float(roc_auc_score(y, p, sample_weight=w))


def probability_metrics(preds: pd.DataFrame, by: str | None = "prop_type") -> pd.DataFrame:
    """
    Model vs de-vigged market on rows with a fair price and a decided result
    (pushes excluded). Lower log loss / Brier than the market means the model
    adds information beyond the line.
    """
    rows = preds[preds["fair_prob_over"].notna() & ~preds["push"]]
    groups = rows.groupby(by) if by else [("all", rows)]
    records = []
    for key, g in groups:
        y, w = g["actual_result"].astype(int), g["weight"]
        records.append({
            "scope":              key,
            "n_props":            round(float(w.sum())),
            "hit_rate":           float(np.average(y, weights=w)),
            "mean_model_prob":    float(np.average(g["model_prob"], weights=w)),
            "mean_fair_prob":     float(np.average(g["fair_prob_over"], weights=w)),
            "log_loss_model":     _weighted_log_loss(y, g["model_prob"], w),
            "log_loss_market":    _weighted_log_loss(y, g["fair_prob_over"], w),
            "brier_model":        _weighted_brier(y, g["model_prob"], w),
            "brier_market":       _weighted_brier(y, g["fair_prob_over"], w),
            "auc_model":          _weighted_auc(y, g["model_prob"], w),
            "auc_market":         _weighted_auc(y, g["fair_prob_over"], w),
        })
    result = pd.DataFrame(records)
    if not result.empty:
        result["log_loss_delta"] = result["log_loss_model"] - result["log_loss_market"]
    return result


def settle(picks: pd.DataFrame) -> pd.DataFrame:
    """Add profit in units for a 1-unit Over bet at the posted price (push returns the stake)."""
    picks = picks.copy()
    decimal_odds = 1.0 / picks["implied_prob_over"]
    picks["profit"] = np.where(
        picks["push"], 0.0,
        np.where(picks["actual_result"] == 1, decimal_odds - 1.0, -1.0),
    )
    return picks


def replay_picks(preds: pd.DataFrame, top_n: int = TOP_N) -> pd.DataFrame:
    """
    Apply score()'s pick rule to each game date: edge = model_prob - implied
    Over probability, top-N slots allocated across prop types by _allocate_slots.
    Returns the settled picks.
    """
    picks = []
    for _, day in preds.assign(edge=preds["model_prob"] - preds["implied_prob_over"]).groupby("game_date"):
        ranked = {pt: g.sort_values("edge", ascending=False) for pt, g in day.groupby("prop_type")}
        top_edges = {pt: float(g["edge"].iloc[0]) for pt, g in ranked.items()}
        allocation = _allocate_slots(sorted(ranked), top_edges, top_n)
        picks.extend(g.head(allocation[pt]) for pt, g in ranked.items())
    return settle(pd.concat(picks, ignore_index=True)) if picks else pd.DataFrame()


def betting_summary(picks: pd.DataFrame, by: str | None = None) -> pd.DataFrame:
    """Hit rate (pushes excluded), mean implied probability, units won and ROI per 1-unit bet."""
    groups = picks.groupby(by) if by else [("all", picks)]
    records = []
    for key, g in groups:
        decided = g[~g["push"]]
        records.append({
            "scope":           key,
            "bets":            len(g),
            "hit_rate":        float(decided["actual_result"].mean()) if len(decided) else float("nan"),
            "mean_implied":    float(g["implied_prob_over"].mean()),
            "mean_model_prob": float(g["model_prob"].mean()),
            "units":           float(g["profit"].sum()),
            "roi":             float(g["profit"].mean()),
        })
    return pd.DataFrame(records)


def main(argv=None):
    from shared.plugins.db_client import get_data_db_conn

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--features-dir", default=FEATURES_DIR)
    parser.add_argument("--fold-days", type=int, default=7)
    parser.add_argument("--min-train-dates", type=int, default=14)
    parser.add_argument("--calibration-days", type=int, default=VALIDATION_DAYS)
    parser.add_argument("--out", help="write per-row test predictions to this CSV")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING)

    conn = get_data_db_conn()
    try:
        df = attach_market_probs(conn, load_training_data(args.features_dir))
    finally:
        conn.close()
    df = add_eval_columns(df)

    preds = run_backtest(df, args.fold_days, args.min_train_dates, args.calibration_days)
    if preds.empty:
        print("No folds to evaluate — not enough labeled dates.")
        return

    picks = replay_picks(preds)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 30)
    pd.set_option("display.float_format", "{:.4f}".format)

    print(f"Walk-forward: {preds['fold_start'].nunique()} folds of {args.fold_days}d, "
          f"{preds['game_date'].min().date()} → {preds['game_date'].max().date()}, "
          f"calibration_days={args.calibration_days}, "
          f"{len(preds)} rows / {round(preds['weight'].sum())} unique props, "
          f"{preds['fair_prob_over'].isna().mean():.1%} without a fair price, "
          f"{preds['push'].mean():.1%} pushes\n")
    print("Probability quality vs de-vigged market (log_loss_delta < 0 = model beats market)")
    print(pd.concat([probability_metrics(preds, by=None), probability_metrics(preds)], ignore_index=True)
          .to_string(index=False), "\n")
    print(f"Top-{TOP_N} pick replay (1 unit per pick at posted Over price)")
    print(pd.concat([betting_summary(picks), betting_summary(picks, by="prop_type")], ignore_index=True)
          .to_string(index=False), "\n")
    print("Top-10 picks by fold")
    print(betting_summary(picks.assign(fold=picks["fold_start"].dt.date), by="fold").to_string(index=False))

    if args.out:
        preds.to_csv(args.out, index=False)
        print(f"\nWrote {len(preds)} predictions to {args.out}")


if __name__ == "__main__":
    main()
