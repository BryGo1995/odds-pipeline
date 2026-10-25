# nba/plugins/ml/backtest.py
"""
Walk-forward backtest for the NBA per-prop-type models.

Replays the production training recipe (train.py) fold by fold: each fold
fits MarketAnchoredModel on every labeled date before it and predicts the
fold. Nothing a fold is scored on was seen during fitting, including the
medians used to fill missing values.

Predictions are compared against the de-vigged market probability and run
through the production pick rule (better side of each prop by edge, top-N
slots allocated per prop type), settled at the posted price of the side
picked. The replay is also run Over-only for comparison (#45).

Feature rows are duplicated per bookmaker/line, so every metric weights rows
so that each (player, game_date, prop_type) counts once.

--importance adds walk-forward permutation importance: each fold's model is
scored on its own test rows with one feature shuffled, so importance is
measured out of sample with the same folds as the backtest. --ablate re-runs
the backtest with each feature (or group) left out and compares the result
with the full feature set; this is the keep/drop test for features (#36).

Run against live data with the Airflow image (see README "Backtesting the
model"), capped to a few cores. XGBoost grabs every core, so running this
inside the scheduler starves the live DAGs:
    python -m nba.plugins.ml.backtest [--fold-days 7] [--min-train-dates 14]
        [--importance] [--ablate] [--drop-features a,b] [--out preds.csv]
"""
import argparse
import logging

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

from nba.plugins.ml.score import TOP_N, _allocate_slots, choose_side
from nba.plugins.ml.train import (
    FEATURES_DIR,
    PER_PROP_FEATURES,
    MarketAnchoredModel,
    load_training_data,
    prepare_features,
)
from shared.plugins.odds_math import american_odds_to_implied_prob

log = logging.getLogger(__name__)

GROUP_KEYS = ["player_id", "game_date", "prop_type"]
MIN_TRAIN_ROWS = 50  # matches train_model's per-prop minimum
_EPS = 1e-6

# Features left out together by --ablate, on top of one run per single feature
ABLATION_GROUPS = {
    "rolling_avg_5g+20g": ["rolling_avg_5g", "rolling_avg_20g"],
    "all_rolling":        ["rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g", "rolling_std_10g"],
}


def attach_market_probs(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Add implied_prob_under and fair_prob_over (Over probability with the vig
    removed) from the latest Under snapshot at the same book and line in
    player_props, as build_features does. Rows with no matching Under get NaN.

    Recomputed from player_props even when the feature files carry these
    columns, because files built before #45 don't.
    """
    query = """
        SELECT DISTINCT ON (pp.nba_player_id, pp.game_id, pp.bookmaker, pp.prop_type, pp.point)
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
        ORDER BY pp.nba_player_id, pp.game_id, pp.bookmaker, pp.prop_type, pp.point, pp.last_update DESC
    """
    with conn.cursor() as cur:
        cur.execute(query)
        unders = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

    df = df.drop(columns=["implied_prob_under", "fair_prob_over"], errors="ignore")
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


def fit_recipe(history: pd.DataFrame, features: list[str] = PER_PROP_FEATURES):
    """
    Fit the train_model recipe (MarketAnchoredModel) on history. Returns None
    when there isn't enough data to train.
    """
    if len(history) < MIN_TRAIN_ROWS or history["actual_result"].nunique() < 2:
        return None
    X, y, _ = prepare_features(history, features=features)
    return MarketAnchoredModel().fit(X, y)


def _walk_forward(df, fold_days, min_train_dates, features):
    """
    Yield (fold_start, prop_type, model, test, X_test) for every fold and prop
    type with enough history, fitting the recipe on dates before the fold only.
    """
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])
    for fold_start, fold_end in make_folds(df["game_date"], fold_days, min_train_dates):
        for prop_type, prop_df in df.groupby("prop_type"):
            history = prop_df[prop_df["game_date"] < fold_start]
            test = prop_df[(prop_df["game_date"] >= fold_start) & (prop_df["game_date"] < fold_end)]
            if test.empty:
                continue
            model = fit_recipe(history, features)
            if model is None:
                log.info("Skipping %s fold %s: not enough history", prop_type, fold_start.date())
                continue
            X_test, _, _ = prepare_features(test, features=features)
            yield fold_start, prop_type, model, test, X_test


def run_backtest(
    df: pd.DataFrame,
    fold_days: int = 7,
    min_train_dates: int = 14,
    features: list[str] = PER_PROP_FEATURES,
) -> pd.DataFrame:
    """
    Walk-forward predictions for every labeled row in a test fold.
    Returns the test rows with fold_start and model_prob columns added.
    """
    out = [
        test.assign(fold_start=fold_start, model_prob=model.predict_proba(X_test)[:, 1])
        for fold_start, _, model, test, X_test
        in _walk_forward(df, fold_days, min_train_dates, features)
    ]
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def permutation_importance(
    df: pd.DataFrame,
    fold_days: int = 7,
    min_train_dates: int = 14,
    features: list[str] = PER_PROP_FEATURES,
    n_repeats: int = 5,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Out-of-sample permutation importance: for each fold and prop type, the
    increase in weighted log loss on the fold's test rows when one feature is
    shuffled (mean over n_repeats). Pushes are excluded. Positive = the model
    relies on the feature; around zero or negative = it adds nothing.
    Returns one row per (fold_start, prop_type, feature).
    """
    rng = np.random.default_rng(seed)
    records = []
    for fold_start, prop_type, model, test, X_test in _walk_forward(
        df, fold_days, min_train_dates, features
    ):
        keep = ~test["push"].to_numpy()
        X, y, w = X_test[keep], test["actual_result"].astype(int)[keep], test["weight"][keep]
        if len(X) == 0:
            continue
        base = _weighted_log_loss(y, model.predict_proba(X)[:, 1], w)
        for feature in features:
            losses = []
            for _ in range(n_repeats):
                shuffled = X.copy()
                shuffled[feature] = rng.permutation(shuffled[feature].to_numpy())
                losses.append(_weighted_log_loss(y, model.predict_proba(shuffled)[:, 1], w))
            records.append({
                "fold_start":     fold_start,
                "prop_type":      prop_type,
                "feature":        feature,
                "n_props":        float(w.sum()),
                "delta_log_loss": float(np.mean(losses)) - base,
            })
    return pd.DataFrame(records)


def importance_summary(records: pd.DataFrame, by: str | None = "prop_type") -> pd.DataFrame:
    """
    Aggregate permutation_importance across folds, weighting each fold by its
    props: mean delta log loss, its standard error across folds, and the share
    of folds where shuffling the feature hurt.
    """
    keys = ["feature"] if by is None else [by, "feature"]
    rows = []
    for key, g in records.groupby(keys):
        w = g["n_props"] / g["n_props"].sum()
        mean = float((g["delta_log_loss"] * w).sum())
        se = float(g["delta_log_loss"].std(ddof=1) / np.sqrt(len(g))) if len(g) > 1 else float("nan")
        rows.append({
            **dict(zip(keys, key if isinstance(key, tuple) else (key,))),
            "folds":            len(g),
            "delta_log_loss":   mean,
            "se":               se,
            "share_folds_hurt": float((g["delta_log_loss"] > 0).mean()),
        })
    result = pd.DataFrame(rows)
    sort = ["delta_log_loss"] if by is None else [by, "delta_log_loss"]
    return result.sort_values(sort, ascending=[True] * (len(sort) - 1) + [False], ignore_index=True)


def ablation_variants(features: list[str] = PER_PROP_FEATURES) -> dict[str, list[str]]:
    """Feature sets for --ablate: the full set, minus each feature, minus each ABLATION_GROUPS group."""
    variants = {"full": list(features)}
    for feature in features:
        variants[f"-{feature}"] = [f for f in features if f != feature]
    for name, group in ABLATION_GROUPS.items():
        if set(group) <= set(features):
            variants[f"-{name}"] = [f for f in features if f not in group]
    return variants


def ablation(
    df: pd.DataFrame,
    fold_days: int = 7,
    min_train_dates: int = 14,
    features: list[str] = PER_PROP_FEATURES,
) -> pd.DataFrame:
    """
    Re-run the backtest once per ablation_variants() feature set. Deltas are
    against the full set: d_log_loss > 0 means leaving the feature out made
    the model worse, i.e. it earns its place.
    """
    rows = []
    for name, feature_set in ablation_variants(features).items():
        preds = run_backtest(df, fold_days, min_train_dates, feature_set)
        overall = probability_metrics(preds, by=None).iloc[0]
        picks = betting_summary(replay_picks(preds)).iloc[0]
        rows.append({
            "variant":        name,
            "log_loss_model": overall["log_loss_model"],
            "log_loss_delta": overall["log_loss_delta"],
            "auc_model":      overall["auc_model"],
            "pick_roi":       picks["roi"],
        })
    result = pd.DataFrame(rows)
    full = result.iloc[0]
    result["d_log_loss"] = result["log_loss_model"] - full["log_loss_model"]
    result["d_auc"] = result["auc_model"] - full["auc_model"]
    return result


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
    """
    Add won and profit in units for a 1-unit bet on each pick's side
    (outcome, Over when absent) at that side's posted price (pick_implied,
    implied_prob_over when absent). A push returns the stake.
    """
    picks = picks.copy()
    under = picks["outcome"].eq("Under") if "outcome" in picks else pd.Series(False, index=picks.index)
    implied = picks["pick_implied"] if "pick_implied" in picks else picks["implied_prob_over"]
    picks["won"] = np.where(under, picks["actual_result"] == 0, picks["actual_result"] == 1)
    picks["profit"] = np.where(
        picks["push"], 0.0,
        np.where(picks["won"], 1.0 / implied - 1.0, -1.0),
    )
    return picks


def replay_picks(preds: pd.DataFrame, top_n: int = TOP_N, unders: bool = True) -> pd.DataFrame:
    """
    Apply score()'s pick rule to each game date: choose_side picks the better
    side of each prop (Over only when unders=False), then top-N slots are
    allocated across prop types by _allocate_slots. Returns the settled picks.
    """
    sided = preds if unders else preds.drop(columns="implied_prob_under", errors="ignore")
    picks = []
    for _, day in choose_side(sided).groupby("game_date"):
        ranked = {pt: g.sort_values("edge", ascending=False) for pt, g in day.groupby("prop_type")}
        top_edges = {pt: float(g["edge"].iloc[0]) for pt, g in ranked.items()}
        allocation = _allocate_slots(sorted(ranked), top_edges, top_n)
        picks.extend(g.head(allocation[pt]) for pt, g in ranked.items())
    return settle(pd.concat(picks, ignore_index=True)) if picks else pd.DataFrame()


def betting_summary(picks: pd.DataFrame, by: str | None = None) -> pd.DataFrame:
    """
    Hit rate (pushes excluded), mean implied and model probability of the side
    picked, units won and ROI per 1-unit bet.
    """
    groups = picks.groupby(by) if by else [("all", picks)]
    records = []
    for key, g in groups:
        decided = g[~g["push"]]
        records.append({
            "scope":           key,
            "bets":            len(g),
            "hit_rate":        float(decided["won"].mean()) if len(decided) else float("nan"),
            "mean_implied":    float(g["pick_implied"].mean()),
            "mean_model_prob": float(g["pick_prob"].mean()),
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
    parser.add_argument("--drop-features", default="",
                        help="comma-separated features to leave out of the model")
    parser.add_argument("--importance", action="store_true",
                        help="add walk-forward permutation importance per prop type")
    parser.add_argument("--ablate", action="store_true",
                        help="re-run once per left-out feature/group and compare with the full set")
    parser.add_argument("--out", help="write per-row test predictions to this CSV")
    args = parser.parse_args(argv)

    dropped = {f for f in args.drop_features.split(",") if f}
    unknown = dropped - set(PER_PROP_FEATURES)
    if unknown:
        parser.error(f"unknown features: {', '.join(sorted(unknown))}")
    features = [f for f in PER_PROP_FEATURES if f not in dropped]
    logging.basicConfig(level=logging.WARNING)

    conn = get_data_db_conn()
    try:
        df = attach_market_probs(conn, load_training_data(args.features_dir))
    finally:
        conn.close()
    df = add_eval_columns(df)

    preds = run_backtest(df, args.fold_days, args.min_train_dates, features)
    if preds.empty:
        print("No folds to evaluate — not enough labeled dates.")
        return

    picks = replay_picks(preds)
    over_only = replay_picks(preds, unders=False)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 30)
    pd.set_option("display.float_format", "{:.4f}".format)

    if dropped:
        print(f"Features dropped: {', '.join(sorted(dropped))}")
    print(f"Walk-forward: {preds['fold_start'].nunique()} folds of {args.fold_days}d, "
          f"{preds['game_date'].min().date()} → {preds['game_date'].max().date()}, "
          f"{len(preds)} rows / {round(preds['weight'].sum())} unique props, "
          f"{preds['fair_prob_over'].isna().mean():.1%} without a fair price, "
          f"{preds['push'].mean():.1%} pushes\n")
    print("Probability quality vs de-vigged market (log_loss_delta < 0 = model beats market)")
    print(pd.concat([probability_metrics(preds, by=None), probability_metrics(preds)], ignore_index=True)
          .to_string(index=False), "\n")
    print(f"Top-{TOP_N} pick replay, better side of each prop (1 unit per pick at the posted price)")
    print(pd.concat([betting_summary(picks), betting_summary(picks, by="outcome"),
                     betting_summary(picks, by="prop_type")], ignore_index=True)
          .to_string(index=False), "\n")
    print(f"Top-{TOP_N} pick replay, Over only (the pre-#45 rule)")
    print(pd.concat([betting_summary(over_only), betting_summary(over_only, by="prop_type")], ignore_index=True)
          .to_string(index=False), "\n")
    print("Top-10 picks by fold")
    print(betting_summary(picks.assign(fold=picks["fold_start"].dt.date), by="fold").to_string(index=False))

    fold_args = (args.fold_days, args.min_train_dates, features)
    if args.importance:
        records = permutation_importance(df, *fold_args)
        print("\nPermutation importance (out of sample): log loss increase when the feature is shuffled")
        print(importance_summary(records, by=None).to_string(index=False), "\n")
        print(importance_summary(records).to_string(index=False))

    if args.ablate:
        print("\nAblation: backtest with each feature/group left out "
              "(d_log_loss > 0 = worse without it, so keep)")
        print(ablation(df, *fold_args).to_string(index=False))

    if args.out:
        preds.to_csv(args.out, index=False)
        print(f"\nWrote {len(preds)} predictions to {args.out}")


if __name__ == "__main__":
    main()
