"""
Scoring module for NBA player prop ML model.

score(conn, game_date) loads per-prop-type production models from the MLflow
registry, scores today's feature Parquet file, picks the better side (Over or
Under) of each prop, allocates top-10 slots evenly across prop types, and
writes ranked recommendations to Postgres.
"""
import logging
import os

import duckdb
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd

from nba.plugins.ml.train import PER_PROP_FEATURES, FEATURES_DIR, MODEL_NAME, MLFLOW_TRACKING_URI, prepare_features
from nba.plugins.transformers.features import PROP_STAT_MAP

log = logging.getLogger(__name__)

TOP_N = 10


def load_todays_features(game_date: str, features_dir: str = FEATURES_DIR) -> pd.DataFrame:
    """Load the Parquet feature file for game_date via DuckDB."""
    path = f"{features_dir}/props_features_{game_date}.parquet"
    conn = duckdb.connect()
    try:
        return conn.execute(f"SELECT * FROM read_parquet('{path}')").df()
    except Exception as exc:
        log.warning("Could not load features for %s from %s: %s", game_date, path, exc)
        return pd.DataFrame()
    finally:
        conn.close()


def choose_side(df: pd.DataFrame) -> pd.DataFrame:
    """
    Price both sides of each prop from the model's Over probability in
    df["model_prob"] and keep the side with the larger edge (#45). P(Under) is
    1 - P(Over): lines are half points, so there is no push to account for.
    The Under is only an option when the book posted one at the same line.

    Adds outcome ('Over'/'Under'), pick_prob and pick_implied (the chosen
    side's model and implied probabilities) and edge = pick_prob - pick_implied.
    """
    df = df.copy()
    p_over = df["model_prob"].astype(float)
    implied_over = df["implied_prob_over"].astype(float)
    implied_under = (df["implied_prob_under"].astype(float) if "implied_prob_under" in df
                     else pd.Series(np.nan, index=df.index))
    take_under = ((1 - p_over) - implied_under) > (p_over - implied_over)  # NaN Under -> False
    df["outcome"] = np.where(take_under, "Under", "Over")
    df["pick_prob"] = np.where(take_under, 1 - p_over, p_over)
    df["pick_implied"] = np.where(take_under, implied_under, implied_over)
    df["edge"] = df["pick_prob"] - df["pick_implied"]
    return df


def _allocate_slots(active_prop_types: list[str], top_edges: dict[str, float], total: int = TOP_N) -> dict[str, int]:
    """
    Distribute total slots evenly across active prop types.
    Returns {prop_type: num_slots}. Extra slots go to prop types
    with the highest top-pick edge.

    Args:
        active_prop_types: list of prop types that have scored data
        top_edges: {prop_type: edge_of_best_pick} for tiebreaking extra slots
        total: total slots to allocate (default 10)
    """
    n = len(active_prop_types)
    if n == 0:
        return {}
    base = total // n
    remainder = total % n
    # Extra slots go to prop types with highest top-pick edge
    ranked = sorted(active_prop_types, key=lambda pt: top_edges.get(pt, 0), reverse=True)
    return {
        pt: base + (1 if pt in ranked[:remainder] else 0)
        for pt in active_prop_types
    }


def score(conn, game_date: str, features_dir: str = FEATURES_DIR) -> None:
    """
    Load per-prop-type production models from MLflow, score today's features,
    allocate top-10 slots evenly, and write ranked recommendations to Postgres.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()

    df = load_todays_features(game_date, features_dir)
    if df.empty:
        log.warning("No features for %s — skipping scoring (likely a no-games day)", game_date)
        return

    scored_subsets = []
    active_prop_types = []

    for prop_type in sorted(PROP_STAT_MAP.keys()):
        subset = df[df["prop_type"] == prop_type].copy()
        if subset.empty:
            continue

        model_name = f"{MODEL_NAME}_{prop_type}"
        model_uri = f"models:/{model_name}@production"
        try:
            model = mlflow.sklearn.load_model(model_uri)
        except Exception as exc:
            log.warning("Could not load model %s: %s — skipping %s", model_uri, exc, prop_type)
            continue

        try:
            mv = client.get_model_version_by_alias(model_name, "production")
            model_version = mv.version
        except Exception:
            model_version = "unknown"

        X, _, _ = prepare_features(subset, features=PER_PROP_FEATURES)
        subset["model_prob"] = model.predict_proba(X)[:, 1]
        subset = choose_side(subset)
        subset["model_prob"] = subset["pick_prob"]
        subset["implied_prob"] = subset["pick_implied"]
        subset["model_version"] = model_version
        subset["game_date"] = game_date

        # Sort by edge descending within this prop type
        subset = subset.sort_values("edge", ascending=False).reset_index(drop=True)
        scored_subsets.append(subset)
        active_prop_types.append(prop_type)

    if not scored_subsets:
        raise ValueError(f"No prop types could be scored for {game_date}")

    # Allocate top-N slots — extra slots go to prop types with highest top-pick edge
    top_edges = {
        subset["prop_type"].iloc[0]: float(subset["edge"].iloc[0])
        for subset in scored_subsets
    }
    allocation = _allocate_slots(active_prop_types, top_edges, TOP_N)

    top_picks = []
    remaining = []
    for subset in scored_subsets:
        prop_type = subset["prop_type"].iloc[0]
        n_slots = allocation.get(prop_type, 0)
        top_picks.append(subset.head(n_slots))
        remaining.append(subset.iloc[n_slots:])

    top_df = pd.concat(top_picks, ignore_index=True)
    remaining_df = pd.concat(remaining, ignore_index=True)
    remaining_df = remaining_df.sort_values("edge", ascending=False).reset_index(drop=True)

    # Assign ranks: top-N allocation first, then remaining by edge
    all_df = pd.concat([top_df, remaining_df], ignore_index=True)
    all_df["rank"] = range(1, len(all_df) + 1)

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recommendations WHERE game_date = %s AND sport = 'NBA'",
            (game_date,),
        )
        for _, row in all_df.iterrows():
            cur.execute(
                """
                INSERT INTO recommendations
                    (player_name, prop_type, bookmaker, line, outcome,
                     model_prob, implied_prob, edge, rank, model_version, game_date, sport)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["player_name"],
                    row["prop_type"],
                    row["bookmaker"],
                    float(row["line"]),
                    row["outcome"],
                    float(row["model_prob"]),
                    float(row["implied_prob"]),
                    float(row["edge"]),
                    int(row["rank"]),
                    str(row["model_version"]),
                    game_date,
                    "NBA",
                ),
            )
    conn.commit()
