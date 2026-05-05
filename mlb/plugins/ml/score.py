# mlb/plugins/ml/score.py
"""
Scoring module for MLB batter-prop ML model.

score(conn, game_date) loads per-prop-type production models from MLflow,
scores today's feature parquet, allocates top-10 slots evenly across active
prop types, and writes ranked recommendations to Postgres with sport='MLB'.
"""
import logging

import duckdb
import mlflow
import mlflow.sklearn
import pandas as pd

from mlb.plugins.ml.train import (
    PER_PROP_FEATURES,
    FEATURES_DIR,
    MODEL_NAME,
    MLFLOW_TRACKING_URI,
    prepare_features,
)
from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP

log = logging.getLogger(__name__)

TOP_N = 10


def load_todays_features(game_date: str, features_dir: str = FEATURES_DIR) -> pd.DataFrame:
    """Load today's MLB Parquet feature file via DuckDB."""
    path = f"{features_dir}/mlb_props_features_{game_date}.parquet"
    conn = duckdb.connect()
    try:
        return conn.execute(f"SELECT * FROM read_parquet('{path}')").df()
    except Exception as exc:
        log.warning("Could not load features for %s from %s: %s", game_date, path, exc)
        return pd.DataFrame()
    finally:
        conn.close()


def _allocate_slots(active_prop_types: list[str], top_edges: dict[str, float], total: int = TOP_N) -> dict[str, int]:
    """
    Distribute total slots evenly across active prop types.
    Extra slots go to prop types with the highest top-pick edge.
    """
    n = len(active_prop_types)
    if n == 0:
        return {}
    base = total // n
    remainder = total % n
    ranked = sorted(active_prop_types, key=lambda pt: top_edges.get(pt, 0), reverse=True)
    return {
        pt: base + (1 if pt in ranked[:remainder] else 0)
        for pt in active_prop_types
    }


def score(conn, game_date: str, features_dir: str = FEATURES_DIR) -> None:
    """
    Load per-prop-type production models from MLflow, score today's features,
    allocate top-10 slots evenly, and write ranked recommendations to Postgres
    with sport='MLB'.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()

    df = load_todays_features(game_date, features_dir)
    if df.empty:
        raise ValueError(f"No feature file found for {game_date} in {features_dir}")

    scored_subsets = []
    active_prop_types = []

    for prop_type in sorted(MLB_PROP_STAT_MAP.keys()):
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

        X, _ = prepare_features(subset, features=PER_PROP_FEATURES)
        subset["model_prob"] = model.predict_proba(X)[:, 1]
        subset["outcome"] = "Over"
        subset["implied_prob"] = subset["implied_prob_over"]
        subset["edge"] = subset["model_prob"] - subset["implied_prob"]
        subset["model_version"] = model_version
        subset["game_date"] = game_date

        subset = subset.sort_values("edge", ascending=False).reset_index(drop=True)
        scored_subsets.append(subset)
        active_prop_types.append(prop_type)

    if not scored_subsets:
        raise ValueError(f"No prop types could be scored for {game_date}")

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

    all_df = pd.concat([top_df, remaining_df], ignore_index=True)
    all_df["rank"] = range(1, len(all_df) + 1)

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recommendations WHERE game_date = %s AND sport = 'MLB'",
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
                    "MLB",
                ),
            )
    conn.commit()
