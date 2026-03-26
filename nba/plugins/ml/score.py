"""
Scoring module for NBA player prop ML model.

score(conn, game_date) loads the production model from the MLflow registry,
scores today's feature Parquet file, and writes ranked recommendations to Postgres.
"""
import logging
import os

import duckdb
import mlflow
import mlflow.sklearn
import pandas as pd

from nba.plugins.ml.train import FEATURES, FEATURES_DIR, MODEL_NAME, MLFLOW_TRACKING_URI, prepare_features


def load_todays_features(game_date: str, features_dir: str = FEATURES_DIR) -> pd.DataFrame:
    """Load the Parquet feature file for game_date via DuckDB."""
    path = f"{features_dir}/props_features_{game_date}.parquet"
    conn = duckdb.connect()
    try:
        return conn.execute(f"SELECT * FROM read_parquet('{path}')").df()
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Could not load features for %s from %s: %s", game_date, path, exc
        )
        return pd.DataFrame()
    finally:
        conn.close()


def score(conn, game_date: str, features_dir: str = FEATURES_DIR) -> None:
    """
    Load production model from MLflow, score today's features, and write
    ranked recommendations to the recommendations Postgres table.

    Args:
        conn: psycopg2 connection to data Postgres
        game_date: ISO date string, e.g. "2026-03-26"
        features_dir: path to Parquet feature files (default from FEATURES_DIR env var)
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    df = load_todays_features(game_date, features_dir)
    if df.empty:
        raise ValueError(f"No feature file found for {game_date} in {features_dir}")

    model_uri = f"models:/{MODEL_NAME}/Production"
    model = mlflow.sklearn.load_model(model_uri)

    client = mlflow.tracking.MlflowClient()
    versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
    model_version = versions[0].version if versions else "unknown"

    X, _, _ = prepare_features(df)
    df["model_prob"]  = model.predict_proba(X)[:, 1]
    df["outcome"]     = "Over"
    df["implied_prob"] = df["implied_prob_over"]
    df["edge"]        = df["model_prob"] - df["implied_prob"]
    df["model_version"] = model_version
    df["game_date"]   = game_date

    df = df.sort_values("edge", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1

    with conn.cursor() as cur:
        cur.execute("DELETE FROM recommendations WHERE game_date = %s", (game_date,))
        for _, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO recommendations
                    (player_name, prop_type, bookmaker, line, outcome,
                     model_prob, implied_prob, edge, rank, model_version, game_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                ),
            )
    conn.commit()
