# nba/tests/unit/ml/test_train.py
import numpy as np
import pandas as pd


def _make_synthetic_df(n=200, seed=42):
    """Minimal labeled feature DataFrame for testing."""
    rng = np.random.default_rng(seed)
    prop_types = ["player_points", "player_rebounds", "player_assists", "player_threes"]
    dates = pd.date_range("2025-10-01", periods=n, freq="D")
    return pd.DataFrame({
        "player_id":       rng.integers(1, 20, n),
        "player_name":     ["Player A"] * n,
        "game_date":       dates.strftime("%Y-%m-%d"),
        "prop_type":       rng.choice(prop_types, n),
        "bookmaker":       ["draftkings"] * n,
        "line":            rng.uniform(10, 40, n),
        "implied_prob_over": rng.uniform(0.4, 0.6, n),
        "line_movement":   rng.uniform(-2, 2, n),
        "rolling_avg_5g":  rng.uniform(10, 40, n),
        "rolling_avg_10g": rng.uniform(10, 40, n),
        "rolling_avg_20g": rng.uniform(10, 40, n),
        "rolling_std_10g": rng.uniform(1, 8, n),
        "is_home":         rng.choice([True, False, None], n),
        "rest_days":       rng.integers(1, 7, n).astype(float),
        "actual_result":   rng.integers(0, 2, n),
        "actual_stat_value": rng.uniform(5, 50, n),
    })


def test_prepare_features_encodes_prop_type():
    from nba.plugins.ml.train import prepare_features
    df = _make_synthetic_df(50)
    X, y, _ = prepare_features(df)
    assert "prop_type_encoded" in X.columns
    assert X["prop_type_encoded"].dtype in (int, np.int64, np.int32)


def test_prepare_features_fills_na():
    from nba.plugins.ml.train import prepare_features, FEATURES
    df = _make_synthetic_df(50)
    df.loc[0, "rolling_avg_5g"] = None
    df.loc[1, "rest_days"] = None
    X, y, _ = prepare_features(df)
    assert not X[FEATURES].isnull().any().any()


def test_load_training_data_filters_unlabeled(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from nba.plugins.ml.train import load_training_data

    df_labeled   = _make_synthetic_df(10)
    df_unlabeled = _make_synthetic_df(5)
    df_unlabeled["actual_result"] = None

    pq.write_table(pa.Table.from_pandas(df_labeled),   tmp_path / "features_2026-01-01.parquet")
    pq.write_table(pa.Table.from_pandas(df_unlabeled), tmp_path / "features_2026-01-02.parquet")

    result = load_training_data(str(tmp_path))
    assert len(result) == 10
    assert result["actual_result"].notna().all()
