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


def test_train_model_returns_run_id(tmp_path):
    """train_model() must return the MLflow run_id string."""
    from unittest.mock import MagicMock, patch
    from nba.plugins.ml.train import train_model

    import pyarrow as pa
    import pyarrow.parquet as pq
    df = _make_synthetic_df(200)
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "features_2026-01-01.parquet")

    mock_run = MagicMock()
    mock_run.info.run_id = "test-run-id-12345"
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    with patch("nba.plugins.ml.train.mlflow") as mock_mlflow, \
         patch("nba.plugins.ml.train._get_production_model_auc", return_value=None):
        mock_mlflow.start_run.return_value = mock_run
        mock_mlflow.sklearn.log_model = MagicMock()
        mock_mlflow.register_model = MagicMock()

        result = train_model(str(tmp_path))

    assert result == "test-run-id-12345"


def test_train_model_filters_by_prop_type(tmp_path):
    """train_model(prop_type=...) must only use rows of that prop type."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from unittest.mock import MagicMock, patch
    from nba.plugins.ml.train import train_model

    df = _make_synthetic_df(200)
    # Ensure enough of each prop type for training
    df.loc[:99, "prop_type"] = "player_points"
    df.loc[100:149, "prop_type"] = "player_rebounds"
    df.loc[150:, "prop_type"] = "player_assists"
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "features_2026-01-01.parquet")

    mock_run = MagicMock()
    mock_run.info.run_id = "test-run-points"
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    logged_params = {}
    def capture_params(params):
        logged_params.update(params)

    with patch("nba.plugins.ml.train.mlflow") as mock_mlflow, \
         patch("nba.plugins.ml.train._get_production_model_auc", return_value=None):
        mock_mlflow.start_run.return_value = mock_run
        mock_mlflow.sklearn.log_model = MagicMock()
        mock_mlflow.register_model = MagicMock()
        mock_mlflow.log_params = capture_params

        result = train_model(str(tmp_path), prop_type="player_points")

    assert result == "test-run-points"
    # Should have registered as nba_prop_model_player_points
    register_call = mock_mlflow.register_model.call_args
    assert "nba_prop_model_player_points" in str(register_call)


def test_train_model_uses_per_prop_features(tmp_path):
    """Per-prop-type training must NOT use prop_type_encoded feature."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from unittest.mock import MagicMock, patch
    from nba.plugins.ml.train import train_model, PER_PROP_FEATURES

    assert "prop_type_encoded" not in PER_PROP_FEATURES

    df = _make_synthetic_df(200)
    df["prop_type"] = "player_points"
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "features_2026-01-01.parquet")

    mock_run = MagicMock()
    mock_run.info.run_id = "test-run"
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    with patch("nba.plugins.ml.train.mlflow") as mock_mlflow, \
         patch("nba.plugins.ml.train._get_production_model_auc", return_value=None):
        mock_mlflow.start_run.return_value = mock_run
        mock_mlflow.sklearn.log_model = MagicMock()
        mock_mlflow.register_model = MagicMock()

        train_model(str(tmp_path), prop_type="player_points")

    # The logged feature importance should use PER_PROP_FEATURES (no prop_type_encoded)
    log_dict_call = mock_mlflow.log_dict.call_args
    importance_dict = log_dict_call.args[0]
    assert "prop_type_encoded" not in importance_dict
    assert "implied_prob_over" in importance_dict


def test_train_model_skips_insufficient_data(tmp_path):
    """train_model with prop_type should raise ValueError if <50 rows."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pytest
    from nba.plugins.ml.train import train_model

    df = _make_synthetic_df(30)
    df["prop_type"] = "player_points"
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "features_2026-01-01.parquet")

    with pytest.raises(ValueError, match="Insufficient"):
        train_model(str(tmp_path), prop_type="player_points")


def test_train_all_models_returns_run_ids(tmp_path):
    """train_all_models must return a dict of {prop_type: run_id}."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from unittest.mock import MagicMock, patch
    from nba.plugins.ml.train import train_all_models

    df = _make_synthetic_df(300)
    # Ensure each prop type has enough rows
    df.loc[:99, "prop_type"] = "player_points"
    df.loc[100:199, "prop_type"] = "player_rebounds"
    df.loc[200:, "prop_type"] = "player_assists"
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "features_2026-01-01.parquet")

    call_count = [0]
    def make_mock_run():
        mock_run = MagicMock()
        call_count[0] += 1
        mock_run.info.run_id = f"run-{call_count[0]}"
        mock_run.__enter__ = MagicMock(return_value=mock_run)
        mock_run.__exit__ = MagicMock(return_value=False)
        return mock_run

    with patch("nba.plugins.ml.train.mlflow") as mock_mlflow, \
         patch("nba.plugins.ml.train._get_production_model_auc", return_value=None):
        mock_mlflow.start_run.side_effect = lambda: make_mock_run()
        mock_mlflow.sklearn.log_model = MagicMock()
        mock_mlflow.register_model = MagicMock()

        result = train_all_models(str(tmp_path))

    assert isinstance(result, dict)
    assert "player_points" in result
    assert "player_rebounds" in result
    assert "player_assists" in result
    assert len(result) == 3
