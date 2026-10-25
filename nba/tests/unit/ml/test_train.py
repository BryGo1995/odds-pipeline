# nba/tests/unit/ml/test_train.py
import numpy as np
import pandas as pd
import pytest


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


def test_prepare_features_leaves_numeric_na_for_the_model():
    """Filling with the frame's own median leaked test/scoring data (#44)."""
    from nba.plugins.ml.train import prepare_features
    df = _make_synthetic_df(50)
    df.loc[0, "rolling_avg_5g"] = None
    df.loc[1, "rest_days"] = None
    X, y, _ = prepare_features(df)
    assert np.isnan(X.loc[0, "rolling_avg_5g"])
    assert np.isnan(X.loc[1, "rest_days"])
    assert not X["is_home"].isnull().any()   # constant 0.5 fill, no leakage


def test_load_training_data_filters_unlabeled(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from nba.plugins.ml.train import load_training_data

    df_labeled   = _make_synthetic_df(10)
    df_unlabeled = _make_synthetic_df(5)
    df_unlabeled["actual_result"] = None

    pq.write_table(pa.Table.from_pandas(df_labeled),   tmp_path / "props_features_2026-01-01.parquet")
    pq.write_table(pa.Table.from_pandas(df_unlabeled), tmp_path / "props_features_2026-01-02.parquet")

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
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "props_features_2026-01-01.parquet")

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
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "props_features_2026-01-01.parquet")

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
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "props_features_2026-01-01.parquet")

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

    # Tree importances cover the non-market PER_PROP_FEATURES; the market is the anchor
    log_dict_call = mock_mlflow.log_dict.call_args
    importance_dict = log_dict_call.args[0]
    assert set(importance_dict) == set(PER_PROP_FEATURES) - {"implied_prob_over"}
    metrics = mock_mlflow.log_metrics.call_args.args[0]
    assert {"market_intercept", "market_slope", "log_loss", "log_loss_implied"} <= set(metrics)


def test_train_model_skips_insufficient_data(tmp_path):
    """train_model with prop_type should raise ValueError if <50 rows."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pytest
    from nba.plugins.ml.train import train_model

    df = _make_synthetic_df(30)
    df["prop_type"] = "player_points"
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "props_features_2026-01-01.parquet")

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
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "props_features_2026-01-01.parquet")

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


def test_load_training_data_ignores_mlb_feature_files(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from nba.plugins.ml.train import load_training_data

    pq.write_table(pa.Table.from_pandas(_make_synthetic_df(10)), tmp_path / "props_features_2026-01-01.parquet")
    pq.write_table(pa.Table.from_pandas(_make_synthetic_df(5)), tmp_path / "mlb_props_features_2026-01-01.parquet")

    assert len(load_training_data(str(tmp_path))) == 10


def test_platt_calibrator_never_outputs_certainty():
    """A calibration window where every high score hit must not map to 1.0 (#33)."""
    import numpy as np
    from nba.plugins.ml.train import _PlattCalibrator

    raw = np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95])
    y = np.array([0, 0, 1, 0, 1, 0, 1, 1, 1, 1])
    cal = _PlattCalibrator().fit(raw, y)
    p = cal.predict(np.array([0.0, 0.05, 0.5, 0.99, 1.0]))
    assert ((p > 0) & (p < 1)).all()
    assert np.all(np.diff(p) >= 0)


class _IdentityBase:
    """Picklable stand-in for an XGBoost model: raw probability = the single feature."""
    classes_ = [0, 1]

    def predict_proba(self, X):
        import numpy as np
        p = np.asarray(X, dtype=float).ravel()
        return np.column_stack([1 - p, p])


def test_calibrated_model_loads_legacy_isotonic_pickle():
    """Models registered before #33 stored the calibrator as `isotonic`."""
    import pickle
    import numpy as np
    from sklearn.isotonic import IsotonicRegression
    from nba.plugins.ml.train import _CalibratedModel

    iso = IsotonicRegression(out_of_bounds="clip").fit([0.1, 0.9], [0.2, 0.8])
    legacy = _CalibratedModel.__new__(_CalibratedModel)
    legacy.__dict__.update({"base_model": _IdentityBase(), "isotonic": iso, "classes_": _IdentityBase.classes_})

    restored = pickle.loads(pickle.dumps(legacy))
    np.testing.assert_allclose(restored.predict_proba([[0.1], [0.9]])[:, 1], [0.2, 0.8])


def _anchored_df(n=4000, seed=0, signal=0.0):
    """Rows where P(Over) = recalibrated market (+ signal * rest_days effect)."""
    rng = np.random.default_rng(seed)
    implied = rng.uniform(0.35, 0.7, n)
    rest = rng.integers(0, 4, n).astype(float)
    logit_true = -0.2 + 0.8 * np.log(implied / (1 - implied)) + signal * (rest - 1.5)
    df = pd.DataFrame({
        "implied_prob_over": implied,
        "line_movement":     rng.normal(0, 1, n),
        "rolling_avg_5g":    rng.uniform(5, 30, n),
        "rolling_avg_10g":   rng.uniform(5, 30, n),
        "rolling_avg_20g":   rng.uniform(5, 30, n),
        "rolling_std_10g":   rng.uniform(1, 6, n),
        "is_home":           rng.integers(0, 2, n).astype(float),
        "rest_days":         rest,
    })
    y = (rng.uniform(size=n) < 1 / (1 + np.exp(-logit_true))).astype(int)
    return df, y


def test_market_anchored_model_falls_back_to_recalibrated_market():
    """With noise-only features the model must track the recalibrated market, not drift below it."""
    from sklearn.linear_model import LogisticRegression
    from nba.plugins.ml.train import MarketAnchoredModel, _logit

    X, y = _anchored_df(seed=1)
    model = MarketAnchoredModel().fit(X, y)
    market = LogisticRegression(C=1e6).fit(_logit(X["implied_prob_over"]), y)

    X_new, _ = _anchored_df(n=500, seed=2)
    p_model = model.predict_proba(X_new)[:, 1]
    p_market = market.predict_proba(_logit(X_new["implied_prob_over"]))[:, 1]
    assert np.abs(p_model - p_market).max() < 0.03
    assert np.corrcoef(p_model, p_market)[0, 1] > 0.99
    assert model.market_coefficients["slope"] == pytest.approx(0.8, abs=0.15)


def test_market_anchored_model_learns_a_real_feature():
    from nba.plugins.ml.train import MarketAnchoredModel

    X, y = _anchored_df(n=20000, seed=3, signal=1.0)
    model = MarketAnchoredModel().fit(X, y)
    rested, tired = X.iloc[:1].copy(), X.iloc[:1].copy()
    rested["rest_days"], tired["rest_days"] = 3.0, 0.0
    assert model.predict_proba(rested)[0, 1] > model.predict_proba(tired)[0, 1]
    assert max(model.feature_importances_, key=model.feature_importances_.get) == "rest_days"


def test_market_anchored_model_fills_missing_with_training_medians():
    from nba.plugins.ml.train import MarketAnchoredModel

    X, y = _anchored_df(seed=4)
    model = MarketAnchoredModel().fit(X, y)
    row = X.iloc[:1].copy()
    row["rolling_avg_10g"] = np.nan
    filled = row.copy()
    filled["rolling_avg_10g"] = X["rolling_avg_10g"].median()
    np.testing.assert_allclose(model.predict_proba(row), model.predict_proba(filled))


def test_market_anchored_model_without_market_column_uses_base_rate():
    """Backtest ablations drop implied_prob_over; the anchor becomes the training base rate."""
    from nba.plugins.ml.train import MarketAnchoredModel

    X, y = _anchored_df(seed=5)
    model = MarketAnchoredModel().fit(X.drop(columns="implied_prob_over"), y)
    p = model.predict_proba(X.drop(columns="implied_prob_over"))[:, 1]
    assert abs(p.mean() - y.mean()) < 0.02
    assert model.market_coefficients["slope"] == 0.0


def test_market_anchored_model_survives_pickle():
    """mlflow.sklearn stores the model by pickling it."""
    import pickle
    from nba.plugins.ml.train import MarketAnchoredModel

    X, y = _anchored_df(seed=6)
    model = MarketAnchoredModel().fit(X, y)
    restored = pickle.loads(pickle.dumps(model))
    np.testing.assert_allclose(restored.predict_proba(X), model.predict_proba(X))
    assert list(restored.classes_) == [0, 1]


def test_train_model_registers_model_fit_on_all_rows(tmp_path):
    """Metrics come from the held-out window; the registered model is refit on everything."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from unittest.mock import MagicMock, patch
    from nba.plugins.ml.train import MarketAnchoredModel, train_model

    df = _make_synthetic_df(200)
    df["prop_type"] = "player_points"
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "props_features_2026-01-01.parquet")

    mock_run = MagicMock()
    mock_run.info.run_id = "run"
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)
    with patch("nba.plugins.ml.train.mlflow") as mock_mlflow, \
         patch("nba.plugins.ml.train._get_production_model_auc", return_value=None):
        mock_mlflow.start_run.return_value = mock_run
        train_model(str(tmp_path), prop_type="player_points")

    params = mock_mlflow.log_params.call_args.args[0]
    assert params["recipe"] == "market_anchored_xgb"
    assert params["final_fit_rows"] == 200
    assert params["train_rows"] + params["val_rows"] == 200
    logged = mock_mlflow.sklearn.log_model.call_args.args[0]
    assert isinstance(logged, MarketAnchoredModel)
