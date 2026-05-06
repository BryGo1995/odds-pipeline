# mlb/tests/unit/ml/test_train.py
"""Unit tests for mlb/plugins/ml/train.py — mirrors NBA's test_train.py shape."""
from unittest.mock import MagicMock, patch
import os

import numpy as np
import pandas as pd
import pytest


def _make_labeled_df(n_per_prop: int = 30) -> pd.DataFrame:
    """Build a synthetic labeled feature df covering all 3 MLB prop types."""
    rng = np.random.default_rng(seed=42)
    rows = []
    base_date = pd.Timestamp("2025-08-01")
    for prop_type in ("batter_hits", "batter_total_bases", "batter_home_runs"):
        for i in range(n_per_prop):
            rows.append({
                "player_id":         1000 + i,
                "player_name":       f"P{i}",
                "game_date":         (base_date + pd.Timedelta(days=i)).date().isoformat(),
                "prop_type":         prop_type,
                "bookmaker":         "draftkings",
                "line":              float(rng.integers(1, 5)),
                "implied_prob_over": float(rng.uniform(0.4, 0.6)),
                "line_movement":     float(rng.uniform(-0.5, 0.5)),
                "rolling_avg_5g":    float(rng.uniform(0, 3)),
                "rolling_avg_10g":   float(rng.uniform(0, 3)),
                "rolling_avg_20g":   float(rng.uniform(0, 3)),
                "rolling_std_10g":   float(rng.uniform(0, 1)),
                "is_home":           float(rng.integers(0, 2)),
                "rest_days":         float(rng.integers(0, 4)),
                "actual_result":     int(rng.integers(0, 2)),
                "actual_stat_value": float(rng.integers(0, 4)),
            })
    return pd.DataFrame(rows)


def test_prepare_features_returns_per_prop_columns():
    from mlb.plugins.ml.train import prepare_features, PER_PROP_FEATURES
    df = _make_labeled_df(n_per_prop=10)
    X, y = prepare_features(df, features=PER_PROP_FEATURES)
    assert list(X.columns) == PER_PROP_FEATURES
    assert len(y) == len(df)
    assert y.dtype.kind in ("i", "u")  # int


def test_prepare_features_fills_numeric_nas_with_median():
    from mlb.plugins.ml.train import prepare_features, PER_PROP_FEATURES
    df = _make_labeled_df(n_per_prop=10)
    df.loc[0, "rolling_avg_5g"] = None
    X, _ = prepare_features(df, features=PER_PROP_FEATURES)
    assert pd.notna(X["rolling_avg_5g"].iloc[0])


def test_train_model_per_prop_below_min_rows_raises():
    from mlb.plugins.ml.train import train_model
    with patch("mlb.plugins.ml.train.load_training_data", return_value=_make_labeled_df(n_per_prop=10)):
        with pytest.raises(ValueError, match="Insufficient training data"):
            train_model(prop_type="batter_hits")


def test_train_model_home_runs_min_rows_threshold_is_100():
    """batter_home_runs needs 100 rows (sparser positive class)."""
    from mlb.plugins.ml.train import train_model
    # 60 rows for HR — should fail (>50 but <100)
    df = _make_labeled_df(n_per_prop=60)
    df = df[df["prop_type"] == "batter_home_runs"].reset_index(drop=True)
    with patch("mlb.plugins.ml.train.load_training_data", return_value=df):
        with pytest.raises(ValueError, match="Insufficient training data"):
            train_model(prop_type="batter_home_runs")


def test_train_model_registers_with_mlb_prefix():
    from mlb.plugins.ml.train import train_model
    df = _make_labeled_df(n_per_prop=60)

    captured = {}

    def fake_register(model_uri, name):
        captured["name"] = name

    with patch("mlb.plugins.ml.train.load_training_data", return_value=df), \
         patch("mlb.plugins.ml.train.mlflow") as mlflow_mock, \
         patch("mlb.plugins.ml.train._get_production_model_auc", return_value=None):
        mlflow_mock.start_run.return_value.__enter__.return_value = MagicMock(
            info=MagicMock(run_id="run-123")
        )
        mlflow_mock.register_model.side_effect = fake_register
        train_model(prop_type="batter_hits")
    assert captured.get("name") == "mlb_prop_model_batter_hits"


def test_train_model_tags_promotion_candidate_when_baseline():
    from mlb.plugins.ml.train import train_model
    df = _make_labeled_df(n_per_prop=60)

    set_tag_calls = []

    with patch("mlb.plugins.ml.train.load_training_data", return_value=df), \
         patch("mlb.plugins.ml.train.mlflow") as mlflow_mock, \
         patch("mlb.plugins.ml.train._get_production_model_auc", return_value=None):
        mlflow_mock.start_run.return_value.__enter__.return_value = MagicMock(
            info=MagicMock(run_id="run-123")
        )
        mlflow_mock.set_tag.side_effect = lambda k, v: set_tag_calls.append((k, v))
        train_model(prop_type="batter_hits")
    assert ("promotion_candidate", "true") in set_tag_calls


def test_train_all_models_skips_under_min_rows(caplog):
    from mlb.plugins.ml.train import train_all_models
    df = _make_labeled_df(n_per_prop=10)  # all under 50
    caplog.set_level("WARNING", logger="mlb.plugins.ml.train")
    with patch("mlb.plugins.ml.train.load_training_data", return_value=df), \
         patch("mlb.plugins.ml.train.train_model", side_effect=ValueError("Insufficient training data: 10 labeled rows")):
        results = train_all_models()
    assert results == {}
    assert "Skipping" in caplog.text


def test_train_all_models_iterates_three_mlb_prop_types():
    from mlb.plugins.ml.train import train_all_models, MODEL_NAME
    from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP
    assert MODEL_NAME == "mlb_prop_model"
    assert set(MLB_PROP_STAT_MAP.keys()) == {"batter_hits", "batter_total_bases", "batter_home_runs"}

    called_with = []
    with patch("mlb.plugins.ml.train.train_model", side_effect=lambda features_dir, prop_type: called_with.append(prop_type) or f"run-{prop_type}"):
        results = train_all_models()
    assert set(called_with) == {"batter_hits", "batter_total_bases", "batter_home_runs"}
    assert set(results.keys()) == set(called_with)
