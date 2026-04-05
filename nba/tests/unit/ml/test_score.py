# nba/tests/unit/ml/test_score.py
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch


def _make_feature_df(n_per_type=5):
    """Create a feature DataFrame with all 3 prop types."""
    rng = np.random.default_rng(0)
    frames = []
    for prop_type in ["player_points", "player_rebounds", "player_assists"]:
        frames.append(pd.DataFrame({
            "player_id":        rng.integers(1, 20, n_per_type),
            "player_name":      [f"Player {i}" for i in range(n_per_type)],
            "game_date":        ["2026-03-26"] * n_per_type,
            "prop_type":        [prop_type] * n_per_type,
            "bookmaker":        ["draftkings"] * n_per_type,
            "line":             rng.uniform(15, 30, n_per_type),
            "implied_prob_over": rng.uniform(0.4, 0.6, n_per_type),
            "line_movement":    rng.uniform(-1, 1, n_per_type),
            "rolling_avg_5g":   rng.uniform(15, 30, n_per_type),
            "rolling_avg_10g":  rng.uniform(15, 30, n_per_type),
            "rolling_avg_20g":  rng.uniform(15, 30, n_per_type),
            "rolling_std_10g":  rng.uniform(2, 6, n_per_type),
            "is_home":          rng.choice([True, False], n_per_type),
            "rest_days":        rng.integers(1, 5, n_per_type).astype(float),
            "actual_result":    [None] * n_per_type,
            "actual_stat_value": [None] * n_per_type,
        }))
    return pd.concat(frames, ignore_index=True)


def _mock_model(n, prob=0.6):
    """Return a mock model that returns constant predicted probabilities."""
    model = MagicMock()
    model.predict_proba.return_value = np.column_stack([
        np.full(n, 1 - prob), np.full(n, prob)
    ])
    return model


def _setup_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_score_loads_per_prop_type_models():
    from nba.plugins.ml.score import score

    mock_conn, mock_cursor = _setup_mock_conn()
    feature_df = _make_feature_df(5)  # 15 total rows (5 per type)

    models_loaded = []
    def mock_load_model(uri):
        models_loaded.append(uri)
        n = 5  # each subset has 5 rows
        return _mock_model(n)

    with patch("nba.plugins.ml.score.load_todays_features", return_value=feature_df), \
         patch("nba.plugins.ml.score.mlflow") as mock_mlflow:
        mock_mlflow.sklearn.load_model.side_effect = mock_load_model
        mock_client = MagicMock()
        mock_client.get_model_version_by_alias.return_value.version = "1"
        mock_mlflow.tracking.MlflowClient.return_value = mock_client

        score(mock_conn, "2026-03-26")

    # Should load 3 models (one per prop type)
    assert len(models_loaded) == 3
    assert any("player_points" in uri for uri in models_loaded)
    assert any("player_rebounds" in uri for uri in models_loaded)
    assert any("player_assists" in uri for uri in models_loaded)


def test_score_allocation_4_3_3():
    from nba.plugins.ml.score import score

    mock_conn, mock_cursor = _setup_mock_conn()
    feature_df = _make_feature_df(10)  # 30 total rows

    with patch("nba.plugins.ml.score.load_todays_features", return_value=feature_df), \
         patch("nba.plugins.ml.score.mlflow") as mock_mlflow:
        mock_mlflow.sklearn.load_model.side_effect = lambda uri: _mock_model(10, prob=0.6)
        mock_client = MagicMock()
        mock_client.get_model_version_by_alias.return_value.version = "1"
        mock_mlflow.tracking.MlflowClient.return_value = mock_client

        score(mock_conn, "2026-03-26")

    # Collect all INSERT calls (skip the DELETE)
    insert_calls = [
        c for c in mock_cursor.execute.call_args_list
        if c.args and "INSERT" in str(c.args[0])
    ]
    assert len(insert_calls) == 30  # all rows get inserted

    # Check top-10 allocation: ranks 1-10 should have mix of all prop types
    top_10 = [c.args[1] for c in insert_calls if c.args[1][8] <= 10]  # rank is index 8
    top_10_prop_types = [row[1] for row in top_10]  # prop_type is index 1
    for pt in ["player_points", "player_rebounds", "player_assists"]:
        count = top_10_prop_types.count(pt)
        assert count >= 3, f"{pt} should have at least 3 slots in top 10, got {count}"
        assert count <= 4, f"{pt} should have at most 4 slots in top 10, got {count}"


def test_score_allocation_redistributes_when_prop_type_empty():
    from nba.plugins.ml.score import score

    mock_conn, mock_cursor = _setup_mock_conn()
    # Only points and rebounds — no assists
    rng = np.random.default_rng(0)
    frames = []
    for prop_type in ["player_points", "player_rebounds"]:
        n = 10
        frames.append(pd.DataFrame({
            "player_id":        rng.integers(1, 20, n),
            "player_name":      [f"Player {i}" for i in range(n)],
            "game_date":        ["2026-03-26"] * n,
            "prop_type":        [prop_type] * n,
            "bookmaker":        ["draftkings"] * n,
            "line":             rng.uniform(15, 30, n),
            "implied_prob_over": rng.uniform(0.4, 0.6, n),
            "line_movement":    rng.uniform(-1, 1, n),
            "rolling_avg_5g":   rng.uniform(15, 30, n),
            "rolling_avg_10g":  rng.uniform(15, 30, n),
            "rolling_avg_20g":  rng.uniform(15, 30, n),
            "rolling_std_10g":  rng.uniform(2, 6, n),
            "is_home":          rng.choice([True, False], n),
            "rest_days":        rng.integers(1, 5, n).astype(float),
            "actual_result":    [None] * n,
            "actual_stat_value": [None] * n,
        }))
    feature_df = pd.concat(frames, ignore_index=True)

    with patch("nba.plugins.ml.score.load_todays_features", return_value=feature_df), \
         patch("nba.plugins.ml.score.mlflow") as mock_mlflow:
        mock_mlflow.sklearn.load_model.side_effect = lambda uri: _mock_model(10, prob=0.6)
        mock_client = MagicMock()
        mock_client.get_model_version_by_alias.return_value.version = "1"
        mock_mlflow.tracking.MlflowClient.return_value = mock_client

        score(mock_conn, "2026-03-26")

    insert_calls = [
        c for c in mock_cursor.execute.call_args_list
        if c.args and "INSERT" in str(c.args[0])
    ]
    top_10 = [c.args[1] for c in insert_calls if c.args[1][8] <= 10]
    top_10_prop_types = [row[1] for row in top_10]

    # With 2 active prop types: 5/5 split
    for pt in ["player_points", "player_rebounds"]:
        assert top_10_prop_types.count(pt) == 5, f"{pt} should get 5 slots with 2 active types"
    assert "player_assists" not in top_10_prop_types


def test_score_ranks_within_allocation_by_edge():
    from nba.plugins.ml.score import score

    mock_conn, mock_cursor = _setup_mock_conn()
    # Create 5 points rows with varying implied_prob to produce different edges
    rng = np.random.default_rng(42)
    feature_df = pd.DataFrame({
        "player_id":        [1, 2, 3, 4, 5],
        "player_name":      ["P1", "P2", "P3", "P4", "P5"],
        "game_date":        ["2026-03-26"] * 5,
        "prop_type":        ["player_points"] * 5,
        "bookmaker":        ["draftkings"] * 5,
        "line":             [25.0] * 5,
        "implied_prob_over": [0.55, 0.50, 0.45, 0.40, 0.35],  # lower implied = higher edge
        "line_movement":    [0.0] * 5,
        "rolling_avg_5g":   [25.0] * 5,
        "rolling_avg_10g":  [25.0] * 5,
        "rolling_avg_20g":  [25.0] * 5,
        "rolling_std_10g":  [3.0] * 5,
        "is_home":          [True] * 5,
        "rest_days":        [2.0] * 5,
        "actual_result":    [None] * 5,
        "actual_stat_value": [None] * 5,
    })

    # Model returns constant 0.6 prob for all — so edge = 0.6 - implied_prob
    # P5 has highest edge (0.6-0.35=0.25), P1 has lowest (0.6-0.55=0.05)
    with patch("nba.plugins.ml.score.load_todays_features", return_value=feature_df), \
         patch("nba.plugins.ml.score.mlflow") as mock_mlflow:
        mock_mlflow.sklearn.load_model.return_value = _mock_model(5, prob=0.6)
        mock_client = MagicMock()
        mock_client.get_model_version_by_alias.return_value.version = "1"
        mock_mlflow.tracking.MlflowClient.return_value = mock_client

        score(mock_conn, "2026-03-26")

    insert_calls = [
        c for c in mock_cursor.execute.call_args_list
        if c.args and "INSERT" in str(c.args[0])
    ]
    # Rank 1 should be P5 (highest edge), Rank 5 should be P1 (lowest edge)
    rows_by_rank = {c.args[1][8]: c.args[1] for c in insert_calls}  # rank -> row
    assert rows_by_rank[1][0] == "P5"  # player_name at index 0
    assert rows_by_rank[5][0] == "P1"
