import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch


def _make_feature_df(n=10):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "player_id":        rng.integers(1, 5, n),
        "player_name":      ["Player A"] * n,
        "game_date":        ["2026-03-26"] * n,
        "prop_type":        ["player_points"] * n,
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
    })


def test_score_writes_recommendations_to_postgres():
    from nba.plugins.ml.score import score

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    feature_df = _make_feature_df(5)
    mock_model = MagicMock()
    mock_model.predict_proba.return_value = np.column_stack([
        np.full(5, 0.4), np.full(5, 0.6)
    ])

    mock_versions = [MagicMock()]
    mock_versions[0].version = "3"

    with patch("nba.plugins.ml.score.load_todays_features", return_value=feature_df), \
         patch("nba.plugins.ml.score.mlflow") as mock_mlflow:
        mock_mlflow.xgboost.load_model.return_value = mock_model
        mock_mlflow.tracking.MlflowClient.return_value.get_latest_versions.return_value = mock_versions

        score(mock_conn, "2026-03-26")

    # DELETE + 5 INSERTs = 6 execute calls
    assert mock_cursor.execute.call_count == 6
    mock_conn.commit.assert_called_once()


def test_score_ranks_by_edge_descending():
    from nba.plugins.ml.score import score

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    feature_df = _make_feature_df(3)
    feature_df["implied_prob_over"] = [0.45, 0.45, 0.45]

    # model_prob: row 0 has highest edge (0.9-0.45=0.45), row 1 lowest (0.6-0.45=0.15)
    probs = np.column_stack([[0.1, 0.4, 0.3], [0.9, 0.6, 0.7]])
    mock_model = MagicMock()
    mock_model.predict_proba.return_value = probs

    mock_versions = [MagicMock()]
    mock_versions[0].version = "1"

    inserted_rows = []
    def capture_insert(sql, params=None):
        if params and "DELETE" not in sql:
            inserted_rows.append(params)
    mock_cursor.execute.side_effect = capture_insert

    with patch("nba.plugins.ml.score.load_todays_features", return_value=feature_df), \
         patch("nba.plugins.ml.score.mlflow") as mock_mlflow:
        mock_mlflow.xgboost.load_model.return_value = mock_model
        mock_mlflow.tracking.MlflowClient.return_value.get_latest_versions.return_value = mock_versions
        score(mock_conn, "2026-03-26")

    # rank=1 should have highest model_prob (0.9 → edge 0.9-0.45=0.45)
    rank_1_row = [r for r in inserted_rows if r[8] == 1]
    assert len(rank_1_row) == 1
    assert abs(rank_1_row[0][5] - 0.9) < 1e-6  # model_prob at index 5 (player_name=0, prop_type=1, bookmaker=2, line=3, outcome=4, model_prob=5)
