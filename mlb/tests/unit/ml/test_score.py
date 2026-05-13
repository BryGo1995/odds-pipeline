# mlb/tests/unit/ml/test_score.py
"""Unit tests for mlb/plugins/ml/score.py."""
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


def _make_today_df():
    rows = []
    for prop_type in ("batter_hits", "batter_total_bases"):
        for i in range(8):
            rows.append({
                "player_id":         1000 + i,
                "player_name":       f"P{i}",
                "game_date":         "2026-05-04",
                "prop_type":         prop_type,
                "bookmaker":         "draftkings",
                "line":              float(i % 4 + 1),
                "implied_prob_over": 0.5,
                "line_movement":     0.0,
                "rolling_avg_5g":    1.0,
                "rolling_avg_10g":   1.0,
                "rolling_avg_20g":   1.0,
                "rolling_std_10g":   0.5,
                "is_home":           1.0,
                "rest_days":         1.0,
            })
    return pd.DataFrame(rows)


def _model_returning(prob: float):
    m = MagicMock()
    n_calls = {}
    def predict_proba(X):
        n = len(X)
        n_calls["n"] = n
        return np.column_stack([np.full(n, 1 - prob), np.full(n, prob)])
    m.predict_proba.side_effect = predict_proba
    return m


def test_score_writes_recommendations_with_sport_mlb():
    from mlb.plugins.ml.score import score
    df = _make_today_df()

    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=df), \
         patch("mlb.plugins.ml.score.mlflow") as mlflow_mock:
        mlflow_mock.sklearn.load_model.return_value = _model_returning(0.7)
        client = MagicMock()
        client.get_model_version_by_alias.return_value = MagicMock(version="1")
        mlflow_mock.tracking.MlflowClient.return_value = client
        score(fake_conn, "2026-05-04")

    delete_calls = [c for c in fake_cur.execute.call_args_list if "DELETE FROM recommendations" in c.args[0]]
    insert_calls = [c for c in fake_cur.execute.call_args_list if "INSERT INTO recommendations" in c.args[0]]
    assert len(delete_calls) == 1
    assert "sport = 'MLB'" in delete_calls[0].args[0]
    assert len(insert_calls) >= 10
    for call in insert_calls:
        assert call.args[1][-1] == "MLB"  # sport is the last bound parameter


def test_score_partial_models_split_top10_evenly():
    from mlb.plugins.ml.score import score
    df = _make_today_df()

    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur

    def fake_load(uri):
        if "batter_total_bases" in uri:
            raise RuntimeError("no production version")
        return _model_returning(0.7)

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=df), \
         patch("mlb.plugins.ml.score.mlflow") as mlflow_mock:
        mlflow_mock.sklearn.load_model.side_effect = fake_load
        client = MagicMock()
        client.get_model_version_by_alias.return_value = MagicMock(version="1")
        mlflow_mock.tracking.MlflowClient.return_value = client
        score(fake_conn, "2026-05-04")

    insert_calls = [c for c in fake_cur.execute.call_args_list if "INSERT INTO recommendations" in c.args[0]]
    # Only batter_hits succeeded (batter_total_bases raised), so all top-N are batter_hits
    top_prop_types = [c.args[1][1] for c in insert_calls]
    assert all(pt == "batter_hits" for pt in top_prop_types)
    assert "batter_total_bases" not in top_prop_types


def test_score_zero_models_raises():
    from mlb.plugins.ml.score import score
    df = _make_today_df()
    fake_conn = MagicMock()

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=df), \
         patch("mlb.plugins.ml.score.mlflow") as mlflow_mock:
        mlflow_mock.sklearn.load_model.side_effect = RuntimeError("no production version")
        with pytest.raises(ValueError, match="No prop types could be scored"):
            score(fake_conn, "2026-05-04")


def test_score_empty_features_raises():
    from mlb.plugins.ml.score import score
    fake_conn = MagicMock()

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=pd.DataFrame()):
        with pytest.raises(ValueError, match="No feature file found"):
            score(fake_conn, "2026-05-04")


def test_score_records_model_version_from_alias():
    from mlb.plugins.ml.score import score
    df = _make_today_df()

    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=df), \
         patch("mlb.plugins.ml.score.mlflow") as mlflow_mock:
        mlflow_mock.sklearn.load_model.return_value = _model_returning(0.7)
        client = MagicMock()
        client.get_model_version_by_alias.return_value = MagicMock(version="42")
        mlflow_mock.tracking.MlflowClient.return_value = client
        score(fake_conn, "2026-05-04")

    insert_calls = [c for c in fake_cur.execute.call_args_list if "INSERT INTO recommendations" in c.args[0]]
    for call in insert_calls:
        assert call.args[1][-3] == "42"  # model_version is 3rd-from-last
