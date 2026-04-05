# Per-Prop-Type Models Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single unified ML model with three per-prop-type models (points, rebounds, assists) and use equal allocation for top-10 ranking.

**Architecture:** Each prop type gets its own XGBoost model registered in MLflow. `train_all_models()` replaces `train_model()` as the DAG entry. `score()` loads per-prop-type models, scores each subset, allocates top-10 slots evenly (4/3/3), and writes to `recommendations`. Slack notifications updated to report per-model metrics.

**Tech Stack:** XGBoost, scikit-learn (CalibratedClassifierCV), MLflow, psycopg2, pytest (MagicMock), Airflow PythonOperator

---

## File Map

**odds-pipeline (repo root: `/home/bryang/Dev_Space/python_projects/odds-pipeline`):**
- Modify: `nba/plugins/ml/train.py` — add `PER_PROP_FEATURES`, `train_all_models()`, update `train_model()` to accept `prop_type` param
- Modify: `nba/plugins/ml/score.py` — rewrite `score()` for per-prop-type loading and equal allocation
- Modify: `shared/plugins/slack_notifier.py` — update `notify_model_ready` for multi-model output
- Modify: `nba/dags/nba_train_dag.py` — switch callable to `train_all_models`
- Modify: `nba/tests/unit/ml/test_train.py` — add per-prop-type training tests
- Modify: `nba/tests/unit/ml/test_score.py` — rewrite for per-prop-type scoring and allocation

---

## Task 1: Update train.py — per-prop-type training

**Files:**
- Modify: `nba/plugins/ml/train.py`
- Modify: `nba/tests/unit/ml/test_train.py`

- [ ] **Step 1: Write the failing tests**

Add to the bottom of `nba/tests/unit/ml/test_train.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/ml/test_train.py -v -k "prop_type or per_prop or all_models"
```

Expected: `ImportError` or `TypeError` — `train_model()` does not accept `prop_type` param, `PER_PROP_FEATURES` and `train_all_models` don't exist

- [ ] **Step 3: Implement changes to train.py**

Add `PER_PROP_FEATURES` constant after the existing `FEATURES` list:

```python
PER_PROP_FEATURES = [
    "implied_prob_over",
    "line_movement",
    "rolling_avg_5g",
    "rolling_avg_10g",
    "rolling_avg_20g",
    "rolling_std_10g",
    "is_home",
    "rest_days",
]
```

Update `train_model()` signature and body to accept an optional `prop_type` parameter:

```python
def train_model(features_dir: str = FEATURES_DIR, prop_type: str | None = None) -> str:
```

Inside `train_model()`, after loading data and before the sort/split:

```python
    if prop_type is not None:
        df = df[df["prop_type"] == prop_type]
        model_name = f"{MODEL_NAME}_{prop_type}"
        features = PER_PROP_FEATURES
        min_rows = 50
    else:
        model_name = MODEL_NAME
        features = FEATURES
        min_rows = 100
```

Update the minimum rows check:

```python
    if df.empty or len(df) < min_rows:
        raise ValueError(f"Insufficient training data: {len(df)} labeled rows (minimum {min_rows})")
```

Update `prepare_features` call to use the appropriate feature list. The simplest approach: pass `features` to a local preparation step. Since `prepare_features` is also used by `score.py`, keep it as-is but add a `features` param with a default:

```python
def prepare_features(df: pd.DataFrame, features: list[str] | None = None) -> tuple:
    le = _make_label_encoder()
    df = df.copy()
    if features is None or "prop_type_encoded" in features:
        df["prop_type_encoded"] = le.transform(
            df["prop_type"].map(lambda x: x if x in le.classes_ else le.classes_[0])
        )
    df["is_home"] = df["is_home"].astype(object).fillna(0.5).astype(float)
    for col in ["rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g",
                "rolling_std_10g", "line_movement", "rest_days"]:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            df[col] = numeric.fillna(numeric.median())
    use_features = features if features is not None else FEATURES
    X = df[use_features]
    if "actual_result" in df.columns and df["actual_result"].notna().all():
        y = df["actual_result"].astype(int)
    else:
        y = None
    return X, y, le
```

Update all `mlflow.register_model` and `_get_production_model_auc` calls inside `train_model()` to use the local `model_name` variable instead of the global `MODEL_NAME`:

Replace:
```python
        mlflow.register_model(model_uri, MODEL_NAME)
```
With:
```python
        mlflow.register_model(model_uri, model_name)
```

And update `_get_production_model_auc` to accept a model name:

```python
def _get_production_model_auc(model_name: str = MODEL_NAME) -> float | None:
```

Call it as:
```python
    prod_roc_auc = _get_production_model_auc(model_name)
```

Update the feature importance logging:
```python
        importance = dict(zip(features, model.feature_importances_.tolist()))
```

Update the `MlflowClient().get_model_version_by_alias` call to use `model_name`:
```python
        mv = client.get_model_version_by_alias(model_name, "production")
```

Add `train_all_models()` after `train_model()`:

```python
def train_all_models(features_dir: str = FEATURES_DIR) -> dict[str, str]:
    """
    Train one model per prop type. Returns dict of {prop_type: mlflow_run_id}.
    Skips prop types with insufficient training data.
    """
    log = logging.getLogger(__name__)
    results = {}
    for prop_type in sorted(PROP_STAT_MAP.keys()):
        try:
            run_id = train_model(features_dir, prop_type=prop_type)
            results[prop_type] = run_id
        except ValueError as exc:
            log.warning("Skipping %s: %s", prop_type, exc)
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/ml/test_train.py -v
```

Expected: all tests PASS (both old and new)

- [ ] **Step 5: Commit**

```bash
git add nba/plugins/ml/train.py nba/tests/unit/ml/test_train.py
git commit -m "feat: add per-prop-type model training with train_all_models"
```

---

## Task 2: Update score.py — per-prop-type scoring and allocation

**Files:**
- Modify: `nba/plugins/ml/score.py`
- Modify: `nba/tests/unit/ml/test_score.py`

- [ ] **Step 1: Write the failing tests**

Replace the contents of `nba/tests/unit/ml/test_score.py` with:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/ml/test_score.py -v
```

Expected: failures — `score()` still uses unified model logic

- [ ] **Step 3: Implement changes to score.py**

Replace the contents of `nba/plugins/ml/score.py` with:

```python
"""
Scoring module for NBA player prop ML model.

score(conn, game_date) loads per-prop-type production models from the MLflow
registry, scores today's feature Parquet file, allocates top-10 slots evenly
across prop types, and writes ranked recommendations to Postgres.
"""
import logging
import os

import duckdb
import mlflow
import mlflow.sklearn
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
        raise ValueError(f"No feature file found for {game_date} in {features_dir}")

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
        subset["outcome"] = "Over"
        subset["implied_prob"] = subset["implied_prob_over"]
        subset["edge"] = subset["model_prob"] - subset["implied_prob"]
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
        cur.execute("DELETE FROM recommendations WHERE game_date = %s", (game_date,))
        for _, row in all_df.iterrows():
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/ml/test_score.py -v
```

Expected: all 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add nba/plugins/ml/score.py nba/tests/unit/ml/test_score.py
git commit -m "feat: per-prop-type scoring with equal top-10 allocation"
```

---

## Task 3: Update Slack notifications for multi-model output

**Files:**
- Modify: `shared/plugins/slack_notifier.py`
- Modify: `nba/dags/nba_train_dag.py`

- [ ] **Step 1: Update notify_model_ready in slack_notifier.py**

Replace the `notify_model_ready` function in `shared/plugins/slack_notifier.py` with:

```python
def notify_model_ready(context):
    """on_success_callback for nba_train_dag. Posts per-prop-type model metrics."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    ti = context.get("task_instance")
    run_ids = ti.xcom_pull(task_ids="train_model", key="mlflow_run_ids") if ti else None

    if not run_ids or not isinstance(run_ids, dict):
        _post("\U0001f916 Model training completed (no per-model details available)")
        return

    try:
        import mlflow

        lines = []
        for prop_type, run_id in sorted(run_ids.items()):
            run = mlflow.get_run(run_id)
            metrics = run.data.metrics
            tags = run.data.tags

            roc_auc = metrics.get("roc_auc", 0.0)
            delta = metrics.get("roc_auc_delta_vs_production")
            is_candidate = tags.get("promotion_candidate") == "true"

            if delta is None:
                delta_str = "baseline"
            else:
                delta_str = f"{delta:+.4f}"

            _PROP_LABELS = {
                "player_points": "Points",
                "player_rebounds": "Rebounds",
                "player_assists": "Assists",
            }
            label = _PROP_LABELS.get(prop_type, prop_type)
            status = "\u2705 promoted" if is_candidate else "\u2014 no improvement"
            lines.append(f"  {label}: ROC-AUC {roc_auc:.4f} ({delta_str}) {status}")

        text = "\U0001f680 Models trained\n\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("Failed to fetch MLflow run details: %s", exc)
        text = "\U0001f916 Model training completed (details unavailable)"

    _post(text)
```

- [ ] **Step 2: Update nba_train_dag.py**

Replace the import and wrapper function in `nba/dags/nba_train_dag.py`:

Change the import from:
```python
from nba.plugins.ml.train import train_model
```
To:
```python
from nba.plugins.ml.train import train_all_models
```

Replace the wrapper function:
```python
def run_train_model(**context):
    run_ids = train_all_models()
    context["ti"].xcom_push(key="mlflow_run_ids", value=run_ids)
```

- [ ] **Step 3: Run existing tests to verify no regressions**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/ml/ nba/tests/unit/test_slack_notifier_settle.py -v
```

Expected: all tests PASS

- [ ] **Step 4: Commit**

```bash
git add shared/plugins/slack_notifier.py nba/dags/nba_train_dag.py
git commit -m "feat: update Slack notifications and train DAG for per-prop-type models"
```

---

## Task 4: Update score DAG notification (optional enhancement)

**Files:**
- Modify: `shared/plugins/slack_notifier.py`

- [ ] **Step 1: Update notify_score_ready to include prop type breakdown**

In `shared/plugins/slack_notifier.py`, inside `notify_score_ready`, after building the `checklist` string and before composing the final `text`, add a per-prop-type pick count lookup:

After the line `checklist = "\n".join(lines)`, add:

```python
    # Count top-10 picks per prop type
    try:
        from shared.plugins.db_client import get_data_db_conn
        data_conn = get_data_db_conn()
        try:
            with data_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT prop_type, COUNT(*)
                    FROM recommendations
                    WHERE game_date = %s AND rank <= 10
                    GROUP BY prop_type ORDER BY prop_type
                    """,
                    (execution_date.strftime("%Y-%m-%d"),),
                )
                prop_counts = cur.fetchall()
        finally:
            data_conn.close()

        _PROP_LABELS = {
            "player_points": "Points",
            "player_rebounds": "Rebounds",
            "player_assists": "Assists",
        }
        if prop_counts:
            breakdown = " | ".join(
                f"{_PROP_LABELS.get(pt, pt)}: {count}"
                for pt, count in prop_counts
            )
        else:
            breakdown = ""
    except Exception as exc:
        logger.warning("Could not fetch prop type breakdown: %s", exc)
        breakdown = ""
```

Then update the `text` composition to include the breakdown:

```python
    breakdown_line = f"\n{breakdown}" if breakdown else ""
    text = f"\U0001f3c0 Recommendations ready \u2014 {date_str}\n\n{checklist}{breakdown_line}{warning}"
```

- [ ] **Step 2: Run tests to verify no regressions**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/ -v -k "not dag"
```

Expected: all tests PASS

- [ ] **Step 3: Commit**

```bash
git add shared/plugins/slack_notifier.py
git commit -m "feat: add prop type breakdown to score-ready Slack notification"
```
