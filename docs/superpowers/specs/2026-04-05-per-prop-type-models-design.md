# Per-Prop-Type Models Design Spec

> **Issue:** #10 — Diagnose top-10 ranking underperformance

## Problem

The unified ML model produces edge values that are only predictive for `player_points`, actively inverted for `player_assists` (high edge = lower hit rate), and noise for `player_rebounds`. Because assists and rebounds generate the highest raw edges, they dominate the top-10 — and perform worst (27.7% hit rate vs 50% for the full set).

**Root cause:** A single model cannot learn the different dynamics across prop types. The `prop_type_encoded` feature is insufficient for the model to specialize.

## Solution

Replace the single `nba_prop_model` with three independent models, one per prop type. Use equal allocation (not edge-based merge) for top-10 ranking to guarantee visibility across all prop types.

## Architecture

### Models

Three MLflow-registered models:
- `nba_prop_model_player_points`
- `nba_prop_model_player_rebounds`
- `nba_prop_model_player_assists`

Each model:
- Trains on labeled data filtered to its prop type only
- Uses the same XGBoost hyperparameters as the current unified model
- Calibrates independently (isotonic on its own validation set)
- Has its own production alias in the MLflow registry
- Promotes independently (competes against its own previous production version)

### Features

Same feature list minus `prop_type_encoded` (redundant when each model sees only one prop type):

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

### Training (`train.py`)

New function `train_all_models()` replaces `train_model()` as the DAG entry point:

```
train_all_models(features_dir):
    for each prop_type in PROP_STAT_MAP:
        df = load_training_data(features_dir) filtered to prop_type
        if insufficient data (<50 rows): skip with warning
        train/val split (same VALIDATION_DAYS=2 logic)
        train XGBoost, calibrate, log to MLflow
        register as nba_prop_model_{prop_type}
        promote if ROC-AUC > current production for that prop type
    return dict of {prop_type: run_id}
```

Minimum training rows reduced from 100 to 50 per prop type (since data is split three ways).

The existing `train_model()` (unified) remains available but is no longer called by the DAG.

### Scoring (`score.py`)

Updated `score()` flow:

```
score(conn, game_date):
    df = load_todays_features(game_date)
    
    results = []
    for each prop_type in PROP_STAT_MAP:
        subset = df[df.prop_type == prop_type]
        if empty: continue
        model = load model from MLflow (nba_prop_model_{prop_type}@production)
        score subset, compute edge
        results.append(subset)
    
    allocate top-10 slots (equal allocation)
    assign ranks
    write to recommendations table
```

### Top-10 Allocation

With 3 prop types and 10 slots: **4/3/3 allocation**.

- Each prop type gets `10 // num_active_prop_types` slots (3 each)
- Remaining slots (10 % num_active_prop_types = 1) go to prop types in order of their top pick's edge
- Within each allocation, picks are ranked by edge (descending)
- All remaining recs get rank 11+ sorted by edge across the full set
- If a prop type has no features for a given day, its slots redistribute to the remaining active prop types using the same `10 // num_active` + remainder logic (e.g., 2 active → 5/5, 1 active → all 10)

### Slack Notifications

**`notify_score_ready`** — add per-prop-type pick count to the existing message:

```
🏀 Recommendations ready — Apr 5
Points: 4 picks | Rebounds: 3 picks | Assists: 3 picks
```

**`notify_model_ready`** — update to summarize metrics for all three models:

```
🚀 Models trained — Apr 5
  player_points:   ROC-AUC 0.5823 (+0.0041 vs prod) ✅ promoted
  player_rebounds:  ROC-AUC 0.5512 (-0.0023 vs prod) — no improvement
  player_assists:   ROC-AUC 0.5701 (baseline) ✅ promoted
```

### Settle and Accuracy

No changes. `settle.py`, `notify_picks_settled`, and the `/recommendations/accuracy` endpoint all work off the `recommendations` table which is unchanged.

### DAG Changes

- **`nba_train_dag`**: Replace `train_model` callable with `train_all_models`. Same task structure, just a different function. Update `notify_model_ready` callback to handle multi-model results.
- **`nba_score_dag`**: No DAG structure changes. `score()` internally handles per-prop-type logic.

### Database

No migration needed. The `recommendations` table schema is unchanged. The `model_version` column will contain per-prop-type version strings.

## Testing Strategy

### Unit tests for training
- `test_train_filters_by_prop_type` — verify only rows of the target prop type are used
- `test_train_registers_correct_model_name` — verify MLflow model name includes prop type
- `test_train_skips_prop_type_with_insufficient_data` — verify <50 rows triggers skip, not crash
- `test_train_all_models_returns_run_ids` — verify dict of {prop_type: run_id}

### Unit tests for scoring
- `test_score_loads_per_prop_type_models` — verify correct model URI per prop type
- `test_score_allocation_4_3_3` — verify 10 slots distributed correctly with 3 prop types
- `test_score_allocation_redistributes_when_prop_type_empty` — verify 5/5 split when one prop type missing
- `test_score_ranks_within_allocation_by_edge` — verify edge ordering within each prop type's slots

### Unit tests for Slack
- `test_notify_model_ready_multi_model` — verify message includes all three models' metrics
- `test_notify_score_ready_includes_prop_breakdown` — verify pick count per prop type in message

## Out of Scope

- Per-prop-type hyperparameter tuning (use same XGBoost params for now)
- Adding new prop types (player_threes, player_threes_attempts) — these have no training data yet
- Changing the feature set (tracked in #11)
- Historical odds backfill (tracked in #12)
