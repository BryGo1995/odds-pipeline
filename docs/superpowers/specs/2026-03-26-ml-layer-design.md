# ML Layer Design
**Date:** 2026-03-26
**Status:** Approved

---

## Overview

A stats-enriched machine learning layer built on top of the existing ingest/transform pipeline. Three new Airflow DAGs produce daily ranked player prop recommendations stored in Postgres. The ML layer is purely additive — no changes to existing DAGs or services.

---

## Stack

| Tool | Role |
|---|---|
| **Parquet** | Feature storage — date-partitioned columnar files on a shared volume |
| **DuckDB** | In-process analytical queries over Parquet files — no server needed |
| **XGBoost** | Gradient boosting model — `prop_type` as categorical feature, single model covers all prop types |
| **MLflow** | Experiment tracking, metric logging, and model registry — manual promotion workflow |
| **Airflow** | Orchestration — three new DAGs downstream of existing pipeline |

---

## Scope

**Prop types in scope:** points, rebounds, assists, threes attempted (`fg3a`), threes made (`fg3m`)

All five are handled by a single model with `prop_type` as a categorical feature. No per-prop-type models or filtering logic required. Architecture supports adding additional prop types later without structural changes.

**Downstream consumption:** `recommendations` Postgres table only. Customer-facing application is future scope and out of scope for this implementation.

---

## Architecture

```
── existing ──────────────────────────────────────────────────────
ingest_dag → transform_dag
  ↓ writes to Postgres: player_props, player_game_logs, games

── new (ML layer) ────────────────────────────────────────────────
feature_dag  ← triggered after transform_dag (2x/day)
  ↓ reads player_props + player_game_logs + games (Postgres)
  ↓ computes rolling stats, implied prob, line movement
  ↓ writes /data/features/props_features_YYYY-MM-DD.parquet

score_dag    ← triggered after feature_dag (2x/day)
  ↓ reads today's Parquet via DuckDB
  ↓ loads "production" model from MLflow registry
  ↓ outputs ranked recommendations → Postgres

train_dag    ← weekly / on-demand
  ↓ reads all historical Parquet via DuckDB
  ↓ trains XGBoost (prop_type as categorical feature)
  ↓ logs metrics to MLflow, compares vs. production model
  ↓ tags run "promotion_candidate" if accuracy improves
  ↓ user manually promotes in MLflow UI
```

### Docker Compose Changes

- New `mlflow` service — port `5001`, artifact store at `/mlflow/artifacts` (local volume), registry backed by SQLite
- New shared volume `features-data` mounted at `/data/features` in both `airflow` and `mlflow` services
- No changes to existing services

---

## Parquet Feature Schema

One file per day: `/data/features/props_features_YYYY-MM-DD.parquet`

| Column | Type | Source | Notes |
|---|---|---|---|
| `player_id` | str | player_props | |
| `player_name` | str | player_props | |
| `game_date` | date | player_props | partition key |
| `prop_type` | str | player_props | points / rebounds / assists / fg3a / fg3m |
| `bookmaker` | str | player_props | |
| `line` | float | player_props | e.g. 24.5 |
| `implied_prob_over` | float | computed | derived from over odds |
| `line_movement` | float | computed | current line − opening line |
| `rolling_avg_5g` | float | game_logs | rolling mean for this prop_type, last 5 games |
| `rolling_avg_10g` | float | game_logs | last 10 games |
| `rolling_avg_20g` | float | game_logs | last 20 games |
| `rolling_std_10g` | float | game_logs | standard deviation — captures consistency |
| `is_home` | bool | games | |
| `rest_days` | int | game_logs | days since last game |
| `actual_result` | int | game_logs (post-game) | 1 = over, 0 = under. NULL for future games (scoring mode) |
| `actual_stat_value` | float | game_logs (post-game) | raw stat value — used to compute actual_result |

`actual_result` and `actual_stat_value` are NULL when writing features for upcoming games (used by `score_dag`). They are populated for historical games (used by `train_dag`).

### Design Risks

**Label join:** Training labels require joining `player_props` (line at bet time) with `player_game_logs` (actual stat) on `(player_id, game_date)`. The reliability of this join depends on data consistency across both tables. This must be validated during `feature_dag` implementation before training can proceed.

**`line_movement` availability:** Computing `line_movement` (current line − opening line) requires the opening line to be stored at ingest time. If the Odds API only captures a single snapshot per day, this feature cannot be computed and should be dropped from the feature set. Verify during `feature_dag` implementation.

**`prop_type` naming:** The exact string values for `prop_type` in the `player_props` table must match what `feature_dag` uses to look up the corresponding stat column in `player_game_logs` (e.g. `player_points` → `pts`, `fg3a` → `fg3a`). Validate the mapping against actual ingested data during implementation.

---

## Model Design

**Algorithm:** XGBoost gradient boosting classifier

**Target variable:** `actual_result` (binary: 1 = over, 0 = under)

**Features fed to model:**
- `implied_prob_over`
- `line_movement`
- `rolling_avg_5g`, `rolling_avg_10g`, `rolling_avg_20g`
- `rolling_std_10g`
- `is_home`
- `rest_days`
- `prop_type` (categorical)

**Train/validation split:** Time-based — train on all history except last 30 days, validate on last 30 days.

---

## train_dag — Evaluation and Promotion Workflow

**Metrics logged to MLflow per run:**
- ROC-AUC (primary comparison metric)
- Accuracy, precision, recall
- Calibration (reliability of `model_prob` as a true probability — critical for edge calculation)
- Feature importance

**Promotion workflow:**
1. `train_dag` trains a new model and logs all metrics to MLflow
2. Compares ROC-AUC against the current `production` model in the registry
3. If the new model wins, the run is tagged `promotion_candidate=true` with a note showing the delta (e.g. `+0.023 ROC-AUC vs production`)
4. User reviews in MLflow UI and manually promotes to `production` when satisfied

No auto-promotion. The user always confirms before a new model serves scoring.

---

## recommendations Table (output of score_dag)

One row per player per prop per game date:

| Column | Notes |
|---|---|
| `player_name` | |
| `prop_type` | points / rebounds / assists / fg3a / fg3m |
| `bookmaker` | |
| `line` | |
| `outcome` | Over / Under |
| `model_prob` | Model's estimated P(over) |
| `implied_prob` | Bookmaker's implied P(over) |
| `edge` | `model_prob − implied_prob` — positive = value bet |
| `rank` | Ranked by edge descending within each game date |
| `model_version` | MLflow model version used — for traceability |
| `game_date` | |
| `scored_at` | Timestamp |

---

## New Files

**DAGs:**
- `nba/dags/nba_feature_dag.py`
- `nba/dags/nba_train_dag.py`
- `nba/dags/nba_score_dag.py`

**Plugins:**
- `nba/plugins/transformers/features.py` — feature engineering logic (rolling stats, implied prob, line movement)
- `nba/plugins/ml/train.py` — model training, evaluation, and MLflow logging
- `nba/plugins/ml/score.py` — load model from registry, generate recommendations

**Infrastructure:**
- `docker-compose.yml` — add `mlflow` service and `features-data` volume
- `sql/migrations/` — migration for `recommendations` table

---

## Cloud Migration Path

No rewrites required when moving off local:
- Swap local Parquet volume → S3 (same files, same format)
- Swap MLflow local → MLflow on EC2, or adopt SageMaker / Vertex AI
- Add Apache Iceberg on top of S3 Parquet for time-travel and schema evolution
