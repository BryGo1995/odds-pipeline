# Repository Restructure Design Spec

**Date:** 2026-03-25
**Status:** Approved

---

## Overview

Restructure the `odds-pipeline` repo to support multi-sport expansion (NBA → MLB → NFL) without overcomplicating the current state. The repo keeps its name. NBA code moves into a `nba/` namespace. Shared infrastructure (Odds API client, Docker Compose, Postgres) stays at the root or in `shared/`. The ML layer (Parquet/DuckDB/MLflow) is introduced under `nba/ml/` alongside the existing pipeline, keeping ingest and model code co-located per sport since they evolve together.

---

## Goals

- Namespace NBA code under `nba/` so adding MLB or NFL follows the same pattern
- Move the shared Odds API client to `shared/plugins/` — consumed by all sports
- Introduce `nba/ml/` for feature engineering and model code, with ML DAGs living in `nba/dags/`
- Keep Docker Compose, Postgres migrations, and Dockerfile at the root (shared infrastructure)
- Preserve all existing functionality — this is a structural migration, not a rewrite

---

## Directory Layout

### Target Structure

```
odds-pipeline/
  nba/
    dags/           ← all NBA DAGs (ingest, transform, backfill, + new feature/train/score)
    plugins/        ← nba_api_client.py + NBA-specific transformers
    ml/             ← feature engineering, model training, scoring logic
    tests/          ← NBA-specific tests
  shared/
    plugins/        ← odds_api_client.py (shared across all sports)
  sql/
    migrations/     ← all Postgres migrations, prefixed by sport or "shared"
  docker-compose.yml
  Dockerfile
  requirements.txt
  docs/
  config/
```

### Migration Map

| Current location | New location | Notes |
|---|---|---|
| `dags/` | `nba/dags/` | All existing NBA DAGs move here |
| `plugins/odds_api_client.py` | `shared/plugins/odds_api_client.py` | Shared across sports |
| `plugins/nba_api_client.py` | `nba/plugins/nba_api_client.py` | NBA-specific |
| `plugins/transformers/` | `nba/plugins/transformers/` | NBA-specific transformers |
| `tests/` | `nba/tests/` | NBA-specific tests |
| `sql/migrations/` | `sql/migrations/` | Stays at root; files renamed with `nba_` or `shared_` prefix |
| *(new)* ML feature/train/score logic | `nba/ml/` | New for ML layer |
| *(new)* feature_dag, train_dag, score_dag | `nba/dags/` | New ML DAGs |

---

## Architecture

### Sport Namespace Pattern

Each sport gets a top-level directory with identical internal structure:

```
<sport>/
  dags/       ← Airflow DAGs for this sport (ingest, transform, backfill, ML)
  plugins/    ← API client and transformers specific to this sport
  ml/         ← Feature engineering, model training, scoring code
  tests/      ← Tests for this sport
```

Adding a new sport is: create the directory, implement the stat API client in `plugins/`, wire up DAGs in `dags/`, reuse the Odds API client from `shared/plugins/`.

### Shared Layer

| Path | Contents |
|---|---|
| `shared/plugins/odds_api_client.py` | Odds API client — all sports share the same API and contract |
| `sql/migrations/` | All Postgres migrations — same DB instance for all sports |
| `docker-compose.yml` | Airflow, Postgres, MLflow — sport-agnostic orchestration |
| `Dockerfile` | Single image for Airflow workers |
| `requirements.txt` | All Python dependencies |

### ML Layer Placement

The ML layer lives inside `nba/` rather than a top-level `ml/` directory because NBA ingest and NBA model code are tightly coupled — a new stat column in Postgres immediately becomes a new feature. When MLB is added, `mlb/ml/` follows the same pattern. Shared ML utilities (if any emerge) can move to `shared/` at that point.

```
nba/
  dags/
    feature_dag.py    ← reads Postgres, writes date-partitioned Parquet to /data/features/nba/
    train_dag.py      ← reads Parquet via DuckDB, trains model, logs to MLflow
    score_dag.py      ← loads model from MLflow, scores today's features, writes recommendations
  ml/
    features.py       ← feature computation logic (called by feature_dag)
    train.py          ← model training logic (called by train_dag)
    score.py          ← scoring logic (called by score_dag)
```

DAGs remain thin orchestrators. ML logic lives in `nba/ml/` and is independently testable.

---

## SQL Migrations

All migrations apply to the same Postgres instance, so they stay in `sql/migrations/` at root. Files are prefixed to make origin clear:

```
sql/migrations/
  001_shared_games.sql
  002_shared_odds.sql
  003_nba_players.sql
  004_nba_player_game_logs.sql
  ...
```

Existing migration files get renamed to follow this convention during the restructure.

---

## Airflow DAG Discovery

Airflow discovers DAGs from a configured path. After the restructure, `AIRFLOW__CORE__DAGS_FOLDER` in `docker-compose.yml` points to `nba/dags/`. When MLB is added, Airflow can be configured with multiple DAG folders or a glob pattern (`*/dags/`).

---

## Future Sports

Adding MLB:

1. Create `mlb/dags/`, `mlb/plugins/`, `mlb/ml/`, `mlb/tests/`
2. Implement `mlb/plugins/mlb_stats_client.py` (MLB stats API)
3. Implement ingest/transform/backfill DAGs in `mlb/dags/`
4. Reuse `shared/plugins/odds_api_client.py` for MLB odds
5. Add `mlb/ml/` for MLB-specific feature engineering and model

No changes to NBA code required.

---

## What Does Not Change

- All existing DAG logic, transformer logic, and SQL schemas remain identical
- Docker Compose services (Airflow, Postgres) remain unchanged
- The `recommendations` Postgres table schema remains unchanged (consumed by the web client)
- The web client remains a separate repository and connects only via Postgres

---

## Out of Scope

- MLB or NFL implementation
- CI/CD configuration updates
- Kubernetes or cloud deployment changes
