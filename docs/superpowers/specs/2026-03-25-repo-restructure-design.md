# Repository Restructure Design Spec

**Date:** 2026-03-25
**Status:** Approved

---

## Overview

Restructure the `odds-pipeline` repo to support multi-sport expansion (NBA → MLB → NFL) without overcomplicating the current state. The repo keeps its name. NBA code moves into a `nba/` namespace. Shared infrastructure (Odds API client, db client, Slack notifier, Docker Compose, Postgres) moves to `shared/` or stays at the root. The ML layer (Parquet/DuckDB/MLflow) is introduced under `nba/ml/` alongside the existing pipeline, keeping ingest and model code co-located per sport since they evolve together.

---

## Goals

- Namespace NBA code under `nba/` so adding MLB or NFL follows the same pattern
- Move shared utilities to `shared/plugins/` — consumed by all sports
- Introduce `nba/ml/` for feature engineering and model code, with ML DAGs living in `nba/dags/`
- Keep Docker Compose, Postgres migrations, `config/`, and Dockerfile at the root
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
    plugins/        ← odds_api_client.py, db_client.py, slack_notifier.py
                       (shared across all sports; each has an __init__.py)
  sql/
    migrations/     ← all Postgres migrations, prefixed by sport or "shared_"
  config/           ← stays at repo root unchanged (settings.py imported as config.settings)
  docker-compose.yml
  Dockerfile
  requirements.txt
  pytest.ini
  docs/
  .airflowignore
```

### Migration Map

| Current location | New location | Notes |
|---|---|---|
| `dags/` | `nba/dags/` | All existing NBA DAGs move here |
| `plugins/odds_api_client.py` | `shared/plugins/odds_api_client.py` | Shared across sports |
| `plugins/db_client.py` | `shared/plugins/db_client.py` | Shared across sports |
| `plugins/slack_notifier.py` | `shared/plugins/slack_notifier.py` | Shared across sports |
| `plugins/nba_api_client.py` | `nba/plugins/nba_api_client.py` | NBA-specific |
| `plugins/transformers/` | `nba/plugins/transformers/` | NBA-specific transformers |
| `tests/` | `nba/tests/` | NBA-specific tests |
| `config/` | `config/` | Stays at root, no change |
| `sql/migrations/` | `sql/migrations/` | Stays at root; files get sport prefix (see SQL section) |
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

Adding a new sport is: create the directory, implement the stat API client in `plugins/`, wire up DAGs in `dags/`, reuse `shared/plugins/` for Odds API, db, and Slack.

### Shared Layer

| Path | Contents |
|---|---|
| `shared/plugins/odds_api_client.py` | Odds API client — all sports share the same API |
| `shared/plugins/db_client.py` | Postgres connection helper — shared across sports |
| `shared/plugins/slack_notifier.py` | Slack notifications — shared across sports |
| `config/` | App settings — stays at root, imported as `from config.settings import ...` |
| `sql/migrations/` | All Postgres migrations — same DB instance for all sports |
| `docker-compose.yml` | Airflow, Postgres, MLflow — sport-agnostic orchestration |
| `Dockerfile` | Single image for Airflow workers |
| `requirements.txt` | All Python dependencies |

### ML Layer Placement

The ML layer lives inside `nba/` rather than a top-level `ml/` directory because NBA ingest and NBA model code are tightly coupled — a new stat column in Postgres immediately becomes a new feature. When MLB is added, `mlb/ml/` follows the same pattern.

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

## Airflow DAG Discovery

### DAGS_FOLDER

`AIRFLOW__CORE__DAGS_FOLDER` is set to `/opt/airflow` (the repo root). Airflow scans from the root and finds DAGs under `nba/dags/`. A `.airflowignore` file at the repo root excludes everything except DAG directories:

```
# .airflowignore — exclude non-DAG directories from Airflow's scanner
shared/
sql/
config/
docs/
tests/
nba/ml/
```

When MLB is added, its DAGs in `mlb/dags/` are discovered automatically — no change to `AIRFLOW__CORE__DAGS_FOLDER` required. This avoids Airflow 2.x's limitation of supporting only one DAG folder path.

### Volume Mounts

Current `docker-compose.yml` mounts:

```yaml
- ./dags:/opt/airflow/dags
- ./plugins:/opt/airflow/plugins
```

After restructure, the entire repo root is mounted:

```yaml
- ./:/opt/airflow
```

This gives the Airflow container access to `nba/`, `shared/`, `config/`, and `sql/` under `/opt/airflow`.

### Python Import Paths

`PYTHONPATH` is set to `/opt/airflow` in `docker-compose.yml` for all Airflow services. This allows DAGs to import as:

```python
from nba.plugins.nba_api_client import NBAApiClient
from shared.plugins.odds_api_client import OddsApiClient
from shared.plugins.db_client import get_connection
from config.settings import Settings
```

All existing `sys.path.insert(0, "/opt/airflow/plugins")` calls in DAGs are removed and replaced with the import style above.

---

## Test Configuration

`pytest.ini` is updated to:

```ini
[pytest]
testpaths = nba/tests
pythonpath = .
```

`pythonpath = .` (repo root) gives tests access to `nba.*`, `shared.*`, and `config.*` using the same import style as the DAGs. When MLB tests are added, `testpaths` becomes `nba/tests mlb/tests`.

---

## SQL Migrations

All migrations apply to the same Postgres instance and stay in `sql/migrations/` at root. Existing files are renamed with a sport or `shared_` prefix to make origin clear. No migration runner tracks filenames — renaming is safe.

### Renaming Convention

Prefix the filename, preserve the sequence number:

```
sql/migrations/
  001_nba_add_player_stats_tables.sql    ← was 001_add_player_stats_tables.sql
  002_nba_teams_and_integrity.sql        ← was 002_teams_and_integrity.sql
  003_nba_player_game_logs_extended.sql  ← was 003_player_game_logs_extended_stats.sql
```

New migrations introduced for the ML layer follow the same pattern (e.g., `004_nba_recommendations.sql`). Shared-schema migrations use `shared_` prefix (e.g., `shared_001_games.sql`) if any are added in the future.

`sql/init_schema.sql` is out of scope for this restructure. It stays at `sql/init_schema.sql` and the `docker-compose.yml` `data-postgres` mount for it remains unchanged.

---

## Future Sports

Adding MLB:

1. Create `mlb/dags/`, `mlb/plugins/`, `mlb/ml/`, `mlb/tests/`
2. Implement `mlb/plugins/mlb_stats_client.py` (MLB stats API)
3. Implement ingest/transform/backfill DAGs in `mlb/dags/`
4. Reuse `shared/plugins/` for Odds API, db, and Slack
5. Add MLB migrations to `sql/migrations/` with `mlb_` prefix
6. Update `pytest.ini` `testpaths` to include `mlb/tests`
7. Update `.airflowignore` if needed

No changes to NBA code required.

---

## What Does Not Change

- All existing DAG logic, transformer logic, and SQL schemas remain identical
- Docker Compose services (Airflow, Postgres) remain unchanged beyond the volume mount update
- The `recommendations` Postgres table schema remains unchanged (consumed by the web client)
- The web client remains a separate repository and connects only via Postgres
- `config/` remains at the repo root with no changes

---

## Out of Scope

- MLB or NFL implementation
- CI/CD configuration updates
- Kubernetes or cloud deployment changes
- MLflow service addition to docker-compose (separate ML layer implementation step)
