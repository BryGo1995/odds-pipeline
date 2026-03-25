# Repo Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the `odds-pipeline` repo into a multi-sport namespace layout (`nba/`, `shared/`) so that adding MLB or NFL follows an established pattern without touching existing NBA code.

**Architecture:** NBA-specific code (DAGs, plugins, transformers, tests, ML) moves under `nba/`. Shared utilities (Odds API client, db client, Slack notifier) move to `shared/plugins/`. Airflow is reconfigured to discover DAGs from `/opt/airflow` (repo root) using a `.airflowignore` file, with `PYTHONPATH=/opt/airflow` so all imports resolve as `from nba.plugins.X` or `from shared.plugins.X`.

**Tech Stack:** Python, Apache Airflow 2.9.0, Docker Compose, pytest

**Spec:** `docs/superpowers/specs/2026-03-25-repo-restructure-design.md`

---

## File Map

### Created
- `nba/__init__.py`
- `nba/dags/__init__.py`
- `nba/plugins/__init__.py`
- `nba/plugins/transformers/__init__.py`
- `nba/ml/__init__.py`
- `nba/tests/__init__.py`
- `nba/tests/unit/__init__.py`
- `nba/tests/unit/transformers/__init__.py`
- `nba/tests/integration/__init__.py`
- `shared/__init__.py`
- `shared/plugins/__init__.py`
- `.airflowignore`

### Moved (source → destination)
- `dags/nba_odds_pipeline_dag.py` → `nba/dags/nba_odds_pipeline_dag.py`
- `dags/nba_odds_backfill_dag.py` → `nba/dags/nba_odds_backfill_dag.py`
- `dags/nba_stats_pipeline_dag.py` → `nba/dags/nba_stats_pipeline_dag.py`
- `dags/nba_stats_backfill_dag.py` → `nba/dags/nba_stats_backfill_dag.py`
- `plugins/odds_api_client.py` → `shared/plugins/odds_api_client.py`
- `plugins/db_client.py` → `shared/plugins/db_client.py`
- `plugins/slack_notifier.py` → `shared/plugins/slack_notifier.py`
- `plugins/nba_api_client.py` → `nba/plugins/nba_api_client.py`
- `plugins/transformers/events.py` → `nba/plugins/transformers/events.py`
- `plugins/transformers/game_id_linker.py` → `nba/plugins/transformers/game_id_linker.py`
- `plugins/transformers/odds.py` → `nba/plugins/transformers/odds.py`
- `plugins/transformers/player_game_logs.py` → `nba/plugins/transformers/player_game_logs.py`
- `plugins/transformers/player_name_resolution.py` → `nba/plugins/transformers/player_name_resolution.py`
- `plugins/transformers/player_props.py` → `nba/plugins/transformers/player_props.py`
- `plugins/transformers/players.py` → `nba/plugins/transformers/players.py`
- `plugins/transformers/scores.py` → `nba/plugins/transformers/scores.py`
- `plugins/transformers/team_game_logs.py` → `nba/plugins/transformers/team_game_logs.py`
- `plugins/transformers/team_season_stats.py` → `nba/plugins/transformers/team_season_stats.py`
- `plugins/transformers/teams.py` → `nba/plugins/transformers/teams.py`
- `tests/__init__.py` → `nba/tests/__init__.py`
- `tests/unit/conftest.py` → `nba/tests/unit/conftest.py`
- `tests/unit/__init__.py` → `nba/tests/unit/__init__.py`
- `tests/unit/test_db_client.py` → `nba/tests/unit/test_db_client.py`
- `tests/unit/test_game_id_linker.py` → `nba/tests/unit/test_game_id_linker.py`
- `tests/unit/test_nba_api_client.py` → `nba/tests/unit/test_nba_api_client.py`
- `tests/unit/test_nba_odds_backfill_dag.py` → `nba/tests/unit/test_nba_odds_backfill_dag.py`
- `tests/unit/test_nba_odds_pipeline_dag.py` → `nba/tests/unit/test_nba_odds_pipeline_dag.py`
- `tests/unit/test_nba_stats_backfill_dag.py` → `nba/tests/unit/test_nba_stats_backfill_dag.py`
- `tests/unit/test_nba_stats_pipeline_dag.py` → `nba/tests/unit/test_nba_stats_pipeline_dag.py`
- `tests/unit/test_odds_api_client.py` → `nba/tests/unit/test_odds_api_client.py`
- `tests/unit/test_player_name_resolution.py` → `nba/tests/unit/test_player_name_resolution.py`
- `tests/unit/test_schema.py` → `nba/tests/unit/test_schema.py`
- `tests/unit/test_settings.py` → `nba/tests/unit/test_settings.py`
- `tests/unit/test_slack_notifier.py` → `nba/tests/unit/test_slack_notifier.py`
- `tests/unit/transformers/__init__.py` → `nba/tests/unit/transformers/__init__.py`
- `tests/unit/transformers/test_events.py` → `nba/tests/unit/transformers/test_events.py`
- `tests/unit/transformers/test_odds.py` → `nba/tests/unit/transformers/test_odds.py`
- `tests/unit/transformers/test_player_game_logs.py` → `nba/tests/unit/transformers/test_player_game_logs.py`
- `tests/unit/transformers/test_player_props.py` → `nba/tests/unit/transformers/test_player_props.py`
- `tests/unit/transformers/test_players.py` → `nba/tests/unit/transformers/test_players.py`
- `tests/unit/transformers/test_scores.py` → `nba/tests/unit/transformers/test_scores.py`
- `tests/unit/transformers/test_team_game_logs.py` → `nba/tests/unit/transformers/test_team_game_logs.py`
- `tests/unit/transformers/test_team_season_stats.py` → `nba/tests/unit/transformers/test_team_season_stats.py`
- `tests/integration/conftest.py` → `nba/tests/integration/conftest.py`
- `tests/integration/__init__.py` → `nba/tests/integration/__init__.py`
- `tests/integration/test_ingest_to_raw.py` → `nba/tests/integration/test_ingest_to_raw.py`
- `tests/integration/test_player_stats_pipeline.py` → `nba/tests/integration/test_player_stats_pipeline.py`
- `tests/integration/test_transform_to_normalized.py` → `nba/tests/integration/test_transform_to_normalized.py`

### Modified
- `docker-compose.yml` — update volume mounts, add `PYTHONPATH` and `AIRFLOW__CORE__DAGS_FOLDER`
- `pytest.ini` — update `testpaths`
- `sql/migrations/001_add_player_stats_tables.sql` → renamed to `001_nba_add_player_stats_tables.sql`
- `sql/migrations/002_teams_and_integrity.sql` → renamed to `002_nba_teams_and_integrity.sql`
- `sql/migrations/003_player_game_logs_extended_stats.sql` → renamed to `003_nba_player_game_logs_extended_stats.sql`
- All 4 DAG files — remove `sys.path.insert`, update `from plugins.*` imports
- All test files — update `from plugins.*` and `from dags.*` imports, remove `sys.path.insert`

### Deleted (after verification)
- `dags/` directory (now empty)
- `plugins/` directory (now empty)
- `tests/` directory (now empty)

---

## Import Translation Reference

Use this table when updating any DAG or test file. Apply every matching row in the file.

| Old import | New import |
|---|---|
| `from plugins.db_client import ...` | `from shared.plugins.db_client import ...` |
| `from plugins.odds_api_client import ...` | `from shared.plugins.odds_api_client import ...` |
| `from plugins.slack_notifier import ...` | `from shared.plugins.slack_notifier import ...` |
| `from plugins.nba_api_client import ...` | `from nba.plugins.nba_api_client import ...` |
| `from plugins.transformers.X import ...` | `from nba.plugins.transformers.X import ...` |
| `from dags.X import ...` | `from nba.dags.X import ...` |
| `from config.settings import ...` | `from config.settings import ...` *(unchanged)* |

Also remove any `sys.path.insert(...)` line — `PYTHONPATH=/opt/airflow` in `docker-compose.yml` and `pythonpath = .` in `pytest.ini` make it unnecessary in both environments.

---

## Task 1: Create new directory structure

**Files:**
- Create: `nba/__init__.py`, `nba/dags/__init__.py`, `nba/plugins/__init__.py`, `nba/plugins/transformers/__init__.py`, `nba/ml/__init__.py`
- Create: `nba/tests/__init__.py`, `nba/tests/unit/__init__.py`, `nba/tests/unit/transformers/__init__.py`, `nba/tests/integration/__init__.py`
- Create: `shared/__init__.py`, `shared/plugins/__init__.py`

- [ ] **Step 1: Create all `__init__.py` files in one command**

```bash
mkdir -p nba/dags nba/plugins/transformers nba/ml nba/tests/unit/transformers nba/tests/integration shared/plugins
touch nba/__init__.py nba/dags/__init__.py nba/plugins/__init__.py nba/plugins/transformers/__init__.py nba/ml/__init__.py
touch nba/tests/__init__.py nba/tests/unit/__init__.py nba/tests/unit/transformers/__init__.py nba/tests/integration/__init__.py
touch shared/__init__.py shared/plugins/__init__.py
```

- [ ] **Step 2: Verify structure**

```bash
find nba shared -name "__init__.py" | sort
```

Expected output (11 files):
```
nba/__init__.py
nba/dags/__init__.py
nba/ml/__init__.py
nba/plugins/__init__.py
nba/plugins/transformers/__init__.py
nba/tests/__init__.py
nba/tests/integration/__init__.py
nba/tests/unit/__init__.py
nba/tests/unit/transformers/__init__.py
shared/__init__.py
shared/plugins/__init__.py
```

- [ ] **Step 3: Commit**

```bash
git add nba/ shared/
git commit -m "chore: scaffold nba/ and shared/ directory structure"
```

---

## Task 2: Move shared plugins

**Files:**
- Move: `plugins/db_client.py` → `shared/plugins/db_client.py`
- Move: `plugins/odds_api_client.py` → `shared/plugins/odds_api_client.py`
- Move: `plugins/slack_notifier.py` → `shared/plugins/slack_notifier.py`

- [ ] **Step 1: Copy shared plugin files to their new locations**

```bash
cp plugins/db_client.py shared/plugins/db_client.py
cp plugins/odds_api_client.py shared/plugins/odds_api_client.py
cp plugins/slack_notifier.py shared/plugins/slack_notifier.py
```

- [ ] **Step 2: Commit**

```bash
git add shared/plugins/
git commit -m "chore: copy shared plugins to shared/plugins/"
```

---

## Task 3: Move NBA plugins

**Files:**
- Move: `plugins/nba_api_client.py` → `nba/plugins/nba_api_client.py`
- Move: `plugins/transformers/*.py` → `nba/plugins/transformers/*.py`

- [ ] **Step 1: Copy NBA plugin files**

```bash
cp plugins/nba_api_client.py nba/plugins/nba_api_client.py
cp plugins/transformers/events.py nba/plugins/transformers/events.py
cp plugins/transformers/game_id_linker.py nba/plugins/transformers/game_id_linker.py
cp plugins/transformers/odds.py nba/plugins/transformers/odds.py
cp plugins/transformers/player_game_logs.py nba/plugins/transformers/player_game_logs.py
cp plugins/transformers/player_name_resolution.py nba/plugins/transformers/player_name_resolution.py
cp plugins/transformers/player_props.py nba/plugins/transformers/player_props.py
cp plugins/transformers/players.py nba/plugins/transformers/players.py
cp plugins/transformers/scores.py nba/plugins/transformers/scores.py
cp plugins/transformers/team_game_logs.py nba/plugins/transformers/team_game_logs.py
cp plugins/transformers/team_season_stats.py nba/plugins/transformers/team_season_stats.py
cp plugins/transformers/teams.py nba/plugins/transformers/teams.py
```

- [ ] **Step 2: Update internal import in `nba/plugins/transformers/player_name_resolution.py`**

`player_name_resolution.py` imports from `slack_notifier` internally. After the copy, update line 7:

```python
# Before
from plugins.slack_notifier import send_slack_message

# After
from shared.plugins.slack_notifier import send_slack_message
```

- [ ] **Step 3: Commit**

```bash
git add nba/plugins/
git commit -m "chore: copy NBA plugins to nba/plugins/"
```

---

## Task 4: Move and update DAGs

**Files:**
- Move + Modify: `dags/nba_odds_pipeline_dag.py` → `nba/dags/nba_odds_pipeline_dag.py`
- Move + Modify: `dags/nba_odds_backfill_dag.py` → `nba/dags/nba_odds_backfill_dag.py`
- Move + Modify: `dags/nba_stats_pipeline_dag.py` → `nba/dags/nba_stats_pipeline_dag.py`
- Move + Modify: `dags/nba_stats_backfill_dag.py` → `nba/dags/nba_stats_backfill_dag.py`

Each DAG needs two changes:
1. Remove the `sys.path.insert(0, "/opt/airflow")` line
2. Update all `from plugins.*` imports using the Import Translation Reference table

- [ ] **Step 1: Copy DAGs to `nba/dags/`**

```bash
cp dags/nba_odds_pipeline_dag.py nba/dags/nba_odds_pipeline_dag.py
cp dags/nba_odds_backfill_dag.py nba/dags/nba_odds_backfill_dag.py
cp dags/nba_stats_pipeline_dag.py nba/dags/nba_stats_pipeline_dag.py
cp dags/nba_stats_backfill_dag.py nba/dags/nba_stats_backfill_dag.py
```

- [ ] **Step 2: Update imports in `nba/dags/nba_odds_pipeline_dag.py`**

Remove:
```python
sys.path.insert(0, "/opt/airflow")
```

Update:
```python
# Before
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores, fetch_player_props
from plugins.slack_notifier import notify_failure, notify_success
from plugins.transformers.events import transform_events
from plugins.transformers.odds import transform_odds
from plugins.transformers.player_props import transform_player_props
from plugins.transformers.scores import transform_scores

# After
from shared.plugins.db_client import get_data_db_conn, store_raw_response
from shared.plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores, fetch_player_props
from shared.plugins.slack_notifier import notify_failure, notify_success
from nba.plugins.transformers.events import transform_events
from nba.plugins.transformers.odds import transform_odds
from nba.plugins.transformers.player_props import transform_player_props
from nba.plugins.transformers.scores import transform_scores
```

- [ ] **Step 3: Update imports in `nba/dags/nba_odds_backfill_dag.py`**

Remove:
```python
sys.path.insert(0, "/opt/airflow")
```

Update:
```python
# Before
from config.settings import SPORT, REGIONS, MARKETS, BOOKMAKERS
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores
from plugins.transformers.events import transform_events
from plugins.transformers.odds import transform_odds
from plugins.transformers.scores import transform_scores
from plugins.transformers.player_props import transform_player_props

# After
from config.settings import SPORT, REGIONS, MARKETS, BOOKMAKERS
from shared.plugins.db_client import get_data_db_conn, store_raw_response
from shared.plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores
from nba.plugins.transformers.events import transform_events
from nba.plugins.transformers.odds import transform_odds
from nba.plugins.transformers.scores import transform_scores
from nba.plugins.transformers.player_props import transform_player_props
```

- [ ] **Step 4: Update imports in `nba/dags/nba_stats_pipeline_dag.py`**

Remove:
```python
sys.path.insert(0, "/opt/airflow")
```

Update:
```python
# Before
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.nba_api_client import (
    fetch_players,
    fetch_player_game_logs,
    fetch_team_game_logs,
    fetch_team_season_stats,
    fetch_teams,
)
from plugins.slack_notifier import notify_failure, notify_success
from plugins.transformers.game_id_linker import link_nba_game_ids
from plugins.transformers.player_game_logs import transform_player_game_logs
from plugins.transformers.player_name_resolution import resolve_player_ids
from plugins.transformers.players import transform_players
from plugins.transformers.team_game_logs import transform_team_game_logs
from plugins.transformers.team_season_stats import transform_team_season_stats
from plugins.transformers.teams import transform_teams

# After
from shared.plugins.db_client import get_data_db_conn, store_raw_response
from nba.plugins.nba_api_client import (
    fetch_players,
    fetch_player_game_logs,
    fetch_team_game_logs,
    fetch_team_season_stats,
    fetch_teams,
)
from shared.plugins.slack_notifier import notify_failure, notify_success
from nba.plugins.transformers.game_id_linker import link_nba_game_ids
from nba.plugins.transformers.player_game_logs import transform_player_game_logs
from nba.plugins.transformers.player_name_resolution import resolve_player_ids
from nba.plugins.transformers.players import transform_players
from nba.plugins.transformers.team_game_logs import transform_team_game_logs
from nba.plugins.transformers.team_season_stats import transform_team_season_stats
from nba.plugins.transformers.teams import transform_teams
```

- [ ] **Step 5: Update imports in `nba/dags/nba_stats_backfill_dag.py`**

Remove:
```python
sys.path.insert(0, "/opt/airflow")
```

Update:
```python
# Before
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.nba_api_client import (
    fetch_players,
    fetch_player_game_logs,
    fetch_team_game_logs,
    fetch_team_season_stats,
)
from plugins.transformers.players import transform_players
from plugins.transformers.player_game_logs import transform_player_game_logs
from plugins.transformers.team_game_logs import transform_team_game_logs
from plugins.transformers.team_season_stats import transform_team_season_stats
from plugins.transformers.game_id_linker import link_nba_game_ids

# After
from shared.plugins.db_client import get_data_db_conn, store_raw_response
from nba.plugins.nba_api_client import (
    fetch_players,
    fetch_player_game_logs,
    fetch_team_game_logs,
    fetch_team_season_stats,
)
from nba.plugins.transformers.players import transform_players
from nba.plugins.transformers.player_game_logs import transform_player_game_logs
from nba.plugins.transformers.team_game_logs import transform_team_game_logs
from nba.plugins.transformers.team_season_stats import transform_team_season_stats
from nba.plugins.transformers.game_id_linker import link_nba_game_ids
```

- [ ] **Step 6: Commit**

```bash
git add nba/dags/
git commit -m "chore: copy DAGs to nba/dags/ with updated imports"
```

---

## Task 5: Move and update tests

**Files:**
- Move + Modify: all files under `tests/` → `nba/tests/`

In addition to applying the Import Translation Reference table, three more changes are needed in the DAG test files and `test_nba_api_client.py`:

1. **Remove `sys.path.insert` lines** — `pytest.ini pythonpath = .` handles this
2. **Update `dag_folder` arguments** — `DagBag(dag_folder="dags/", ...)` → `DagBag(dag_folder="nba/dags/", ...)`
3. **Update `from dags.X` imports** — `from dags.nba_odds_pipeline_dag import ...` → `from nba.dags.nba_odds_pipeline_dag import ...`

Affected files for changes 1–3:
- `test_nba_odds_pipeline_dag.py` — has `sys.path.insert` (line 5), four `dag_folder="dags/"`, four `from dags.nba_odds_pipeline_dag import`
- `test_nba_stats_pipeline_dag.py` — has `sys.path.insert` (line 5), four `dag_folder="dags/"`
- `test_nba_odds_backfill_dag.py` — has `sys.path.insert` (line 5), two `dag_folder="dags/"`
- `test_nba_stats_backfill_dag.py` — has `sys.path.insert` (line 5), two `dag_folder="dags/"`
- `test_nba_api_client.py` — has `sys.path.insert(0, ".")` (line 169), one `from dags.nba_stats_pipeline_dag import fetch_player_game_logs_task` (line 188)

- [ ] **Step 1: Copy all test files**

```bash
cp tests/unit/conftest.py nba/tests/unit/conftest.py
cp tests/unit/test_db_client.py nba/tests/unit/test_db_client.py
cp tests/unit/test_game_id_linker.py nba/tests/unit/test_game_id_linker.py
cp tests/unit/test_nba_api_client.py nba/tests/unit/test_nba_api_client.py
cp tests/unit/test_nba_odds_backfill_dag.py nba/tests/unit/test_nba_odds_backfill_dag.py
cp tests/unit/test_nba_odds_pipeline_dag.py nba/tests/unit/test_nba_odds_pipeline_dag.py
cp tests/unit/test_nba_stats_backfill_dag.py nba/tests/unit/test_nba_stats_backfill_dag.py
cp tests/unit/test_nba_stats_pipeline_dag.py nba/tests/unit/test_nba_stats_pipeline_dag.py
cp tests/unit/test_odds_api_client.py nba/tests/unit/test_odds_api_client.py
cp tests/unit/test_player_name_resolution.py nba/tests/unit/test_player_name_resolution.py
cp tests/unit/test_schema.py nba/tests/unit/test_schema.py
cp tests/unit/test_settings.py nba/tests/unit/test_settings.py
cp tests/unit/test_slack_notifier.py nba/tests/unit/test_slack_notifier.py
cp tests/unit/transformers/test_events.py nba/tests/unit/transformers/test_events.py
cp tests/unit/transformers/test_odds.py nba/tests/unit/transformers/test_odds.py
cp tests/unit/transformers/test_player_game_logs.py nba/tests/unit/transformers/test_player_game_logs.py
cp tests/unit/transformers/test_player_props.py nba/tests/unit/transformers/test_player_props.py
cp tests/unit/transformers/test_players.py nba/tests/unit/transformers/test_players.py
cp tests/unit/transformers/test_scores.py nba/tests/unit/transformers/test_scores.py
cp tests/unit/transformers/test_team_game_logs.py nba/tests/unit/transformers/test_team_game_logs.py
cp tests/unit/transformers/test_team_season_stats.py nba/tests/unit/transformers/test_team_season_stats.py
cp tests/integration/conftest.py nba/tests/integration/conftest.py
cp tests/integration/test_ingest_to_raw.py nba/tests/integration/test_ingest_to_raw.py
cp tests/integration/test_player_stats_pipeline.py nba/tests/integration/test_player_stats_pipeline.py
cp tests/integration/test_transform_to_normalized.py nba/tests/integration/test_transform_to_normalized.py
```

- [ ] **Step 2: Update all imports in the copied test files**

Apply the Import Translation Reference table to every file under `nba/tests/`. Per-file guide:

| File | Changes needed |
|---|---|
| `test_db_client.py` | `from plugins.db_client` → `from shared.plugins.db_client` |
| `test_odds_api_client.py` | `from plugins.odds_api_client` → `from shared.plugins.odds_api_client` |
| `test_slack_notifier.py` | `from plugins.slack_notifier` → `from shared.plugins.slack_notifier` |
| `test_nba_api_client.py` | `from plugins.nba_api_client` → `from nba.plugins.nba_api_client`; remove `sys.path.insert(0, ".")` (line 169); `from dags.nba_stats_pipeline_dag` → `from nba.dags.nba_stats_pipeline_dag` (line 188) |
| `test_game_id_linker.py` | `from plugins.transformers.game_id_linker` → `from nba.plugins.transformers.game_id_linker` |
| `test_player_name_resolution.py` | `from plugins.transformers.player_name_resolution` → `from nba.plugins.transformers.player_name_resolution` |
| `test_nba_odds_pipeline_dag.py` | Remove `sys.path.insert` (line 5); `from plugins.slack_notifier` → `from shared.plugins.slack_notifier`; all `dag_folder="dags/"` → `dag_folder="nba/dags/"`; all `from dags.nba_odds_pipeline_dag` → `from nba.dags.nba_odds_pipeline_dag` |
| `test_nba_stats_pipeline_dag.py` | Remove `sys.path.insert` (line 5); `from plugins.slack_notifier` → `from shared.plugins.slack_notifier`; all `dag_folder="dags/"` → `dag_folder="nba/dags/"` |
| `test_nba_odds_backfill_dag.py` | Remove `sys.path.insert` (line 5); all `dag_folder="dags/"` → `dag_folder="nba/dags/"` |
| `test_nba_stats_backfill_dag.py` | Remove `sys.path.insert` (line 5); all `dag_folder="dags/"` → `dag_folder="nba/dags/"` |
| `transformers/test_events.py` | `from plugins.transformers.events` → `from nba.plugins.transformers.events` |
| `transformers/test_odds.py` | `from plugins.transformers.odds` → `from nba.plugins.transformers.odds` |
| `transformers/test_scores.py` | `from plugins.transformers.scores` → `from nba.plugins.transformers.scores` |
| `transformers/test_player_props.py` | `from plugins.transformers.player_props` → `from nba.plugins.transformers.player_props` |
| `transformers/test_players.py` | `from plugins.transformers.players` → `from nba.plugins.transformers.players` |
| `transformers/test_player_game_logs.py` | `from plugins.transformers.player_game_logs` → `from nba.plugins.transformers.player_game_logs` |
| `transformers/test_team_game_logs.py` | `from plugins.transformers.team_game_logs` → `from nba.plugins.transformers.team_game_logs` |
| `transformers/test_team_season_stats.py` | `from plugins.transformers.team_season_stats` → `from nba.plugins.transformers.team_season_stats` |
| `integration/test_ingest_to_raw.py` | `from plugins.db_client` → `from shared.plugins.db_client` |
| `integration/test_transform_to_normalized.py` | `from plugins.transformers.events` → `from nba.plugins.transformers.events`; `from plugins.transformers.player_props` → `from nba.plugins.transformers.player_props` |
| `integration/test_player_stats_pipeline.py` | `from plugins.db_client` → `from shared.plugins.db_client`; `from plugins.transformers.players` → `from nba.plugins.transformers.players`; `from plugins.transformers.player_game_logs` → `from nba.plugins.transformers.player_game_logs`; `from plugins.transformers.team_game_logs` → `from nba.plugins.transformers.team_game_logs`; `from plugins.transformers.team_season_stats` → `from nba.plugins.transformers.team_season_stats`; `from plugins.transformers.player_name_resolution` → `from nba.plugins.transformers.player_name_resolution`; `from plugins.transformers.game_id_linker` → `from nba.plugins.transformers.game_id_linker` |

- [ ] **Step 3: Commit**

```bash
git add nba/tests/
git commit -m "chore: copy tests to nba/tests/ with updated imports"
```

---

## Task 6: Update `pytest.ini`

**Files:**
- Modify: `pytest.ini`

- [ ] **Step 1: Update `testpaths`**

Change `pytest.ini` from:
```ini
[pytest]
testpaths = tests
pythonpath = .
```

To:
```ini
[pytest]
testpaths = nba/tests
pythonpath = .
```

- [ ] **Step 2: Run the unit test suite — verify all tests pass**

```bash
pytest nba/tests/unit -v
```

Expected: all tests that passed before still pass. If any test fails with an `ImportError`, check the failing file against the Import Translation Reference table.

- [ ] **Step 3: Commit**

```bash
git add pytest.ini
git commit -m "chore: update pytest.ini to point at nba/tests/"
```

---

## Task 7: Update `docker-compose.yml`

**Files:**
- Modify: `docker-compose.yml`

Two changes in the `x-airflow-common` block:
1. Add `PYTHONPATH` and `AIRFLOW__CORE__DAGS_FOLDER` to the `environment` section
2. Replace the `./dags`, `./plugins`, `./config` volume mounts with a single repo-root mount

- [ ] **Step 1: Add environment variables**

In the `environment` block under `x-airflow-common`, add after the existing env vars:

```yaml
PYTHONPATH: /opt/airflow
AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow
```

- [ ] **Step 2: Replace volume mounts**

In the `volumes` block under `x-airflow-common`, replace:

```yaml
- ./dags:/opt/airflow/dags
- ./plugins:/opt/airflow/plugins
- ./config:/opt/airflow/config
- ./logs:/opt/airflow/logs
```

With:

```yaml
- ./:/opt/airflow
- ./logs:/opt/airflow/logs
```

The single `./:/opt/airflow` mount makes all repo directories (`nba/`, `shared/`, `config/`, `sql/`, `.airflowignore`) visible to the container without additional per-directory mounts. Adding `mlb/` in the future requires no compose changes.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "chore: update docker-compose volume mounts and Airflow DAG path for nba/ layout"
```

> **Do not start the Airflow container until Task 8 is complete.** `DAGS_FOLDER=/opt/airflow` causes Airflow to scan the entire repo tree. Without `.airflowignore` in place, the scheduler will log a warning for every non-DAG Python file it encounters.

---

## Task 8: Create `.airflowignore`

**Files:**
- Create: `.airflowignore`

Airflow scans from `DAGS_FOLDER=/opt/airflow`. This file tells it which directories to skip. Without it, Airflow logs warnings for every non-DAG Python file it encounters.

- [ ] **Step 1: Create `.airflowignore` at repo root**

```
shared/
sql/
config/
docs/
tests/
nba/ml/
nba/plugins/
nba/tests/
```

- [ ] **Step 2: Commit**

```bash
git add .airflowignore
git commit -m "chore: add .airflowignore for Airflow DAG scanner"
```

---

## Task 9: Rename SQL migrations

**Files:**
- Rename: `sql/migrations/001_add_player_stats_tables.sql` → `sql/migrations/001_nba_add_player_stats_tables.sql`
- Rename: `sql/migrations/002_teams_and_integrity.sql` → `sql/migrations/002_nba_teams_and_integrity.sql`
- Rename: `sql/migrations/003_player_game_logs_extended_stats.sql` → `sql/migrations/003_nba_player_game_logs_extended_stats.sql`

No migration runner tracks filenames — renaming is safe.

- [ ] **Step 1: Rename the files using git mv**

```bash
git mv sql/migrations/001_add_player_stats_tables.sql sql/migrations/001_nba_add_player_stats_tables.sql
git mv sql/migrations/002_teams_and_integrity.sql sql/migrations/002_nba_teams_and_integrity.sql
git mv sql/migrations/003_player_game_logs_extended_stats.sql sql/migrations/003_nba_player_game_logs_extended_stats.sql
```

- [ ] **Step 2: Verify**

```bash
ls sql/migrations/
```

Expected: files listed with `nba_` prefix.

- [ ] **Step 3: Commit**

```bash
git commit -m "chore: add nba_ prefix to SQL migration filenames"
```

---

## Task 10: Remove old directories

Only do this after Task 6 Step 2 (tests pass).

- [ ] **Step 1: Confirm tests still pass**

```bash
pytest nba/tests/unit -v
```

Expected: all pass.

- [ ] **Step 2: Delete old directories**

```bash
git rm -r dags/ plugins/ tests/
```

- [ ] **Step 3: Commit**

```bash
git commit -m "chore: remove old dags/, plugins/, tests/ after migration to nba/ and shared/"
```

---

## Task 11: Final verification

- [ ] **Step 1: Run the full unit test suite**

```bash
pytest nba/tests/unit -v
```

Expected: all tests pass, no import errors.

- [ ] **Step 2: Verify directory structure matches the spec**

```bash
find nba shared sql .airflowignore -not -path "*/__pycache__/*" | sort
```

Confirm: `nba/dags/`, `nba/plugins/transformers/`, `nba/ml/`, `nba/tests/`, `shared/plugins/`, `.airflowignore` all present.

- [ ] **Step 3: Verify no remaining stale imports**

```bash
grep -r "from plugins\." nba/ shared/
grep -r "from dags\." nba/ shared/
grep -r "sys.path.insert" nba/ shared/
grep -r "dag_folder=\"dags/" nba/ shared/
```

Expected: no output from any command. Fix any matches before continuing.

- [ ] **Step 4: Final commit**

```bash
git add nba/ shared/ .airflowignore pytest.ini docker-compose.yml
git commit -m "chore: complete repo restructure to nba/ and shared/ namespacing"
```
