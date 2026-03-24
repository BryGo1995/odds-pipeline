# DAG Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate 7 Airflow DAGs into 4 files with consistent `nba_odds_*` / `nba_stats_*` naming, explicit in-DAG task dependencies, and a graceful skip fix for player props on no-game days.

**Architecture:** Two independent PRs executed sequentially. PR 1 builds `nba_odds_pipeline` (merges `nba_ingest` + `nba_transform` + `nba_player_props`) and renames the odds backfill. PR 2 builds `nba_stats_pipeline` (merges `nba_player_stats_ingest` + `nba_player_stats_transform` with an `ExternalTaskSensor` on `nba_odds_pipeline`) and renames the stats backfill.

**Tech Stack:** Apache Airflow 2.x, Python 3.x, `pendulum`, `psycopg2`, `pytest`, `unittest.mock`

**Spec:** `docs/superpowers/specs/2026-03-24-dag-refactor-design.md`

---

## Task 1: Build `nba_odds_pipeline` and retire old odds DAGs

### Files

- Create: `dags/nba_odds_pipeline_dag.py`
- Create: `tests/unit/test_nba_odds_pipeline_dag.py`
- Rename + edit: `dags/backfill_dag.py` → `dags/nba_odds_backfill_dag.py` (change `dag_id` only)
- Rename + edit: `tests/unit/test_backfill_dag.py` → `tests/unit/test_nba_odds_backfill_dag.py` (update `dag_id` references)
- Delete: `dags/ingest_dag.py`, `dags/transform_dag.py`, `dags/player_props_dag.py`
- Delete: `tests/unit/test_ingest_dag.py`, `tests/unit/test_transform_dag.py`, `tests/unit/test_player_props_dag.py`

---

- [ ] **Step 1: Write the failing DAG structure tests**

Create `tests/unit/test_nba_odds_pipeline_dag.py`:

```python
# tests/unit/test_nba_odds_pipeline_dag.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_odds_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_odds_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "fetch_events", "fetch_odds", "fetch_scores",
        "transform_events", "transform_odds", "transform_scores",
        "fetch_player_props", "transform_player_props",
    }


def test_dag_task_chain():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_odds_pipeline"]

    # All three fetch tasks feed transform_events
    for task_id in ["fetch_events", "fetch_odds", "fetch_scores"]:
        downstream = [t.task_id for t in dag.get_task(task_id).downstream_list]
        assert "transform_events" in downstream, f"{task_id} must feed transform_events"

    # transform_events feeds transform_odds, transform_scores, and fetch_player_props
    xform_events_downstream = [t.task_id for t in dag.get_task("transform_events").downstream_list]
    assert "transform_odds" in xform_events_downstream
    assert "transform_scores" in xform_events_downstream
    assert "fetch_player_props" in xform_events_downstream

    # fetch_player_props feeds transform_player_props
    assert "transform_player_props" in [
        t.task_id for t in dag.get_task("fetch_player_props").downstream_list
    ]


def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from plugins.slack_notifier import notify_success, notify_failure
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_odds_pipeline"]
    assert dag.on_success_callback is notify_success
    assert dag.on_failure_callback is notify_failure
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/unit/test_nba_odds_pipeline_dag.py -v
```

Expected: 4 failures — `nba_odds_pipeline` not found in DagBag.

---

- [ ] **Step 3: Write the failing graceful-skip unit tests**

Append to `tests/unit/test_nba_odds_pipeline_dag.py`:

```python
def test_fetch_player_props_skips_when_no_games():
    from unittest.mock import MagicMock, patch
    from dags.nba_odds_pipeline_dag import fetch_player_props_task

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = []  # no games
    mock_conn.cursor.return_value = mock_cursor
    ti = MagicMock()

    with patch("dags.nba_odds_pipeline_dag.get_data_db_conn", return_value=mock_conn), \
         patch.dict("os.environ", {"ODDS_API_KEY": "test_key"}):
        fetch_player_props_task(ti=ti)

    ti.xcom_push.assert_called_once_with(key="skipped", value=True)


def test_fetch_player_props_pushes_api_remaining_when_games_exist():
    from unittest.mock import MagicMock, patch, call
    from dags.nba_odds_pipeline_dag import fetch_player_props_task

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = [("game-abc",)]
    mock_conn.cursor.return_value = mock_cursor
    ti = MagicMock()

    fake_props = {"markets": []}
    with patch("dags.nba_odds_pipeline_dag.get_data_db_conn", return_value=mock_conn), \
         patch.dict("os.environ", {"ODDS_API_KEY": "test_key"}), \
         patch("dags.nba_odds_pipeline_dag.fetch_player_props",
               return_value=(fake_props, 400)) as mock_fetch, \
         patch("dags.nba_odds_pipeline_dag.store_raw_response"):
        fetch_player_props_task(ti=ti)

    ti.xcom_push.assert_called_once_with(key="api_remaining", value=400)


def test_transform_player_props_skips_when_fetch_skipped():
    from unittest.mock import MagicMock, patch
    from dags.nba_odds_pipeline_dag import transform_player_props_task

    ti = MagicMock()
    ti.xcom_pull.return_value = True  # fetch was skipped

    mock_conn = MagicMock()
    with patch("dags.nba_odds_pipeline_dag.get_data_db_conn", return_value=mock_conn):
        transform_player_props_task(ti=ti)

    # DB should not be touched when skipped
    mock_conn.cursor.assert_not_called()


def test_transform_player_props_runs_when_not_skipped():
    from unittest.mock import MagicMock, patch, call
    from dags.nba_odds_pipeline_dag import transform_player_props_task

    ti = MagicMock()
    ti.xcom_pull.return_value = None  # not skipped

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = ([{"props": "data"}],)
    mock_conn.cursor.return_value = mock_cursor

    with patch("dags.nba_odds_pipeline_dag.get_data_db_conn", return_value=mock_conn), \
         patch("dags.nba_odds_pipeline_dag.transform_player_props") as mock_transform:
        transform_player_props_task(ti=ti)

    mock_transform.assert_called_once()
```

- [ ] **Step 4: Run graceful-skip tests to verify they fail**

```bash
pytest tests/unit/test_nba_odds_pipeline_dag.py -v -k "skip or api_remaining"
```

Expected: ImportError — `dags.nba_odds_pipeline_dag` does not exist yet.

---

- [ ] **Step 5: Create `dags/nba_odds_pipeline_dag.py`**

```python
# dags/nba_odds_pipeline_dag.py
import logging
import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from config.settings import (
    SPORT, REGIONS, MARKETS, PLAYER_PROP_MARKETS, BOOKMAKERS, ODDS_FORMAT, SCORES_DAYS_FROM,
)
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores, fetch_player_props
from plugins.slack_notifier import notify_failure, notify_success
from plugins.transformers.events import transform_events
from plugins.transformers.odds import transform_odds
from plugins.transformers.player_props import transform_player_props
from plugins.transformers.scores import transform_scores


# ---------------------------------------------------------------------------
# Shared ingest helper
# ---------------------------------------------------------------------------

def _fetch_and_store(endpoint_name, fetch_fn, fetch_kwargs, ti):
    api_key = os.environ["ODDS_API_KEY"]
    conn = get_data_db_conn()
    try:
        data, remaining = fetch_fn(api_key=api_key, **fetch_kwargs)
        store_raw_response(conn, endpoint=endpoint_name, params=fetch_kwargs, response=data, status="success")
        ti.xcom_push(key="api_remaining", value=remaining)
    except Exception:
        store_raw_response(conn, endpoint=endpoint_name, params=fetch_kwargs, response=None, status="error")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Ingest tasks
# ---------------------------------------------------------------------------

def fetch_events_task(**context):
    _fetch_and_store("events", fetch_events, {"sport": SPORT}, context["ti"])


def fetch_odds_task(**context):
    _fetch_and_store("odds", fetch_odds, {
        "sport": SPORT,
        "regions": REGIONS,
        "markets": MARKETS,
        "bookmakers": BOOKMAKERS,
        "odds_format": ODDS_FORMAT,
    }, context["ti"])


def fetch_scores_task(**context):
    _fetch_and_store("scores", fetch_scores, {"sport": SPORT, "days_from": SCORES_DAYS_FROM}, context["ti"])


# ---------------------------------------------------------------------------
# Shared transform helper
# ---------------------------------------------------------------------------

def _get_latest_raw(conn, endpoint):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT response FROM raw_api_responses
            WHERE endpoint = %s AND status = 'success'
            ORDER BY fetched_at DESC LIMIT 1
            """,
            (endpoint,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"No successful raw data found for endpoint: '{endpoint}'.")
    return row[0]


# ---------------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------------

def transform_events_task(**context):
    conn = get_data_db_conn()
    try:
        transform_events(conn, _get_latest_raw(conn, "events"))
    finally:
        conn.close()


def transform_odds_task(**context):
    conn = get_data_db_conn()
    try:
        transform_odds(conn, _get_latest_raw(conn, "odds"))
    finally:
        conn.close()


def transform_scores_task(**context):
    conn = get_data_db_conn()
    try:
        transform_scores(conn, _get_latest_raw(conn, "scores"))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Player props tasks (ingest + transform in one task group)
# ---------------------------------------------------------------------------

def fetch_player_props_task(**context):
    ti = context["ti"]
    api_key = os.environ["ODDS_API_KEY"]
    conn = get_data_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT game_id FROM games WHERE commence_time >= CURRENT_DATE ORDER BY commence_time"
            )
            game_ids = [row[0] for row in cur.fetchall()]

        if not game_ids:
            logging.warning("No today/upcoming games found — skipping player props fetch.")
            ti.xcom_push(key="skipped", value=True)
            return

        fetch_kwargs = {
            "sport": SPORT,
            "regions": REGIONS,
            "markets": PLAYER_PROP_MARKETS,
            "bookmakers": BOOKMAKERS,
            "odds_format": ODDS_FORMAT,
        }
        all_props = []
        remaining = 0
        for game_id in game_ids:
            try:
                data, remaining = fetch_player_props(
                    api_key=api_key,
                    event_id=game_id,
                    **fetch_kwargs,
                )
                all_props.append(data)
            except Exception as e:
                logging.warning("Failed to fetch player props for event %s: %s", game_id, e)

        store_raw_response(
            conn,
            endpoint="player_props",
            params=fetch_kwargs,
            response=all_props,
            status="success" if all_props else "error",
        )
        ti.xcom_push(key="api_remaining", value=remaining)

        if not all_props:
            raise ValueError("All player props fetches failed. Check API key and event IDs.")
    finally:
        conn.close()


def transform_player_props_task(**context):
    ti = context["ti"]
    if ti.xcom_pull(task_ids="fetch_player_props", key="skipped") == True:
        logging.info("Player props fetch was skipped (no games) — nothing to transform.")
        return
    conn = get_data_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT response FROM raw_api_responses
                WHERE endpoint = %s AND status = 'success'
                ORDER BY fetched_at DESC LIMIT 1
                """,
                ("player_props",),
            )
            row = cur.fetchone()
        if row is None:
            raise ValueError("No successful player_props raw data found.")
        transform_player_props(conn, row[0])
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_odds_pipeline",
    default_args=default_args,
    description="Fetch and transform NBA odds, scores, and player props from The-Odds-API",
    schedule_interval="0 15 * * *",  # 8am MT (3pm UTC)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "odds"],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
) as dag:
    t_fetch_events  = PythonOperator(task_id="fetch_events",           python_callable=fetch_events_task)
    t_fetch_odds    = PythonOperator(task_id="fetch_odds",             python_callable=fetch_odds_task)
    t_fetch_scores  = PythonOperator(task_id="fetch_scores",           python_callable=fetch_scores_task)
    t_xform_events  = PythonOperator(task_id="transform_events",       python_callable=transform_events_task)
    t_xform_odds    = PythonOperator(task_id="transform_odds",         python_callable=transform_odds_task)
    t_xform_scores  = PythonOperator(task_id="transform_scores",       python_callable=transform_scores_task)
    t_fetch_props   = PythonOperator(task_id="fetch_player_props",     python_callable=fetch_player_props_task)
    t_xform_props   = PythonOperator(task_id="transform_player_props", python_callable=transform_player_props_task)

    [t_fetch_events, t_fetch_odds, t_fetch_scores] >> t_xform_events
    t_xform_events >> [t_xform_odds, t_xform_scores, t_fetch_props]
    t_fetch_props >> t_xform_props
```

- [ ] **Step 6: Run all odds pipeline tests**

```bash
pytest tests/unit/test_nba_odds_pipeline_dag.py -v
```

Expected: All 8 tests pass. (DagBag tests require Airflow installed — if running in a local env without Airflow, expect those 4 to be skipped or error with ImportError; the 4 unit tests must pass.)

---

- [ ] **Step 7: Rename backfill file and update its dag_id**

```bash
git mv dags/backfill_dag.py dags/nba_odds_backfill_dag.py
```

Edit `dags/nba_odds_backfill_dag.py` — change one line:

```python
# old
    dag_id="nba_backfill",
# new
    dag_id="nba_odds_backfill",
```

Also update the file comment at the top from `# dags/backfill_dag.py` to `# dags/nba_odds_backfill_dag.py`.

- [ ] **Step 8: Rename and update the backfill test**

```bash
git mv tests/unit/test_backfill_dag.py tests/unit/test_nba_odds_backfill_dag.py
```

Edit `tests/unit/test_nba_odds_backfill_dag.py` — update both `dag_id` references:

```python
# old
    assert "nba_backfill" in dagbag.dags
# new
    assert "nba_odds_backfill" in dagbag.dags
```

```python
# old
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_backfill"]
# new
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_odds_backfill"]
```

- [ ] **Step 9: Run the backfill test to confirm rename is clean**

```bash
pytest tests/unit/test_nba_odds_backfill_dag.py -v
```

Expected: Both tests pass (with Airflow) or graceful ImportError (without).

---

- [ ] **Step 10: Delete the old DAG files and their tests**

```bash
git rm dags/ingest_dag.py dags/transform_dag.py dags/player_props_dag.py
git rm tests/unit/test_ingest_dag.py tests/unit/test_transform_dag.py tests/unit/test_player_props_dag.py
```

- [ ] **Step 11: Run the full unit test suite to confirm nothing is broken**

```bash
pytest tests/unit/ -v
```

Expected: All tests pass. No references to deleted files. If any test imports from a deleted module, fix it before proceeding.

- [ ] **Step 12: Commit**

```bash
git add dags/nba_odds_pipeline_dag.py dags/nba_odds_backfill_dag.py \
        tests/unit/test_nba_odds_pipeline_dag.py tests/unit/test_nba_odds_backfill_dag.py
git commit -m "$(cat <<'EOF'
refactor: consolidate odds DAGs into nba_odds_pipeline

- Merge nba_ingest + nba_transform + nba_player_props into single DAG
- Explicit in-DAG task chain replaces ExternalTaskSensor
- Fix: player props tasks skip gracefully on no-game days (was ValueError)
- Rename nba_backfill -> nba_odds_backfill

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Build `nba_stats_pipeline` and retire old stats DAGs

### Files

- Create: `dags/nba_stats_pipeline_dag.py`
- Create: `tests/unit/test_nba_stats_pipeline_dag.py`
- Rename + edit: `dags/nba_player_stats_backfill_dag.py` → `dags/nba_stats_backfill_dag.py` (change `dag_id` only)
- Create: `tests/unit/test_nba_stats_backfill_dag.py` (replaces nonexistent test for old backfill)
- Delete: `dags/nba_player_stats_ingest_dag.py`, `dags/nba_player_stats_transform_dag.py`

---

- [ ] **Step 1: Write the failing DAG structure tests**

Create `tests/unit/test_nba_stats_pipeline_dag.py`:

```python
# tests/unit/test_nba_stats_pipeline_dag.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_stats_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_stats_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "wait_for_nba_odds_pipeline",
        "fetch_teams", "fetch_players", "fetch_player_game_logs",
        "fetch_team_game_logs", "fetch_team_season_stats",
        "transform_teams", "transform_players",
        "transform_player_game_logs", "transform_team_game_logs", "transform_team_season_stats",
        "resolve_player_ids", "link_nba_game_ids",
    }


def test_dag_task_chain():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_stats_pipeline"]

    # sensor feeds all 5 fetch tasks
    sensor_downstream = [t.task_id for t in dag.get_task("wait_for_nba_odds_pipeline").downstream_list]
    for task_id in ["fetch_teams", "fetch_players", "fetch_player_game_logs",
                    "fetch_team_game_logs", "fetch_team_season_stats"]:
        assert task_id in sensor_downstream, f"sensor must feed {task_id}"

    # all 5 fetch tasks feed transform_teams
    for task_id in ["fetch_teams", "fetch_players", "fetch_player_game_logs",
                    "fetch_team_game_logs", "fetch_team_season_stats"]:
        downstream = [t.task_id for t in dag.get_task(task_id).downstream_list]
        assert "transform_teams" in downstream, f"{task_id} must feed transform_teams"

    # transform_teams >> transform_players
    assert "transform_players" in [t.task_id for t in dag.get_task("transform_teams").downstream_list]

    # transform_players >> the three log transforms
    players_downstream = [t.task_id for t in dag.get_task("transform_players").downstream_list]
    for task_id in ["transform_player_game_logs", "transform_team_game_logs", "transform_team_season_stats"]:
        assert task_id in players_downstream, f"transform_players must feed {task_id}"

    # all three log transforms feed resolve_player_ids
    for task_id in ["transform_player_game_logs", "transform_team_game_logs", "transform_team_season_stats"]:
        downstream = [t.task_id for t in dag.get_task(task_id).downstream_list]
        assert "resolve_player_ids" in downstream, f"{task_id} must feed resolve_player_ids"

    # resolve_player_ids >> link_nba_game_ids
    assert "link_nba_game_ids" in [t.task_id for t in dag.get_task("resolve_player_ids").downstream_list]


def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from plugins.slack_notifier import notify_success, notify_failure
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_stats_pipeline"]
    assert dag.on_success_callback is notify_success
    assert dag.on_failure_callback is notify_failure


def test_sensor_targets_nba_odds_pipeline():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_stats_pipeline"]
    sensor = dag.get_task("wait_for_nba_odds_pipeline")
    assert sensor.external_dag_id == "nba_odds_pipeline"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/unit/test_nba_stats_pipeline_dag.py -v
```

Expected: Failures — `nba_stats_pipeline` not found in DagBag.

---

- [ ] **Step 3: Create `dags/nba_stats_pipeline_dag.py`**

```python
# dags/nba_stats_pipeline_dag.py
import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, "/opt/airflow")

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

CURRENT_SEASON = "2024-25"  # Update manually each season
INGEST_DELAY_SECONDS = 1


def _get_current_season():
    return CURRENT_SEASON


# ---------------------------------------------------------------------------
# Ingest tasks
# ---------------------------------------------------------------------------

def fetch_teams_task(**context):
    conn = get_data_db_conn()
    try:
        data = fetch_teams()
        store_raw_response(conn, "nba_api/teams", {}, data)
    except Exception:
        store_raw_response(conn, "nba_api/teams", {}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_players_task(**context):
    conn = get_data_db_conn()
    try:
        data = fetch_players(delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, "nba_api/players", {}, data)
    except Exception:
        store_raw_response(conn, "nba_api/players", {}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_player_game_logs_task(**context):
    import logging
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT m.nba_player_id
                FROM player_props pp
                JOIN player_name_mappings m ON m.odds_api_name = pp.player_name
                WHERE m.nba_player_id IS NOT NULL
                """
            )
            prop_player_ids = {row[0] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT DISTINCT player_id
                FROM player_game_logs
                WHERE season = %s
                GROUP BY player_id
                HAVING AVG(min) > 20
                """,
                (season,),
            )
            mins_player_ids = {row[0] for row in cur.fetchall()}

        player_ids = prop_player_ids | mins_player_ids
        if not player_ids:
            logging.getLogger(__name__).info(
                "No player filter available — fetching all (bootstrap mode). "
                "Run nba_stats_backfill first."
            )

        data = fetch_player_game_logs(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        if player_ids:
            data = [row for row in data if row["player_id"] in player_ids]

        store_raw_response(conn, "nba_api/player_game_logs", {"season": season}, data)
    except Exception:
        store_raw_response(conn, "nba_api/player_game_logs", {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_team_game_logs_task(**context):
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        data = fetch_team_game_logs(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, "nba_api/team_game_logs", {"season": season}, data)
    except Exception:
        store_raw_response(conn, "nba_api/team_game_logs", {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_team_season_stats_task(**context):
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        data = fetch_team_season_stats(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, "nba_api/team_season_stats", {"season": season}, data)
    except Exception:
        store_raw_response(conn, "nba_api/team_season_stats", {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------------

def _get_latest_raw(conn, endpoint):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT response FROM raw_api_responses
            WHERE endpoint = %s AND status = 'success'
            ORDER BY fetched_at DESC LIMIT 1
            """,
            (endpoint,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"No successful raw data for endpoint '{endpoint}'. Run ingest first.")
    return row[0]


def run_transform_teams():
    conn = get_data_db_conn()
    try:
        transform_teams(conn, _get_latest_raw(conn, "nba_api/teams"))
    finally:
        conn.close()


def run_transform_players():
    conn = get_data_db_conn()
    try:
        transform_players(conn, _get_latest_raw(conn, "nba_api/players"))
    finally:
        conn.close()


def run_transform_player_game_logs():
    conn = get_data_db_conn()
    try:
        transform_player_game_logs(conn, _get_latest_raw(conn, "nba_api/player_game_logs"))
    finally:
        conn.close()


def run_transform_team_game_logs():
    conn = get_data_db_conn()
    try:
        transform_team_game_logs(conn, _get_latest_raw(conn, "nba_api/team_game_logs"))
    finally:
        conn.close()


def run_transform_team_season_stats():
    conn = get_data_db_conn()
    try:
        transform_team_season_stats(conn, _get_latest_raw(conn, "nba_api/team_season_stats"))
    finally:
        conn.close()


def run_resolve_player_ids():
    conn = get_data_db_conn()
    try:
        slack_url = os.environ.get("SLACK_WEBHOOK_URL")
        resolve_player_ids(conn, slack_webhook_url=slack_url)
    finally:
        conn.close()


def run_link_nba_game_ids():
    conn = get_data_db_conn()
    try:
        link_nba_game_ids(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_stats_pipeline",
    default_args=default_args,
    description="Fetch and transform NBA player and team stats from nba_api",
    schedule_interval="20 15 * * *",  # 8:20am MT (3:20pm UTC)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "stats"],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
) as dag:
    wait_for_odds = ExternalTaskSensor(
        task_id="wait_for_nba_odds_pipeline",
        external_dag_id="nba_odds_pipeline",
        external_task_id=None,   # wait for full DAG completion
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    t_fetch_teams      = PythonOperator(task_id="fetch_teams",            python_callable=fetch_teams_task)
    t_fetch_players    = PythonOperator(task_id="fetch_players",          python_callable=fetch_players_task)
    t_fetch_pgl        = PythonOperator(task_id="fetch_player_game_logs", python_callable=fetch_player_game_logs_task)
    t_fetch_tgl        = PythonOperator(task_id="fetch_team_game_logs",   python_callable=fetch_team_game_logs_task)
    t_fetch_tss        = PythonOperator(task_id="fetch_team_season_stats",python_callable=fetch_team_season_stats_task)

    t_xform_teams      = PythonOperator(task_id="transform_teams",            python_callable=run_transform_teams)
    t_xform_players    = PythonOperator(task_id="transform_players",          python_callable=run_transform_players)
    t_xform_pgl        = PythonOperator(task_id="transform_player_game_logs", python_callable=run_transform_player_game_logs)
    t_xform_tgl        = PythonOperator(task_id="transform_team_game_logs",   python_callable=run_transform_team_game_logs)
    t_xform_tss        = PythonOperator(task_id="transform_team_season_stats",python_callable=run_transform_team_season_stats)
    t_resolve          = PythonOperator(task_id="resolve_player_ids",         python_callable=run_resolve_player_ids)
    t_link_games       = PythonOperator(task_id="link_nba_game_ids",          python_callable=run_link_nba_game_ids)

    fetches = [t_fetch_teams, t_fetch_players, t_fetch_pgl, t_fetch_tgl, t_fetch_tss]

    wait_for_odds >> fetches
    fetches >> t_xform_teams
    t_xform_teams >> t_xform_players
    t_xform_players >> [t_xform_pgl, t_xform_tgl, t_xform_tss]
    [t_xform_pgl, t_xform_tgl, t_xform_tss] >> t_resolve
    t_resolve >> t_link_games
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_nba_stats_pipeline_dag.py -v
```

Expected: All 5 tests pass (DagBag tests require Airflow installed).

---

- [ ] **Step 5: Rename the stats backfill and update its dag_id**

```bash
git mv dags/nba_player_stats_backfill_dag.py dags/nba_stats_backfill_dag.py
```

Edit `dags/nba_stats_backfill_dag.py` — change two things:

1. File comment at top: `# dags/nba_player_stats_backfill_dag.py` → `# dags/nba_stats_backfill_dag.py`
2. The `dag_id`:
```python
# old
    dag_id="nba_player_stats_backfill",
# new
    dag_id="nba_stats_backfill",
```

- [ ] **Step 6: Create the stats backfill test**

Create `tests/unit/test_nba_stats_backfill_dag.py`:

```python
# tests/unit/test_nba_stats_backfill_dag.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


def test_stats_backfill_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_stats_backfill" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_stats_backfill_dag_has_no_schedule():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_stats_backfill"]
    assert dag.schedule_interval is None
```

- [ ] **Step 7: Run the backfill test**

```bash
pytest tests/unit/test_nba_stats_backfill_dag.py -v
```

Expected: Both tests pass (with Airflow installed).

---

- [ ] **Step 8: Delete the old stats DAG files**

```bash
git rm dags/nba_player_stats_ingest_dag.py dags/nba_player_stats_transform_dag.py
```

- [ ] **Step 9: Run the full unit test suite**

```bash
pytest tests/unit/ -v
```

Expected: All tests pass. No import errors or broken references.

- [ ] **Step 10: Commit**

```bash
git add dags/nba_stats_pipeline_dag.py dags/nba_stats_backfill_dag.py \
        tests/unit/test_nba_stats_pipeline_dag.py tests/unit/test_nba_stats_backfill_dag.py
git commit -m "$(cat <<'EOF'
refactor: consolidate stats DAGs into nba_stats_pipeline

- Merge nba_player_stats_ingest + nba_player_stats_transform into single DAG
- ExternalTaskSensor on nba_odds_pipeline replaces time-based offset
- Add DAG-level Slack callbacks (previously absent from transform DAG)
- Rename nba_player_stats_backfill -> nba_stats_backfill

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```
