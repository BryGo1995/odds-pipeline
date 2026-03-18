# Player Props Ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a dedicated `nba_player_props` DAG that fetches player prop odds (points, rebounds, assists) once daily at 3pm MT and stores them in the `player_props` table.

**Architecture:** A new `fetch_player_props()` function in the API client wraps `fetch_odds()` with player prop market names. A new standalone DAG fetches and then transforms props in sequence, storing raw JSON under endpoint `"player_props"` in `raw_api_responses`. The existing `nba_transform` DAG is cleaned up to remove now-redundant player props processing.

**Tech Stack:** Python, Apache Airflow, Postgres (psycopg2), requests, pytest

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `config/settings.py` | Modify | Add `PLAYER_PROP_MARKETS` list |
| `plugins/odds_api_client.py` | Modify | Add `fetch_player_props()` |
| `dags/player_props_dag.py` | Create | New `nba_player_props` DAG |
| `dags/transform_dag.py` | Modify | Remove player props task and processing |
| `tests/unit/test_odds_api_client.py` | Modify | Add `fetch_player_props` tests, fix stale fixture |
| `tests/unit/test_player_props_dag.py` | Create | DAG structure tests |
| `tests/unit/test_transform_dag.py` | Modify | Remove `transform_player_props` task assertion |

---

## Task 1: Add `PLAYER_PROP_MARKETS` to config

**Files:**
- Modify: `config/settings.py`

- [ ] **Step 1: Add `PLAYER_PROP_MARKETS` to settings**

In `config/settings.py`, add after the existing `MARKETS` block:

```python
PLAYER_PROP_MARKETS = [
    "player_points",
    "player_rebounds",
    "player_assists",
]
```

- [ ] **Step 2: Commit**

```bash
git add config/settings.py
git commit -m "feat: add PLAYER_PROP_MARKETS config"
```

---

## Task 2: Add `fetch_player_props` to API client (TDD)

**Files:**
- Modify: `tests/unit/test_odds_api_client.py`
- Modify: `plugins/odds_api_client.py`

- [ ] **Step 1: Fix the stale fixture in the existing `test_fetch_odds_sends_markets_and_bookmakers` test**

In `tests/unit/test_odds_api_client.py`, the test at line 33 uses `"player_props"` which is not a valid Odds API market name. Update it:

```python
def test_fetch_odds_sends_markets_and_bookmakers():
    from plugins.odds_api_client import fetch_odds
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([])
        fetch_odds(
            api_key="test_key",
            sport="basketball_nba",
            regions=["us"],
            markets=["h2h", "player_points"],
            bookmakers=["draftkings"],
        )
        params = mock_get.call_args[1]["params"]
        assert params["markets"] == "h2h,player_points"
        assert params["bookmakers"] == "draftkings"
```

- [ ] **Step 2: Write failing tests for `fetch_player_props`**

Append to `tests/unit/test_odds_api_client.py`:

```python
def test_fetch_player_props_sends_player_markets():
    from plugins.odds_api_client import fetch_player_props
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([])
        fetch_player_props(
            api_key="test_key",
            sport="basketball_nba",
            regions=["us"],
            markets=["player_points", "player_rebounds", "player_assists"],
            bookmakers=["draftkings"],
        )
        params = mock_get.call_args[1]["params"]
        assert params["markets"] == "player_points,player_rebounds,player_assists"
        assert "basketball_nba/odds" in mock_get.call_args[0][0]


def test_fetch_player_props_returns_data_and_remaining():
    from plugins.odds_api_client import fetch_player_props
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([{"id": "game1"}], remaining=150)
        data, remaining = fetch_player_props(
            api_key="test_key",
            sport="basketball_nba",
            regions=["us"],
            markets=["player_points"],
            bookmakers=["draftkings"],
        )
        assert data == [{"id": "game1"}]
        assert remaining == 150
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
pytest tests/unit/test_odds_api_client.py::test_fetch_player_props_sends_player_markets tests/unit/test_odds_api_client.py::test_fetch_player_props_returns_data_and_remaining -v
```

Expected: `ImportError` or `AttributeError` — `fetch_player_props` does not exist yet.

- [ ] **Step 4: Implement `fetch_player_props` in `plugins/odds_api_client.py`**

Append after `fetch_odds`:

```python
def fetch_player_props(api_key, sport, regions, markets, bookmakers, odds_format="american"):
    return fetch_odds(api_key, sport, regions, markets, bookmakers, odds_format)
```

- [ ] **Step 5: Run all API client tests to verify they pass**

```bash
pytest tests/unit/test_odds_api_client.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add plugins/odds_api_client.py tests/unit/test_odds_api_client.py
git commit -m "feat: add fetch_player_props to odds_api_client"
```

---

## Task 3: Create `nba_player_props` DAG (TDD)

**Files:**
- Create: `tests/unit/test_player_props_dag.py`
- Create: `dags/player_props_dag.py`

- [ ] **Step 1: Write failing DAG tests**

Create `tests/unit/test_player_props_dag.py`:

```python
# tests/unit/test_player_props_dag.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


def test_player_props_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_player_props" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_player_props_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_player_props"]
    task_ids = [t.task_id for t in dag.tasks]
    assert "fetch_player_props" in task_ids
    assert "transform_player_props" in task_ids


def test_player_props_dag_task_order():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_player_props"]
    fetch_task = dag.get_task("fetch_player_props")
    assert "transform_player_props" in [t.task_id for t in fetch_task.downstream_list]


def test_player_props_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from plugins.slack_notifier import notify_success, notify_failure
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_player_props"]
    assert dag.on_success_callback is notify_success
    assert dag.on_failure_callback is notify_failure
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/unit/test_player_props_dag.py -v
```

Expected: `KeyError` — `nba_player_props` not in dagbag.

- [ ] **Step 3: Create `dags/player_props_dag.py`**

```python
# dags/player_props_dag.py
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from config.settings import SPORT, REGIONS, PLAYER_PROP_MARKETS, BOOKMAKERS, ODDS_FORMAT
from plugins.odds_api_client import fetch_player_props
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.slack_notifier import notify_success, notify_failure
from plugins.transformers.player_props import transform_player_props


# Intentional copy of _fetch_and_store from ingest_dag.py — DAG files are kept
# self-contained to avoid cross-DAG imports, consistent with the existing pattern.
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


def fetch_player_props_task(**context):
    ti = context["ti"]
    _fetch_and_store("player_props", fetch_player_props, {
        "sport": SPORT,
        "regions": REGIONS,
        "markets": PLAYER_PROP_MARKETS,
        "bookmakers": BOOKMAKERS,
        "odds_format": ODDS_FORMAT,
    }, ti)


def transform_player_props_task(**context):
    conn = get_data_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT response FROM raw_api_responses
                WHERE endpoint = 'player_props' AND status = 'success'
                ORDER BY fetched_at DESC LIMIT 1
                """
            )
            row = cur.fetchone()
        if row is None:
            raise ValueError("No successful player_props raw data found. Run fetch_player_props first.")
        transform_player_props(conn, row[0])
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_player_props",
    default_args=default_args,
    description="Fetch and transform NBA player prop odds",
    schedule_interval="0 22 * * *",  # 10pm UTC / 3pm MT daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "player_props"],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
) as dag:
    t_fetch     = PythonOperator(task_id="fetch_player_props",     python_callable=fetch_player_props_task)
    t_transform = PythonOperator(task_id="transform_player_props", python_callable=transform_player_props_task)

    t_fetch >> t_transform
```

- [ ] **Step 4: Run DAG tests to verify they pass**

```bash
pytest tests/unit/test_player_props_dag.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dags/player_props_dag.py tests/unit/test_player_props_dag.py
git commit -m "feat: add nba_player_props DAG"
```

---

## Task 4: Clean up `nba_transform` DAG

**Files:**
- Modify: `tests/unit/test_transform_dag.py`
- Modify: `dags/transform_dag.py`

- [ ] **Step 1: Update the transform DAG test to remove the `transform_player_props` assertion**

In `tests/unit/test_transform_dag.py`, remove line 23:

```python
assert "transform_player_props" in task_ids
```

The test should now read:

```python
def test_transform_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_transform"]
    task_ids = [t.task_id for t in dag.tasks]
    assert "wait_for_ingest"  in task_ids
    assert "transform_events" in task_ids
    assert "transform_odds"   in task_ids
    assert "transform_scores" in task_ids
```

- [ ] **Step 2: Run the transform DAG tests to verify the updated test still passes as-is (before any DAG changes)**

```bash
pytest tests/unit/test_transform_dag.py -v
```

Expected: all PASS (the assertion we removed was about a task that still exists — confirming removal doesn't break anything yet).

- [ ] **Step 3: Remove player props processing from `dags/transform_dag.py`**

Remove the import of `transform_player_props` at the top of the file:
```python
from plugins.transformers.player_props import transform_player_props
```

In `run_transform_odds()`, remove the `transform_player_props` call:
```python
# BEFORE:
def run_transform_odds():
    conn = get_data_db_conn()
    try:
        raw = _get_latest_raw(conn, "odds")
        transform_odds(conn, raw)
        transform_player_props(conn, raw)
    finally:
        conn.close()

# AFTER:
def run_transform_odds():
    conn = get_data_db_conn()
    try:
        raw = _get_latest_raw(conn, "odds")
        transform_odds(conn, raw)
    finally:
        conn.close()
```

Remove `run_transform_player_props()` entirely:
```python
# DELETE this function:
def run_transform_player_props():
    # Player props are sourced from the odds response and already processed
    # in run_transform_odds. This task exists for visibility in the Airflow UI.
    pass
```

Remove the `t_props` task and its dependency from the DAG block:
```python
# BEFORE:
    t_events = PythonOperator(task_id="transform_events",       python_callable=run_transform_events)
    t_odds   = PythonOperator(task_id="transform_odds",         python_callable=run_transform_odds)
    t_scores = PythonOperator(task_id="transform_scores",       python_callable=run_transform_scores)
    t_props  = PythonOperator(task_id="transform_player_props", python_callable=run_transform_player_props)

    wait_for_ingest >> [t_events, t_odds, t_scores]
    t_odds >> t_props

# AFTER:
    t_events = PythonOperator(task_id="transform_events", python_callable=run_transform_events)
    t_odds   = PythonOperator(task_id="transform_odds",   python_callable=run_transform_odds)
    t_scores = PythonOperator(task_id="transform_scores", python_callable=run_transform_scores)

    wait_for_ingest >> [t_events, t_odds, t_scores]
```

- [ ] **Step 4: Run all transform DAG tests to verify they pass**

```bash
pytest tests/unit/test_transform_dag.py -v
```

Expected: all PASS.

- [ ] **Step 5: Run the full unit test suite to confirm nothing is broken**

```bash
pytest tests/unit/ -v --ignore=tests/unit/test_schema.py
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add dags/transform_dag.py tests/unit/test_transform_dag.py
git commit -m "refactor: remove player_props processing from nba_transform DAG"
```

---

## Task 5: Push and verify

- [ ] **Step 1: Push to main**

```bash
git push origin main
```

- [ ] **Step 2: In the Airflow UI, verify `nba_player_props` appears in the DAG list**

Navigate to http://localhost:8080. Confirm `nba_player_props` is listed with schedule `0 22 * * *`.

- [ ] **Step 3: Trigger a manual run to verify end-to-end**

Trigger `nba_player_props` manually. Both `fetch_player_props` and `transform_player_props` should show success. Check that rows appear in the `player_props` table in pgAdmin (http://localhost:5050):

```sql
SELECT prop_type, COUNT(*) FROM player_props GROUP BY prop_type ORDER BY prop_type;
```

Expected: rows for `player_points`, `player_rebounds`, `player_assists`.
