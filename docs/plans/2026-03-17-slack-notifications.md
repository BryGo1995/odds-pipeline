# Slack Notifications Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Slack notifications to the `nba_ingest` DAG so that a success message (with remaining API quota) and a failure alert (with task name and error) are posted to Slack on every run.

**Architecture:** DAG-level `on_success_callback` and `on_failure_callback` are registered on the `nba_ingest` DAG and point to a shared `plugins/slack_notifier.py` module. The Odds API `x-requests-remaining` header is captured by updating `odds_api_client.py` to return `(data, remaining)` tuples; each fetch task pushes the quota value to XCom so the success callback can read it.

**Tech Stack:** Python 3, Airflow 2.x, `requests`, `unittest.mock`, pytest

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `plugins/odds_api_client.py` | Modify | Return `(data, remaining)` tuples; parse `x-requests-remaining` header |
| `plugins/slack_notifier.py` | Create | `notify_success` and `notify_failure` DAG-level callbacks |
| `dags/ingest_dag.py` | Modify | Accept `**context` in task callables, push quota to XCom, register callbacks |
| `tests/unit/test_odds_api_client.py` | Modify | Extend to cover new `(data, remaining)` return signature |
| `tests/unit/test_slack_notifier.py` | Create | Unit tests for both notifier callbacks |

---

## Task 1: Update `odds_api_client.py` to return quota

**Files:**
- Modify: `plugins/odds_api_client.py`
- Modify: `tests/unit/test_odds_api_client.py`

The Odds API returns `x-requests-remaining` in response headers. All three fetch functions must return `(data, remaining)` instead of just `data`.

- [ ] **Step 1.1: Update `make_mock_response` helper to support headers**

Open `tests/unit/test_odds_api_client.py` and update the helper:

```python
def make_mock_response(json_data, status_code=200, remaining=500):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    mock.headers = {"x-requests-remaining": str(remaining)}
    return mock
```

- [ ] **Step 1.2: Write failing tests for the new return signature**

Add these three tests to `tests/unit/test_odds_api_client.py`:

```python
def test_fetch_events_returns_data_and_remaining():
    from plugins.odds_api_client import fetch_events
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([{"id": "abc"}], remaining=423)
        data, remaining = fetch_events(api_key="test_key", sport="basketball_nba")
        assert data == [{"id": "abc"}]
        assert remaining == 423


def test_fetch_odds_returns_data_and_remaining():
    from plugins.odds_api_client import fetch_odds
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([], remaining=300)
        data, remaining = fetch_odds(
            api_key="test_key",
            sport="basketball_nba",
            regions=["us"],
            markets=["h2h"],
            bookmakers=["draftkings"],
        )
        assert remaining == 300


def test_fetch_scores_returns_data_and_remaining():
    from plugins.odds_api_client import fetch_scores
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([], remaining=0)
        data, remaining = fetch_scores(api_key="test_key", sport="basketball_nba")
        assert remaining == 0
```

- [ ] **Step 1.3: Run new tests to confirm they fail**

```bash
cd ~/Dev_Space/python_projects/odds-pipeline
pytest tests/unit/test_odds_api_client.py::test_fetch_events_returns_data_and_remaining \
       tests/unit/test_odds_api_client.py::test_fetch_odds_returns_data_and_remaining \
       tests/unit/test_odds_api_client.py::test_fetch_scores_returns_data_and_remaining -v
```

Expected: FAILED (returns a single value, not a tuple)

- [ ] **Step 1.4: Update `plugins/odds_api_client.py`**

```python
# plugins/odds_api_client.py
import requests

BASE_URL = "https://api.the-odds-api.com/v4"


def fetch_events(api_key, sport, **extra_params):
    url = f"{BASE_URL}/sports/{sport}/events"
    params = {"apiKey": api_key}
    params.update(extra_params)
    response = requests.get(url, params=params)
    response.raise_for_status()
    remaining = int(response.headers.get("x-requests-remaining", 0))
    return response.json(), remaining


def fetch_odds(api_key, sport, regions, markets, bookmakers, odds_format="american"):
    url = f"{BASE_URL}/sports/{sport}/odds"
    response = requests.get(url, params={
        "apiKey": api_key,
        "regions": ",".join(regions),
        "markets": ",".join(markets),
        "bookmakers": ",".join(bookmakers),
        "oddsFormat": odds_format,
    })
    response.raise_for_status()
    remaining = int(response.headers.get("x-requests-remaining", 0))
    return response.json(), remaining


def fetch_scores(api_key, sport, days_from=3):
    url = f"{BASE_URL}/sports/{sport}/scores"
    response = requests.get(url, params={
        "apiKey": api_key,
        "daysFrom": days_from,
    })
    response.raise_for_status()
    remaining = int(response.headers.get("x-requests-remaining", 0))
    return response.json(), remaining
```

- [ ] **Step 1.5: Run all tests in `test_odds_api_client.py` to confirm all pass**

```bash
pytest tests/unit/test_odds_api_client.py -v
```

Expected: All tests PASS. Only one existing test asserts on the return value and needs updating: **`test_fetch_events_calls_correct_url`**. Update it to unpack the tuple:

```python
# existing test — update these two lines:
result = fetch_events(api_key="test_key", sport="basketball_nba")
assert result == [{"id": "abc"}]

# becomes:
data, remaining = fetch_events(api_key="test_key", sport="basketball_nba")
assert data == [{"id": "abc"}]
```

The other existing tests (`test_fetch_odds_sends_markets_and_bookmakers`, `test_fetch_scores_calls_correct_url_with_days_from`) do not capture the return value and need no changes.

Note: `test_fetch_raises_on_http_error` also needs no changes — `raise_for_status()` fires before the header read, so the tuple return is never reached when an error is raised.

- [ ] **Step 1.6: Commit**

```bash
git add plugins/odds_api_client.py tests/unit/test_odds_api_client.py
git commit -m "feat: return (data, remaining) from odds_api_client fetch functions"
```

---

## Task 2: Create `plugins/slack_notifier.py`

**Files:**
- Create: `plugins/slack_notifier.py`
- Create: `tests/unit/test_slack_notifier.py`

The notifier reads `SLACK_WEBHOOK_URL` from the environment at import time (logging a warning if absent) and re-checks it at call time. Both callbacks are fire-and-forget — failed POSTs log a warning but never raise.

**Test isolation note:** `_WEBHOOK_URL` is assigned at module import time. Tests must patch `plugins.slack_notifier._WEBHOOK_URL` directly (not the env var) to control its value after import. All tests below use this pattern.

**XCom note:** `XCom.get_one` in Airflow 2.9.0 uses `NEW_SESSION` as the default `session` parameter, so no explicit session management is required. The tests mock it at the classmethod level — mock accepts any arguments including the session default.

- [ ] **Step 2.1: Write failing tests for `notify_success`**

Create `tests/unit/test_slack_notifier.py`:

```python
# tests/unit/test_slack_notifier.py
from datetime import datetime
from unittest.mock import MagicMock, patch


def make_context(dag_id="nba_ingest", exec_time=None):
    """Build a minimal Airflow DAG-level callback context dict."""
    dag = MagicMock()
    dag.dag_id = dag_id
    dag_run = MagicMock()
    dag_run.run_id = "scheduled__2024-01-01T20:00:00+00:00"
    return {
        "dag": dag,
        "dag_run": dag_run,
        "execution_date": exec_time or datetime(2024, 1, 1, 20, 2),
    }


def make_failure_context(**kwargs):
    ctx = make_context(**kwargs)
    ti = MagicMock()
    ti.task_id = "fetch_odds"
    ctx["task_instance"] = ti
    ctx["exception"] = Exception("HTTPError 429 Too Many Requests")
    return ctx


# --- notify_success ---

def test_notify_success_posts_message_with_quota():
    from plugins.slack_notifier import notify_success
    ctx = make_context()
    with patch("plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("plugins.slack_notifier.XCom.get_one", return_value=423), \
         patch("plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_success(ctx)
        payload = mock_post.call_args[1]["json"]
        assert "✅" in payload["text"]
        assert "nba_ingest" in payload["text"]
        assert "423" in payload["text"]


def test_notify_success_omits_quota_when_xcom_returns_none():
    from plugins.slack_notifier import notify_success
    ctx = make_context()
    with patch("plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("plugins.slack_notifier.XCom.get_one", return_value=None), \
         patch("plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_success(ctx)
        payload = mock_post.call_args[1]["json"]
        assert "✅" in payload["text"]
        assert "quota" not in payload["text"].lower()


def test_notify_success_skips_when_no_webhook(caplog):
    from plugins.slack_notifier import notify_success
    import logging
    ctx = make_context()
    with patch("plugins.slack_notifier._WEBHOOK_URL", None), \
         patch("plugins.slack_notifier.requests.post") as mock_post, \
         caplog.at_level(logging.WARNING, logger="plugins.slack_notifier"):
        notify_success(ctx)
        mock_post.assert_not_called()
        assert "SLACK_WEBHOOK_URL" in caplog.text


def test_notify_success_logs_warning_on_failed_post(caplog):
    from plugins.slack_notifier import notify_success
    import logging
    ctx = make_context()
    with patch("plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("plugins.slack_notifier.XCom.get_one", return_value=100), \
         patch("plugins.slack_notifier.requests.post", side_effect=Exception("timeout")), \
         caplog.at_level(logging.WARNING, logger="plugins.slack_notifier"):
        notify_success(ctx)  # must not raise
        assert "Failed to post" in caplog.text


# --- notify_failure ---

def test_notify_failure_posts_message_with_task_and_error():
    from plugins.slack_notifier import notify_failure
    ctx = make_failure_context()
    with patch("plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_failure(ctx)
        payload = mock_post.call_args[1]["json"]
        assert "❌" in payload["text"]
        assert "nba_ingest" in payload["text"]
        assert "fetch_odds" in payload["text"]
        assert "429" in payload["text"]


def test_notify_failure_degrades_gracefully_when_context_missing():
    from plugins.slack_notifier import notify_failure
    ctx = make_context()  # no task_instance or exception keys
    with patch("plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_failure(ctx)  # must not raise
        payload = mock_post.call_args[1]["json"]
        assert "❌" in payload["text"]
        assert "unknown" in payload["text"]
```

- [ ] **Step 2.2: Run tests to confirm they fail**

```bash
pytest tests/unit/test_slack_notifier.py -v
```

Expected: FAILED (module does not exist yet)

- [ ] **Step 2.3: Implement `plugins/slack_notifier.py`**

```python
# plugins/slack_notifier.py
import logging
import os

import requests
from airflow.models import XCom

logger = logging.getLogger(__name__)

_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
if not _WEBHOOK_URL:
    logger.warning("SLACK_WEBHOOK_URL is not set — Slack notifications will be skipped")


def notify_success(context):
    """DAG-level on_success_callback. Posts a brief success message with API quota."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    time_str = execution_date.strftime("%-I:%M%p").lower()

    dag_run = context["dag_run"]
    remaining = XCom.get_one(
        run_id=dag_run.run_id,
        task_id="fetch_odds",
        key="api_remaining",
        dag_id=dag_id,
    )

    if remaining is not None:
        text = f"✅ {dag_id} | {time_str} | API quota remaining: {remaining}"
    else:
        text = f"✅ {dag_id} | {time_str}"

    _post(text)


def notify_failure(context):
    """DAG-level on_failure_callback. Posts a detailed failure alert."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    time_str = execution_date.strftime("%-I:%M%p").lower()

    task_instance = context.get("task_instance")
    exception = context.get("exception")

    task_id = task_instance.task_id if task_instance else "unknown"
    error = str(exception) if exception else "unknown error"

    text = f"❌ {dag_id} FAILED | {time_str} | Task: {task_id} | Error: {error}"
    _post(text)


def _post(text):
    try:
        response = requests.post(_WEBHOOK_URL, json={"text": text})
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to post Slack notification: %s", exc)
```

- [ ] **Step 2.4: Run tests to confirm they all pass**

```bash
pytest tests/unit/test_slack_notifier.py -v
```

Expected: All 7 tests PASS

- [ ] **Step 2.5: Commit**

```bash
git add plugins/slack_notifier.py tests/unit/test_slack_notifier.py
git commit -m "feat: add slack_notifier with notify_success and notify_failure callbacks"
```

---

## Task 3: Update `ingest_dag.py` to push quota and register callbacks

**Files:**
- Modify: `dags/ingest_dag.py`
- Modify: `tests/unit/test_ingest_dag.py`

The wrapper task callables accept `**context` (Airflow 2.x context injection), extract `ti`, and pass it to `_fetch_and_store` for XCom push. The DAG registers both callbacks.

- [ ] **Step 3.1: Write failing tests**

Add these tests to `tests/unit/test_ingest_dag.py`:

```python
def test_ingest_dag_has_success_and_failure_callbacks():
    from airflow.models import DagBag
    from plugins.slack_notifier import notify_success, notify_failure
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_ingest"]
    assert dag.on_success_callback is notify_success
    assert dag.on_failure_callback is notify_failure


def test_fetch_and_store_pushes_quota_to_xcom():
    from unittest.mock import MagicMock, patch
    from dags.ingest_dag import _fetch_and_store

    ti = MagicMock()
    mock_fetch = MagicMock(return_value=({"event": "data"}, 350))
    mock_conn = MagicMock()

    with patch("dags.ingest_dag.get_data_db_conn", return_value=mock_conn), \
         patch.dict("os.environ", {"ODDS_API_KEY": "test_key"}):
        _fetch_and_store("events", mock_fetch, {"sport": "basketball_nba"}, ti)

    ti.xcom_push.assert_called_once_with(key="api_remaining", value=350)
```

- [ ] **Step 3.2: Run new tests to confirm they fail**

```bash
pytest tests/unit/test_ingest_dag.py::test_ingest_dag_has_success_and_failure_callbacks \
       tests/unit/test_ingest_dag.py::test_fetch_and_store_pushes_quota_to_xcom -v
```

Expected: FAILED

- [ ] **Step 3.3: Update `dags/ingest_dag.py`**

```python
# dags/ingest_dag.py
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from config.settings import SPORT, REGIONS, MARKETS, BOOKMAKERS, ODDS_FORMAT, SCORES_DAYS_FROM
from plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.slack_notifier import notify_success, notify_failure


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


def fetch_events_task(**context):
    ti = context["ti"]
    _fetch_and_store("events", fetch_events, {"sport": SPORT}, ti)


def fetch_odds_task(**context):
    ti = context["ti"]
    _fetch_and_store("odds", fetch_odds, {
        "sport": SPORT,
        "regions": REGIONS,
        "markets": MARKETS,
        "bookmakers": BOOKMAKERS,
        "odds_format": ODDS_FORMAT,
    }, ti)


def fetch_scores_task(**context):
    ti = context["ti"]
    _fetch_and_store("scores", fetch_scores, {"sport": SPORT, "days_from": SCORES_DAYS_FROM}, ti)


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_ingest",
    default_args=default_args,
    description="Fetch NBA data from Odds-API and store raw JSON",
    schedule_interval="0 8,20 * * *",  # 8am and 8pm daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "ingest"],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
) as dag:
    t_events = PythonOperator(task_id="fetch_events", python_callable=fetch_events_task)
    t_odds   = PythonOperator(task_id="fetch_odds",   python_callable=fetch_odds_task)
    t_scores = PythonOperator(task_id="fetch_scores", python_callable=fetch_scores_task)

    # All three tasks run independently
    [t_events, t_odds, t_scores]
```

- [ ] **Step 3.4: Run all `test_ingest_dag.py` tests**

```bash
pytest tests/unit/test_ingest_dag.py -v
```

Expected: All tests PASS

- [ ] **Step 3.5: Run the full unit test suite to catch regressions**

```bash
pytest tests/unit/ -v
```

Expected: All tests PASS

- [ ] **Step 3.6: Commit**

```bash
git add dags/ingest_dag.py tests/unit/test_ingest_dag.py
git commit -m "feat: wire slack notifications into nba_ingest DAG"
```

---

## Task 4: Configure `SLACK_WEBHOOK_URL` in Docker environment

**Files:**
- Modify: `docker-compose.yml` (odds-pipeline)

The webhook URL must be available to the Airflow worker container at runtime.

- [ ] **Step 4.1: Add `SLACK_WEBHOOK_URL` to the Airflow service environment in `docker-compose.yml`**

Open `docker-compose.yml` and find the Airflow service's `environment` block. Add:

```yaml
SLACK_WEBHOOK_URL: ${SLACK_WEBHOOK_URL}
```

This passes the value through from the host environment (or `.env` file) without hardcoding the secret in the compose file.

- [ ] **Step 4.2: Add `SLACK_WEBHOOK_URL` to `.env` and `.env.example`**

Add to `.env` (your actual webhook URL):

```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

Add to `.env.example` (placeholder, safe to commit):

```
SLACK_WEBHOOK_URL=your_slack_webhook_url_here
```

To get a webhook URL: in your Slack workspace, go to **Apps → Incoming Webhooks → Add to Slack**, select or create a channel, and copy the generated URL.

- [ ] **Step 4.3: Restart Airflow and verify the DAG loads without errors**

```bash
docker compose down && docker compose up -d
```

Then open the Airflow UI at `http://localhost:8080` and confirm:
- `nba_ingest` DAG appears with no import errors
- No warnings in the Airflow scheduler logs about `SLACK_WEBHOOK_URL` (if the env var is set)

- [ ] **Step 4.4: Commit the `docker-compose.yml` and `.env.example` changes**

```bash
git add docker-compose.yml .env.example
git commit -m "feat: pass SLACK_WEBHOOK_URL into Airflow container"
```
