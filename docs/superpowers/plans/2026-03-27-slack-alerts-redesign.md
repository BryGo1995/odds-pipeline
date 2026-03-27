# Slack Alerts Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the generic `notify_success`/`notify_failure` Slack callbacks with three purpose-built alerts: a daily recommendations summary, failure-only alerts in MT, and a model training notification with MLflow metrics.

**Architecture:** Add `notify_score_ready` and `notify_model_ready` to `shared/plugins/slack_notifier.py`, update `notify_failure` to display time in MT, then wire up the right callbacks in each DAG and remove the now-unused `notify_success`. `train_model()` is updated to return its MLflow `run_id` so the callback can fetch metrics.

**Tech Stack:** Python, Airflow 2.x (callbacks + DagRun model), MLflow (tracking client), pendulum (timezone conversion), pytest + unittest.mock

---

## File Map

| File | Change |
|---|---|
| `shared/plugins/slack_notifier.py` | Add `notify_score_ready`, `notify_model_ready`; fix MT time in `notify_failure`; remove `notify_success` + XCom import |
| `nba/plugins/ml/train.py` | Return `run.info.run_id` from `train_model()` |
| `nba/dags/nba_train_dag.py` | Push `mlflow_run_id` to XCom; swap `on_success_callback` to `notify_model_ready` |
| `nba/dags/nba_score_dag.py` | Swap `on_success_callback` to `notify_score_ready` |
| `nba/dags/nba_odds_pipeline_dag.py` | Remove `on_success_callback`; drop `notify_success` import |
| `nba/dags/nba_stats_pipeline_dag.py` | Remove `on_success_callback`; drop `notify_success` import |
| `nba/dags/nba_feature_dag.py` | Remove `on_success_callback`; drop `notify_success` import |
| `nba/tests/unit/test_slack_notifier.py` | Add 8 new tests; remove 4 `notify_success` tests; update `make_context` to use pendulum |
| `nba/tests/unit/ml/test_train.py` | Add `test_train_model_returns_run_id` |
| `nba/tests/unit/test_nba_train_dag.py` | Update callback assertion; add XCom push test |
| `nba/tests/unit/test_nba_score_dag.py` | Update callback assertion |
| `nba/tests/unit/test_nba_odds_pipeline_dag.py` | Update callback assertion |
| `nba/tests/unit/test_nba_stats_pipeline_dag.py` | Update callback assertion |
| `nba/tests/unit/test_nba_feature_dag.py` | Update callback assertion |

---

### Task 1: Fix `notify_failure` to display time in MT

**Files:**
- Modify: `shared/plugins/slack_notifier.py`
- Test: `nba/tests/unit/test_slack_notifier.py`

- [ ] **Step 1: Write the failing test**

Add to `nba/tests/unit/test_slack_notifier.py`, after the existing `test_notify_failure_degrades_gracefully_when_context_missing` test:

```python
def test_notify_failure_uses_mt_time():
    """execution_date in UTC must be displayed as MT in the failure message."""
    import pendulum
    from shared.plugins.slack_notifier import notify_failure

    # 2024-01-02 03:02 UTC = 2024-01-01 20:02 MST (UTC-7, January is standard time)
    exec_time = pendulum.datetime(2024, 1, 2, 3, 2, tz="UTC")
    ctx = make_failure_context(exec_time=exec_time)
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_failure(ctx)
        payload = mock_post.call_args[1]["json"]
        assert "8:02pm" in payload["text"].lower()
        assert "MT" in payload["text"]
```

- [ ] **Step 2: Run test to verify it fails**

```
pytest nba/tests/unit/test_slack_notifier.py::test_notify_failure_uses_mt_time -v
```

Expected: FAIL — the current implementation uses `execution_date.strftime(...)` which won't produce `"MT"` or the correct MT time.

- [ ] **Step 3: Update `make_context` to use a pendulum-aware datetime**

In `nba/tests/unit/test_slack_notifier.py`, replace the `make_context` helper:

```python
def make_context(dag_id="nba_ingest", exec_time=None):
    """Build a minimal Airflow DAG-level callback context dict."""
    import pendulum
    dag = MagicMock()
    dag.dag_id = dag_id
    dag_run = MagicMock()
    dag_run.run_id = "scheduled__2024-01-01T20:00:00+00:00"
    return {
        "dag": dag,
        "dag_run": dag_run,
        # 2024-01-02 03:02 UTC = 2024-01-01 20:02 MST
        "execution_date": exec_time or pendulum.datetime(2024, 1, 2, 3, 2, tz="UTC"),
    }
```

- [ ] **Step 4: Implement MT conversion in `notify_failure`**

In `shared/plugins/slack_notifier.py`, add `import pendulum` at the top (after `import os`) and replace the `time_str` line in `notify_failure`:

```python
# Before:
time_str = execution_date.strftime("%-I:%M%p").lower()

# After:
mt_time = pendulum.instance(execution_date).in_timezone("America/Denver")
time_str = mt_time.strftime("%-I:%M%p").lower() + " MT"
```

The full updated `notify_failure`:

```python
def notify_failure(context):
    """DAG-level on_failure_callback. Posts a detailed failure alert with time in MT."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    mt_time = pendulum.instance(execution_date).in_timezone("America/Denver")
    time_str = mt_time.strftime("%-I:%M%p").lower() + " MT"

    task_instance = context.get("task_instance")
    exception = context.get("exception")

    task_id = task_instance.task_id if task_instance else "unknown"
    error = str(exception) if exception else "unknown error"

    text = f"❌ {dag_id} FAILED | {time_str} | Task: {task_id} | Error: {error}"
    _post(text)
```

Also add `import pendulum` at the top of `shared/plugins/slack_notifier.py`, after `import os`:

```python
import logging
import os

import pendulum
import requests
from airflow.models import XCom
```

- [ ] **Step 5: Run test to verify it passes**

```
pytest nba/tests/unit/test_slack_notifier.py -v
```

Expected: all existing tests pass plus the new `test_notify_failure_uses_mt_time`.

- [ ] **Step 6: Commit**

```bash
git add shared/plugins/slack_notifier.py nba/tests/unit/test_slack_notifier.py
git commit -m "fix: display notify_failure time in MT instead of UTC"
```

---

### Task 2: Add `notify_score_ready` to `slack_notifier.py`

**Files:**
- Modify: `shared/plugins/slack_notifier.py`
- Test: `nba/tests/unit/test_slack_notifier.py`

- [ ] **Step 1: Write the failing tests**

Add these three tests to `nba/tests/unit/test_slack_notifier.py`:

```python
# --- notify_score_ready ---

def test_notify_score_ready_all_success():
    """All four pipeline DAGs found with success state — shows ✅ and MT times."""
    import pendulum
    from shared.plugins.slack_notifier import notify_score_ready

    exec_time = pendulum.datetime(2024, 1, 2, 16, 0, tz="UTC")  # 9:00am MST
    ctx = make_context(dag_id="nba_score_dag", exec_time=exec_time)

    def make_run(state, end_hour, end_minute):
        run = MagicMock()
        run.state = state
        run.end_date = pendulum.datetime(2024, 1, 2, end_hour, end_minute, tz="UTC")
        return run

    fake_runs = {
        "nba_odds_pipeline":  make_run("success", 15, 3),   # 8:03am MST
        "nba_stats_pipeline": make_run("success", 15, 24),  # 8:24am MST
        "nba_feature_dag":    make_run("success", 15, 44),  # 8:44am MST
        "nba_score_dag":      make_run("success", 16, 2),   # 9:02am MST
    }

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._get_dag_run",
               side_effect=lambda dag_id, s, e: fake_runs.get(dag_id)), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_score_ready(ctx)
        text = mock_post.call_args[1]["json"]["text"]
        assert "🏀" in text
        assert "✅ nba_odds_pipeline" in text
        assert "8:03am mt" in text.lower()
        assert "✅ nba_stats_pipeline" in text
        assert "✅ nba_feature_dag" in text
        assert "✅ nba_score_dag" in text


def test_notify_score_ready_missing_dag():
    """A DAG with no run found for the day shows ⚠️."""
    import pendulum
    from shared.plugins.slack_notifier import notify_score_ready

    exec_time = pendulum.datetime(2024, 1, 2, 16, 0, tz="UTC")
    ctx = make_context(dag_id="nba_score_dag", exec_time=exec_time)

    def mock_get(dag_id, day_start, day_end):
        if dag_id == "nba_stats_pipeline":
            return None
        run = MagicMock()
        run.state = "success"
        run.end_date = pendulum.datetime(2024, 1, 2, 15, 5, tz="UTC")
        return run

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._get_dag_run", side_effect=mock_get), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_score_ready(ctx)
        text = mock_post.call_args[1]["json"]["text"]
        assert "⚠️ nba_stats_pipeline — not found" in text


def test_notify_score_ready_failed_dag():
    """A DAG run with state 'failed' shows ❌."""
    import pendulum
    from shared.plugins.slack_notifier import notify_score_ready

    exec_time = pendulum.datetime(2024, 1, 2, 16, 0, tz="UTC")
    ctx = make_context(dag_id="nba_score_dag", exec_time=exec_time)

    def mock_get(dag_id, day_start, day_end):
        run = MagicMock()
        run.state = "failed" if dag_id == "nba_feature_dag" else "success"
        run.end_date = pendulum.datetime(2024, 1, 2, 15, 45, tz="UTC")
        return run

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._get_dag_run", side_effect=mock_get), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_score_ready(ctx)
        text = mock_post.call_args[1]["json"]["text"]
        assert "❌ nba_feature_dag" in text
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest nba/tests/unit/test_slack_notifier.py::test_notify_score_ready_all_success \
       nba/tests/unit/test_slack_notifier.py::test_notify_score_ready_missing_dag \
       nba/tests/unit/test_slack_notifier.py::test_notify_score_ready_failed_dag -v
```

Expected: FAIL — `notify_score_ready` does not exist yet.

- [ ] **Step 3: Implement `notify_score_ready` and `_get_dag_run`**

Add the following to `shared/plugins/slack_notifier.py`, after `notify_failure` and before `_post`:

```python
_DAILY_PIPELINE_DAGS = [
    "nba_odds_pipeline",
    "nba_stats_pipeline",
    "nba_feature_dag",
    "nba_score_dag",
]


def notify_score_ready(context):
    """on_success_callback for nba_score_dag. Posts a checklist of all upstream DAG statuses."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    execution_date = context["execution_date"]
    mt_exec = pendulum.instance(execution_date).in_timezone("America/Denver")
    date_str = mt_exec.strftime("%a %b %-d")

    day_start = pendulum.instance(execution_date).start_of("day")
    day_end = day_start.add(days=1)

    lines = []
    for dag_id in _DAILY_PIPELINE_DAGS:
        run = _get_dag_run(dag_id, day_start, day_end)
        if run is None:
            lines.append(f"⚠️ {dag_id} — not found")
        else:
            end = run.end_date or execution_date
            mt_end = pendulum.instance(end).in_timezone("America/Denver")
            time_str = mt_end.strftime("%-I:%M%p").lower() + " MT"
            emoji = "✅" if run.state == "success" else "❌"
            lines.append(f"{emoji} {dag_id} — {time_str}")

    text = f"🏀 Recommendations ready — {date_str}\n\n" + "\n".join(lines)
    _post(text)


def _get_dag_run(dag_id, day_start, day_end):
    """Return the most recent DagRun for dag_id within the UTC day window, or None."""
    try:
        from airflow.models import DagRun
        from airflow.utils.session import create_session
        with create_session() as session:
            return (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == dag_id,
                    DagRun.execution_date >= day_start,
                    DagRun.execution_date < day_end,
                )
                .order_by(DagRun.execution_date.desc())
                .first()
            )
    except Exception as exc:
        logger.warning("Failed to query DagRun for %s: %s", dag_id, exc)
        return None
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest nba/tests/unit/test_slack_notifier.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add shared/plugins/slack_notifier.py nba/tests/unit/test_slack_notifier.py
git commit -m "feat: add notify_score_ready with daily pipeline DAG checklist"
```

---

### Task 3: Add `notify_model_ready` to `slack_notifier.py`

**Files:**
- Modify: `shared/plugins/slack_notifier.py`
- Test: `nba/tests/unit/test_slack_notifier.py`

- [ ] **Step 1: Write the failing tests**

Add these four tests to `nba/tests/unit/test_slack_notifier.py`:

```python
# --- notify_model_ready ---

def _make_model_context(run_id="run-abc-123"):
    ctx = make_context(dag_id="nba_train_dag")
    ctx["task_instance"] = MagicMock()
    ctx["task_instance"].xcom_pull.return_value = run_id
    return ctx


def test_notify_model_ready_promotion_candidate():
    """Promotion candidate: 🚀 message with delta and model version link."""
    from shared.plugins.slack_notifier import notify_model_ready

    ctx = _make_model_context("run-abc-123")

    mock_run = MagicMock()
    mock_run.data.metrics = {
        "roc_auc": 0.6821,
        "accuracy": 0.6312,
        "brier_score": 0.2341,
        "roc_auc_delta_vs_production": 0.0134,
    }
    mock_run.data.tags = {"promotion_candidate": "true"}

    mock_version = MagicMock()
    mock_version.version = "12"
    mock_version.name = "nba_prop_model"

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier.mlflow") as mock_mlflow, \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_mlflow.get_run.return_value = mock_run
        mock_mlflow.tracking.MlflowClient.return_value.search_model_versions.return_value = [mock_version]
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())

        notify_model_ready(ctx)
        text = mock_post.call_args[1]["json"]["text"]
        assert "🚀" in text
        assert "nba_prop_model" in text
        assert "0.6821" in text
        assert "+0.0134" in text
        assert "http://mlflow.internal/#/models/nba_prop_model/versions/12" in text


def test_notify_model_ready_no_improvement():
    """No improvement: 🤖 message with negative delta and run link."""
    from shared.plugins.slack_notifier import notify_model_ready

    ctx = _make_model_context("run-xyz-456")

    mock_run = MagicMock()
    mock_run.data.metrics = {
        "roc_auc": 0.6542,
        "accuracy": 0.6100,
        "brier_score": 0.2500,
        "roc_auc_delta_vs_production": -0.0145,
    }
    mock_run.data.tags = {"promotion_candidate": "false"}

    mock_version = MagicMock()
    mock_version.version = "11"
    mock_version.name = "nba_prop_model"

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier.mlflow") as mock_mlflow, \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_mlflow.get_run.return_value = mock_run
        mock_mlflow.tracking.MlflowClient.return_value.search_model_versions.return_value = [mock_version]
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())

        notify_model_ready(ctx)
        text = mock_post.call_args[1]["json"]["text"]
        assert "🤖" in text
        assert "0.6542" in text
        assert "-0.0145" in text
        assert "http://mlflow.internal/#/runs/run-xyz-456" in text


def test_notify_model_ready_mlflow_unreachable(caplog):
    """MLflow raises → logs warning, posts fallback message, does not raise."""
    import logging
    from shared.plugins.slack_notifier import notify_model_ready

    ctx = _make_model_context("run-abc")

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier.mlflow") as mock_mlflow, \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post, \
         caplog.at_level(logging.WARNING, logger="shared.plugins.slack_notifier"):
        mock_mlflow.get_run.side_effect = Exception("Connection refused")
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())

        notify_model_ready(ctx)  # must not raise
        assert "Failed to fetch MLflow" in caplog.text
        mock_post.assert_called_once()


def test_notify_model_ready_no_xcom():
    """Missing run_id from XCom → MLflow query fails gracefully, fallback message posted."""
    from shared.plugins.slack_notifier import notify_model_ready

    ctx = make_context(dag_id="nba_train_dag")
    ctx["task_instance"] = MagicMock()
    ctx["task_instance"].xcom_pull.return_value = None

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier.mlflow") as mock_mlflow, \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_mlflow.get_run.side_effect = Exception("run_id is None")
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())

        notify_model_ready(ctx)  # must not raise
        mock_post.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest nba/tests/unit/test_slack_notifier.py::test_notify_model_ready_promotion_candidate \
       nba/tests/unit/test_slack_notifier.py::test_notify_model_ready_no_improvement \
       nba/tests/unit/test_slack_notifier.py::test_notify_model_ready_mlflow_unreachable \
       nba/tests/unit/test_slack_notifier.py::test_notify_model_ready_no_xcom -v
```

Expected: FAIL — `notify_model_ready` does not exist yet.

- [ ] **Step 3: Add `import mlflow` to `slack_notifier.py` and implement `notify_model_ready`**

Add `import mlflow` to the imports at the top of `shared/plugins/slack_notifier.py`:

```python
import logging
import os

import mlflow
import pendulum
import requests
from airflow.models import XCom
```

Add `_MLFLOW_BASE_URL` constant after `_WEBHOOK_URL`:

```python
_MLFLOW_BASE_URL = os.environ.get("MLFLOW_BASE_URL", "http://mlflow.internal")
```

Add `notify_model_ready` to `shared/plugins/slack_notifier.py`, after `notify_score_ready`:

```python
def notify_model_ready(context):
    """on_success_callback for nba_train_dag. Posts model metrics and promotion status."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    ti = context.get("task_instance")
    run_id = ti.xcom_pull(task_ids="train_model", key="mlflow_run_id") if ti else None

    try:
        run = mlflow.get_run(run_id)
        metrics = run.data.metrics
        tags = run.data.tags

        roc_auc = metrics.get("roc_auc", 0.0)
        accuracy = metrics.get("accuracy", 0.0)
        brier = metrics.get("brier_score", 0.0)
        delta = metrics.get("roc_auc_delta_vs_production")
        is_candidate = tags.get("promotion_candidate") == "true"

        client = mlflow.tracking.MlflowClient()
        versions = client.search_model_versions(f"run_id='{run_id}'")
        model_name = versions[0].name if versions else "nba_prop_model"
        version_num = versions[0].version if versions else None

        if is_candidate:
            if delta is None:
                delta_str = "baseline"
            elif delta >= 0:
                delta_str = f"+{delta:.4f}"
            else:
                delta_str = f"{delta:.4f}"
            link = (
                f"{_MLFLOW_BASE_URL}/#/models/{model_name}/versions/{version_num}"
                if version_num
                else f"{_MLFLOW_BASE_URL}/#/runs/{run_id}"
            )
            text = (
                f"🚀 New model ready for promotion — {model_name}\n\n"
                f"ROC-AUC: {roc_auc:.4f} ({delta_str} vs production)\n"
                f"Accuracy: {accuracy:.4f} | Brier: {brier:.4f}\n\n"
                f"Review: {link}"
            )
        else:
            delta_str = f"{delta:+.4f}" if delta is not None else "n/a"
            text = (
                f"🤖 Model trained — no improvement over production\n\n"
                f"ROC-AUC: {roc_auc:.4f} ({delta_str} vs production)\n\n"
                f"{_MLFLOW_BASE_URL}/#/runs/{run_id}"
            )
    except Exception as exc:
        logger.warning("Failed to fetch MLflow run details: %s", exc)
        text = "🤖 Model training completed (details unavailable)"

    _post(text)
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest nba/tests/unit/test_slack_notifier.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add shared/plugins/slack_notifier.py nba/tests/unit/test_slack_notifier.py
git commit -m "feat: add notify_model_ready with MLflow metrics and promotion status"
```

---

### Task 4: Update `train_model()` to return the MLflow run ID

**Files:**
- Modify: `nba/plugins/ml/train.py`
- Test: `nba/tests/unit/ml/test_train.py`

- [ ] **Step 1: Write the failing test**

Add to `nba/tests/unit/ml/test_train.py`:

```python
def test_train_model_returns_run_id(tmp_path):
    """train_model() must return the MLflow run_id string."""
    from unittest.mock import MagicMock, patch
    from nba.plugins.ml.train import train_model

    import pyarrow as pa
    import pyarrow.parquet as pq
    df = _make_synthetic_df(200)
    pq.write_table(pa.Table.from_pandas(df), tmp_path / "features_2026-01-01.parquet")

    mock_run = MagicMock()
    mock_run.info.run_id = "test-run-id-12345"
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    with patch("nba.plugins.ml.train.mlflow") as mock_mlflow, \
         patch("nba.plugins.ml.train._get_production_model_auc", return_value=None):
        mock_mlflow.start_run.return_value = mock_run
        mock_mlflow.sklearn.log_model = MagicMock()
        mock_mlflow.register_model = MagicMock()

        result = train_model(str(tmp_path))

    assert result == "test-run-id-12345"
```

- [ ] **Step 2: Run test to verify it fails**

```
pytest nba/tests/unit/ml/test_train.py::test_train_model_returns_run_id -v
```

Expected: FAIL — `train_model()` currently returns `None`.

- [ ] **Step 3: Update `train_model()` to return the run ID**

In `nba/plugins/ml/train.py`, find the end of `train_model()`. The `with mlflow.start_run() as run:` block is the last thing in the function. After the `with` block closes, add a return statement:

```python
    with mlflow.start_run() as run:
        mlflow.log_params({...})
        mlflow.log_metrics({...})
        # ... rest of existing logging code ...
        if prod_roc_auc is None or roc_auc > prod_roc_auc:
            mlflow.set_tag("promotion_candidate", "true")
            mlflow.set_tag("promotion_note", ...)
        else:
            mlflow.set_tag("promotion_candidate", "false")
            mlflow.set_tag("promotion_note", ...)

    return run.info.run_id   # <-- add this line after the with block
```

The `run` variable is still accessible after the `with` block exits.

- [ ] **Step 4: Run test to verify it passes**

```
pytest nba/tests/unit/ml/test_train.py -v
```

Expected: all tests pass including `test_train_model_returns_run_id`.

- [ ] **Step 5: Commit**

```bash
git add nba/plugins/ml/train.py nba/tests/unit/ml/test_train.py
git commit -m "feat: return MLflow run_id from train_model()"
```

---

### Task 5: Update `nba_train_dag.py` to push run ID and use `notify_model_ready`

**Files:**
- Modify: `nba/dags/nba_train_dag.py`
- Test: `nba/tests/unit/test_nba_train_dag.py`

- [ ] **Step 1: Write the failing tests**

Replace the entire contents of `nba/tests/unit/test_nba_train_dag.py` with:

```python
def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="nba/dags/", include_examples=False)
    assert "nba_train_dag" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_train_model_task():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_train_dag"]
    task_ids = {t.task_id for t in dag.tasks}
    assert "train_model" in task_ids


def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_model_ready, notify_failure
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_train_dag"]
    assert dag.on_success_callback is notify_model_ready
    assert dag.on_failure_callback is notify_failure


def test_run_train_model_pushes_run_id_to_xcom():
    """run_train_model() must capture the run_id from train_model() and push it to XCom."""
    from unittest.mock import MagicMock, patch
    from nba.dags.nba_train_dag import run_train_model

    ti = MagicMock()
    with patch("nba.dags.nba_train_dag.train_model", return_value="run-abc-123"):
        run_train_model(ti=ti)

    ti.xcom_push.assert_called_once_with(key="mlflow_run_id", value="run-abc-123")
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest nba/tests/unit/test_nba_train_dag.py -v
```

Expected: `test_dag_has_slack_callbacks` fails (still references `notify_success`), `test_run_train_model_pushes_run_id_to_xcom` fails (XCom push doesn't happen yet).

- [ ] **Step 3: Update `nba/dags/nba_train_dag.py`**

Replace the entire file:

```python
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.plugins.slack_notifier import notify_failure, notify_model_ready
from nba.plugins.ml.train import train_model


def run_train_model(**context):
    run_id = train_model()
    context["ti"].xcom_push(key="mlflow_run_id", value=run_id)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="nba_train_dag",
    default_args=default_args,
    description="Train XGBoost player prop model and log to MLflow registry",
    schedule_interval="0 10 * * 1",  # Every Monday 10:00 UTC (3:00am MT)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "ml"],
    on_success_callback=notify_model_ready,
    on_failure_callback=notify_failure,
) as dag:
    t_train = PythonOperator(
        task_id="train_model",
        python_callable=run_train_model,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest nba/tests/unit/test_nba_train_dag.py -v
```

Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add nba/dags/nba_train_dag.py nba/tests/unit/test_nba_train_dag.py
git commit -m "feat: wire notify_model_ready and push mlflow_run_id to XCom in nba_train_dag"
```

---

### Task 6: Update `nba_score_dag.py` to use `notify_score_ready`

**Files:**
- Modify: `nba/dags/nba_score_dag.py`
- Test: `nba/tests/unit/test_nba_score_dag.py`

- [ ] **Step 1: Update the callback assertion in the test**

In `nba/tests/unit/test_nba_score_dag.py`, replace `test_dag_has_slack_callbacks`:

```python
def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_score_ready, notify_failure
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_score_dag"]
    assert dag.on_success_callback is notify_score_ready
    assert dag.on_failure_callback is notify_failure
```

- [ ] **Step 2: Run test to verify it fails**

```
pytest nba/tests/unit/test_nba_score_dag.py::test_dag_has_slack_callbacks -v
```

Expected: FAIL — DAG still uses `notify_success`.

- [ ] **Step 3: Update `nba/dags/nba_score_dag.py`**

Change the import line from:
```python
from shared.plugins.slack_notifier import notify_failure, notify_success
```
to:
```python
from shared.plugins.slack_notifier import notify_failure, notify_score_ready
```

Change the DAG definition from:
```python
    on_success_callback=notify_success,
```
to:
```python
    on_success_callback=notify_score_ready,
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest nba/tests/unit/test_nba_score_dag.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add nba/dags/nba_score_dag.py nba/tests/unit/test_nba_score_dag.py
git commit -m "feat: wire notify_score_ready callback to nba_score_dag"
```

---

### Task 7: Remove `on_success_callback` from odds, stats, and feature DAGs

**Files:**
- Modify: `nba/dags/nba_odds_pipeline_dag.py`, `nba/dags/nba_stats_pipeline_dag.py`, `nba/dags/nba_feature_dag.py`
- Test: `nba/tests/unit/test_nba_odds_pipeline_dag.py`, `nba/tests/unit/test_nba_stats_pipeline_dag.py`, `nba/tests/unit/test_nba_feature_dag.py`

- [ ] **Step 1: Update the three callback tests**

In `nba/tests/unit/test_nba_odds_pipeline_dag.py`, replace `test_dag_has_slack_callbacks`:

```python
def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_failure
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_odds_pipeline"]
    assert dag.on_success_callback is None
    assert dag.on_failure_callback is notify_failure
```

In `nba/tests/unit/test_nba_stats_pipeline_dag.py`, replace `test_dag_has_slack_callbacks`:

```python
def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_failure
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_stats_pipeline"]
    assert dag.on_success_callback is None
    assert dag.on_failure_callback is notify_failure
```

In `nba/tests/unit/test_nba_feature_dag.py`, replace `test_dag_has_slack_callbacks`:

```python
def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_failure
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_feature_dag"]
    assert dag.on_success_callback is None
    assert dag.on_failure_callback is notify_failure
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest nba/tests/unit/test_nba_odds_pipeline_dag.py::test_dag_has_slack_callbacks \
       nba/tests/unit/test_nba_stats_pipeline_dag.py::test_dag_has_slack_callbacks \
       nba/tests/unit/test_nba_feature_dag.py::test_dag_has_slack_callbacks -v
```

Expected: FAIL — all three DAGs still have `on_success_callback=notify_success`.

- [ ] **Step 3: Update `nba/dags/nba_odds_pipeline_dag.py`**

Change the import from:
```python
from shared.plugins.slack_notifier import notify_failure, notify_success
```
to:
```python
from shared.plugins.slack_notifier import notify_failure
```

In the `with DAG(...)` definition, remove the line:
```python
    on_success_callback=notify_success,
```

- [ ] **Step 4: Update `nba/dags/nba_stats_pipeline_dag.py`**

Change the import from:
```python
from shared.plugins.slack_notifier import notify_failure, notify_success
```
to:
```python
from shared.plugins.slack_notifier import notify_failure
```

In the `with DAG(...)` definition, remove the line:
```python
    on_success_callback=notify_success,
```

- [ ] **Step 5: Update `nba/dags/nba_feature_dag.py`**

Change the import from:
```python
from shared.plugins.slack_notifier import notify_failure, notify_success
```
to:
```python
from shared.plugins.slack_notifier import notify_failure
```

In the `with DAG(...)` definition, remove the line:
```python
    on_success_callback=notify_success,
```

- [ ] **Step 6: Run tests to verify they pass**

```
pytest nba/tests/unit/test_nba_odds_pipeline_dag.py \
       nba/tests/unit/test_nba_stats_pipeline_dag.py \
       nba/tests/unit/test_nba_feature_dag.py -v
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add nba/dags/nba_odds_pipeline_dag.py nba/dags/nba_stats_pipeline_dag.py \
        nba/dags/nba_feature_dag.py \
        nba/tests/unit/test_nba_odds_pipeline_dag.py \
        nba/tests/unit/test_nba_stats_pipeline_dag.py \
        nba/tests/unit/test_nba_feature_dag.py
git commit -m "feat: remove on_success_callback from odds, stats, and feature DAGs"
```

---

### Task 8: Remove `notify_success` and clean up `slack_notifier.py`

**Files:**
- Modify: `shared/plugins/slack_notifier.py`
- Test: `nba/tests/unit/test_slack_notifier.py`

- [ ] **Step 1: Delete the four `notify_success` tests**

In `nba/tests/unit/test_slack_notifier.py`, delete the entire `# --- notify_success ---` section including all four tests:
- `test_notify_success_posts_message_with_quota`
- `test_notify_success_omits_quota_when_xcom_returns_none`
- `test_notify_success_skips_when_no_webhook`
- `test_notify_success_logs_warning_on_failed_post`

- [ ] **Step 2: Remove `notify_success` and the XCom import from `slack_notifier.py`**

In `shared/plugins/slack_notifier.py`:

Remove the top-level import:
```python
from airflow.models import XCom
```

Delete the entire `notify_success` function (lines 15–38 in the original file).

- [ ] **Step 3: Run the full test suite**

```
pytest nba/tests/unit/ -v
```

Expected: all remaining tests pass. No references to `notify_success` remain.

- [ ] **Step 4: Verify no remaining references to `notify_success`**

```
grep -r "notify_success" nba/ shared/
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add shared/plugins/slack_notifier.py nba/tests/unit/test_slack_notifier.py
git commit -m "refactor: remove notify_success — replaced by notify_score_ready and notify_model_ready"
```
