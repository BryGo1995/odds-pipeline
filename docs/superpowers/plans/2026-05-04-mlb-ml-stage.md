# MLB ML Stage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land `train.py`, `score.py`, `settle.py`, `mlb_train_dag.py`, `mlb_score_dag.py`, and a settle task on `mlb_stats_pipeline_dag.py`, closing the MVP loop for MLB batter-prop recommendations. Refactor `shared/plugins/slack_notifier.py` to be sport-aware so both NBA and MLB use the same callbacks.

**Architecture:** Mirror `nba/plugins/ml/{train,score,settle}.py` and the corresponding DAGs into `mlb/`, with three deviations spelled out by the spec: (1) per-prop-type registry names `mlb_prop_model_<type>`, (2) day-aggregated doubleheader settlement via `SUM(stat) GROUP BY player_id, game_date`, (3) `mlb_player_name_mappings` + `mlb_player_id` in the join. Slack callbacks resolve sport from each DAG's `tags`.

**Tech Stack:** Python 3.11+, XGBoost, scikit-learn (IsotonicRegression), MLflow, pandas, DuckDB (parquet I/O), psycopg2, Airflow, pytest, pendulum.

**Spec:** `docs/superpowers/specs/2026-05-04-mlb-ml-stage-design.md`

---

## Reference files

These already exist and are the templates. Read them once before starting; tasks below cite them by line:

- `nba/plugins/ml/train.py` — clone target for `mlb/plugins/ml/train.py`
- `nba/plugins/ml/score.py` — clone target for `mlb/plugins/ml/score.py`
- `nba/plugins/ml/settle.py` — clone target for `mlb/plugins/ml/settle.py` (with DH aggregation deviation)
- `nba/dags/nba_train_dag.py` — clone target for `mlb/dags/mlb_train_dag.py`
- `nba/dags/nba_score_dag.py` — clone target for `mlb/dags/mlb_score_dag.py`
- `nba/dags/nba_stats_pipeline_dag.py` — see lines 27, 215–218, 267, 276 for the settle wiring pattern
- `nba/tests/unit/ml/{test_train,test_score,test_settle}.py` — clone targets for MLB test files
- `nba/tests/unit/test_slack_notifier.py` and `test_slack_notifier_settle.py` — existing tests that will be updated by the slack refactor
- `mlb/plugins/transformers/features.py` — exports `MLB_PROP_STAT_MAP`
- `shared/plugins/slack_notifier.py` — refactored in Task 2

---

## Task 1: Scaffold MLB ML package directories

**Files:**
- Create: `mlb/plugins/ml/__init__.py`
- Create: `mlb/tests/unit/ml/__init__.py`

- [ ] **Step 1: Create `mlb/plugins/ml/__init__.py`** (empty file)

```bash
mkdir -p mlb/plugins/ml mlb/tests/unit/ml
: > mlb/plugins/ml/__init__.py
: > mlb/tests/unit/ml/__init__.py
```

- [ ] **Step 2: Verify package imports**

```bash
python -c "import mlb.plugins.ml; import mlb.tests.unit.ml; print('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add mlb/plugins/ml/__init__.py mlb/tests/unit/ml/__init__.py
git commit -m "feat(mlb): scaffold ml package + test directories"
```

---

## Task 2: Sport-aware Slack notifier refactor

This is one atomic refactor: it changes module-level config, adds a sport-detection helper, updates all four callbacks, updates existing NBA tests and `nba/plugins/ml/settle.py` callsite, and adds a small set of new MLB-specific notifier tests. Doing it piecemeal would leave the module in a broken state between commits.

**Files:**
- Modify: `shared/plugins/slack_notifier.py`
- Modify: `nba/plugins/ml/settle.py:171` (notify_picks_settled call)
- Modify: `nba/tests/unit/test_slack_notifier.py`
- Modify: `nba/tests/unit/test_slack_notifier_settle.py`
- Test: `nba/tests/unit/test_slack_notifier.py` (existing) and `nba/tests/unit/test_slack_notifier_settle.py` (existing)

- [ ] **Step 1: Update `nba/tests/unit/test_slack_notifier.py` — add tags to context helpers and assert sport prefix**

Replace the `make_context` helper (lines 5-17) and add the tags:

```python
def make_context(dag_id="nba_ingest", exec_time=None, tags=("nba",)):
    """Build a minimal Airflow DAG-level callback context dict."""
    import pendulum
    dag = MagicMock()
    dag.dag_id = dag_id
    dag.tags = list(tags)
    dag_run = MagicMock()
    dag_run.run_id = "scheduled__2024-01-01T20:00:00+00:00"
    return {
        "dag": dag,
        "dag_run": dag_run,
        "execution_date": exec_time or pendulum.datetime(2024, 1, 2, 3, 2, tz="UTC"),
    }
```

Then update each existing test assertion that inspects message text to also assert the `[NBA]` prefix:

In `test_notify_failure_posts_message_with_task_and_error`, add after line 42:

```python
        assert "[NBA]" in payload["text"]
```

In `test_notify_failure_degrades_gracefully_when_context_missing`, add after line 54:

```python
        assert "[NBA]" in payload["text"]
```

In `test_notify_score_ready_all_success`, after line 104 (`assert "🏀" in text`), add:

```python
        assert "[NBA]" in text
```

In `test_notify_model_ready_promotion_candidate` and `test_notify_model_ready_no_improvement`, add after the existing `mock_post.call_args` extraction:

```python
        text = mock_post.call_args[1]["json"]["text"]
        assert "[NBA]" in text
```

(If those tests don't already pull `text`, add the line.)

Also add a new test at the end of `test_slack_notifier.py`:

```python
def test_resolve_sport_picks_first_nba_or_mlb_tag():
    from shared.plugins.slack_notifier import _resolve_sport
    ctx = make_context(tags=("nba", "ml"))
    assert _resolve_sport(ctx) == "nba"
    ctx = make_context(tags=("ml", "mlb"))
    assert _resolve_sport(ctx) == "mlb"


def test_resolve_sport_raises_when_no_sport_tag():
    import pytest
    from shared.plugins.slack_notifier import _resolve_sport
    ctx = make_context(tags=("ml",))
    with pytest.raises(ValueError, match="no nba/mlb tag"):
        _resolve_sport(ctx)
```

- [ ] **Step 2: Update `nba/tests/unit/test_slack_notifier_settle.py` — pass sport kwarg**

In every call to `notify_picks_settled(_GAME_DATE, _RESULTS)` (lines 19, 27, 37, 47, 57, 69), change to `notify_picks_settled(_GAME_DATE, _RESULTS, sport="nba")`. Also add `[NBA]` prefix assertion to `test_notify_picks_settled_posts_to_slack`:

```python
def test_notify_picks_settled_posts_to_slack():
    from shared.plugins.slack_notifier import notify_picks_settled
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, _RESULTS, sport="nba")
    mock_post.assert_called_once()
    text = mock_post.call_args.args[0]
    assert "[NBA]" in text
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
pytest nba/tests/unit/test_slack_notifier.py nba/tests/unit/test_slack_notifier_settle.py -v
```

Expected: many failures — `_resolve_sport` import error, `[NBA]` substring absent, `notify_picks_settled` rejecting unexpected kwarg `sport`.

- [ ] **Step 4: Refactor `shared/plugins/slack_notifier.py`**

Replace the entire file with:

```python
# plugins/slack_notifier.py
import logging
import os

import pendulum
import requests

logger = logging.getLogger(__name__)

_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
_MLFLOW_BASE_URL = os.environ.get("MLFLOW_BASE_URL", "http://mlflow.internal")
if not _WEBHOOK_URL:
    logger.warning("SLACK_WEBHOOK_URL is not set — Slack notifications will be skipped")


_DAILY_PIPELINE_DAGS_BY_SPORT = {
    "nba": ["nba_odds_pipeline", "nba_stats_pipeline", "nba_feature_dag", "nba_score_dag"],
    "mlb": ["mlb_odds_pipeline", "mlb_stats_pipeline", "mlb_feature_dag", "mlb_score_dag"],
}

_PROP_LABELS = {
    # NBA
    "player_points":          "Points",
    "player_rebounds":        "Rebounds",
    "player_assists":         "Assists",
    "player_threes":          "3-Pointers",
    "player_threes_attempts": "3PA",
    # MLB
    "batter_hits":        "Hits",
    "batter_total_bases": "Total Bases",
    "batter_home_runs":   "Home Runs",
}

_SPORT_DISPLAY = {
    "nba": {"prefix": "[NBA]", "emoji": "\U0001f3c0", "filter": "NBA"},
    "mlb": {"prefix": "[MLB]", "emoji": "⚾",     "filter": "MLB"},
}


def _resolve_sport(context) -> str:
    """Return 'nba' or 'mlb' based on the DAG tags. Raises if neither tag is present."""
    tags = set(context["dag"].tags or [])
    for s in ("nba", "mlb"):
        if s in tags:
            return s
    raise ValueError(f"DAG {context['dag'].dag_id} has no nba/mlb tag")


def notify_failure(context):
    """DAG-level on_failure_callback. Posts a detailed failure alert with time in MT."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    try:
        sport = _resolve_sport(context)
        prefix = _SPORT_DISPLAY[sport]["prefix"]
    except (ValueError, KeyError, AttributeError):
        prefix = ""

    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    mt_time = pendulum.instance(execution_date).in_timezone("America/Denver")
    time_str = mt_time.strftime("%-I:%M%p").lower() + " MT"

    task_instance = context.get("task_instance")
    exception = context.get("exception")

    task_id = task_instance.task_id if task_instance else "unknown"
    error = str(exception) if exception else "unknown error"

    text = f"{prefix} ❌ {dag_id} FAILED | {time_str} | Task: {task_id} | Error: {error}".lstrip()
    _post(text)


def notify_score_ready(context):
    """on_success_callback for {sport}_score_dag. Posts a checklist of all upstream DAG statuses."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    sport = _resolve_sport(context)
    display = _SPORT_DISPLAY[sport]
    prefix = display["prefix"]
    emoji = display["emoji"]
    sport_filter = display["filter"]
    pipeline_dags = _DAILY_PIPELINE_DAGS_BY_SPORT[sport]

    execution_date = context["execution_date"]
    mt_exec = pendulum.instance(execution_date).in_timezone("America/Denver")
    date_str = mt_exec.strftime("%a %b %-d")

    day_start = pendulum.instance(execution_date).start_of("day")
    day_end = day_start.add(days=1)

    lines = []
    for dag_id in pipeline_dags:
        run = _get_dag_run(dag_id, day_start, day_end)
        if run is None:
            lines.append(f"⚠️ {dag_id} — not found")
        else:
            end = run.end_date or execution_date
            mt_end = pendulum.instance(end).in_timezone("America/Denver")
            time_str = mt_end.strftime("%-I:%M%p").lower() + " MT"
            state_emoji = "✅" if run.state == "success" else "❌"
            lines.append(f"{state_emoji} {dag_id} — {time_str}")

    checklist = "\n".join(lines)
    has_issues = any(line.startswith("❌") or line.startswith("⚠️") for line in lines)
    warning = "\n\n⚠️ One or more upstream DAGs had issues — review checklist above" if has_issues else ""

    try:
        from shared.plugins.db_client import get_data_db_conn
        data_conn = get_data_db_conn()
        try:
            with data_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT prop_type, COUNT(*)
                    FROM recommendations
                    WHERE game_date = %s AND rank <= 10 AND sport = %s
                    GROUP BY prop_type ORDER BY prop_type
                    """,
                    (execution_date.strftime("%Y-%m-%d"), sport_filter),
                )
                prop_counts = cur.fetchall()
        finally:
            data_conn.close()

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

    breakdown_line = f"\n{breakdown}" if breakdown else ""
    text = f"{prefix} {emoji} Recommendations ready — {date_str}\n\n{checklist}{breakdown_line}{warning}"
    _post(text)


def notify_model_ready(context):
    """on_success_callback for {sport}_train_dag. Posts per-prop-type model metrics."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    sport = _resolve_sport(context)
    prefix = _SPORT_DISPLAY[sport]["prefix"]

    ti = context.get("task_instance")
    run_ids = ti.xcom_pull(task_ids="train_model", key="mlflow_run_ids") if ti else None

    if not run_ids or not isinstance(run_ids, dict):
        _post(f"{prefix} 🤖 Model training completed (no per-model details available)")
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

            delta_str = "baseline" if delta is None else f"{delta:+.4f}"

            label = _PROP_LABELS.get(prop_type, prop_type)
            status = "✅ promoted" if is_candidate else "— no improvement"
            lines.append(f"  {label}: ROC-AUC {roc_auc:.4f} ({delta_str}) {status}")

        text = f"{prefix} 🚀 Models trained\n\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("Failed to fetch MLflow run details: %s", exc)
        text = f"{prefix} 🤖 Model training completed (details unavailable)"

    _post(text)


def notify_picks_settled(game_date, results: list[dict], *, sport: str) -> None:
    """
    Post a picks recap to Slack for a settled game date.

    Args:
        game_date: datetime.date of the game day
        results:   list of dicts with keys:
                   player_name, prop_type, line, outcome, actual_result,
                   actual_stat_value, edge
                   actual_result=None means unresolvable (DNP / postponed)
        sport:     'nba' or 'mlb' — kw-only
    """
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    prefix = _SPORT_DISPLAY[sport]["prefix"]
    date_str = game_date.strftime("%b %-d, %Y")

    hits   = sum(1 for r in results if r["actual_result"] is True)
    total  = sum(1 for r in results if r["actual_result"] is not None)
    pct    = int(round(hits / total * 100)) if total else 0
    avgedge = (
        sum(r["edge"] for r in results if r["edge"] is not None) / len(results)
        if results else 0.0
    )

    header = (
        f"{prefix} 📊 Picks recap — {date_str}\n"
        f"Top-{len(results)}: {hits}/{total} hit ({pct}%) | Avg edge: {avgedge:+.3f}"
    )

    lines = []
    for r in results:
        prop_label = _PROP_LABELS.get(r["prop_type"], r["prop_type"])
        line_str   = f"O {r['line']}"
        edge_str   = f"{r['edge']:+.3f}" if r["edge"] is not None else "n/a"

        if r["actual_result"] is True:
            emoji   = "✅"
            stat_str = str(int(r["actual_stat_value"])) if r["actual_stat_value"] is not None else "—"
        elif r["actual_result"] is False:
            emoji   = "❌"
            stat_str = str(int(r["actual_stat_value"])) if r["actual_stat_value"] is not None else "—"
        else:
            emoji   = "❓"
            stat_str = "—"

        lines.append(
            f"{emoji} {r['player_name']} — {prop_label} {line_str} | Scored: {stat_str} | Edge: {edge_str}"
        )

    text = header + "\n\n" + "\n".join(lines)
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


def _post(text):
    try:
        response = requests.post(_WEBHOOK_URL, json={"text": text})
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to post Slack notification: %s", exc)


def send_slack_message(webhook_url, text):
    """Send a message to Slack via the provided webhook URL."""
    if not webhook_url:
        return
    try:
        response = requests.post(webhook_url, json={"text": text})
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to post Slack notification: %s", exc)
```

- [ ] **Step 5: Update `nba/plugins/ml/settle.py:171` to pass sport kwarg**

In the `_send_recap` function, change:

```python
    if notify_picks_settled is not None:
        notify_picks_settled(game_date, results)
```

To:

```python
    if notify_picks_settled is not None:
        notify_picks_settled(game_date, results, sport="nba")
```

- [ ] **Step 6: Run NBA tests to verify they pass**

```bash
pytest nba/tests/unit/test_slack_notifier.py nba/tests/unit/test_slack_notifier_settle.py nba/tests/unit/ml/test_settle.py -v
```

Expected: all green. (If `nba/tests/unit/ml/test_settle.py` asserts `notify_picks_settled` was called with `(game_date, results)`, update those assertions to use `sport="nba"` kwarg before re-running.)

- [ ] **Step 7: Add MLB-specific notifier tests**

Append to `nba/tests/unit/test_slack_notifier.py` (or create `shared/tests/unit/test_slack_notifier_mlb.py` — choose whichever fits the existing layout; the file name matters less than the assertions). Tests:

```python
# --- MLB sport-aware behavior ---

def test_notify_failure_uses_mlb_prefix():
    from shared.plugins.slack_notifier import notify_failure
    ctx = make_failure_context()
    ctx["dag"].tags = ["mlb", "ml"]
    ctx["dag"].dag_id = "mlb_odds_pipeline"
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_failure(ctx)
        payload = mock_post.call_args[1]["json"]
        assert "[MLB]" in payload["text"]
        assert "mlb_odds_pipeline" in payload["text"]


def test_notify_score_ready_uses_mlb_pipeline_dags_and_filter():
    import pendulum
    from shared.plugins.slack_notifier import notify_score_ready

    exec_time = pendulum.datetime(2024, 5, 4, 16, 0, tz="UTC")
    ctx = make_context(dag_id="mlb_score_dag", exec_time=exec_time, tags=("mlb", "ml"))

    fake_runs = {
        "mlb_odds_pipeline":  MagicMock(state="success", end_date=pendulum.datetime(2024, 5, 4, 15, 3, tz="UTC")),
        "mlb_stats_pipeline": MagicMock(state="success", end_date=pendulum.datetime(2024, 5, 4, 15, 24, tz="UTC")),
        "mlb_feature_dag":    MagicMock(state="success", end_date=pendulum.datetime(2024, 5, 4, 15, 44, tz="UTC")),
        "mlb_score_dag":      MagicMock(state="success", end_date=pendulum.datetime(2024, 5, 4, 16, 2, tz="UTC")),
    }

    captured_sql_args = []

    class FakeCursor:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, sql, args=None):
            captured_sql_args.append((sql, args))
        def fetchall(self): return [("batter_hits", 4), ("batter_home_runs", 6)]

    fake_conn = MagicMock()
    fake_conn.cursor.return_value = FakeCursor()

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._get_dag_run",
               side_effect=lambda dag_id, s, e: fake_runs.get(dag_id)), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post, \
         patch("shared.plugins.db_client.get_data_db_conn", return_value=fake_conn):
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_score_ready(ctx)
        text = mock_post.call_args[1]["json"]["text"]
        assert "[MLB]" in text
        assert "⚾" in text
        assert "✅ mlb_odds_pipeline" in text
        assert "✅ mlb_stats_pipeline" in text
        assert "✅ mlb_feature_dag" in text
        assert "✅ mlb_score_dag" in text
        assert "Hits: 4" in text
        assert "Home Runs: 6" in text

    assert captured_sql_args, "expected one SQL call to recommendations table"
    sql, args = captured_sql_args[0]
    assert "sport = %s" in sql
    assert args[1] == "MLB"


def test_notify_picks_settled_uses_mlb_prefix_and_labels():
    from datetime import date
    from shared.plugins.slack_notifier import notify_picks_settled

    game_date = date(2026, 5, 4)
    results = [
        {"player_name": "Aaron Judge", "prop_type": "batter_home_runs", "line": 0.5,
         "outcome": "Over", "actual_result": True,  "actual_stat_value": 1.0, "edge": 0.18},
        {"player_name": "Mookie Betts", "prop_type": "batter_hits",    "line": 1.5,
         "outcome": "Over", "actual_result": False, "actual_stat_value": 1.0, "edge": 0.04},
    ]
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(game_date, results, sport="mlb")
    text = mock_post.call_args.args[0]
    assert "[MLB]" in text
    assert "Home Runs" in text
    assert "Hits" in text
    assert "Aaron Judge" in text
```

- [ ] **Step 8: Run all notifier tests**

```bash
pytest nba/tests/unit/test_slack_notifier.py nba/tests/unit/test_slack_notifier_settle.py nba/tests/unit/ml/test_settle.py -v
```

Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add shared/plugins/slack_notifier.py \
        nba/plugins/ml/settle.py \
        nba/tests/unit/test_slack_notifier.py \
        nba/tests/unit/test_slack_notifier_settle.py
git commit -m "refactor(slack): make notifier sport-aware via DAG tags"
```

---

## Task 3: MLB train.py + tests

**Files:**
- Create: `mlb/plugins/ml/train.py`
- Create: `mlb/tests/unit/ml/test_train.py`

- [ ] **Step 1: Extend `mlb/tests/unit/conftest.py` with MLflow stubs**

Insert after the existing pendulum stub helper:

```python
def _stub_mlflow():
    """Stub mlflow + mlflow.sklearn so train/score modules can be imported."""
    if "mlflow" in sys.modules:
        return
    mlflow_stub = MagicMock()
    mlflow_sklearn_stub = MagicMock()
    mlflow_tracking_stub = MagicMock()
    sys.modules.setdefault("mlflow", mlflow_stub)
    sys.modules.setdefault("mlflow.sklearn", mlflow_sklearn_stub)
    sys.modules.setdefault("mlflow.tracking", mlflow_tracking_stub)


_stub_airflow()
_stub_pendulum()
_stub_mlflow()
```

(Note: the final two existing calls `_stub_airflow()`/`_stub_pendulum()` already exist; replace them with the three-call block above.)

- [ ] **Step 2: Write `mlb/tests/unit/ml/test_train.py`**

```python
# mlb/tests/unit/ml/test_train.py
"""Unit tests for mlb/plugins/ml/train.py — mirrors NBA's test_train.py shape."""
from unittest.mock import MagicMock, patch
import os

import numpy as np
import pandas as pd
import pytest


def _make_labeled_df(n_per_prop: int = 30) -> pd.DataFrame:
    """Build a synthetic labeled feature df covering all 3 MLB prop types."""
    rng = np.random.default_rng(seed=42)
    rows = []
    base_date = pd.Timestamp("2025-08-01")
    for prop_type in ("batter_hits", "batter_total_bases", "batter_home_runs"):
        for i in range(n_per_prop):
            rows.append({
                "player_id":         1000 + i,
                "player_name":       f"P{i}",
                "game_date":         (base_date + pd.Timedelta(days=i)).date().isoformat(),
                "prop_type":         prop_type,
                "bookmaker":         "draftkings",
                "line":              float(rng.integers(1, 5)),
                "implied_prob_over": float(rng.uniform(0.4, 0.6)),
                "line_movement":     float(rng.uniform(-0.5, 0.5)),
                "rolling_avg_5g":    float(rng.uniform(0, 3)),
                "rolling_avg_10g":   float(rng.uniform(0, 3)),
                "rolling_avg_20g":   float(rng.uniform(0, 3)),
                "rolling_std_10g":   float(rng.uniform(0, 1)),
                "is_home":           float(rng.integers(0, 2)),
                "rest_days":         float(rng.integers(0, 4)),
                "actual_result":     int(rng.integers(0, 2)),
                "actual_stat_value": float(rng.integers(0, 4)),
            })
    return pd.DataFrame(rows)


def test_prepare_features_returns_per_prop_columns():
    from mlb.plugins.ml.train import prepare_features, PER_PROP_FEATURES
    df = _make_labeled_df(n_per_prop=10)
    X, y, _ = prepare_features(df, features=PER_PROP_FEATURES)
    assert list(X.columns) == PER_PROP_FEATURES
    assert len(y) == len(df)
    assert y.dtype.kind in ("i", "u")  # int


def test_prepare_features_fills_numeric_nas_with_median():
    from mlb.plugins.ml.train import prepare_features, PER_PROP_FEATURES
    df = _make_labeled_df(n_per_prop=10)
    df.loc[0, "rolling_avg_5g"] = None
    X, _, _ = prepare_features(df, features=PER_PROP_FEATURES)
    assert pd.notna(X["rolling_avg_5g"].iloc[0])


def test_train_model_per_prop_below_min_rows_raises():
    from mlb.plugins.ml.train import train_model
    with patch("mlb.plugins.ml.train.load_training_data", return_value=_make_labeled_df(n_per_prop=10)):
        with pytest.raises(ValueError, match="Insufficient training data"):
            train_model(prop_type="batter_hits")


def test_train_model_home_runs_min_rows_threshold_is_100():
    """batter_home_runs needs 100 rows (sparser positive class)."""
    from mlb.plugins.ml.train import train_model
    # 60 rows for HR — should fail (>50 but <100)
    df = _make_labeled_df(n_per_prop=60)
    df = df[df["prop_type"] == "batter_home_runs"].reset_index(drop=True)
    with patch("mlb.plugins.ml.train.load_training_data", return_value=df):
        with pytest.raises(ValueError, match="Insufficient training data"):
            train_model(prop_type="batter_home_runs")


def test_train_model_registers_with_mlb_prefix(tmp_path):
    from mlb.plugins.ml.train import train_model
    df = _make_labeled_df(n_per_prop=60)

    captured = {}

    def fake_register(model_uri, name):
        captured["name"] = name

    with patch("mlb.plugins.ml.train.load_training_data", return_value=df), \
         patch("mlb.plugins.ml.train.mlflow") as mlflow_mock, \
         patch("mlb.plugins.ml.train._get_production_model_auc", return_value=None):
        mlflow_mock.start_run.return_value.__enter__.return_value = MagicMock(
            info=MagicMock(run_id="run-123")
        )
        mlflow_mock.register_model.side_effect = fake_register
        train_model(prop_type="batter_hits")
    assert captured.get("name") == "mlb_prop_model_batter_hits"


def test_train_model_tags_promotion_candidate_when_baseline():
    from mlb.plugins.ml.train import train_model
    df = _make_labeled_df(n_per_prop=60)

    set_tag_calls = []

    with patch("mlb.plugins.ml.train.load_training_data", return_value=df), \
         patch("mlb.plugins.ml.train.mlflow") as mlflow_mock, \
         patch("mlb.plugins.ml.train._get_production_model_auc", return_value=None):
        mlflow_mock.start_run.return_value.__enter__.return_value = MagicMock(
            info=MagicMock(run_id="run-123")
        )
        mlflow_mock.set_tag.side_effect = lambda k, v: set_tag_calls.append((k, v))
        train_model(prop_type="batter_hits")
    assert ("promotion_candidate", "true") in set_tag_calls


def test_train_all_models_skips_under_min_rows(caplog):
    from mlb.plugins.ml.train import train_all_models
    df = _make_labeled_df(n_per_prop=10)  # all under 50
    with patch("mlb.plugins.ml.train.load_training_data", return_value=df), \
         patch("mlb.plugins.ml.train.train_model", side_effect=ValueError("Insufficient training data: 10 labeled rows")):
        results = train_all_models()
    assert results == {}


def test_train_all_models_iterates_three_mlb_prop_types():
    from mlb.plugins.ml.train import train_all_models, MODEL_NAME
    from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP
    assert MODEL_NAME == "mlb_prop_model"
    assert set(MLB_PROP_STAT_MAP.keys()) == {"batter_hits", "batter_total_bases", "batter_home_runs"}

    called_with = []
    with patch("mlb.plugins.ml.train.train_model", side_effect=lambda features_dir, prop_type: called_with.append(prop_type) or f"run-{prop_type}"):
        results = train_all_models()
    assert set(called_with) == {"batter_hits", "batter_total_bases", "batter_home_runs"}
    assert set(results.keys()) == set(called_with)
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
pytest mlb/tests/unit/ml/test_train.py -v
```

Expected: ImportError on `mlb.plugins.ml.train`.

- [ ] **Step 4: Write `mlb/plugins/ml/train.py`**

```python
# mlb/plugins/ml/train.py
"""
XGBoost training module for MLB batter-prop ML model.

Mirrors nba/plugins/ml/train.py with three substitutions:
- MLB_PROP_STAT_MAP from mlb.plugins.transformers.features
- MODEL_NAME = "mlb_prop_model"
- batter_home_runs has min_rows=100 (sparser positive class)

train_model() reads all labeled Parquet files via DuckDB, trains a calibrated
XGBoost classifier, logs metrics to MLflow, and tags the run as
'promotion_candidate' if ROC-AUC exceeds the current production model.
"""
import logging
import os

import duckdb
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import LabelEncoder

from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP

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
MODEL_NAME = "mlb_prop_model"
VALIDATION_DAYS = 2
FEATURES_DIR = os.environ.get("FEATURES_DIR", "/data/features")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# Min-rows threshold per prop type. Defaults to 50; HR is sparser (100).
_MIN_ROWS = {
    "batter_hits":        50,
    "batter_total_bases": 50,
    "batter_home_runs":  100,
}


def _make_label_encoder() -> LabelEncoder:
    """Deterministic encoder fitted on the full known prop type list."""
    le = LabelEncoder()
    le.fit(sorted(MLB_PROP_STAT_MAP.keys()))
    return le


def load_training_data(features_dir: str) -> pd.DataFrame:
    """Load all MLB Parquet feature files; return rows with actual_result populated."""
    conn = duckdb.connect()
    try:
        df = conn.execute(
            f"SELECT * FROM read_parquet('{features_dir}/mlb_props_features_*.parquet', union_by_name=true) WHERE actual_result IS NOT NULL"
        ).df()
    finally:
        conn.close()
    return df.dropna(subset=["actual_result"])


def prepare_features(df: pd.DataFrame, features: list[str] | None = None) -> tuple:
    """
    Encode categorical features and fill NAs. Returns (X, y, label_encoder).
    """
    le = _make_label_encoder()
    df = df.copy()
    df["is_home"] = df["is_home"].astype(object).fillna(0.5).astype(float)
    for col in ["rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g",
                "rolling_std_10g", "line_movement", "rest_days"]:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            df[col] = numeric.fillna(numeric.median())
    use_features = features if features is not None else PER_PROP_FEATURES
    X = df[use_features]
    if "actual_result" in df.columns and df["actual_result"].notna().all():
        y = df["actual_result"].astype(int)
    else:
        y = None
    return X, y, le


class _CalibratedModel:
    """XGBoost + isotonic calibrator wrapped as one sklearn-compatible object."""

    def __init__(self, base_model, isotonic: IsotonicRegression):
        self.base_model = base_model
        self.isotonic = isotonic
        self.classes_ = base_model.classes_

    def predict_proba(self, X):
        raw = self.base_model.predict_proba(X)[:, 1]
        calibrated = self.isotonic.predict(raw)
        return np.column_stack([1 - calibrated, calibrated])

    def predict(self, X):
        proba = self.predict_proba(X)[:, 1]
        return (proba >= 0.5).astype(int)


def train_model(features_dir: str = FEATURES_DIR, prop_type: str | None = None) -> str:
    """
    Train and log a new XGBoost model. Tags run 'promotion_candidate' if ROC-AUC
    exceeds the current production model.

    If prop_type is given, trains only on rows of that prop type and registers
    the model as mlb_prop_model_{prop_type}.
    """
    if prop_type is None:
        raise ValueError("prop_type is required for MLB training (per-prop models only)")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    df = load_training_data(features_dir)
    df = df[df["prop_type"] == prop_type]
    model_name = f"{MODEL_NAME}_{prop_type}"
    features = PER_PROP_FEATURES
    min_rows = _MIN_ROWS.get(prop_type, 50)

    if df.empty or len(df) < min_rows:
        raise ValueError(f"Insufficient training data: {len(df)} labeled rows (minimum {min_rows})")

    df = df.sort_values("game_date")
    cutoff = pd.to_datetime(df["game_date"].max()) - pd.Timedelta(days=VALIDATION_DAYS)
    train_df = df[pd.to_datetime(df["game_date"]) <= cutoff]
    val_df   = df[pd.to_datetime(df["game_date"]) >  cutoff]

    log = logging.getLogger(__name__)
    log.info(
        "Training split: %d total labeled rows | cutoff=%s | train=%d | val=%d",
        len(df), cutoff.date(), len(train_df), len(val_df),
    )

    if train_df.empty:
        raise ValueError(
            f"Training set is empty — all {len(df)} labeled rows fall within the "
            f"{VALIDATION_DAYS}-day validation window (cutoff={cutoff.date()}). "
            "Run the feature backfill for older dates to generate training data."
        )
    if val_df.empty:
        raise ValueError("Validation set is empty — add more recent data before training")

    X_train, y_train, _ = prepare_features(train_df, features=features)
    X_val,   y_val,   _ = prepare_features(val_df, features=features)

    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        random_state=42,
    )
    model.fit(X_train, y_train)

    val_has_both_classes = len(np.unique(y_val)) >= 2
    n_val = len(y_val)
    min_per_class = min(np.unique(y_val, return_counts=True)[1])
    can_calibrate = val_has_both_classes and min_per_class >= 2
    if can_calibrate:
        raw_prob = model.predict_proba(X_val)[:, 1]
        iso = IsotonicRegression(y_min=0, y_max=1, out_of_bounds="clip")
        iso.fit(raw_prob, y_val)
        y_prob = iso.predict(raw_prob)
        calibrated = _CalibratedModel(model, iso)
    else:
        log.warning(
            "Validation set too small for calibration (n=%d, both_classes=%s) "
            "— skipping isotonic calibration, using raw model probabilities.",
            n_val, val_has_both_classes,
        )
        y_prob = model.predict_proba(X_val)[:, 1]
        calibrated = model

    y_pred = (y_prob >= 0.5).astype(int)

    roc_auc   = roc_auc_score(y_val, y_prob) if val_has_both_classes else float("nan")
    accuracy  = accuracy_score(y_val, y_pred)
    precision = precision_score(y_val, y_pred, zero_division=0)
    recall    = recall_score(y_val, y_pred, zero_division=0)
    brier     = brier_score_loss(y_val, y_prob) if val_has_both_classes else float("nan")

    prod_roc_auc = _get_production_model_auc(model_name)

    with mlflow.start_run() as run:
        mlflow.log_params({
            **{k: v for k, v in model.get_params().items()
               if k in ("n_estimators", "max_depth", "learning_rate", "subsample", "colsample_bytree")},
            "validation_days":  VALIDATION_DAYS,
            "train_rows":       len(train_df),
            "val_rows":         len(val_df),
        })
        mlflow.log_metrics({
            "roc_auc":   roc_auc,
            "accuracy":  accuracy,
            "precision": precision,
            "recall":    recall,
            "brier_score": brier,
        })
        if prod_roc_auc is not None:
            mlflow.log_metric("roc_auc_delta_vs_production", roc_auc - prod_roc_auc)

        importance = dict(zip(features, model.feature_importances_.tolist()))
        mlflow.log_dict(importance, "feature_importance.json")
        mlflow.sklearn.log_model(calibrated, artifact_path="model")

        model_uri = f"runs:/{run.info.run_id}/model"
        mlflow.register_model(model_uri, model_name)

        if prod_roc_auc is None or roc_auc > prod_roc_auc:
            delta_str = f"+{roc_auc - prod_roc_auc:.4f}" if prod_roc_auc is not None else "baseline"
            mlflow.set_tag("promotion_candidate", "true")
            mlflow.set_tag("promotion_note",
                           f"ROC-AUC {roc_auc:.4f} ({delta_str} vs production)")
        else:
            mlflow.set_tag("promotion_candidate", "false")
            mlflow.set_tag("promotion_note",
                           f"ROC-AUC {roc_auc:.4f} — no improvement over production ({prod_roc_auc:.4f})")

    return run.info.run_id


def _get_production_model_auc(model_name: str) -> float | None:
    """Return the ROC-AUC of the current production model, or None if none exists."""
    try:
        client = mlflow.tracking.MlflowClient()
        mv = client.get_model_version_by_alias(model_name, "production")
        run = mlflow.get_run(mv.run_id)
        auc = run.data.metrics.get("roc_auc")
        return float(auc) if auc is not None else None
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Could not retrieve production model AUC (treating as no production model): %s", exc
        )
        return None


def train_all_models(features_dir: str = FEATURES_DIR) -> dict[str, str]:
    """
    Train one model per prop type. Returns dict of {prop_type: mlflow_run_id}.
    Skips prop types with insufficient training data.
    """
    log = logging.getLogger(__name__)
    results = {}
    for prop_type in sorted(MLB_PROP_STAT_MAP.keys()):
        try:
            run_id = train_model(features_dir, prop_type=prop_type)
            results[prop_type] = run_id
        except ValueError as exc:
            log.warning("Skipping %s: %s", prop_type, exc)
    return results
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest mlb/tests/unit/ml/test_train.py -v
```

Expected: 8 passed.

- [ ] **Step 6: Commit**

```bash
git add mlb/plugins/ml/train.py mlb/tests/unit/ml/test_train.py mlb/tests/unit/conftest.py
git commit -m "feat(mlb): add per-prop-type ml training module"
```

---

## Task 4: MLB score.py + tests

**Files:**
- Create: `mlb/plugins/ml/score.py`
- Create: `mlb/tests/unit/ml/test_score.py`

- [ ] **Step 1: Write `mlb/tests/unit/ml/test_score.py`**

```python
# mlb/tests/unit/ml/test_score.py
"""Unit tests for mlb/plugins/ml/score.py."""
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


def _make_today_df():
    rows = []
    for prop_type in ("batter_hits", "batter_total_bases", "batter_home_runs"):
        for i in range(8):
            rows.append({
                "player_id":         1000 + i,
                "player_name":       f"P{i}",
                "game_date":         "2026-05-04",
                "prop_type":         prop_type,
                "bookmaker":         "draftkings",
                "line":              float(i % 4 + 1),
                "implied_prob_over": 0.5,
                "line_movement":     0.0,
                "rolling_avg_5g":    1.0,
                "rolling_avg_10g":   1.0,
                "rolling_avg_20g":   1.0,
                "rolling_std_10g":   0.5,
                "is_home":           1.0,
                "rest_days":         1.0,
            })
    return pd.DataFrame(rows)


def _model_returning(prob: float):
    m = MagicMock()
    n_calls = {}
    def predict_proba(X):
        n = len(X)
        n_calls["n"] = n
        return np.column_stack([np.full(n, 1 - prob), np.full(n, prob)])
    m.predict_proba.side_effect = predict_proba
    return m


def test_score_writes_recommendations_with_sport_mlb():
    from mlb.plugins.ml.score import score
    df = _make_today_df()

    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=df), \
         patch("mlb.plugins.ml.score.mlflow") as mlflow_mock:
        mlflow_mock.sklearn.load_model.return_value = _model_returning(0.7)
        client = MagicMock()
        client.get_model_version_by_alias.return_value = MagicMock(version="1")
        mlflow_mock.tracking.MlflowClient.return_value = client
        score(fake_conn, "2026-05-04")

    delete_calls = [c for c in fake_cur.execute.call_args_list if "DELETE FROM recommendations" in c.args[0]]
    insert_calls = [c for c in fake_cur.execute.call_args_list if "INSERT INTO recommendations" in c.args[0]]
    assert len(delete_calls) == 1
    assert "sport = 'MLB'" in delete_calls[0].args[0]
    assert len(insert_calls) >= 10
    for call in insert_calls:
        assert call.args[1][-1] == "MLB"  # sport is the last bound parameter


def test_score_partial_models_split_top10_evenly():
    from mlb.plugins.ml.score import score
    df = _make_today_df()

    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur

    def fake_load(uri):
        if "batter_total_bases" in uri:
            raise RuntimeError("no production version")
        return _model_returning(0.7)

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=df), \
         patch("mlb.plugins.ml.score.mlflow") as mlflow_mock:
        mlflow_mock.sklearn.load_model.side_effect = fake_load
        client = MagicMock()
        client.get_model_version_by_alias.return_value = MagicMock(version="1")
        mlflow_mock.tracking.MlflowClient.return_value = client
        score(fake_conn, "2026-05-04")

    insert_calls = [c for c in fake_cur.execute.call_args_list if "INSERT INTO recommendations" in c.args[0]]
    # First 10 ranks are evenly split — only 2 active prop types, so 5/5
    top10_prop_types = [c.args[1][1] for c in insert_calls[:10]]
    assert top10_prop_types.count("batter_hits") == 5
    assert top10_prop_types.count("batter_home_runs") == 5
    assert "batter_total_bases" not in top10_prop_types


def test_score_zero_models_raises():
    from mlb.plugins.ml.score import score
    df = _make_today_df()
    fake_conn = MagicMock()

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=df), \
         patch("mlb.plugins.ml.score.mlflow") as mlflow_mock:
        mlflow_mock.sklearn.load_model.side_effect = RuntimeError("no production version")
        with pytest.raises(ValueError, match="No prop types could be scored"):
            score(fake_conn, "2026-05-04")


def test_score_empty_features_raises():
    from mlb.plugins.ml.score import score
    fake_conn = MagicMock()

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=pd.DataFrame()):
        with pytest.raises(ValueError, match="No feature file found"):
            score(fake_conn, "2026-05-04")


def test_score_records_model_version_from_alias():
    from mlb.plugins.ml.score import score
    df = _make_today_df()

    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur

    with patch("mlb.plugins.ml.score.load_todays_features", return_value=df), \
         patch("mlb.plugins.ml.score.mlflow") as mlflow_mock:
        mlflow_mock.sklearn.load_model.return_value = _model_returning(0.7)
        client = MagicMock()
        client.get_model_version_by_alias.return_value = MagicMock(version="42")
        mlflow_mock.tracking.MlflowClient.return_value = client
        score(fake_conn, "2026-05-04")

    insert_calls = [c for c in fake_cur.execute.call_args_list if "INSERT INTO recommendations" in c.args[0]]
    for call in insert_calls:
        assert call.args[1][-3] == "42"  # model_version is 3rd-from-last
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest mlb/tests/unit/ml/test_score.py -v
```

Expected: ImportError on `mlb.plugins.ml.score`.

- [ ] **Step 3: Write `mlb/plugins/ml/score.py`**

```python
# mlb/plugins/ml/score.py
"""
Scoring module for MLB batter-prop ML model.

score(conn, game_date) loads per-prop-type production models from MLflow,
scores today's feature parquet, allocates top-10 slots evenly across active
prop types, and writes ranked recommendations to Postgres with sport='MLB'.
"""
import logging

import duckdb
import mlflow
import mlflow.sklearn
import pandas as pd

from mlb.plugins.ml.train import (
    PER_PROP_FEATURES,
    FEATURES_DIR,
    MODEL_NAME,
    MLFLOW_TRACKING_URI,
    prepare_features,
)
from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP

log = logging.getLogger(__name__)

TOP_N = 10


def load_todays_features(game_date: str, features_dir: str = FEATURES_DIR) -> pd.DataFrame:
    """Load today's MLB Parquet feature file via DuckDB."""
    path = f"{features_dir}/mlb_props_features_{game_date}.parquet"
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
    Extra slots go to prop types with the highest top-pick edge.
    """
    n = len(active_prop_types)
    if n == 0:
        return {}
    base = total // n
    remainder = total % n
    ranked = sorted(active_prop_types, key=lambda pt: top_edges.get(pt, 0), reverse=True)
    return {
        pt: base + (1 if pt in ranked[:remainder] else 0)
        for pt in active_prop_types
    }


def score(conn, game_date: str, features_dir: str = FEATURES_DIR) -> None:
    """
    Load per-prop-type production models from MLflow, score today's features,
    allocate top-10 slots evenly, and write ranked recommendations to Postgres
    with sport='MLB'.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()

    df = load_todays_features(game_date, features_dir)
    if df.empty:
        raise ValueError(f"No feature file found for {game_date} in {features_dir}")

    scored_subsets = []
    active_prop_types = []

    for prop_type in sorted(MLB_PROP_STAT_MAP.keys()):
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

        subset = subset.sort_values("edge", ascending=False).reset_index(drop=True)
        scored_subsets.append(subset)
        active_prop_types.append(prop_type)

    if not scored_subsets:
        raise ValueError(f"No prop types could be scored for {game_date}")

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

    all_df = pd.concat([top_df, remaining_df], ignore_index=True)
    all_df["rank"] = range(1, len(all_df) + 1)

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recommendations WHERE game_date = %s AND sport = 'MLB'",
            (game_date,),
        )
        for _, row in all_df.iterrows():
            cur.execute(
                """
                INSERT INTO recommendations
                    (player_name, prop_type, bookmaker, line, outcome,
                     model_prob, implied_prob, edge, rank, model_version, game_date, sport)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    "MLB",
                ),
            )
    conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest mlb/tests/unit/ml/test_score.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/ml/score.py mlb/tests/unit/ml/test_score.py
git commit -m "feat(mlb): add daily scoring module"
```

---

## Task 5: MLB settle.py + tests (with DH aggregation)

**Files:**
- Create: `mlb/plugins/ml/settle.py`
- Create: `mlb/tests/unit/ml/test_settle.py`

- [ ] **Step 1: Write `mlb/tests/unit/ml/test_settle.py`**

```python
# mlb/tests/unit/ml/test_settle.py
"""Unit tests for mlb/plugins/ml/settle.py."""
from datetime import date
from unittest.mock import MagicMock, patch

import pytest


def _make_conn(resolvable_rows, stale_ids=None, recap_rows=None, completed_dates=None):
    """Build a mock Postgres connection that returns canned fetchall results in order."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    fetchall_queue = [
        resolvable_rows,
        list(stale_ids or []),
        completed_dates or [],
    ]
    if recap_rows is not None:
        fetchall_queue.append(recap_rows)
    cur.fetchall.side_effect = fetchall_queue

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def test_settle_single_game_day_updates_actual_result():
    from mlb.plugins.ml.settle import settle_recommendations
    rows = [(101, "Aaron Judge", "batter_hits", 1.5, date(2026, 5, 3), 2, 4, 1)]
    conn, cur = _make_conn(rows)
    settle_recommendations(conn)
    update_calls = [c for c in cur.execute.call_args_list if "UPDATE recommendations" in c.args[0] and "actual_result" in c.args[0]]
    assert len(update_calls) == 1
    args = update_calls[0].args[1]
    assert args[0] is True   # actual_result: 2 hits > 1.5 line
    assert args[1] == 2      # actual_stat_value
    assert args[2] == 101    # rec id


def test_settle_doubleheader_aggregates_stats():
    """Day-aggregate via SQL — test asserts the SUM/GROUP BY join is in the SELECT."""
    from mlb.plugins.ml.settle import settle_recommendations
    rows = [(202, "Mookie Betts", "batter_total_bases", 2.5, date(2026, 5, 3), 3, 6, 0)]
    conn, cur = _make_conn(rows)
    settle_recommendations(conn)

    select_calls = [c for c in cur.execute.call_args_list
                    if "SELECT" in c.args[0] and "settled_at IS NULL" in c.args[0]]
    assert select_calls, "expected an unsettled-rec SELECT"
    sql = select_calls[0].args[0]
    assert "SUM(hits)" in sql
    assert "SUM(total_bases)" in sql
    assert "SUM(home_runs)" in sql
    assert "GROUP BY player_id, game_date" in sql

    update_calls = [c for c in cur.execute.call_args_list if "UPDATE recommendations" in c.args[0] and "actual_result" in c.args[0]]
    args = update_calls[0].args[1]
    assert args[0] is True   # 6 total_bases > 2.5 line
    assert args[1] == 6


def test_settle_uses_mlb_player_name_mappings_table():
    from mlb.plugins.ml.settle import settle_recommendations
    conn, cur = _make_conn([])
    settle_recommendations(conn)
    select_calls = [c for c in cur.execute.call_args_list if "SELECT" in c.args[0]]
    sql = select_calls[0].args[0]
    assert "mlb_player_name_mappings" in sql
    assert "mlb_player_id" in sql


def test_settle_filters_sport_mlb_in_all_queries():
    from mlb.plugins.ml.settle import settle_recommendations
    conn, cur = _make_conn([])
    settle_recommendations(conn)
    selects = [c.args[0] for c in cur.execute.call_args_list if "SELECT" in c.args[0]]
    assert all("sport = 'MLB'" in s for s in selects), \
        f"some SELECTs missing sport='MLB' filter: {[s for s in selects if 'MLB' not in s]}"


def test_settle_marks_stale_recs_after_seven_days(caplog):
    from mlb.plugins.ml.settle import settle_recommendations
    conn, cur = _make_conn(resolvable_rows=[], stale_ids=[(901,), (902,)])
    settle_recommendations(conn)
    update_calls = [c for c in cur.execute.call_args_list
                    if "UPDATE recommendations" in c.args[0] and "settled_at = NOW()" in c.args[0]
                    and "actual_result" not in c.args[0]]
    assert len(update_calls) == 1
    assert update_calls[0].args[1][0] == [901, 902]


def test_settle_calls_notify_picks_settled_with_sport_mlb():
    from mlb.plugins.ml.settle import settle_recommendations
    rows = [(101, "Aaron Judge", "batter_hits", 1.5, date(2026, 5, 3), 2, 4, 1)]
    completed_dates = [(date(2026, 5, 3),)]
    recap_rows = [
        ("Aaron Judge", "batter_hits", 1.5, "Over", True, 2.0, 0.10),
    ]
    conn, _ = _make_conn(rows, completed_dates=completed_dates, recap_rows=recap_rows)
    with patch("mlb.plugins.ml.settle.notify_picks_settled") as mock_notify:
        settle_recommendations(conn)
    mock_notify.assert_called_once()
    _, kwargs = mock_notify.call_args.args, mock_notify.call_args.kwargs
    assert kwargs.get("sport") == "mlb" or (len(mock_notify.call_args.args) >= 3 and mock_notify.call_args.args[-1] == "mlb")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest mlb/tests/unit/ml/test_settle.py -v
```

Expected: ImportError on `mlb.plugins.ml.settle`.

- [ ] **Step 3: Write `mlb/plugins/ml/settle.py`**

```python
# mlb/plugins/ml/settle.py
"""
Settle daily MLB recommendations against actual game outcomes.

settle_recommendations(conn) looks up each unsettled recommendation in
mlb_player_game_logs (via mlb_player_name_mappings), records actual_result
and actual_stat_value, and triggers a Slack recap when a game date's top-10
are fully resolved.

Doubleheader handling: stats are aggregated per (player_id, game_date) via
SUM/GROUP BY, so a player with two same-day game logs has their stats summed
before settlement. Single-game days collapse to a one-row sum (no behavioral
difference vs a direct join).
"""
import logging
from datetime import date

from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP

try:
    from shared.plugins.slack_notifier import notify_picks_settled
except ImportError:
    notify_picks_settled = None

log = logging.getLogger(__name__)

_STAT_COLS = list(MLB_PROP_STAT_MAP.values())  # ['hits', 'total_bases', 'home_runs']


def settle_recommendations(conn) -> None:
    """
    Settle unsettled recommendations and send Slack recap for any game date
    whose top-10 picks are now fully resolved.
    """
    newly_settled_dates = set()

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH agg AS (
                SELECT player_id, game_date,
                       SUM(hits)        AS hits,
                       SUM(total_bases) AS total_bases,
                       SUM(home_runs)   AS home_runs
                FROM mlb_player_game_logs
                GROUP BY player_id, game_date
            )
            SELECT
                r.id,
                r.player_name,
                r.prop_type,
                r.line,
                r.game_date,
                agg.hits,
                agg.total_bases,
                agg.home_runs
            FROM recommendations r
            JOIN mlb_player_name_mappings m ON m.odds_api_name = r.player_name
            JOIN agg                          ON agg.player_id = m.mlb_player_id
                                             AND agg.game_date = r.game_date
            WHERE r.settled_at IS NULL
              AND r.game_date < CURRENT_DATE
              AND r.sport = 'MLB'
            """
        )
        resolvable = cur.fetchall()

        cur.execute(
            """
            SELECT id FROM recommendations
            WHERE settled_at IS NULL
              AND game_date < CURRENT_DATE - INTERVAL '7 days'
              AND sport = 'MLB'
            """
        )
        stale_ids = [row[0] for row in cur.fetchall()]

    _settle_resolvable(conn, resolvable, newly_settled_dates)
    _mark_stale(conn, stale_ids, newly_settled_dates)

    conn.commit()

    _notify_completed_dates(conn, newly_settled_dates)


def _settle_resolvable(conn, rows, newly_settled_dates: set) -> None:
    # rows: id, player_name, prop_type, line, game_date, hits, total_bases, home_runs
    stats_offset = 5

    for row in rows:
        rec_id, player_name, prop_type, line, game_date = row[:5]
        stat_col = MLB_PROP_STAT_MAP.get(prop_type)
        if not stat_col:
            log.warning("Unknown prop_type '%s' for rec id=%d — skipping", prop_type, rec_id)
            continue

        stat_val = row[stats_offset + _STAT_COLS.index(stat_col)]
        if stat_val is None:
            continue

        actual_result = float(stat_val) > float(line)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE recommendations
                SET actual_result = %s, actual_stat_value = %s, settled_at = NOW()
                WHERE id = %s
                """,
                (actual_result, stat_val, rec_id),
            )
        newly_settled_dates.add(game_date)


def _mark_stale(conn, stale_ids: list, newly_settled_dates: set) -> None:
    if not stale_ids:
        return
    log.warning("Marking %d stale recommendation(s) as unresolvable: ids=%s", len(stale_ids), stale_ids)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE recommendations
            SET settled_at = NOW()
            WHERE id = ANY(%s) AND settled_at IS NULL
            """,
            (stale_ids,),
        )


def _notify_completed_dates(conn, newly_settled_dates: set) -> None:
    """For each date settled in this run, check if all top-10 recs are now done."""
    if not newly_settled_dates:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT game_date
            FROM recommendations
            WHERE game_date = ANY(%s)
              AND rank <= 10
              AND sport = 'MLB'
            GROUP BY game_date
            HAVING COUNT(*) FILTER (WHERE settled_at IS NULL) = 0
            """,
            (list(newly_settled_dates),),
        )
        completed_dates = [row[0] for row in cur.fetchall()]

    for game_date in completed_dates:
        _send_recap(conn, game_date)


def _send_recap(conn, game_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT player_name, prop_type, line, outcome, actual_result,
                   actual_stat_value, edge
            FROM recommendations
            WHERE game_date = %s AND rank <= 10 AND sport = 'MLB'
            ORDER BY rank
            """,
            (game_date,),
        )
        rows = cur.fetchall()

    results = [
        {
            "player_name":      r[0],
            "prop_type":        r[1],
            "line":             float(r[2]) if r[2] is not None else None,
            "outcome":          r[3],
            "actual_result":    r[4],
            "actual_stat_value": float(r[5]) if r[5] is not None else None,
            "edge":             float(r[6]) if r[6] is not None else None,
        }
        for r in rows
    ]
    if notify_picks_settled is not None:
        notify_picks_settled(game_date, results, sport="mlb")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest mlb/tests/unit/ml/test_settle.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/ml/settle.py mlb/tests/unit/ml/test_settle.py
git commit -m "feat(mlb): add settlement with day-aggregated doubleheader handling"
```

---

## Task 6: mlb_train_dag.py + tests

**Files:**
- Create: `mlb/dags/mlb_train_dag.py`
- Create: `mlb/tests/unit/test_mlb_train_dag.py`

- [ ] **Step 1: Write `mlb/tests/unit/test_mlb_train_dag.py`**

```python
# mlb/tests/unit/test_mlb_train_dag.py
"""Unit tests for mlb_train_dag.py — structural assertions only.

DagBag tests are intentionally omitted: a known Python 3.14 / SQLAlchemy bug
breaks DagBag instantiation across all NBA + MLB DAGs. The structural tests
below import the module directly and assert on the DAG object.
"""
from unittest.mock import MagicMock, patch


def test_train_dag_is_registered_with_mlb_tags():
    import mlb.dags.mlb_train_dag as mod
    assert mod.dag.dag_id == "mlb_train_dag"
    assert "mlb" in mod.dag.tags
    assert "ml" in mod.dag.tags


def test_train_dag_schedule_is_monday_3am_mt():
    import mlb.dags.mlb_train_dag as mod
    assert mod.dag.schedule_interval == "0 10 * * 1"


def test_train_dag_uses_sport_aware_callbacks():
    import mlb.dags.mlb_train_dag as mod
    from shared.plugins.slack_notifier import notify_failure, notify_model_ready
    assert mod.dag.on_failure_callback is notify_failure
    assert mod.dag.on_success_callback is notify_model_ready


def test_run_train_model_pushes_run_ids_to_xcom():
    from mlb.dags.mlb_train_dag import run_train_model
    ctx = {"ti": MagicMock()}
    with patch("mlb.dags.mlb_train_dag.train_all_models", return_value={"batter_hits": "run-1"}):
        run_train_model(**ctx)
    ctx["ti"].xcom_push.assert_called_once_with(key="mlflow_run_ids", value={"batter_hits": "run-1"})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest mlb/tests/unit/test_mlb_train_dag.py -v
```

Expected: ImportError on `mlb.dags.mlb_train_dag`.

- [ ] **Step 3: Write `mlb/dags/mlb_train_dag.py`**

```python
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.plugins.slack_notifier import notify_failure, notify_model_ready
from mlb.plugins.ml.train import train_all_models


def run_train_model(**context):
    run_ids = train_all_models()
    context["ti"].xcom_push(key="mlflow_run_ids", value=run_ids)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="mlb_train_dag",
    default_args=default_args,
    description="Train per-prop-type XGBoost MLB batter-prop models and log to MLflow",
    schedule_interval="0 10 * * 1",  # Every Monday 10:00 UTC (3:00am MT, standard time)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["mlb", "ml"],
    on_success_callback=notify_model_ready,
    on_failure_callback=notify_failure,
) as dag:
    t_train = PythonOperator(
        task_id="train_model",
        python_callable=run_train_model,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest mlb/tests/unit/test_mlb_train_dag.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add mlb/dags/mlb_train_dag.py mlb/tests/unit/test_mlb_train_dag.py
git commit -m "feat(mlb): add mlb_train_dag (weekly Mon 3am MT)"
```

---

## Task 7: mlb_score_dag.py + tests

**Files:**
- Create: `mlb/dags/mlb_score_dag.py`
- Create: `mlb/tests/unit/test_mlb_score_dag.py`

- [ ] **Step 1: Write `mlb/tests/unit/test_mlb_score_dag.py`**

```python
# mlb/tests/unit/test_mlb_score_dag.py
"""Structural tests for mlb_score_dag.py."""
from unittest.mock import MagicMock, patch


def test_score_dag_is_registered_with_mlb_tags():
    import mlb.dags.mlb_score_dag as mod
    assert mod.dag.dag_id == "mlb_score_dag"
    assert "mlb" in mod.dag.tags
    assert "ml" in mod.dag.tags


def test_score_dag_schedule_is_daily_9am_mt():
    import mlb.dags.mlb_score_dag as mod
    assert mod.dag.schedule_interval == "0 16 * * *"


def test_score_dag_uses_sport_aware_callbacks():
    import mlb.dags.mlb_score_dag as mod
    from shared.plugins.slack_notifier import notify_failure, notify_score_ready
    assert mod.dag.on_failure_callback is notify_failure
    assert mod.dag.on_success_callback is notify_score_ready


def test_score_dag_has_external_task_sensor_on_mlb_feature_dag():
    import mlb.dags.mlb_score_dag as mod
    task_ids = [t.task_id for t in mod.dag.tasks]
    assert "wait_for_mlb_feature_dag" in task_ids
    assert "score" in task_ids


def test_run_score_passes_ds_to_score_function():
    from mlb.dags.mlb_score_dag import run_score
    fake_conn = MagicMock()
    with patch("mlb.dags.mlb_score_dag.get_data_db_conn", return_value=fake_conn), \
         patch("mlb.dags.mlb_score_dag.score") as mock_score:
        run_score(ds="2026-05-04")
    mock_score.assert_called_once_with(fake_conn, "2026-05-04")
    fake_conn.close.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest mlb/tests/unit/test_mlb_score_dag.py -v
```

Expected: ImportError on `mlb.dags.mlb_score_dag`.

- [ ] **Step 3: Write `mlb/dags/mlb_score_dag.py`**

```python
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from shared.plugins.db_client import get_data_db_conn
from shared.plugins.slack_notifier import notify_failure, notify_score_ready
from mlb.plugins.ml.score import score


def run_score(**context):
    game_date = context["ds"]
    conn = get_data_db_conn()
    try:
        score(conn, game_date)
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mlb_score_dag",
    default_args=default_args,
    description="Score today's MLB batter props and write ranked recommendations to Postgres",
    schedule_interval="0 16 * * *",  # 9:00am MT — after mlb_feature_dag at 8:40am
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["mlb", "ml"],
    on_success_callback=notify_score_ready,
    on_failure_callback=notify_failure,
) as dag:
    wait_for_features = ExternalTaskSensor(
        task_id="wait_for_mlb_feature_dag",
        external_dag_id="mlb_feature_dag",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
        execution_delta=timedelta(minutes=20),
    )

    t_score = PythonOperator(
        task_id="score",
        python_callable=run_score,
    )

    wait_for_features >> t_score
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest mlb/tests/unit/test_mlb_score_dag.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add mlb/dags/mlb_score_dag.py mlb/tests/unit/test_mlb_score_dag.py
git commit -m "feat(mlb): add mlb_score_dag (daily 9am MT)"
```

---

## Task 8: Append settle_recommendations task to mlb_stats_pipeline_dag

**Files:**
- Modify: `mlb/dags/mlb_stats_pipeline_dag.py`
- Modify: `mlb/tests/unit/test_mlb_stats_pipeline_dag.py`

- [ ] **Step 1: Read current `mlb_stats_pipeline_dag.py` to identify the terminal task**

```bash
grep -n "task_id\|>>" mlb/dags/mlb_stats_pipeline_dag.py
```

Identify the terminal `task_id` (expected: `resolve_player_ids`) and the final `>>` chain. Verify before modifying.

- [ ] **Step 2: Update the test file `mlb/tests/unit/test_mlb_stats_pipeline_dag.py` to assert the settle task**

Add the following test at the end of the file:

```python
def test_pipeline_chains_settle_after_resolve():
    import mlb.dags.mlb_stats_pipeline_dag as mod
    task_ids = [t.task_id for t in mod.dag.tasks]
    assert "settle_recommendations" in task_ids

    settle = mod.dag.get_task("settle_recommendations")
    upstream_ids = {t.task_id for t in settle.upstream_list}
    assert "resolve_player_ids" in upstream_ids


def test_run_settle_recommendations_uses_data_conn():
    from unittest.mock import MagicMock, patch
    from mlb.dags.mlb_stats_pipeline_dag import run_settle_recommendations
    fake_conn = MagicMock()
    with patch("mlb.dags.mlb_stats_pipeline_dag.get_data_db_conn", return_value=fake_conn), \
         patch("mlb.dags.mlb_stats_pipeline_dag._settle_recommendations") as mock_settle:
        run_settle_recommendations()
    mock_settle.assert_called_once_with(fake_conn)
    fake_conn.close.assert_called_once()
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
pytest mlb/tests/unit/test_mlb_stats_pipeline_dag.py -v -k "settle"
```

Expected: failure — `settle_recommendations` task not in DAG.

- [ ] **Step 4: Modify `mlb/dags/mlb_stats_pipeline_dag.py`**

Add at the top of the file (with the other imports, after the existing transformer imports):

```python
from mlb.plugins.ml.settle import settle_recommendations as _settle_recommendations
from shared.plugins.db_client import get_data_db_conn
```

(If `get_data_db_conn` is already imported, don't duplicate. Verify with `grep "get_data_db_conn" mlb/dags/mlb_stats_pipeline_dag.py` first.)

Add the runner function above the `with DAG(...)` block (place it next to the other `run_*` callables in the file):

```python
def run_settle_recommendations(**context):
    conn = get_data_db_conn()
    try:
        _settle_recommendations(conn)
    finally:
        conn.close()
```

Inside the `with DAG(...)` block, after the existing tasks, add:

```python
    t_settle = PythonOperator(
        task_id="settle_recommendations",
        python_callable=run_settle_recommendations,
    )
```

Append the dependency at the end of the existing dependency chain:

```python
    t_resolve >> t_settle
```

(Replace `t_resolve` with whatever variable name the existing DAG uses for the `resolve_player_ids` task — confirm via `grep -n "resolve_player_ids" mlb/dags/mlb_stats_pipeline_dag.py`.)

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest mlb/tests/unit/test_mlb_stats_pipeline_dag.py -v
```

Expected: all green (the new tests pass and existing tests stay green).

- [ ] **Step 6: Commit**

```bash
git add mlb/dags/mlb_stats_pipeline_dag.py mlb/tests/unit/test_mlb_stats_pipeline_dag.py
git commit -m "feat(mlb): add settle_recommendations tail task to stats pipeline"
```

---

## Task 9: Final verification

- [ ] **Step 1: Run full MLB test suite**

```bash
pytest mlb/tests/ -v
```

Expected: all green (except for the pre-existing Python 3.14 / SQLAlchemy DagBag-test class — unchanged from before this slice).

- [ ] **Step 2: Run NBA + shared test suites to confirm no regressions**

```bash
pytest nba/tests/ shared/tests/ -v
```

Expected: all green.

- [ ] **Step 3: Smoke-import the new DAG modules**

```bash
python -c "import mlb.dags.mlb_train_dag; import mlb.dags.mlb_score_dag; print('imports ok')"
```

Expected: `imports ok`.

- [ ] **Step 4: Check git status is clean**

```bash
git status
```

Expected: `working tree clean`.

- [ ] **Step 5: Print the new commit log for review**

```bash
git log --oneline main..HEAD
```

Expected: 8 commits, one per task (Tasks 1–8).

---

## Self-review notes

This plan covers every requirement in `docs/superpowers/specs/2026-05-04-mlb-ml-stage-design.md`:

| Spec section | Plan task |
|---|---|
| `mlb/plugins/ml/__init__.py` scaffold | Task 1 |
| Sport-aware Slack notifier (Sections 4 + NBA touch points) | Task 2 |
| `mlb/plugins/ml/train.py` (Components / train.py) | Task 3 |
| `mlb/plugins/ml/score.py` (Components / score.py) | Task 4 |
| `mlb/plugins/ml/settle.py` + DH aggregation (Components / settle.py + Edge cases) | Task 5 |
| `mlb_train_dag.py` (Components / DAGs) | Task 6 |
| `mlb_score_dag.py` (Components / DAGs) | Task 7 |
| Append `settle_recommendations` to `mlb_stats_pipeline_dag.py` (Architecture) | Task 8 |
| Test parity targets (Testing) | Tasks 3–7 (one set per module) |
| NBA touch: `nba/plugins/ml/settle.py` callsite update | Task 2 |
| NBA touch: existing slack-notifier tests updated | Task 2 |
| Acceptance criteria (Testing) | Task 9 |
