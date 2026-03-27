# Slack Alerts Redesign

**Date:** 2026-03-27
**Status:** Approved

## Overview

Replace the generic `notify_success` / `notify_failure` pattern with three purpose-built alerts:

1. **Recommendations ready** — summary fired when `nba_score_dag` completes, showing a checklist of all upstream daily DAG runs with status and completion time in MT.
2. **Failure alerts** — fire on any DAG failure (same as today), with time updated to MT.
3. **Model notification** — fired when `nba_train_dag` completes, with different messages for promotion candidates vs. no-improvement runs.

Success notifications are removed from `nba_odds_pipeline`, `nba_stats_pipeline`, and `nba_feature_dag`.

---

## Message Formats

### Recommendations Ready (`notify_score_ready`)

```
🏀 Recommendations ready — Fri Mar 27

✅ nba_odds_pipeline — 8:03am MT
✅ nba_stats_pipeline — 8:24am MT
✅ nba_feature_dag — 8:44am MT
✅ nba_score_dag — 9:02am MT
```

- Status emoji: `✅` success, `❌` failed, `⚠️` run not found for that day.
- Times are each DAG run's `end_date` converted to `America/Denver`.

### Failure (`notify_failure`)

```
❌ nba_stats_pipeline FAILED | 8:24am MT | Task: fetch_player_game_logs | Error: ...
```

- Time is `execution_date` converted to `America/Denver` (was previously displayed in UTC).

### Model Notification — Promotion Candidate (`notify_model_ready`)

```
🚀 New model ready for promotion — nba_prop_model

ROC-AUC: 0.6821 (+0.0134 vs production)
Accuracy: 0.6312 | Brier: 0.2341

Review: http://mlflow.internal/#/models/nba_prop_model/versions/12
```

### Model Notification — No Improvement (`notify_model_ready`)

```
🤖 Model trained — no improvement over production

ROC-AUC: 0.6542 (−0.0145 vs production)

http://mlflow.internal/#/runs/{run_id}
```

---

## Architecture

### `shared/plugins/slack_notifier.py`

Three public callback functions:

| Function | Trigger |
|---|---|
| `notify_failure(context)` | `on_failure_callback` on all 5 DAGs |
| `notify_score_ready(context)` | `on_success_callback` on `nba_score_dag` |
| `notify_model_ready(context)` | `on_success_callback` on `nba_train_dag` |

`notify_success` is removed entirely.

**`notify_failure`:** Only change is timezone — replace `execution_date.strftime(...)` with `execution_date.in_timezone("America/Denver").strftime(...)` using pendulum (already a dependency).

**`notify_score_ready`:** Queries Airflow's `DagRun` model for the most recent run of each of the 4 daily pipeline DAGs (`nba_odds_pipeline`, `nba_stats_pipeline`, `nba_feature_dag`, `nba_score_dag`) on the same UTC calendar day as the score DAG's `execution_date`. Converts each run's `end_date` to MT for display. Posts the checklist message.

**`notify_model_ready`:** Reads `mlflow_run_id` from XCom (task `train_model`, key `mlflow_run_id`). Queries MLflow for:
- Metrics: `roc_auc`, `accuracy`, `brier_score`
- Metric: `roc_auc_delta_vs_production`
- Tag: `promotion_candidate`
- Registered model version number (for the deep link)

Constructs the appropriate message (promotion candidate vs. no improvement) and posts.

### `nba/dags/nba_train_dag.py`

`run_train_model` is updated to capture the return value of `train_model()` and push it to XCom:

```python
def run_train_model(**context):
    run_id = train_model()
    context["ti"].xcom_push(key="mlflow_run_id", value=run_id)
```

### `nba/plugins/ml/train.py`

`train_model()` returns `run.info.run_id` at the end of the `with mlflow.start_run() as run:` block.

### DAG Callback Changes

| DAG | `on_success_callback` | `on_failure_callback` |
|---|---|---|
| `nba_odds_pipeline` | removed | `notify_failure` |
| `nba_stats_pipeline` | removed | `notify_failure` |
| `nba_feature_dag` | removed | `notify_failure` |
| `nba_score_dag` | `notify_score_ready` | `notify_failure` |
| `nba_train_dag` | `notify_model_ready` | `notify_failure` |

---

## Data Flow

```
Daily pipeline (each day):
  nba_odds_pipeline → nba_stats_pipeline → nba_feature_dag → nba_score_dag
                                                                    │
                                                         on_success_callback
                                                                    │
                                                        notify_score_ready()
                                                          ├── DagRun query for each upstream DAG
                                                          ├── end_date → MT for each
                                                          └── POST checklist to Slack

Weekly training (every Monday):
  nba_train_dag (train_model → xcom_push run_id)
        │
  on_success_callback
        │
  notify_model_ready()
    ├── xcom_pull(task_id="train_model", key="mlflow_run_id")
    ├── MLflow: get metrics, tags, registered version
    └── POST promotion or no-improvement message to Slack
```

---

## Error Handling

All three functions are fire-and-forget — a failed Slack POST never affects DAG state.

| Scenario | Behavior |
|---|---|
| Webhook POST fails | Log warning, no raise |
| `DagRun` not found for a DAG | Show `⚠️ {dag_id} — not found` in checklist |
| `end_date` is `None` on a DagRun | Fall back to `execution_date` converted to MT |
| XCom has no `mlflow_run_id` | Log warning, post minimal fallback message |
| MLflow unreachable | Log warning, post minimal fallback message |
| Missing `SLACK_WEBHOOK_URL` | Log warning at import + call time, skip POST |

---

## Testing

All tests in `nba/tests/unit/test_slack_notifier.py` (extending existing file).

| Test | Covers |
|---|---|
| `test_notify_failure_uses_mt_time` | Time string is MT, not UTC |
| `test_notify_score_ready_all_success` | All 4 DAGs found with ✅ and MT times |
| `test_notify_score_ready_missing_dag` | Missing DagRun shows ⚠️ |
| `test_notify_score_ready_failed_dag` | Failed DagRun shows ❌ |
| `test_notify_model_ready_promotion_candidate` | 🚀 message with delta, link to model version |
| `test_notify_model_ready_no_improvement` | 🤖 message with negative delta |
| `test_notify_model_ready_mlflow_unreachable` | MLflow raises → fallback message, no raise |
| `test_notify_model_ready_no_xcom` | Missing run_id → fallback, no raise |

---

## Files Changed

| File | Change |
|---|---|
| `shared/plugins/slack_notifier.py` | Replace `notify_success` with `notify_score_ready` + `notify_model_ready`; update `notify_failure` for MT time |
| `nba/plugins/ml/train.py` | Return `run_id` from `train_model()` |
| `nba/dags/nba_train_dag.py` | Push `mlflow_run_id` to XCom; update `on_success_callback` |
| `nba/dags/nba_score_dag.py` | Update `on_success_callback` to `notify_score_ready` |
| `nba/dags/nba_odds_pipeline_dag.py` | Remove `on_success_callback`; update `notify_success` import |
| `nba/dags/nba_stats_pipeline_dag.py` | Remove `on_success_callback`; update `notify_success` import |
| `nba/dags/nba_feature_dag.py` | Remove `on_success_callback`; update `notify_success` import |
| `nba/tests/unit/test_slack_notifier.py` | Add tests for new callbacks and MT time fix |

---

## Environment Variables

| Variable | Description |
|---|---|
| `SLACK_WEBHOOK_URL` | Incoming webhook URL (unchanged) |
| `MLFLOW_TRACKING_URI` | MLflow server URL, used in `notify_model_ready` for client queries |
