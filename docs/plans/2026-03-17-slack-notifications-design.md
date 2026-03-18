# Slack Notifications Design

**Date:** 2026-03-17
**Status:** Approved

## Overview

Add Slack notifications to the Airflow DAGs in odds-pipeline. The goal is passive awareness — knowing when DAGs run and whether they succeeded or failed without needing to open Airflow or Grafana. Success messages include a brief summary with remaining Odds API quota; failure messages include a detailed alert with the failed task name and error.

## Architecture

Three focused changes across two files, plus one new file:

1. **`plugins/odds_api_client.py`** — each fetch function returns `(data, remaining_requests)` instead of just `data`, reading `x-requests-remaining` from the Odds API response headers.

2. **`dags/ingest_dag.py`** — `_fetch_and_store` captures `remaining_requests` and pushes it to XCom via `context['ti'].xcom_push`. The DAG definition adds `on_success_callback=notify_success` and `on_failure_callback=notify_failure`.

3. **`plugins/slack_notifier.py`** (new) — shared module with two callback functions:
   - `notify_success(context)` — reads DAG run metadata and pulls `api_remaining` from XCom on the `fetch_odds` task, posts a brief Slack message
   - `notify_failure(context)` — reads the failed task instance and exception from context, posts a detailed alert

The webhook URL is read from the `SLACK_WEBHOOK_URL` environment variable, consistent with the existing pattern of secrets via environment variables.

## Message Format

**Success:**
```
✅ nba_ingest | 8:02pm | API quota remaining: 423
```

**Failure:**
```
❌ nba_ingest FAILED | 8:02pm | Task: fetch_odds | Error: HTTPError 429 Too Many Requests
```

## Data Flow

```
fetch_events_task()
  → odds_api_client.fetch_events() returns (data, remaining)
  → store_raw_response(conn, ..., response=data)
  → context['ti'].xcom_push(key='api_remaining', value=remaining)

fetch_odds_task() / fetch_scores_task() — same pattern

DAG completes successfully
  → on_success_callback fires with Airflow context
  → notify_success() reads xcom_pull('fetch_odds', key='api_remaining')
  → POST to SLACK_WEBHOOK_URL

Any task fails
  → on_failure_callback fires with Airflow context
  → notify_failure() reads context['exception'] + context['task_instance']
  → POST to SLACK_WEBHOOK_URL
```

The quota is pulled from `fetch_odds` specifically — it is the most expensive endpoint and most representative of quota consumption. All three fetch tasks push their quota value to XCom; they should be identical within a single run.

## Error Handling

- Both notifier functions are fire-and-forget. A failed Slack POST never affects DAG state or masks the real failure.
- Both wrap the webhook POST in `try/except`, logging a warning on failure. No retries, no re-raise.
- `notify_failure` degrades gracefully if XCom or context fields are missing — the message omits unavailable fields rather than raising.
- A missing `SLACK_WEBHOOK_URL` env var raises a `ValueError` at import time with a descriptive message, so misconfiguration is caught on DAG load rather than silently at runtime.

## Testing

**`tests/test_slack_notifier.py`**
- Unit tests for `notify_success` and `notify_failure` using a mock Airflow context dict
- `unittest.mock.patch` on `requests.post`
- Verifies correct message format
- Verifies XCom is read from the correct task (`fetch_odds`) and key (`api_remaining`)
- Verifies a failed webhook POST logs a warning without raising

**`tests/test_odds_api_client.py`**
- Extends existing tests to verify the updated return signature `(data, remaining)`
- Verifies `x-requests-remaining` is correctly parsed from response headers

No integration tests against a real Slack workspace. The mock is sufficient given the webhook is external infrastructure.

## Files Changed

| File | Change |
|------|--------|
| `plugins/odds_api_client.py` | Return `(data, remaining)` tuples from all fetch functions |
| `plugins/slack_notifier.py` | New file — `notify_success` and `notify_failure` callbacks |
| `dags/ingest_dag.py` | Capture quota in `_fetch_and_store`, push to XCom, add DAG callbacks |
| `tests/test_slack_notifier.py` | New test file |
| `tests/test_odds_api_client.py` | Extend to cover new return signature |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SLACK_WEBHOOK_URL` | Incoming webhook URL for the target Slack channel |
