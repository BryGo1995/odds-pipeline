# plugins/slack_notifier.py
import logging
import os

import pendulum
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
