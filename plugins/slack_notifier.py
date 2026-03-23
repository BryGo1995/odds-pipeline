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


def send_slack_message(webhook_url, text):
    """Send a message to Slack via the provided webhook URL."""
    if not webhook_url:
        return
    try:
        response = requests.post(webhook_url, json={"text": text})
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to post Slack notification: %s", exc)
