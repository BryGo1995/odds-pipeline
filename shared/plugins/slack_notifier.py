# plugins/slack_notifier.py
import logging
import os

import pendulum
import requests
from airflow.models import XCom

logger = logging.getLogger(__name__)

_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
_MLFLOW_BASE_URL = os.environ.get("MLFLOW_BASE_URL", "http://mlflow.internal")
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

    checklist = "\n".join(lines)
    has_issues = any(line.startswith("❌") or line.startswith("⚠️") for line in lines)
    warning = "\n\n⚠️ One or more upstream DAGs had issues — review checklist above" if has_issues else ""
    text = f"🏀 Recommendations ready — {date_str}\n\n{checklist}{warning}"
    _post(text)


def notify_model_ready(context):
    """on_success_callback for nba_train_dag. Posts model metrics and promotion status."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    ti = context.get("task_instance")
    run_id = ti.xcom_pull(task_ids="train_model", key="mlflow_run_id") if ti else None

    try:
        import mlflow  # lazy import — mlflow only needed when this callback runs
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
            else:
                delta_str = f"{delta:+.4f}"
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
