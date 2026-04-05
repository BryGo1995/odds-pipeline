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
    """on_success_callback for nba_train_dag. Posts per-prop-type model metrics."""
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    ti = context.get("task_instance")
    run_ids = ti.xcom_pull(task_ids="train_model", key="mlflow_run_ids") if ti else None

    if not run_ids or not isinstance(run_ids, dict):
        _post("\U0001f916 Model training completed (no per-model details available)")
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

            if delta is None:
                delta_str = "baseline"
            else:
                delta_str = f"{delta:+.4f}"

            _PROP_LABELS = {
                "player_points": "Points",
                "player_rebounds": "Rebounds",
                "player_assists": "Assists",
            }
            label = _PROP_LABELS.get(prop_type, prop_type)
            status = "\u2705 promoted" if is_candidate else "\u2014 no improvement"
            lines.append(f"  {label}: ROC-AUC {roc_auc:.4f} ({delta_str}) {status}")

        text = "\U0001f680 Models trained\n\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("Failed to fetch MLflow run details: %s", exc)
        text = "\U0001f916 Model training completed (details unavailable)"

    _post(text)


def notify_picks_settled(game_date, results: list[dict]) -> None:
    """
    Post a picks recap to Slack for a settled game date.

    Args:
        game_date: datetime.date of the game day
        results:   list of dicts with keys:
                   player_name, prop_type, line, outcome, actual_result,
                   actual_stat_value, edge
                   actual_result=None means unresolvable (DNP / postponed)
    """
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    date_str = game_date.strftime("%b %-d, %Y")

    hits   = sum(1 for r in results if r["actual_result"] is True)
    total  = sum(1 for r in results if r["actual_result"] is not None)
    pct    = int(round(hits / total * 100)) if total else 0
    avgedge = (
        sum(r["edge"] for r in results if r["edge"] is not None) / len(results)
        if results else 0.0
    )

    header = (
        f"\U0001f4ca Picks recap \u2014 {date_str}\n"
        f"Top-{len(results)}: {hits}/{total} hit ({pct}%) | Avg edge: {avgedge:+.3f}"
    )

    lines = []
    _PROP_LABELS = {
        "player_points":           "Points",
        "player_rebounds":         "Rebounds",
        "player_assists":          "Assists",
        "player_threes":           "3-Pointers",
        "player_threes_attempts":  "3PA",
    }
    for r in results:
        prop_label = _PROP_LABELS.get(r["prop_type"], r["prop_type"])
        line_str   = f"O {r['line']}"
        edge_str   = f"{r['edge']:+.3f}" if r["edge"] is not None else "n/a"

        if r["actual_result"] is True:
            emoji   = "\u2705"
            stat_str = str(int(r["actual_stat_value"])) if r["actual_stat_value"] is not None else "\u2014"
        elif r["actual_result"] is False:
            emoji   = "\u274c"
            stat_str = str(int(r["actual_stat_value"])) if r["actual_stat_value"] is not None else "\u2014"
        else:
            emoji   = "\u2753"
            stat_str = "\u2014"

        lines.append(
            f"{emoji} {r['player_name']} \u2014 {prop_label} {line_str} | Scored: {stat_str} | Edge: {edge_str}"
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
