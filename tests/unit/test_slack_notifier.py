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
