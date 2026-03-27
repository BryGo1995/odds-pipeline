# tests/unit/test_slack_notifier.py
from unittest.mock import MagicMock, patch


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


def make_failure_context(**kwargs):
    ctx = make_context(**kwargs)
    ti = MagicMock()
    ti.task_id = "fetch_odds"
    ctx["task_instance"] = ti
    ctx["exception"] = Exception("HTTPError 429 Too Many Requests")
    return ctx


# --- notify_failure ---

def test_notify_failure_posts_message_with_task_and_error():
    from shared.plugins.slack_notifier import notify_failure
    ctx = make_failure_context()
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_failure(ctx)
        payload = mock_post.call_args[1]["json"]
        assert "❌" in payload["text"]
        assert "nba_ingest" in payload["text"]
        assert "fetch_odds" in payload["text"]
        assert "429" in payload["text"]


def test_notify_failure_degrades_gracefully_when_context_missing():
    from shared.plugins.slack_notifier import notify_failure
    ctx = make_context()  # no task_instance or exception keys
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())
        notify_failure(ctx)  # must not raise
        payload = mock_post.call_args[1]["json"]
        assert "❌" in payload["text"]
        assert "unknown" in payload["text"]


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
        assert "⚠️ One or more upstream DAGs had issues" in text


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
        assert "⚠️ One or more upstream DAGs had issues" in text


def test_notify_score_ready_skips_when_no_webhook():
    """No SLACK_WEBHOOK_URL → no HTTP call made."""
    import pendulum
    from shared.plugins.slack_notifier import notify_score_ready

    exec_time = pendulum.datetime(2024, 1, 2, 16, 0, tz="UTC")
    ctx = make_context(dag_id="nba_score_dag", exec_time=exec_time)

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", ""), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        notify_score_ready(ctx)
        mock_post.assert_not_called()


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
         patch("mlflow.get_run") as mock_get_run, \
         patch("mlflow.tracking.MlflowClient") as mock_client_cls, \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_get_run.return_value = mock_run
        mock_client_cls.return_value.search_model_versions.return_value = [mock_version]
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
         patch("mlflow.get_run") as mock_get_run, \
         patch("mlflow.tracking.MlflowClient") as mock_client_cls, \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_get_run.return_value = mock_run
        mock_client_cls.return_value.search_model_versions.return_value = [mock_version]
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
         patch("mlflow.get_run") as mock_get_run, \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post, \
         caplog.at_level(logging.WARNING, logger="shared.plugins.slack_notifier"):
        mock_get_run.side_effect = Exception("Connection refused")
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
         patch("mlflow.get_run") as mock_get_run, \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_get_run.side_effect = Exception("run_id is None")
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())

        notify_model_ready(ctx)  # must not raise
        mock_post.assert_called_once()
