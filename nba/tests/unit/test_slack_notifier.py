# tests/unit/test_slack_notifier.py
from unittest.mock import MagicMock, patch


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
        # 2024-01-02 03:02 UTC = 2024-01-01 20:02 MST
        "execution_date": exec_time or pendulum.datetime(2024, 1, 2, 3, 2, tz="UTC"),
    }


def make_failure_context(tags=("nba",), **kwargs):
    ctx = make_context(tags=tags, **kwargs)
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
        assert "[NBA]" in payload["text"]


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
        assert "[NBA]" in payload["text"]


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
        assert "[NBA]" in text
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

def _make_model_context(run_ids=None):
    if run_ids is None:
        run_ids = {
            "player_points": "run-points-123",
            "player_rebounds": "run-rebounds-456",
            "player_assists": "run-assists-789",
        }
    ctx = make_context(dag_id="nba_train_dag")
    ctx["task_instance"] = MagicMock()
    ctx["task_instance"].xcom_pull.return_value = run_ids
    return ctx


def test_notify_model_ready_promotion_candidate():
    """Promotion candidates: 🚀 header with per-prop-type ROC-AUC lines and ✅ promoted status."""
    from shared.plugins.slack_notifier import notify_model_ready

    run_ids = {
        "player_points": "run-points-123",
        "player_rebounds": "run-rebounds-456",
        "player_assists": "run-assists-789",
    }
    ctx = _make_model_context(run_ids)

    def make_mock_run(roc_auc, delta):
        mock_run = MagicMock()
        mock_run.data.metrics = {
            "roc_auc": roc_auc,
            "roc_auc_delta_vs_production": delta,
        }
        mock_run.data.tags = {"promotion_candidate": "true"}
        return mock_run

    run_map = {
        "run-points-123": make_mock_run(0.6821, 0.0134),
        "run-rebounds-456": make_mock_run(0.5823, 0.0041),
        "run-assists-789": make_mock_run(0.6100, 0.0088),
    }

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("mlflow.get_run", side_effect=lambda run_id: run_map[run_id]), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())

        notify_model_ready(ctx)
        text = mock_post.call_args[1]["json"]["text"]
        assert "🚀 Models trained" in text
        assert "[NBA]" in text
        assert "Points: ROC-AUC" in text
        assert "Rebounds: ROC-AUC" in text
        assert "Assists: ROC-AUC" in text
        assert "0.6821" in text
        assert "+0.0134" in text
        assert "✅ promoted" in text


def test_notify_model_ready_no_improvement():
    """No improvement: 🚀 header with per-prop-type lines showing — no improvement."""
    from shared.plugins.slack_notifier import notify_model_ready

    run_ids = {
        "player_points": "run-points-111",
        "player_rebounds": "run-rebounds-222",
        "player_assists": "run-assists-333",
    }
    ctx = _make_model_context(run_ids)

    def make_mock_run(roc_auc, delta):
        mock_run = MagicMock()
        mock_run.data.metrics = {
            "roc_auc": roc_auc,
            "roc_auc_delta_vs_production": delta,
        }
        mock_run.data.tags = {"promotion_candidate": "false"}
        return mock_run

    run_map = {
        "run-points-111": make_mock_run(0.6542, -0.0145),
        "run-rebounds-222": make_mock_run(0.5512, -0.0023),
        "run-assists-333": make_mock_run(0.5900, -0.0060),
    }

    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("mlflow.get_run", side_effect=lambda run_id: run_map[run_id]), \
         patch("shared.plugins.slack_notifier.requests.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=MagicMock())

        notify_model_ready(ctx)
        text = mock_post.call_args[1]["json"]["text"]
        assert "🚀 Models trained" in text
        assert "[NBA]" in text
        assert "Points: ROC-AUC" in text
        assert "Rebounds: ROC-AUC" in text
        assert "Assists: ROC-AUC" in text
        assert "0.6542" in text
        assert "-0.0145" in text
        assert "— no improvement" in text


def test_notify_model_ready_mlflow_unreachable(caplog):
    """MLflow raises → logs warning, posts fallback message, does not raise."""
    import logging
    from shared.plugins.slack_notifier import notify_model_ready

    run_ids = {
        "player_points": "run-points-abc",
        "player_rebounds": "run-rebounds-def",
    }
    ctx = _make_model_context(run_ids)

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


# --- _resolve_sport ---

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
