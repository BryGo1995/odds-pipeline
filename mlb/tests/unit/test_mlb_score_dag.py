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
