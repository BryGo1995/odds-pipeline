# mlb/tests/unit/test_mlb_stats_pipeline_dag.py
"""Structural tests for mlb_stats_pipeline DAG.

These tests require real Airflow installed (DagBag). When run in an env
where Airflow is stubbed by conftest, they'll fail early with an
ImportError — that's expected; run this file only with Airflow available.
"""


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="mlb/dags/", include_examples=False)
    assert "mlb_stats_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "wait_for_mlb_odds_pipeline",
        "fetch_teams", "fetch_players", "fetch_batter_game_logs",
        "transform_teams", "transform_players", "transform_player_game_logs",
        "resolve_player_ids",
    }


def test_dag_task_chain():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_pipeline"]

    # Sensor feeds all 3 fetch tasks
    sensor_downstream = {t.task_id for t in dag.get_task("wait_for_mlb_odds_pipeline").downstream_list}
    assert sensor_downstream == {"fetch_teams", "fetch_players", "fetch_batter_game_logs"}

    # Each fetch feeds its corresponding transform
    assert {"transform_teams"} == {t.task_id for t in dag.get_task("fetch_teams").downstream_list}
    # fetch_players must feed transform_players (and may feed more via chain)
    assert "transform_players" in {t.task_id for t in dag.get_task("fetch_players").downstream_list}
    assert "transform_player_game_logs" in {
        t.task_id for t in dag.get_task("fetch_batter_game_logs").downstream_list
    }

    # FK-safe transform ordering: teams → players → player_game_logs
    assert "transform_players" in {
        t.task_id for t in dag.get_task("transform_teams").downstream_list
    }
    assert "transform_player_game_logs" in {
        t.task_id for t in dag.get_task("transform_players").downstream_list
    }

    # transform_player_game_logs → resolve_player_ids
    assert "resolve_player_ids" in {
        t.task_id for t in dag.get_task("transform_player_game_logs").downstream_list
    }


def test_dag_has_slack_callback():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_failure
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_pipeline"]
    assert dag.on_failure_callback is notify_failure


def test_sensor_targets_mlb_odds_pipeline():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_pipeline"]
    sensor = dag.get_task("wait_for_mlb_odds_pipeline")
    assert sensor.external_dag_id == "mlb_odds_pipeline"
