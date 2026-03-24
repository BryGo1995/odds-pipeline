# tests/unit/test_nba_stats_pipeline_dag.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_stats_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_stats_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "wait_for_nba_odds_pipeline",
        "fetch_teams", "fetch_players", "fetch_player_game_logs",
        "fetch_team_game_logs", "fetch_team_season_stats",
        "transform_teams", "transform_players",
        "transform_player_game_logs", "transform_team_game_logs", "transform_team_season_stats",
        "resolve_player_ids", "link_nba_game_ids",
    }


def test_dag_task_chain():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_stats_pipeline"]

    # sensor feeds all 5 fetch tasks
    sensor_downstream = [t.task_id for t in dag.get_task("wait_for_nba_odds_pipeline").downstream_list]
    for task_id in ["fetch_teams", "fetch_players", "fetch_player_game_logs",
                    "fetch_team_game_logs", "fetch_team_season_stats"]:
        assert task_id in sensor_downstream, f"sensor must feed {task_id}"

    # all 5 fetch tasks feed transform_teams
    for task_id in ["fetch_teams", "fetch_players", "fetch_player_game_logs",
                    "fetch_team_game_logs", "fetch_team_season_stats"]:
        downstream = [t.task_id for t in dag.get_task(task_id).downstream_list]
        assert "transform_teams" in downstream, f"{task_id} must feed transform_teams"

    # transform_teams >> transform_players
    assert "transform_players" in [t.task_id for t in dag.get_task("transform_teams").downstream_list]

    # transform_players >> the three log transforms
    players_downstream = [t.task_id for t in dag.get_task("transform_players").downstream_list]
    for task_id in ["transform_player_game_logs", "transform_team_game_logs", "transform_team_season_stats"]:
        assert task_id in players_downstream, f"transform_players must feed {task_id}"

    # all three log transforms feed resolve_player_ids
    for task_id in ["transform_player_game_logs", "transform_team_game_logs", "transform_team_season_stats"]:
        downstream = [t.task_id for t in dag.get_task(task_id).downstream_list]
        assert "resolve_player_ids" in downstream, f"{task_id} must feed resolve_player_ids"

    # resolve_player_ids >> link_nba_game_ids
    assert "link_nba_game_ids" in [t.task_id for t in dag.get_task("resolve_player_ids").downstream_list]


def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from plugins.slack_notifier import notify_success, notify_failure
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_stats_pipeline"]
    assert dag.on_success_callback is notify_success
    assert dag.on_failure_callback is notify_failure


def test_sensor_targets_nba_odds_pipeline():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_stats_pipeline"]
    sensor = dag.get_task("wait_for_nba_odds_pipeline")
    assert sensor.external_dag_id == "nba_odds_pipeline"
