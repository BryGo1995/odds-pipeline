# nba/dags/nba_stats_pipeline_dag.py
import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from shared.plugins.db_client import get_data_db_conn, store_raw_response
from nba.plugins.nba_api_client import (
    fetch_players,
    fetch_player_game_logs,
    fetch_team_game_logs,
    fetch_team_season_stats,
    fetch_teams,
)
from shared.plugins.slack_notifier import notify_failure
from nba.plugins.transformers.game_id_linker import link_nba_game_ids
from nba.plugins.transformers.player_game_logs import transform_player_game_logs
from nba.plugins.transformers.player_name_resolution import resolve_player_ids
from nba.plugins.transformers.players import transform_players
from nba.plugins.transformers.team_game_logs import transform_team_game_logs
from nba.plugins.transformers.team_season_stats import transform_team_season_stats
from nba.plugins.transformers.teams import transform_teams

CURRENT_SEASON = "2025-26"  # Update manually each season
INGEST_DELAY_SECONDS = 1


def _get_current_season():
    return CURRENT_SEASON


# ---------------------------------------------------------------------------
# Ingest tasks
# ---------------------------------------------------------------------------

def fetch_teams_task(**context):
    conn = get_data_db_conn()
    try:
        data = fetch_teams()
        store_raw_response(conn, "nba_api/teams", {}, data)
    except Exception:
        store_raw_response(conn, "nba_api/teams", {}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_players_task(**context):
    conn = get_data_db_conn()
    try:
        data = fetch_players(delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, "nba_api/players", {}, data)
    except Exception:
        store_raw_response(conn, "nba_api/players", {}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_player_game_logs_task(**context):
    import logging
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT m.nba_player_id
                FROM player_props pp
                JOIN player_name_mappings m ON m.odds_api_name = pp.player_name
                WHERE m.nba_player_id IS NOT NULL
                """
            )
            prop_player_ids = {row[0] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT DISTINCT player_id
                FROM player_game_logs
                WHERE season = %s
                GROUP BY player_id
                HAVING AVG(min) > 20
                """,
                (season,),
            )
            mins_player_ids = {row[0] for row in cur.fetchall()}

        player_ids = prop_player_ids | mins_player_ids
        if not player_ids:
            logging.getLogger(__name__).info(
                "No player filter available — fetching all (bootstrap mode). "
                "Run nba_stats_backfill first."
            )

        data = fetch_player_game_logs(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        if player_ids:
            data = [row for row in data if row["player_id"] in player_ids]

        store_raw_response(conn, "nba_api/player_game_logs", {"season": season}, data)
    except Exception:
        store_raw_response(conn, "nba_api/player_game_logs", {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_team_game_logs_task(**context):
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        data = fetch_team_game_logs(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, "nba_api/team_game_logs", {"season": season}, data)
    except Exception:
        store_raw_response(conn, "nba_api/team_game_logs", {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_team_season_stats_task(**context):
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        data = fetch_team_season_stats(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, "nba_api/team_season_stats", {"season": season}, data)
    except Exception:
        store_raw_response(conn, "nba_api/team_season_stats", {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------------

def _get_latest_raw(conn, endpoint):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT response FROM raw_api_responses
            WHERE endpoint = %s AND status = 'success'
            ORDER BY fetched_at DESC LIMIT 1
            """,
            (endpoint,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"No successful raw data for endpoint '{endpoint}'. Run ingest first.")
    return row[0]


def run_transform_teams():
    conn = get_data_db_conn()
    try:
        transform_teams(conn, _get_latest_raw(conn, "nba_api/teams"))
    finally:
        conn.close()


def run_transform_players():
    conn = get_data_db_conn()
    try:
        transform_players(conn, _get_latest_raw(conn, "nba_api/players"))
    finally:
        conn.close()


def run_transform_player_game_logs():
    conn = get_data_db_conn()
    try:
        transform_player_game_logs(conn, _get_latest_raw(conn, "nba_api/player_game_logs"))
    finally:
        conn.close()


def run_transform_team_game_logs():
    conn = get_data_db_conn()
    try:
        transform_team_game_logs(conn, _get_latest_raw(conn, "nba_api/team_game_logs"))
    finally:
        conn.close()


def run_transform_team_season_stats():
    conn = get_data_db_conn()
    try:
        transform_team_season_stats(conn, _get_latest_raw(conn, "nba_api/team_season_stats"))
    finally:
        conn.close()


def run_resolve_player_ids():
    conn = get_data_db_conn()
    try:
        slack_url = os.environ.get("SLACK_WEBHOOK_URL")
        resolve_player_ids(conn, slack_webhook_url=slack_url)
    finally:
        conn.close()


def run_link_nba_game_ids():
    conn = get_data_db_conn()
    try:
        link_nba_game_ids(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_stats_pipeline",
    default_args=default_args,
    description="Fetch and transform NBA player and team stats from nba_api",
    schedule_interval="20 15 * * *",  # 8:20am MT (3:20pm UTC)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "stats"],
    on_failure_callback=notify_failure,
) as dag:
    wait_for_odds = ExternalTaskSensor(
        task_id="wait_for_nba_odds_pipeline",
        external_dag_id="nba_odds_pipeline",
        external_task_id=None,   # wait for full DAG completion
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
        execution_delta=timedelta(minutes=20),
    )

    t_fetch_teams      = PythonOperator(task_id="fetch_teams",            python_callable=fetch_teams_task)
    t_fetch_players    = PythonOperator(task_id="fetch_players",          python_callable=fetch_players_task)
    t_fetch_pgl        = PythonOperator(task_id="fetch_player_game_logs", python_callable=fetch_player_game_logs_task)
    t_fetch_tgl        = PythonOperator(task_id="fetch_team_game_logs",   python_callable=fetch_team_game_logs_task)
    t_fetch_tss        = PythonOperator(task_id="fetch_team_season_stats",python_callable=fetch_team_season_stats_task)

    t_xform_teams      = PythonOperator(task_id="transform_teams",            python_callable=run_transform_teams)
    t_xform_players    = PythonOperator(task_id="transform_players",          python_callable=run_transform_players)
    t_xform_pgl        = PythonOperator(task_id="transform_player_game_logs", python_callable=run_transform_player_game_logs)
    t_xform_tgl        = PythonOperator(task_id="transform_team_game_logs",   python_callable=run_transform_team_game_logs)
    t_xform_tss        = PythonOperator(task_id="transform_team_season_stats",python_callable=run_transform_team_season_stats)
    t_resolve          = PythonOperator(task_id="resolve_player_ids",         python_callable=run_resolve_player_ids)
    t_link_games       = PythonOperator(task_id="link_nba_game_ids",          python_callable=run_link_nba_game_ids)

    fetches = [t_fetch_teams, t_fetch_players, t_fetch_pgl, t_fetch_tgl, t_fetch_tss]

    wait_for_odds >> fetches
    fetches >> t_xform_teams
    t_xform_teams >> t_xform_players
    t_xform_players >> [t_xform_pgl, t_xform_tgl, t_xform_tss]
    [t_xform_pgl, t_xform_tgl, t_xform_tss] >> t_resolve
    t_resolve >> t_link_games
