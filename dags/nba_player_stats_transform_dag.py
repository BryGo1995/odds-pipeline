# dags/nba_player_stats_transform_dag.py
import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, "/opt/airflow")

from plugins.db_client import get_data_db_conn
from plugins.transformers.players import transform_players
from plugins.transformers.player_game_logs import transform_player_game_logs
from plugins.transformers.team_game_logs import transform_team_game_logs
from plugins.transformers.team_season_stats import transform_team_season_stats
from plugins.transformers.player_name_resolution import resolve_player_ids
from plugins.transformers.game_id_linker import link_nba_game_ids


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
    # psycopg2 deserializes JSONB columns to Python dicts/lists automatically.
    # row[0] is already a Python list — transformers receive it directly.
    return row[0]


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


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nba_player_stats_transform",
    default_args=default_args,
    description="Normalize raw nba_api data into structured player stats tables",
    schedule_interval="20 13 * * *",  # 6:20am MT = 1:20pm UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "player_stats", "transform"],
) as dag:
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="nba_player_stats_ingest",
        external_task_id=None,
        execution_delta=timedelta(minutes=20),
        timeout=600,
        mode="reschedule",
    )

    t_players      = PythonOperator(task_id="transform_players",          python_callable=run_transform_players)
    t_player_logs  = PythonOperator(task_id="transform_player_game_logs", python_callable=run_transform_player_game_logs)
    t_team_logs    = PythonOperator(task_id="transform_team_game_logs",   python_callable=run_transform_team_game_logs)
    t_team_stats   = PythonOperator(task_id="transform_team_season_stats",python_callable=run_transform_team_season_stats)
    t_resolve      = PythonOperator(task_id="resolve_player_ids",         python_callable=run_resolve_player_ids)
    t_link_games   = PythonOperator(task_id="link_nba_game_ids",          python_callable=run_link_nba_game_ids)

    wait_for_ingest >> t_players >> [t_player_logs, t_team_logs, t_team_stats] >> t_resolve >> t_link_games
