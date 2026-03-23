# dags/nba_player_stats_ingest_dag.py
import json
import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.nba_api_client import (
    fetch_players,
    fetch_player_game_logs,
    fetch_team_game_logs,
    fetch_team_season_stats,
)
from plugins.slack_notifier import notify_success, notify_failure

CURRENT_SEASON = "2024-25"  # Update manually each season; programmatic derivation is a future improvement
INGEST_DELAY_SECONDS = 1


def _get_current_season():
    """Return the current NBA season string e.g. '2024-25'."""
    return CURRENT_SEASON


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
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        # Build props-driven player filter
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

            # Minutes fallback: players averaging >20 min/game this season
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
        # Bootstrap: if no data yet, fetch all (backfill must run first)
        if not player_ids:
            import logging
            logging.getLogger(__name__).info(
                "No player filter available — fetching all (bootstrap mode). "
                "Run nba_player_stats_backfill first."
            )

        # nba_api's PlayerGameLogs endpoint does not support server-side player-ID filtering,
        # so we always fetch the full league dataset (~450 players) and filter client-side.
        # This is intentional: the API call is cheap (one HTTP request per season) and
        # filtering after the fact is simpler than iterating per-player.
        data = fetch_player_game_logs(season=season, delay_seconds=INGEST_DELAY_SECONDS)

        # Filter to relevant players if we have a list
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


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_player_stats_ingest",
    default_args=default_args,
    description="Fetch NBA player and team stats from nba_api",
    schedule_interval="0 13 * * *",  # 6am MT = 1pm UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "player_stats", "ingest"],
    # Note: notify_success reads XCom from task_id="fetch_odds" which does not exist in this DAG.
    # The callback will degrade gracefully to a plain success message (no quota info).
    # Decoupling notify_success from the fetch_odds XCom key is a future improvement.
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
) as dag:
    t_players      = PythonOperator(task_id="fetch_players",          python_callable=fetch_players_task)
    t_player_logs  = PythonOperator(task_id="fetch_player_game_logs", python_callable=fetch_player_game_logs_task)
    t_team_logs    = PythonOperator(task_id="fetch_team_game_logs",   python_callable=fetch_team_game_logs_task)
    t_team_stats   = PythonOperator(task_id="fetch_team_season_stats",python_callable=fetch_team_season_stats_task)

    # All four tasks run independently in parallel
    [t_players, t_player_logs, t_team_logs, t_team_stats]
