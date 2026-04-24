# mlb/dags/mlb_stats_pipeline_dag.py
from datetime import date, timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from shared.plugins.db_client import get_data_db_conn, store_raw_response
from mlb.plugins.mlb_stats_client import (
    fetch_teams,
    fetch_players,
    fetch_batter_game_logs,
)
from mlb.plugins.transformers.teams import transform_teams
from mlb.plugins.transformers.players import transform_players
from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
from shared.plugins.slack_notifier import notify_failure

import os

INGEST_DELAY_SECONDS = 1.0

# Namespace raw_api_responses.endpoint values so NBA and MLB rows don't collide.
EP_TEAMS = "mlb_api/teams"
EP_PLAYERS = "mlb_api/players"
EP_PGL = "mlb_api/player_game_logs"


def _current_season():
    """MLB season equals the current calendar year — no split-year format."""
    return str(date.today().year)


def _game_log_window():
    """Return (start_date, end_date) for the daily 3-day lookback."""
    today = date.today()
    return today - timedelta(days=3), today - timedelta(days=1)


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


# ---------------------------------------------------------------------------
# Ingest tasks
# ---------------------------------------------------------------------------

def fetch_teams_task(**context):
    conn = get_data_db_conn()
    try:
        data = fetch_teams(delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, EP_TEAMS, {}, data)
    except Exception:
        store_raw_response(conn, EP_TEAMS, {}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_players_task(**context):
    conn = get_data_db_conn()
    season = _current_season()
    try:
        data = fetch_players(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, EP_PLAYERS, {"season": season}, data)
    except Exception:
        store_raw_response(conn, EP_PLAYERS, {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_batter_game_logs_task(**context):
    conn = get_data_db_conn()
    start, end = _game_log_window()
    params = {"start_date": start.isoformat(), "end_date": end.isoformat()}
    try:
        data = fetch_batter_game_logs(start, end, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, EP_PGL, params, data)
    except Exception:
        store_raw_response(conn, EP_PGL, params, None, status="error")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------------

def transform_teams_task(**context):
    conn = get_data_db_conn()
    try:
        transform_teams(conn, _get_latest_raw(conn, EP_TEAMS))
    finally:
        conn.close()


def transform_players_task(**context):
    conn = get_data_db_conn()
    try:
        transform_players(conn, _get_latest_raw(conn, EP_PLAYERS))
    finally:
        conn.close()


def transform_player_game_logs_task(**context):
    conn = get_data_db_conn()
    try:
        transform_player_game_logs(conn, _get_latest_raw(conn, EP_PGL))
    finally:
        conn.close()


def resolve_player_ids_task(**context):
    conn = get_data_db_conn()
    try:
        slack_url = os.environ.get("SLACK_WEBHOOK_URL")
        resolve_player_ids(conn, slack_webhook_url=slack_url)
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
    dag_id="mlb_stats_pipeline",
    default_args=default_args,
    description="Fetch and transform MLB teams, players, and batter game logs",
    schedule_interval="20 15 * * *",  # 8:20am MT (3:20pm UTC)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["mlb", "stats"],
    on_failure_callback=notify_failure,
) as dag:
    wait_for_odds = ExternalTaskSensor(
        task_id="wait_for_mlb_odds_pipeline",
        external_dag_id="mlb_odds_pipeline",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
        execution_delta=timedelta(minutes=20),
    )

    t_fetch_teams   = PythonOperator(task_id="fetch_teams",              python_callable=fetch_teams_task)
    t_fetch_players = PythonOperator(task_id="fetch_players",            python_callable=fetch_players_task)
    t_fetch_pgl     = PythonOperator(task_id="fetch_batter_game_logs",   python_callable=fetch_batter_game_logs_task)

    t_xform_teams   = PythonOperator(task_id="transform_teams",              python_callable=transform_teams_task)
    t_xform_players = PythonOperator(task_id="transform_players",            python_callable=transform_players_task)
    t_xform_pgl     = PythonOperator(task_id="transform_player_game_logs",   python_callable=transform_player_game_logs_task)
    t_resolve       = PythonOperator(task_id="resolve_player_ids",           python_callable=resolve_player_ids_task)

    wait_for_odds >> [t_fetch_teams, t_fetch_players, t_fetch_pgl]

    t_fetch_teams >> t_xform_teams
    t_fetch_players >> t_xform_players
    t_fetch_pgl >> t_xform_pgl

    # FK-safe transform ordering: teams populate abbrev lookup; players satisfy
    # mlb_player_game_logs FK
    t_xform_teams >> t_xform_players
    t_xform_players >> t_xform_pgl
    t_xform_pgl >> t_resolve
