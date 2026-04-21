# nba/dags/nba_odds_pipeline_dag.py
import logging
import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from nba.config import (
    SPORT, REGIONS, MARKETS, PLAYER_PROP_MARKETS, BOOKMAKERS, ODDS_FORMAT, SCORES_DAYS_FROM,
)
from shared.plugins.db_client import get_data_db_conn, store_raw_response
from shared.plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores, fetch_player_props
from shared.plugins.slack_notifier import notify_failure
from shared.plugins.transformers.events import transform_events
from shared.plugins.transformers.scores import transform_scores
from shared.plugins.transformers.odds import transform_odds
from shared.plugins.transformers.player_props import transform_player_props


# ---------------------------------------------------------------------------
# Shared ingest helper
# ---------------------------------------------------------------------------

def _fetch_and_store(endpoint_name, fetch_fn, fetch_kwargs, ti):
    api_key = os.environ["ODDS_API_KEY"]
    conn = get_data_db_conn()
    try:
        data, remaining = fetch_fn(api_key=api_key, **fetch_kwargs)
        store_raw_response(conn, endpoint=endpoint_name, params=fetch_kwargs, response=data, status="success")
        ti.xcom_push(key="api_remaining", value=remaining)
    except Exception:
        store_raw_response(conn, endpoint=endpoint_name, params=fetch_kwargs, response=None, status="error")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Ingest tasks
# ---------------------------------------------------------------------------

def fetch_events_task(**context):
    _fetch_and_store("events", fetch_events, {"sport": SPORT}, context["ti"])


def fetch_odds_task(**context):
    _fetch_and_store("odds", fetch_odds, {
        "sport": SPORT,
        "regions": REGIONS,
        "markets": MARKETS,
        "bookmakers": BOOKMAKERS,
        "odds_format": ODDS_FORMAT,
    }, context["ti"])


def fetch_scores_task(**context):
    _fetch_and_store("scores", fetch_scores, {"sport": SPORT, "days_from": SCORES_DAYS_FROM}, context["ti"])


# ---------------------------------------------------------------------------
# Shared transform helper
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
        raise ValueError(f"No successful raw data found for endpoint: '{endpoint}'.")
    return row[0]


# ---------------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------------

def transform_events_task(**context):
    conn = get_data_db_conn()
    try:
        transform_events(conn, _get_latest_raw(conn, "events"))
    finally:
        conn.close()


def transform_odds_task(**context):
    conn = get_data_db_conn()
    try:
        transform_odds(conn, _get_latest_raw(conn, "odds"))
    finally:
        conn.close()


def transform_scores_task(**context):
    conn = get_data_db_conn()
    try:
        transform_scores(conn, _get_latest_raw(conn, "scores"))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Player props tasks (ingest + transform in one task group)
# ---------------------------------------------------------------------------

def fetch_player_props_task(**context):
    ti = context["ti"]
    api_key = os.environ["ODDS_API_KEY"]
    conn = get_data_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT game_id FROM games WHERE commence_time >= CURRENT_DATE ORDER BY commence_time"
            )
            game_ids = [row[0] for row in cur.fetchall()]

        if not game_ids:
            logging.warning("No today/upcoming games found — skipping player props fetch.")
            ti.xcom_push(key="skipped", value=True)
            return

        fetch_kwargs = {
            "sport": SPORT,
            "regions": REGIONS,
            "markets": PLAYER_PROP_MARKETS,
            "bookmakers": BOOKMAKERS,
            "odds_format": ODDS_FORMAT,
        }
        all_props = []
        remaining = 0
        for game_id in game_ids:
            try:
                data, remaining = fetch_player_props(
                    api_key=api_key,
                    event_id=game_id,
                    **fetch_kwargs,
                )
                all_props.append(data)
            except Exception as e:
                logging.warning("Failed to fetch player props for event %s: %s", game_id, e)

        store_raw_response(
            conn,
            endpoint="player_props",
            params=fetch_kwargs,
            response=all_props,
            status="success" if all_props else "error",
        )
        ti.xcom_push(key="api_remaining", value=remaining)

        if not all_props:
            raise ValueError("All player props fetches failed. Check API key and event IDs.")
    finally:
        conn.close()


def transform_player_props_task(**context):
    ti = context["ti"]
    if ti.xcom_pull(task_ids="fetch_player_props", key="skipped") == True:
        logging.info("Player props fetch was skipped (no games) — nothing to transform.")
        return
    conn = get_data_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT response FROM raw_api_responses
                WHERE endpoint = %s AND status = 'success'
                ORDER BY fetched_at DESC LIMIT 1
                """,
                ("player_props",),
            )
            row = cur.fetchone()
        if row is None:
            raise ValueError("No successful player_props raw data found.")
        transform_player_props(conn, row[0])
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
    dag_id="nba_odds_pipeline",
    default_args=default_args,
    description="Fetch and transform NBA odds, scores, and player props from The-Odds-API",
    schedule_interval="0 15 * * *",  # 8am MT (3pm UTC)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "odds"],
    on_failure_callback=notify_failure,
) as dag:
    t_fetch_events  = PythonOperator(task_id="fetch_events",           python_callable=fetch_events_task)
    t_fetch_odds    = PythonOperator(task_id="fetch_odds",             python_callable=fetch_odds_task)
    t_fetch_scores  = PythonOperator(task_id="fetch_scores",           python_callable=fetch_scores_task)
    t_xform_events  = PythonOperator(task_id="transform_events",       python_callable=transform_events_task)
    t_xform_odds    = PythonOperator(task_id="transform_odds",         python_callable=transform_odds_task)
    t_xform_scores  = PythonOperator(task_id="transform_scores",       python_callable=transform_scores_task)
    t_fetch_props   = PythonOperator(task_id="fetch_player_props",     python_callable=fetch_player_props_task)
    t_xform_props   = PythonOperator(task_id="transform_player_props", python_callable=transform_player_props_task)

    [t_fetch_events, t_fetch_odds, t_fetch_scores] >> t_xform_events
    t_xform_events >> [t_xform_odds, t_xform_scores, t_fetch_props]
    t_fetch_props >> t_xform_props
