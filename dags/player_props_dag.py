# dags/player_props_dag.py
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from config.settings import SPORT, REGIONS, PLAYER_PROP_MARKETS, BOOKMAKERS, ODDS_FORMAT
from plugins.odds_api_client import fetch_player_props
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.slack_notifier import notify_success, notify_failure
from plugins.transformers.player_props import transform_player_props


def fetch_player_props_task(**context):
    api_key = os.environ["ODDS_API_KEY"]
    conn = get_data_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT game_id FROM games WHERE commence_time >= CURRENT_DATE ORDER BY commence_time"
            )
            game_ids = [row[0] for row in cur.fetchall()]

        if not game_ids:
            raise ValueError("No today/upcoming games found in DB. Run nba_ingest first.")

        all_props = []
        remaining = 0
        fetch_kwargs = {
            "sport": SPORT,
            "regions": REGIONS,
            "markets": PLAYER_PROP_MARKETS,
            "bookmakers": BOOKMAKERS,
            "odds_format": ODDS_FORMAT,
        }
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
        context["ti"].xcom_push(key="api_remaining", value=remaining)

        if not all_props:
            raise ValueError("All player props fetches failed. Check API key and event IDs.")
    finally:
        conn.close()


def transform_player_props_task(**context):
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
            raise ValueError("No successful player_props raw data found. Run fetch_player_props first.")
        transform_player_props(conn, row[0])
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_player_props",
    default_args=default_args,
    description="Fetch and transform NBA player prop odds",
    schedule_interval="0 22 * * *",  # 10pm UTC / 3pm MT daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "player_props"],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
) as dag:
    t_fetch     = PythonOperator(task_id="fetch_player_props",     python_callable=fetch_player_props_task)
    t_transform = PythonOperator(task_id="transform_player_props", python_callable=transform_player_props_task)

    t_fetch >> t_transform
