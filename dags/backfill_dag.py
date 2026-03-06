# dags/backfill_dag.py
import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from config.settings import SPORT, REGIONS, MARKETS, BOOKMAKERS, ODDS_FORMAT
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores
from plugins.transformers.events import transform_events
from plugins.transformers.odds import transform_odds
from plugins.transformers.scores import transform_scores
from plugins.transformers.player_props import transform_player_props

# Seconds to sleep between API calls during backfill to protect quota
BACKFILL_SLEEP_SECONDS = 2


def run_backfill(**context):
    api_key = os.environ["ODDS_API_KEY"]
    params = context["params"]
    date_from = params.get("date_from")
    date_to = params.get("date_to")

    event_params = {"sport": SPORT}
    if date_from:
        event_params["commenceTimeFrom"] = f"{date_from}T00:00:00Z"
    if date_to:
        event_params["commenceTimeTo"] = f"{date_to}T23:59:59Z"

    conn = get_data_db_conn()
    try:
        events = fetch_events(api_key=api_key, sport=SPORT)
        store_raw_response(conn, "events", event_params, events)
        transform_events(conn, events)
        time.sleep(BACKFILL_SLEEP_SECONDS)

        odds = fetch_odds(
            api_key=api_key,
            sport=SPORT,
            regions=REGIONS,
            markets=MARKETS,
            bookmakers=BOOKMAKERS,
        )
        store_raw_response(conn, "odds", {"sport": SPORT}, odds)
        transform_odds(conn, odds)
        transform_player_props(conn, odds)
        time.sleep(BACKFILL_SLEEP_SECONDS)

        scores = fetch_scores(api_key=api_key, sport=SPORT, days_from=30)
        store_raw_response(conn, "scores", {"sport": SPORT, "days_from": 30}, scores)
        transform_scores(conn, scores)
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nba_backfill",
    default_args=default_args,
    description="On-demand historical NBA data backfill",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "backfill"],
    params={
        "date_from": Param(None, type=["null", "string"], description="Start date YYYY-MM-DD"),
        "date_to":   Param(None, type=["null", "string"], description="End date YYYY-MM-DD"),
    },
) as dag:
    PythonOperator(task_id="run_backfill", python_callable=run_backfill)
