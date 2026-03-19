# dags/ingest_dag.py
import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from config.settings import SPORT, REGIONS, MARKETS, BOOKMAKERS, ODDS_FORMAT, SCORES_DAYS_FROM
from plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.slack_notifier import notify_success, notify_failure


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


def fetch_events_task(**context):
    ti = context["ti"]
    _fetch_and_store("events", fetch_events, {"sport": SPORT}, ti)


def fetch_odds_task(**context):
    ti = context["ti"]
    _fetch_and_store("odds", fetch_odds, {
        "sport": SPORT,
        "regions": REGIONS,
        "markets": MARKETS,
        "bookmakers": BOOKMAKERS,
        "odds_format": ODDS_FORMAT,
    }, ti)


def fetch_scores_task(**context):
    ti = context["ti"]
    _fetch_and_store("scores", fetch_scores, {"sport": SPORT, "days_from": SCORES_DAYS_FROM}, ti)


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_ingest",
    default_args=default_args,
    description="Fetch NBA data from Odds-API and store raw JSON",
    schedule_interval="0 8,16 * * *",  # 8am and 4pm MT
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "ingest"],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
) as dag:
    t_events = PythonOperator(task_id="fetch_events", python_callable=fetch_events_task)
    t_odds   = PythonOperator(task_id="fetch_odds",   python_callable=fetch_odds_task)
    t_scores = PythonOperator(task_id="fetch_scores", python_callable=fetch_scores_task)

    # All three tasks run independently
    [t_events, t_odds, t_scores]
