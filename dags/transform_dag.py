# dags/transform_dag.py
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, "/opt/airflow")

from plugins.db_client import get_data_db_conn
from plugins.transformers.events import transform_events
from plugins.transformers.odds import transform_odds
from plugins.transformers.scores import transform_scores
from plugins.transformers.player_props import transform_player_props


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
    return row[0] if row else []


def run_transform_events():
    conn = get_data_db_conn()
    try:
        transform_events(conn, _get_latest_raw(conn, "events"))
    finally:
        conn.close()


def run_transform_odds():
    conn = get_data_db_conn()
    try:
        raw = _get_latest_raw(conn, "odds")
        transform_odds(conn, raw)
        transform_player_props(conn, raw)
    finally:
        conn.close()


def run_transform_scores():
    conn = get_data_db_conn()
    try:
        transform_scores(conn, _get_latest_raw(conn, "scores"))
    finally:
        conn.close()


def run_transform_player_props():
    # Player props are sourced from the odds response and already processed
    # in run_transform_odds. This task exists for visibility in the Airflow UI.
    pass


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nba_transform",
    default_args=default_args,
    description="Normalize raw NBA Odds-API data into structured tables",
    schedule_interval="15 8,20 * * *",  # 15 min after ingest
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "transform"],
) as dag:
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="nba_ingest",
        external_task_id=None,  # wait for full DAG completion
        timeout=600,
        mode="reschedule",
    )

    t_events = PythonOperator(task_id="transform_events",       python_callable=run_transform_events)
    t_odds   = PythonOperator(task_id="transform_odds",         python_callable=run_transform_odds)
    t_scores = PythonOperator(task_id="transform_scores",       python_callable=run_transform_scores)
    t_props  = PythonOperator(task_id="transform_player_props", python_callable=run_transform_player_props)

    wait_for_ingest >> [t_events, t_odds, t_scores]
    t_odds >> t_props
