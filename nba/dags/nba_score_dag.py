from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from shared.plugins.db_client import get_data_db_conn
from shared.plugins.slack_notifier import notify_failure, notify_score_ready
from nba.plugins.ml.score import score


def run_score(**context):
    game_date = context["ds"]
    conn = get_data_db_conn()
    try:
        score(conn, game_date)
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nba_score_dag",
    default_args=default_args,
    description="Score today's player props and write ranked recommendations to Postgres",
    schedule_interval="0 16 * * *",  # 9:00am MT — after nba_feature_dag at 8:40am
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "ml"],
    on_success_callback=notify_score_ready,
    on_failure_callback=notify_failure,
) as dag:
    wait_for_features = ExternalTaskSensor(
        task_id="wait_for_nba_feature_dag",
        external_dag_id="nba_feature_dag",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
        execution_delta=timedelta(minutes=20),
    )

    t_score = PythonOperator(
        task_id="score",
        python_callable=run_score,
    )

    wait_for_features >> t_score
