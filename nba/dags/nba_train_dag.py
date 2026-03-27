from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.plugins.slack_notifier import notify_failure, notify_model_ready
from nba.plugins.ml.train import train_model


def run_train_model(**context):
    run_id = train_model()
    context["ti"].xcom_push(key="mlflow_run_id", value=run_id)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="nba_train_dag",
    default_args=default_args,
    description="Train XGBoost player prop model and log to MLflow registry",
    schedule_interval="0 10 * * 1",  # Every Monday 10:00 UTC (3:00am MT)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "ml"],
    on_success_callback=notify_model_ready,
    on_failure_callback=notify_failure,
) as dag:
    t_train = PythonOperator(
        task_id="train_model",
        python_callable=run_train_model,
    )
