from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.plugins.slack_notifier import notify_failure, notify_model_ready
from mlb.plugins.ml.train import train_all_models


def run_train_model(**context):
    run_ids = train_all_models()
    context["ti"].xcom_push(key="mlflow_run_ids", value=run_ids)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="mlb_train_dag",
    default_args=default_args,
    description="Train per-prop-type XGBoost MLB batter-prop models and log to MLflow",
    schedule_interval="0 10 * * 1",  # Every Monday 10:00 UTC (3:00am MT, standard time)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["mlb", "ml"],
    on_success_callback=notify_model_ready,
    on_failure_callback=notify_failure,
) as dag:
    t_train = PythonOperator(
        task_id="train_model",
        python_callable=run_train_model,
    )
