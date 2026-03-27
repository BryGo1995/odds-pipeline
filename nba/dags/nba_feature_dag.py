import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from shared.plugins.db_client import get_data_db_conn
from shared.plugins.slack_notifier import notify_failure
from nba.plugins.transformers.features import build_features

FEATURES_DIR = os.environ.get("FEATURES_DIR", "/data/features")


def run_build_features(**context):
    import logging
    game_date = context["ds"]  # YYYY-MM-DD
    conn = get_data_db_conn()
    try:
        df = build_features(conn, game_date)
        if df.empty:
            logging.getLogger(__name__).warning("No features generated for %s", game_date)
            return
        output_path = f"{FEATURES_DIR}/props_features_{game_date}.parquet"
        df.to_parquet(output_path, index=False)
        logging.getLogger(__name__).info("Wrote %d rows to %s", len(df), output_path)
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nba_feature_dag",
    default_args=default_args,
    description="Build player prop features from Postgres and write to Parquet",
    schedule_interval="40 15 * * *",  # 8:40am MT — 20 min after nba_stats_pipeline
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "ml"],
    on_failure_callback=notify_failure,
) as dag:
    wait_for_stats = ExternalTaskSensor(
        task_id="wait_for_nba_stats_pipeline",
        external_dag_id="nba_stats_pipeline",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
        execution_delta=timedelta(minutes=20),
    )

    t_build_features = PythonOperator(
        task_id="build_features",
        python_callable=run_build_features,
    )

    wait_for_stats >> t_build_features
