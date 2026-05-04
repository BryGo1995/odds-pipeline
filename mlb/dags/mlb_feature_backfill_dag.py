# mlb/dags/mlb_feature_backfill_dag.py
import logging
import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from shared.plugins.db_client import get_data_db_conn
from mlb.plugins.transformers.features import build_features

FEATURES_DIR = os.environ.get("FEATURES_DIR", "/data/features")

log = logging.getLogger(__name__)


def run_backfill(**context):
    params = context["params"]
    date_from = date.fromisoformat(params["date_from"])
    date_to   = date.fromisoformat(params["date_to"])

    conn = get_data_db_conn()
    try:
        current = date_from
        while current <= date_to:
            game_date = current.isoformat()
            output_path = f"{FEATURES_DIR}/mlb_props_features_{game_date}.parquet"

            if os.path.exists(output_path):
                log.info("Skipping %s — file already exists", game_date)
                current += timedelta(days=1)
                continue

            df = build_features(conn, game_date)
            if df.empty:
                log.info("No MLB prop data for %s — skipping", game_date)
            else:
                df.to_parquet(output_path, index=False)
                log.info("Wrote %d MLB rows to %s", len(df), output_path)

            current += timedelta(days=1)
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mlb_feature_backfill",
    default_args=default_args,
    description="On-demand historical MLB feature file generation for ML training",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlb", "ml", "backfill"],
    params={
        "date_from": Param(..., type="string", description="Start date YYYY-MM-DD"),
        "date_to":   Param(..., type="string", description="End date YYYY-MM-DD"),
    },
) as dag:
    PythonOperator(task_id="run_backfill", python_callable=run_backfill)
