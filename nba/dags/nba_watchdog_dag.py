import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.plugins.db_client import get_data_db_conn
from shared.plugins.slate import slate_date
from shared.plugins.slack_notifier import notify_failure, notify_missed_recommendations
from shared.plugins.watchdog import find_missed_slates

logger = logging.getLogger(__name__)


def check_recommendations(**context):
    game_date = slate_date(context)
    conn = get_data_db_conn()
    try:
        missed, last_rec_date = find_missed_slates(conn, "basketball_nba", "NBA", game_date)
    finally:
        conn.close()

    if missed:
        logger.warning("No NBA recommendations for %d game day(s): %s", len(missed), missed)
        notify_missed_recommendations(context, missed, last_rec_date)
    else:
        logger.info("NBA recommendations are current as of %s", game_date)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# No sensors on purpose: this has to run when the daily chain is broken,
# stuck or paused. See #51.
with DAG(
    dag_id="nba_watchdog_dag",
    default_args=default_args,
    description="Alert when NBA game days go by without recommendations",
    schedule_interval="30 17 * * *",  # 5:30pm MT — after nba_score_dag (4pm + sensor/retry slack)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "monitoring"],
    on_failure_callback=notify_failure,
) as dag:
    PythonOperator(
        task_id="check_recommendations",
        python_callable=check_recommendations,
    )
