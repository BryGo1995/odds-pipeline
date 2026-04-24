# mlb/dags/mlb_stats_backfill_dag.py
import os
import re
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from shared.plugins.db_client import get_data_db_conn, store_raw_response
from mlb.plugins.mlb_stats_client import (
    fetch_teams,
    fetch_players,
    fetch_batter_game_logs,
)
from mlb.plugins.transformers.teams import transform_teams
from mlb.plugins.transformers.players import transform_players
from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
from mlb.plugins.transformers.player_name_resolution import resolve_player_ids

BACKFILL_DELAY_SECONDS = 1.0
SEASON_PATTERN = re.compile(r"^\d{4}$")
SEASON_START_MONTH_DAY = "03-15"
SEASON_END_MONTH_DAY = "11-15"

EP_TEAMS = "mlb_api/teams"
EP_PLAYERS = "mlb_api/players"
EP_PGL = "mlb_api/player_game_logs"


def _season_range(season_start: str, season_end: str):
    """Yield year-string seasons from start to end inclusive, e.g. '2023'..'2025'."""
    for year in range(int(season_start), int(season_end) + 1):
        yield str(year)


def run_backfill(**context):
    params = context["params"]
    season_start = params.get("season_start", "2023")
    season_end = params.get("season_end", "2025")

    if not SEASON_PATTERN.match(season_start):
        raise ValueError(
            f"Invalid season_start format '{season_start}'. Expected YYYY (e.g. 2023)."
        )
    if not SEASON_PATTERN.match(season_end):
        raise ValueError(
            f"Invalid season_end format '{season_end}'. Expected YYYY (e.g. 2025)."
        )
    if season_start > season_end:
        raise ValueError(
            f"season_start '{season_start}' must be <= season_end '{season_end}'."
        )

    conn = get_data_db_conn()
    try:
        # Teams: fetch once (MLB team set is stable across the backfill range)
        teams = fetch_teams(delay_seconds=BACKFILL_DELAY_SECONDS)
        store_raw_response(conn, EP_TEAMS, {}, teams)
        transform_teams(conn, teams)

        for season in _season_range(season_start, season_end):
            # Historical rosters — active_only=False so retired/traded players
            # exist in mlb_players before their game logs land (FK constraint).
            players = fetch_players(
                season=season, active_only=False,
                delay_seconds=BACKFILL_DELAY_SECONDS,
            )
            store_raw_response(conn, EP_PLAYERS, {"season": season}, players)
            transform_players(conn, players)

            start_date = f"{season}-{SEASON_START_MONTH_DAY}"
            end_date = f"{season}-{SEASON_END_MONTH_DAY}"
            logs = fetch_batter_game_logs(
                start_date, end_date,
                delay_seconds=BACKFILL_DELAY_SECONDS,
            )
            store_raw_response(
                conn, EP_PGL, {"season": season}, logs,
            )
            transform_player_game_logs(conn, logs)
            time.sleep(BACKFILL_DELAY_SECONDS)

        resolve_player_ids(
            conn, slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL"),
        )
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="mlb_stats_backfill",
    default_args=default_args,
    description="Historical MLB batter stats seed (manual trigger)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlb", "stats", "backfill"],
    params={
        "season_start": Param(
            "2023", type="string",
            description="Start season YYYY (e.g. 2023)",
        ),
        "season_end": Param(
            "2025", type="string",
            description="End season YYYY (e.g. 2025)",
        ),
    },
) as dag:
    PythonOperator(task_id="run_backfill", python_callable=run_backfill)
