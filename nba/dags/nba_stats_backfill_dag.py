# nba/dags/nba_stats_backfill_dag.py
import re
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from shared.plugins.db_client import get_data_db_conn, store_raw_response
from nba.plugins.nba_api_client import (
    fetch_players,
    fetch_player_game_logs,
    fetch_team_game_logs,
    fetch_team_season_stats,
)
from nba.plugins.transformers.players import transform_players
from nba.plugins.transformers.player_game_logs import transform_player_game_logs
from nba.plugins.transformers.team_game_logs import transform_team_game_logs
from nba.plugins.transformers.team_season_stats import transform_team_season_stats
from nba.plugins.transformers.game_id_linker import link_nba_game_ids

BACKFILL_DELAY_SECONDS = 5
SEASON_PATTERN = re.compile(r"^\d{4}-\d{2}$")


def _season_range(season_start: str, season_end: str):
    """Yield seasons from season_start to season_end inclusive, e.g. '2022-23'..'2024-25'."""
    def season_year(s):
        return int(s[:4])

    start_year = season_year(season_start)
    end_year = season_year(season_end)
    for year in range(start_year, end_year + 1):
        yield f"{year}-{str(year + 1)[-2:]}"


def run_backfill(**context):
    params = context["params"]
    season_start = params.get("season_start", "2022-23")
    season_end = params.get("season_end", "2024-25")

    if not SEASON_PATTERN.match(season_start):
        raise ValueError(f"Invalid season_start format '{season_start}'. Expected YYYY-YY (e.g. 2022-23).")
    if not SEASON_PATTERN.match(season_end):
        raise ValueError(f"Invalid season_end format '{season_end}'. Expected YYYY-YY (e.g. 2024-25).")
    # String comparison works correctly for YYYY-YY format within any single century
    # (e.g. "2022-23" < "2024-25" is True). Sufficient for all realistic NBA seasons.
    if season_start > season_end:
        raise ValueError(f"season_start '{season_start}' must be <= season_end '{season_end}'.")

    conn = get_data_db_conn()
    try:
        # Fetch all historical players (not just current-season active roster)
        # so game logs from past seasons don't violate the FK constraint
        players = fetch_players(delay_seconds=BACKFILL_DELAY_SECONDS, is_only_current_season=0)
        store_raw_response(conn, "nba_api/players", {}, players)
        transform_players(conn, players)

        for season in _season_range(season_start, season_end):
            # Player game logs
            player_logs = fetch_player_game_logs(season=season, delay_seconds=BACKFILL_DELAY_SECONDS)
            store_raw_response(conn, "nba_api/player_game_logs", {"season": season}, player_logs)
            transform_player_game_logs(conn, player_logs)
            time.sleep(BACKFILL_DELAY_SECONDS)

            # Team game logs
            team_logs = fetch_team_game_logs(season=season, delay_seconds=BACKFILL_DELAY_SECONDS)
            store_raw_response(conn, "nba_api/team_game_logs", {"season": season}, team_logs)
            transform_team_game_logs(conn, team_logs)
            time.sleep(BACKFILL_DELAY_SECONDS)

            # Team season stats
            team_stats = fetch_team_season_stats(season=season, delay_seconds=BACKFILL_DELAY_SECONDS)
            store_raw_response(conn, "nba_api/team_season_stats", {"season": season}, team_stats)
            transform_team_season_stats(conn, team_stats)
            time.sleep(BACKFILL_DELAY_SECONDS)

        # Link game IDs after all seasons are loaded
        link_nba_game_ids(conn)
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="nba_stats_backfill",
    default_args=default_args,
    description="Historical NBA player stats seed (manual trigger)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "player_stats", "backfill"],
    params={
        "season_start": Param("2022-23", type="string", description="Start season YYYY-YY (e.g. 2022-23)"),
        "season_end":   Param("2024-25", type="string", description="End season YYYY-YY (e.g. 2024-25)"),
    },
) as dag:
    PythonOperator(task_id="run_backfill", python_callable=run_backfill)
