# tests/integration/test_player_stats_pipeline.py
"""
Integration tests for the player stats pipeline.
Requires: data-postgres container running (docker compose up -d data-postgres).
Run with: python -m pytest tests/integration/test_player_stats_pipeline.py -v
"""
import pytest
from shared.plugins.db_client import get_data_db_conn
from nba.plugins.transformers.players import transform_players
from nba.plugins.transformers.player_game_logs import transform_player_game_logs
from nba.plugins.transformers.team_game_logs import transform_team_game_logs
from nba.plugins.transformers.team_season_stats import transform_team_season_stats
from nba.plugins.transformers.player_name_resolution import resolve_player_ids
from nba.plugins.transformers.game_id_linker import link_nba_game_ids


@pytest.fixture
def conn():
    c = get_data_db_conn()
    yield c
    # Cleanup test data
    with c.cursor() as cur:
        cur.execute("DELETE FROM player_name_mappings WHERE odds_api_name LIKE 'Test%'")
        cur.execute("DELETE FROM player_game_logs WHERE nba_game_id = 'TEST_GAME_001'")
        cur.execute("DELETE FROM team_game_logs WHERE nba_game_id = 'TEST_GAME_001'")
        cur.execute("DELETE FROM team_season_stats WHERE team_id = 9999")
        cur.execute("DELETE FROM players WHERE player_id = 99999")
    c.commit()
    c.close()


def test_players_upsert_is_idempotent(conn):
    players = [{"player_id": 99999, "full_name": "Test Player", "position": "G",
                "team_id": 1, "team_abbreviation": "TST", "is_active": True}]
    transform_players(conn, players)
    transform_players(conn, players)  # second run — should not error
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM players WHERE player_id = 99999")
        assert cur.fetchone()[0] == 1


def test_player_game_logs_insert_is_idempotent(conn):
    # Requires player 99999 to exist (from test above or re-inserted here)
    transform_players(conn, [{"player_id": 99999, "full_name": "Test Player", "position": "G",
                               "team_id": 1, "team_abbreviation": "TST", "is_active": True}])
    log = {"player_id": 99999, "nba_game_id": "TEST_GAME_001", "season": "2024-25",
           "game_date": "2025-01-15", "matchup": "TST vs. OPP", "team_id": 1,
           "wl": "W", "min": 30.0, "fga": 10, "fta": 3, "usg_pct": 0.25,
           "pts": 20, "reb": 5, "ast": 5, "blk": 0, "stl": 1, "plus_minus": 5}
    transform_player_game_logs(conn, [log])
    transform_player_game_logs(conn, [log])  # second run — ON CONFLICT DO NOTHING
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM player_game_logs WHERE nba_game_id = 'TEST_GAME_001' AND player_id = 99999")
        assert cur.fetchone()[0] == 1


def test_team_season_stats_upsert_updates_values(conn):
    stats = [{"team_id": 9999, "team_name": "Test Team", "season": "2024-25",
              "pace": 98.0, "off_rating": 110.0, "def_rating": 108.0, "opp_pts_pg": 40.0}]
    transform_team_season_stats(conn, stats)
    # Update pace
    stats[0]["pace"] = 102.0
    transform_team_season_stats(conn, stats)
    with conn.cursor() as cur:
        cur.execute("SELECT pace FROM team_season_stats WHERE team_id = 9999 AND season = '2024-25'")
        assert cur.fetchone()[0] == 102.0


def test_resolve_player_ids_populates_mapping(conn):
    # Insert a known player and a player_props row referencing them by name
    transform_players(conn, [{"player_id": 99999, "full_name": "Test Player Integration", "position": "G",
                               "team_id": 1, "team_abbreviation": "TST", "is_active": True}])
    with conn.cursor() as cur:
        # Insert a fake game and player_prop for the test player
        cur.execute(
            "INSERT INTO games (game_id, home_team, away_team, commence_time, sport) "
            "VALUES ('TEST_GAME_INT_001', 'TST', 'OPP', NOW(), 'basketball_nba') ON CONFLICT DO NOTHING"
        )
        cur.execute(
            "INSERT INTO player_props (game_id, bookmaker, player_name, prop_type, outcome, price, point) "
            "VALUES ('TEST_GAME_INT_001', 'draftkings', 'Test Player Integration', 'player_points', 'Over', -110, 19.5)"
        )
    conn.commit()

    resolve_player_ids(conn, slack_webhook_url=None)

    with conn.cursor() as cur:
        cur.execute("SELECT nba_player_id FROM player_name_mappings WHERE odds_api_name = 'Test Player Integration'")
        row = cur.fetchone()
    assert row is not None
    assert row[0] == 99999

    # Cleanup extra rows
    with conn.cursor() as cur:
        cur.execute("DELETE FROM player_props WHERE game_id = 'TEST_GAME_INT_001'")
        cur.execute("DELETE FROM games WHERE game_id = 'TEST_GAME_INT_001'")
        cur.execute("DELETE FROM player_name_mappings WHERE odds_api_name = 'Test Player Integration'")
    conn.commit()
