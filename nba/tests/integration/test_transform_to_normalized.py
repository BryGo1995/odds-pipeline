# tests/integration/test_transform_to_normalized.py
"""
Integration tests for normalized data transformations.
Requires data-postgres container:
    docker compose up data-postgres -d
"""
import os
import psycopg2
import pytest

CONN_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "dbname": os.getenv("DATA_DB_NAME", "odds_db"),
    "user": os.getenv("DATA_DB_USER", "odds"),
    "password": os.getenv("DATA_DB_PASSWORD", "odds_password"),
}

SAMPLE_EVENTS = [
    {
        "id": "intg_game_001",
        "sport_key": "basketball_nba",
        "commence_time": "2024-01-15T00:00:00Z",
        "home_team": "Lakers",
        "away_team": "Celtics",
    }
]

SAMPLE_PLAYER_PROPS = [
    {
        "id": "intg_game_001",
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {
                        "key": "player_points",
                        "last_update": "2024-01-15T00:00:00Z",
                        "outcomes": [
                            {"name": "Over",  "description": "LeBron James", "price": -115, "point": 24.5},
                            {"name": "Under", "description": "LeBron James", "price": -105, "point": 24.5},
                        ],
                    }
                ],
            }
        ],
    }
]


@pytest.fixture
def db_conn():
    conn = psycopg2.connect(**CONN_PARAMS)
    yield conn
    conn.rollback()
    conn.close()


def test_transform_events_writes_to_games_table(db_conn):
    from shared.plugins.transformers.events import transform_events

    transform_events(conn=db_conn, raw_events=SAMPLE_EVENTS)

    cur = db_conn.cursor()
    cur.execute("SELECT game_id, home_team FROM games WHERE game_id = 'intg_game_001'")
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "intg_game_001"
    assert row[1] == "Lakers"


def test_transform_events_is_idempotent(db_conn):
    from shared.plugins.transformers.events import transform_events

    transform_events(conn=db_conn, raw_events=SAMPLE_EVENTS)
    transform_events(conn=db_conn, raw_events=SAMPLE_EVENTS)

    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM games WHERE game_id = 'intg_game_001'")
    assert cur.fetchone()[0] == 1


def test_transform_player_props_writes_to_table(db_conn):
    from shared.plugins.transformers.events import transform_events
    from nba.plugins.transformers.player_props import transform_player_props

    # game must exist first (FK constraint)
    transform_events(conn=db_conn, raw_events=SAMPLE_EVENTS)
    transform_player_props(conn=db_conn, raw_odds=SAMPLE_PLAYER_PROPS)

    cur = db_conn.cursor()
    cur.execute(
        "SELECT player_name, prop_type, point FROM player_props "
        "WHERE game_id = 'intg_game_001' AND player_name = 'LeBron James'"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "LeBron James"
    assert row[1] == "player_points"
    assert float(row[2]) == 24.5
