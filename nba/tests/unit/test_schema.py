# tests/unit/test_schema.py
"""
Schema existence tests.
Requires data-postgres container running:
    docker compose up data-postgres -d
"""
import os
import psycopg2
import pytest


@pytest.fixture
def db_conn():
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname=os.getenv("DATA_DB_NAME", "odds_db"),
        user=os.getenv("DATA_DB_USER", "odds"),
        password=os.getenv("DATA_DB_PASSWORD", "odds_password"),
    )
    yield conn
    conn.close()


def test_raw_api_responses_table_exists(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'raw_api_responses'
    """)
    assert cur.fetchone() is not None


def test_games_table_exists(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'games'
    """)
    assert cur.fetchone() is not None


def test_player_props_table_exists(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'player_props'
    """)
    assert cur.fetchone() is not None


def test_scores_table_has_unique_game_id(db_conn):
    # game_id is now the PRIMARY KEY (migration 002 promoted it from UNIQUE);
    # a PK implies uniqueness, so we check for PRIMARY KEY.
    cur = db_conn.cursor()
    cur.execute("""
        SELECT constraint_name FROM information_schema.table_constraints
        WHERE table_name = 'scores' AND constraint_type = 'PRIMARY KEY'
    """)
    assert cur.fetchone() is not None
