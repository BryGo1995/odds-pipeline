# tests/integration/test_ingest_to_raw.py
"""
Integration tests for raw data storage.
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


@pytest.fixture
def db_conn():
    conn = psycopg2.connect(**CONN_PARAMS)
    yield conn
    conn.rollback()
    conn.close()


def test_store_and_retrieve_raw_response(db_conn):
    from shared.plugins.db_client import store_raw_response

    store_raw_response(
        conn=db_conn,
        endpoint="events",
        params={"sport": "basketball_nba"},
        response=[{"id": "intg_raw_001"}],
        status="success",
    )

    cur = db_conn.cursor()
    cur.execute(
        "SELECT endpoint, status FROM raw_api_responses "
        "WHERE endpoint = 'events' ORDER BY fetched_at DESC LIMIT 1"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "events"
    assert row[1] == "success"


def test_store_error_status(db_conn):
    from shared.plugins.db_client import store_raw_response

    store_raw_response(
        conn=db_conn,
        endpoint="odds",
        params={},
        response=None,
        status="error",
    )

    cur = db_conn.cursor()
    cur.execute(
        "SELECT status FROM raw_api_responses "
        "WHERE endpoint = 'odds' ORDER BY fetched_at DESC LIMIT 1"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "error"
