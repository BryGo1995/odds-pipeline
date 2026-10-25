# plugins/db_client.py
import json
import os

import psycopg2


def get_data_db_conn():
    return psycopg2.connect(
        host=os.environ["DATA_DB_HOST"],
        port=os.environ["DATA_DB_PORT"],
        dbname=os.environ["DATA_DB_NAME"],
        user=os.environ["DATA_DB_USER"],
        password=os.environ["DATA_DB_PASSWORD"],
    )


def store_raw_response(conn, endpoint, params, response, status="success"):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_api_responses (endpoint, params, response, status)
            VALUES (%s, %s, %s, %s)
            """,
            (endpoint, json.dumps(params), json.dumps(response), status),
        )
    conn.commit()


def prune_raw_responses(conn, endpoint_prefix, retention_days):
    """
    Delete raw responses for endpoints starting with endpoint_prefix that are
    older than retention_days. The newest successful response per endpoint is
    always kept, because transforms read their input back from this table
    (e.g. _get_latest_raw) and an off-season or outage must not leave them
    with nothing. Returns the number of rows deleted. See #41.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM raw_api_responses
            WHERE starts_with(endpoint, %(prefix)s)
              AND fetched_at < LOCALTIMESTAMP - make_interval(days => %(days)s)
              AND id NOT IN (
                  SELECT DISTINCT ON (endpoint) id
                  FROM raw_api_responses
                  WHERE starts_with(endpoint, %(prefix)s) AND status = 'success'
                  ORDER BY endpoint, fetched_at DESC, id DESC
              )
            """,
            {"prefix": endpoint_prefix, "days": retention_days},
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted
