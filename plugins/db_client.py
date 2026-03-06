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
