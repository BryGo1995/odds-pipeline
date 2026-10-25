# tests/unit/test_db_client.py
from unittest.mock import MagicMock, patch


def test_store_raw_response_inserts_row():
    from shared.plugins.db_client import store_raw_response

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    store_raw_response(
        conn=mock_conn,
        endpoint="events",
        params={"sport": "basketball_nba"},
        response=[{"id": "abc123"}],
        status="success",
    )

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO raw_api_responses" in sql
    mock_conn.commit.assert_called_once()


def test_store_raw_response_records_error_status():
    from shared.plugins.db_client import store_raw_response

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    store_raw_response(
        conn=mock_conn,
        endpoint="odds",
        params={},
        response=None,
        status="error",
    )

    args = mock_cursor.execute.call_args[0][1]
    assert args[3] == "error"


def test_get_data_db_conn_uses_env_vars():
    with patch("shared.plugins.db_client.psycopg2.connect") as mock_connect:
        with patch.dict("os.environ", {
            "DATA_DB_HOST": "localhost",
            "DATA_DB_PORT": "5433",
            "DATA_DB_NAME": "odds_db",
            "DATA_DB_USER": "odds",
            "DATA_DB_PASSWORD": "odds_password",
        }):
            from shared.plugins.db_client import get_data_db_conn
            get_data_db_conn()
            mock_connect.assert_called_once_with(
                host="localhost",
                port="5433",
                dbname="odds_db",
                user="odds",
                password="odds_password",
            )


def _mock_conn(rowcount=0):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = rowcount
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_prune_raw_responses_scopes_to_prefix_and_age():
    from shared.plugins.db_client import prune_raw_responses
    mock_conn, mock_cursor = _mock_conn(rowcount=42)

    deleted = prune_raw_responses(mock_conn, "nba_api/", 14)

    assert deleted == 42
    sql, params = mock_cursor.execute.call_args[0]
    sql = " ".join(sql.split())
    assert sql.startswith("DELETE FROM raw_api_responses")
    assert "starts_with(endpoint, %(prefix)s)" in sql  # not LIKE: '_' is a wildcard
    assert "make_interval(days => %(days)s)" in sql
    assert params == {"prefix": "nba_api/", "days": 14}
    mock_conn.commit.assert_called_once()


def test_prune_raw_responses_keeps_newest_success_per_endpoint():
    from shared.plugins.db_client import prune_raw_responses
    mock_conn, mock_cursor = _mock_conn()

    prune_raw_responses(mock_conn, "nba_api/", 14)

    sql = " ".join(mock_cursor.execute.call_args[0][0].split())
    keep = sql.split("NOT IN", 1)[1]
    assert "DISTINCT ON (endpoint) id" in keep
    assert "status = 'success'" in keep
    assert "ORDER BY endpoint, fetched_at DESC" in keep
