# NBA Odds Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a daily NBA data ingestion pipeline that fetches events, odds, scores, and player props from the Odds-API and stores them in Postgres using Airflow for orchestration.

**Architecture:** Layered DAG approach — an Ingest DAG fetches raw data and stores full JSON responses, a Transform DAG normalizes raw data into structured tables, and a Backfill DAG handles on-demand historical seeding. All services run via Docker Compose locally.

**Tech Stack:** Python 3.11, Apache Airflow 2.9, PostgreSQL 15, Docker Compose, pytest, requests

---

## Task 1: Project Scaffolding

**Files:**
- Create: `.gitignore`
- Create: `.env.example`
- Create: `docker-compose.yml`
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `pytest.ini`

**Step 1: Create `.gitignore`**

```
.env
__pycache__/
*.pyc
*.pyo
.pytest_cache/
.coverage
logs/
```

**Step 2: Create `.env.example`**

```
ODDS_API_KEY=your_odds_api_key_here
DATA_DB_USER=odds
DATA_DB_PASSWORD=odds_password
DATA_DB_NAME=odds_db
DATA_DB_HOST=data-postgres
DATA_DB_PORT=5432
PGADMIN_EMAIL=admin@admin.com
PGADMIN_PASSWORD=admin
```

**Step 3: Copy `.env.example` to `.env` and fill in your real API key**

```bash
cp .env.example .env
# Edit .env with your real ODDS_API_KEY
```

**Step 4: Create `requirements.txt`**

```
apache-airflow==2.9.0
apache-airflow-providers-postgres==5.10.0
requests==2.31.0
psycopg2-binary==2.9.9
python-dotenv==1.0.0
```

**Step 5: Create `requirements-dev.txt`**

```
-r requirements.txt
pytest==8.1.1
pytest-mock==3.14.0
```

**Step 6: Create `pytest.ini`**

```ini
[pytest]
testpaths = tests
pythonpath = .
```

**Step 7: Create `docker-compose.yml`**

```yaml
version: '3.8'

x-airflow-common:
  &airflow-common
  image: apache/airflow:2.9.0
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
    ODDS_API_KEY: ${ODDS_API_KEY}
    DATA_DB_USER: ${DATA_DB_USER:-odds}
    DATA_DB_PASSWORD: ${DATA_DB_PASSWORD:-odds_password}
    DATA_DB_NAME: ${DATA_DB_NAME:-odds_db}
    DATA_DB_HOST: data-postgres
    DATA_DB_PORT: 5432
  volumes:
    - ./dags:/opt/airflow/dags
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
    - ./logs:/opt/airflow/logs
  depends_on:
    airflow-postgres:
      condition: service_healthy
    data-postgres:
      condition: service_healthy

services:
  airflow-postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      timeout: 5s
      retries: 5
    volumes:
      - airflow-postgres-data:/var/lib/postgresql/data

  data-postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: ${DATA_DB_USER:-odds}
      POSTGRES_PASSWORD: ${DATA_DB_PASSWORD:-odds_password}
      POSTGRES_DB: ${DATA_DB_NAME:-odds_db}
    ports:
      - "5433:5432"
    volumes:
      - ./sql/init_schema.sql:/docker-entrypoint-initdb.d/init_schema.sql
      - data-postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${DATA_DB_USER:-odds}"]
      interval: 5s
      timeout: 5s
      retries: 5

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db init &&
        airflow users create \
          --username admin \
          --password admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com
    restart: "no"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  pgadmin:
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: ${PGADMIN_EMAIL:-admin@admin.com}
      PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_PASSWORD:-admin}
    ports:
      - "5050:80"
    depends_on:
      - data-postgres

volumes:
  airflow-postgres-data:
  data-postgres-data:
```

**Step 8: Create `logs/` directory (Airflow requires it)**

```bash
mkdir -p logs
```

**Step 9: Commit**

```bash
git add .gitignore .env.example docker-compose.yml requirements.txt requirements-dev.txt pytest.ini
git commit -m "chore: add project scaffolding and docker-compose"
```

---

## Task 2: Database Schema

**Files:**
- Create: `sql/init_schema.sql`
- Create: `tests/unit/test_schema.py`

**Step 1: Write failing test**

```python
# tests/unit/test_schema.py
import os
import psycopg2
import pytest

# Requires: docker compose up data-postgres -d

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
```

**Step 2: Run test to verify it fails**

```bash
docker compose up data-postgres -d
pytest tests/unit/test_schema.py -v
```

Expected: FAIL — tables don't exist yet.

**Step 3: Create `sql/init_schema.sql`**

```sql
-- Raw layer: preserves full API responses for replayability
CREATE TABLE IF NOT EXISTS raw_api_responses (
    id          SERIAL PRIMARY KEY,
    endpoint    VARCHAR(50) NOT NULL,
    params      JSONB,
    response    JSONB,
    fetched_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    status      VARCHAR(20) NOT NULL DEFAULT 'success'
);

CREATE INDEX IF NOT EXISTS idx_raw_endpoint   ON raw_api_responses(endpoint);
CREATE INDEX IF NOT EXISTS idx_raw_fetched_at ON raw_api_responses(fetched_at);

-- Normalized layer
CREATE TABLE IF NOT EXISTS games (
    game_id         VARCHAR(100) PRIMARY KEY,
    home_team       VARCHAR(100),
    away_team       VARCHAR(100),
    commence_time   TIMESTAMP,
    season          VARCHAR(20),
    sport           VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS odds (
    id              SERIAL PRIMARY KEY,
    game_id         VARCHAR(100) REFERENCES games(game_id),
    bookmaker       VARCHAR(50),
    market_type     VARCHAR(50),
    outcome_name    VARCHAR(100),
    price           NUMERIC(10, 2),
    point           NUMERIC(10, 2),
    last_update     TIMESTAMP,
    fetched_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_odds_game_id   ON odds(game_id);
CREATE INDEX IF NOT EXISTS idx_odds_bookmaker ON odds(bookmaker);

CREATE TABLE IF NOT EXISTS player_props (
    id              SERIAL PRIMARY KEY,
    game_id         VARCHAR(100) REFERENCES games(game_id),
    bookmaker       VARCHAR(50),
    player_name     VARCHAR(100),
    prop_type       VARCHAR(100),
    outcome         VARCHAR(20),
    price           NUMERIC(10, 2),
    point           NUMERIC(10, 2),
    last_update     TIMESTAMP,
    fetched_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_props_game_id ON player_props(game_id);
CREATE INDEX IF NOT EXISTS idx_props_player  ON player_props(player_name);
CREATE INDEX IF NOT EXISTS idx_props_type    ON player_props(prop_type);

CREATE TABLE IF NOT EXISTS scores (
    id              SERIAL PRIMARY KEY,
    game_id         VARCHAR(100) UNIQUE REFERENCES games(game_id),
    home_score      INTEGER,
    away_score      INTEGER,
    completed       BOOLEAN DEFAULT FALSE,
    last_update     TIMESTAMP,
    fetched_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scores_game_id ON scores(game_id);
```

**Step 4: Recreate data-postgres to apply schema**

```bash
docker compose down data-postgres -v
docker compose up data-postgres -d
# Wait ~5 seconds for init to complete
```

**Step 5: Run tests to verify they pass**

```bash
pytest tests/unit/test_schema.py -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add sql/init_schema.sql tests/unit/test_schema.py
git commit -m "feat: add database schema with raw and normalized layers"
```

---

## Task 3: Config Module

**Files:**
- Create: `config/__init__.py`
- Create: `config/settings.py`
- Create: `tests/unit/test_settings.py`

**Step 1: Write failing test**

```python
# tests/unit/test_settings.py

def test_sport_is_nba():
    from config.settings import SPORT
    assert SPORT == "basketball_nba"

def test_markets_is_list_with_player_props():
    from config.settings import MARKETS
    assert isinstance(MARKETS, list)
    assert "player_props" in MARKETS

def test_bookmakers_is_nonempty_list():
    from config.settings import BOOKMAKERS
    assert isinstance(BOOKMAKERS, list)
    assert len(BOOKMAKERS) > 0

def test_regions_is_nonempty_list():
    from config.settings import REGIONS
    assert isinstance(REGIONS, list)
    assert len(REGIONS) > 0
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_settings.py -v
```

Expected: FAIL — module not found.

**Step 3: Create `config/__init__.py`** (empty file)

**Step 4: Create `config/settings.py`**

```python
# config/settings.py

SPORT = "basketball_nba"

REGIONS = ["us"]

# Markets to fetch from Odds-API.
# Add or remove to control API quota usage.
# Options: h2h, spreads, totals, player_props
MARKETS = [
    "h2h",
    "spreads",
    "totals",
    "player_props",
]

# Bookmakers to include.
# Fewer bookmakers = fewer API requests consumed.
BOOKMAKERS = [
    "draftkings",
    "fanduel",
    "betmgm",
]

ODDS_FORMAT = "american"

# How many days back to fetch scores for
SCORES_DAYS_FROM = 3
```

**Step 5: Run tests to verify they pass**

```bash
pytest tests/unit/test_settings.py -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add config/__init__.py config/settings.py tests/unit/test_settings.py
git commit -m "feat: add config settings module"
```

---

## Task 4: DB Client

**Files:**
- Create: `plugins/__init__.py`
- Create: `plugins/db_client.py`
- Create: `tests/unit/test_db_client.py`

**Step 1: Write failing tests**

```python
# tests/unit/test_db_client.py
from unittest.mock import MagicMock, patch


def test_store_raw_response_inserts_row():
    from plugins.db_client import store_raw_response

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
    from plugins.db_client import store_raw_response

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
    with patch("plugins.db_client.psycopg2.connect") as mock_connect:
        with patch.dict("os.environ", {
            "DATA_DB_HOST": "localhost",
            "DATA_DB_PORT": "5433",
            "DATA_DB_NAME": "odds_db",
            "DATA_DB_USER": "odds",
            "DATA_DB_PASSWORD": "odds_password",
        }):
            from plugins.db_client import get_data_db_conn
            get_data_db_conn()
            mock_connect.assert_called_once_with(
                host="localhost",
                port="5433",
                dbname="odds_db",
                user="odds",
                password="odds_password",
            )
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_db_client.py -v
```

Expected: FAIL — module not found.

**Step 3: Create `plugins/__init__.py`** (empty file)

**Step 4: Create `plugins/db_client.py`**

```python
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
```

**Step 5: Run tests to verify they pass**

```bash
pytest tests/unit/test_db_client.py -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add plugins/__init__.py plugins/db_client.py tests/unit/test_db_client.py
git commit -m "feat: add db client with raw response storage"
```

---

## Task 5: Odds API Client

**Files:**
- Create: `plugins/odds_api_client.py`
- Create: `tests/unit/test_odds_api_client.py`

**Step 1: Write failing tests**

```python
# tests/unit/test_odds_api_client.py
import pytest
from unittest.mock import MagicMock, patch


def make_mock_response(json_data, status_code=200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    return mock


def test_fetch_events_calls_correct_url():
    from plugins.odds_api_client import fetch_events
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([{"id": "abc"}])
        result = fetch_events(api_key="test_key", sport="basketball_nba")
        url = mock_get.call_args[0][0]
        assert "basketball_nba/events" in url
        assert result == [{"id": "abc"}]


def test_fetch_odds_sends_markets_and_bookmakers():
    from plugins.odds_api_client import fetch_odds
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([])
        fetch_odds(
            api_key="test_key",
            sport="basketball_nba",
            regions=["us"],
            markets=["h2h", "player_props"],
            bookmakers=["draftkings"],
        )
        params = mock_get.call_args[1]["params"]
        assert params["markets"] == "h2h,player_props"
        assert params["bookmakers"] == "draftkings"


def test_fetch_scores_calls_correct_url_with_days_from():
    from plugins.odds_api_client import fetch_scores
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([])
        fetch_scores(api_key="test_key", sport="basketball_nba", days_from=3)
        url = mock_get.call_args[0][0]
        assert "basketball_nba/scores" in url
        params = mock_get.call_args[1]["params"]
        assert params["daysFrom"] == 3


def test_fetch_raises_on_http_error():
    import requests
    from plugins.odds_api_client import fetch_events
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("401")
        mock_get.return_value = mock_response
        with pytest.raises(requests.HTTPError):
            fetch_events(api_key="bad_key", sport="basketball_nba")
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_odds_api_client.py -v
```

Expected: FAIL — module not found.

**Step 3: Create `plugins/odds_api_client.py`**

```python
# plugins/odds_api_client.py
import requests

BASE_URL = "https://api.the-odds-api.com/v4"


def fetch_events(api_key, sport):
    url = f"{BASE_URL}/sports/{sport}/events"
    response = requests.get(url, params={"apiKey": api_key})
    response.raise_for_status()
    return response.json()


def fetch_odds(api_key, sport, regions, markets, bookmakers, odds_format="american"):
    url = f"{BASE_URL}/sports/{sport}/odds"
    response = requests.get(url, params={
        "apiKey": api_key,
        "regions": ",".join(regions),
        "markets": ",".join(markets),
        "bookmakers": ",".join(bookmakers),
        "oddsFormat": odds_format,
    })
    response.raise_for_status()
    return response.json()


def fetch_scores(api_key, sport, days_from=3):
    url = f"{BASE_URL}/sports/{sport}/scores"
    response = requests.get(url, params={
        "apiKey": api_key,
        "daysFrom": days_from,
    })
    response.raise_for_status()
    return response.json()
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_odds_api_client.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add plugins/odds_api_client.py tests/unit/test_odds_api_client.py
git commit -m "feat: add Odds-API client for events, odds, and scores"
```

---

## Task 6: Transformers

**Files:**
- Create: `plugins/transformers/__init__.py`
- Create: `plugins/transformers/events.py`
- Create: `plugins/transformers/odds.py`
- Create: `plugins/transformers/scores.py`
- Create: `plugins/transformers/player_props.py`
- Create: `tests/unit/transformers/__init__.py`
- Create: `tests/unit/transformers/test_events.py`
- Create: `tests/unit/transformers/test_odds.py`
- Create: `tests/unit/transformers/test_scores.py`
- Create: `tests/unit/transformers/test_player_props.py`

**Step 1: Write failing tests for events transformer**

```python
# tests/unit/transformers/test_events.py
from unittest.mock import MagicMock

SAMPLE_EVENTS = [
    {
        "id": "abc123",
        "sport_key": "basketball_nba",
        "commence_time": "2024-01-15T00:00:00Z",
        "home_team": "Los Angeles Lakers",
        "away_team": "Boston Celtics",
    }
]


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_transform_events_upserts_game():
    from plugins.transformers.events import transform_events
    mock_conn, mock_cursor = _make_mock_conn()

    transform_events(conn=mock_conn, raw_events=SAMPLE_EVENTS)

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO games" in sql
    assert "ON CONFLICT" in sql
    mock_conn.commit.assert_called_once()


def test_transform_events_skips_empty_list():
    from plugins.transformers.events import transform_events
    mock_conn, mock_cursor = _make_mock_conn()

    transform_events(conn=mock_conn, raw_events=[])

    mock_cursor.execute.assert_not_called()
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/transformers/test_events.py -v
```

Expected: FAIL

**Step 3: Create `plugins/transformers/__init__.py`** (empty file)

**Step 4: Create `plugins/transformers/events.py`**

```python
# plugins/transformers/events.py

def transform_events(conn, raw_events):
    if not raw_events:
        return
    with conn.cursor() as cur:
        for event in raw_events:
            cur.execute(
                """
                INSERT INTO games (game_id, home_team, away_team, commence_time, sport)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET
                    home_team     = EXCLUDED.home_team,
                    away_team     = EXCLUDED.away_team,
                    commence_time = EXCLUDED.commence_time
                """,
                (
                    event["id"],
                    event["home_team"],
                    event["away_team"],
                    event["commence_time"],
                    event["sport_key"],
                ),
            )
    conn.commit()
```

**Step 5: Run test to verify it passes**

```bash
pytest tests/unit/transformers/test_events.py -v
```

Expected: PASS

**Step 6: Write failing tests for odds transformer**

```python
# tests/unit/transformers/test_odds.py
from unittest.mock import MagicMock

SAMPLE_ODDS = [
    {
        "id": "abc123",
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {
                        "key": "h2h",
                        "last_update": "2024-01-15T00:00:00Z",
                        "outcomes": [
                            {"name": "Los Angeles Lakers", "price": -110},
                            {"name": "Boston Celtics", "price": -110},
                        ],
                    }
                ],
            }
        ],
    }
]


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_transform_odds_inserts_one_row_per_outcome():
    from plugins.transformers.odds import transform_odds
    mock_conn, mock_cursor = _make_mock_conn()

    transform_odds(conn=mock_conn, raw_odds=SAMPLE_ODDS)

    assert mock_cursor.execute.call_count == 2  # 2 outcomes
    mock_conn.commit.assert_called_once()


def test_transform_odds_skips_player_props_market():
    from plugins.transformers.odds import transform_odds
    mock_conn, mock_cursor = _make_mock_conn()

    raw = [{"id": "x", "bookmakers": [{"key": "dk", "markets": [
        {"key": "player_points", "outcomes": [{"name": "Over", "price": -115}]}
    ]}]}]
    transform_odds(conn=mock_conn, raw_odds=raw)

    mock_cursor.execute.assert_not_called()


def test_transform_odds_empty():
    from plugins.transformers.odds import transform_odds
    mock_conn, mock_cursor = _make_mock_conn()

    transform_odds(conn=mock_conn, raw_odds=[])

    mock_cursor.execute.assert_not_called()
```

**Step 7: Create `plugins/transformers/odds.py`**

```python
# plugins/transformers/odds.py

def transform_odds(conn, raw_odds):
    if not raw_odds:
        return
    with conn.cursor() as cur:
        for game in raw_odds:
            game_id = game["id"]
            for bookmaker in game.get("bookmakers", []):
                bookmaker_key = bookmaker["key"]
                for market in bookmaker.get("markets", []):
                    market_key = market["key"]
                    if market_key.startswith("player_"):
                        continue  # player props handled by separate transformer
                    last_update = market.get("last_update")
                    for outcome in market.get("outcomes", []):
                        cur.execute(
                            """
                            INSERT INTO odds
                                (game_id, bookmaker, market_type, outcome_name, price, point, last_update)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                game_id,
                                bookmaker_key,
                                market_key,
                                outcome["name"],
                                outcome.get("price"),
                                outcome.get("point"),
                                last_update,
                            ),
                        )
    conn.commit()
```

**Step 8: Write failing tests for scores transformer**

```python
# tests/unit/transformers/test_scores.py
from unittest.mock import MagicMock

SAMPLE_SCORES = [
    {
        "id": "abc123",
        "completed": True,
        "last_update": "2024-01-15T03:00:00Z",
        "home_team": "Los Angeles Lakers",
        "away_team": "Boston Celtics",
        "scores": [
            {"name": "Los Angeles Lakers", "score": "110"},
            {"name": "Boston Celtics", "score": "105"},
        ],
    }
]


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_transform_scores_upserts_row():
    from plugins.transformers.scores import transform_scores
    mock_conn, mock_cursor = _make_mock_conn()

    transform_scores(conn=mock_conn, raw_scores=SAMPLE_SCORES)

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO scores" in sql
    assert "ON CONFLICT" in sql
    mock_conn.commit.assert_called_once()


def test_transform_scores_handles_null_scores():
    from plugins.transformers.scores import transform_scores
    mock_conn, mock_cursor = _make_mock_conn()

    raw = [{"id": "abc", "completed": False, "last_update": None,
            "home_team": "Lakers", "away_team": "Celtics", "scores": None}]
    transform_scores(conn=mock_conn, raw_scores=raw)

    args = mock_cursor.execute.call_args[0][1]
    assert args[1] is None  # home_score
    assert args[2] is None  # away_score
```

**Step 9: Create `plugins/transformers/scores.py`**

```python
# plugins/transformers/scores.py

def transform_scores(conn, raw_scores):
    if not raw_scores:
        return
    with conn.cursor() as cur:
        for game in raw_scores:
            scores = game.get("scores") or []
            home_score, away_score = None, None
            for s in scores:
                if s["name"] == game["home_team"]:
                    home_score = int(s["score"]) if s.get("score") else None
                elif s["name"] == game["away_team"]:
                    away_score = int(s["score"]) if s.get("score") else None
            cur.execute(
                """
                INSERT INTO scores (game_id, home_score, away_score, completed, last_update)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET
                    home_score   = EXCLUDED.home_score,
                    away_score   = EXCLUDED.away_score,
                    completed    = EXCLUDED.completed,
                    last_update  = EXCLUDED.last_update
                """,
                (
                    game["id"],
                    home_score,
                    away_score,
                    game.get("completed", False),
                    game.get("last_update"),
                ),
            )
    conn.commit()
```

**Step 10: Write failing tests for player_props transformer**

```python
# tests/unit/transformers/test_player_props.py
from unittest.mock import MagicMock

SAMPLE_PLAYER_PROPS = [
    {
        "id": "abc123",
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


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_transform_player_props_inserts_one_row_per_outcome():
    from plugins.transformers.player_props import transform_player_props
    mock_conn, mock_cursor = _make_mock_conn()

    transform_player_props(conn=mock_conn, raw_odds=SAMPLE_PLAYER_PROPS)

    assert mock_cursor.execute.call_count == 2
    args = mock_cursor.execute.call_args_list[0][0][1]
    assert args[2] == "LeBron James"   # player_name
    assert args[3] == "player_points"  # prop_type
    assert args[4] == "Over"           # outcome
    assert args[5] == -115             # price
    assert args[6] == 24.5             # point


def test_transform_player_props_skips_non_prop_markets():
    from plugins.transformers.player_props import transform_player_props
    mock_conn, mock_cursor = _make_mock_conn()

    raw = [{"id": "x", "bookmakers": [{"key": "dk", "markets": [
        {"key": "h2h", "outcomes": [{"name": "Lakers"}]}
    ]}]}]
    transform_player_props(conn=mock_conn, raw_odds=raw)

    mock_cursor.execute.assert_not_called()
```

**Step 11: Create `plugins/transformers/player_props.py`**

```python
# plugins/transformers/player_props.py

def transform_player_props(conn, raw_odds):
    if not raw_odds:
        return
    with conn.cursor() as cur:
        for game in raw_odds:
            game_id = game["id"]
            for bookmaker in game.get("bookmakers", []):
                bookmaker_key = bookmaker["key"]
                for market in bookmaker.get("markets", []):
                    market_key = market["key"]
                    if not market_key.startswith("player_"):
                        continue
                    last_update = market.get("last_update")
                    for outcome in market.get("outcomes", []):
                        cur.execute(
                            """
                            INSERT INTO player_props
                                (game_id, bookmaker, player_name, prop_type, outcome, price, point, last_update)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                game_id,
                                bookmaker_key,
                                outcome.get("description"),  # player name lives in description
                                market_key,
                                outcome["name"],             # Over / Under
                                outcome.get("price"),
                                outcome.get("point"),
                                last_update,
                            ),
                        )
    conn.commit()
```

**Step 12: Run all transformer tests**

```bash
pytest tests/unit/transformers/ -v
```

Expected: PASS all 8 tests.

**Step 13: Commit**

```bash
git add plugins/transformers/ tests/unit/transformers/
git commit -m "feat: add transformers for events, odds, scores, and player props"
```

---

## Task 7: Ingest DAG

**Files:**
- Create: `dags/ingest_dag.py`
- Create: `tests/unit/test_ingest_dag.py`

**Step 1: Write failing test**

```python
# tests/unit/test_ingest_dag.py

def test_ingest_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_ingest" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_ingest_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_ingest"]
    task_ids = [t.task_id for t in dag.tasks]
    assert "fetch_events" in task_ids
    assert "fetch_odds" in task_ids
    assert "fetch_scores" in task_ids
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_ingest_dag.py -v
```

Expected: FAIL — DAG not found.

**Step 3: Create `dags/ingest_dag.py`**

```python
# dags/ingest_dag.py
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from config.settings import SPORT, REGIONS, MARKETS, BOOKMAKERS, ODDS_FORMAT, SCORES_DAYS_FROM
from plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores
from plugins.db_client import get_data_db_conn, store_raw_response


def _fetch_and_store(endpoint_name, fetch_fn, fetch_kwargs):
    api_key = os.environ["ODDS_API_KEY"]
    conn = get_data_db_conn()
    try:
        data = fetch_fn(api_key=api_key, **fetch_kwargs)
        store_raw_response(conn, endpoint=endpoint_name, params=fetch_kwargs, response=data, status="success")
    except Exception:
        store_raw_response(conn, endpoint=endpoint_name, params=fetch_kwargs, response=None, status="error")
        raise
    finally:
        conn.close()


def fetch_events_task():
    _fetch_and_store("events", fetch_events, {"sport": SPORT})


def fetch_odds_task():
    _fetch_and_store("odds", fetch_odds, {
        "sport": SPORT,
        "regions": REGIONS,
        "markets": MARKETS,
        "bookmakers": BOOKMAKERS,
        "odds_format": ODDS_FORMAT,
    })


def fetch_scores_task():
    _fetch_and_store("scores", fetch_scores, {"sport": SPORT, "days_from": SCORES_DAYS_FROM})


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_ingest",
    default_args=default_args,
    description="Fetch NBA data from Odds-API and store raw JSON",
    schedule_interval="0 8,20 * * *",  # 8am and 8pm daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "ingest"],
) as dag:
    t_events = PythonOperator(task_id="fetch_events", python_callable=fetch_events_task)
    t_odds   = PythonOperator(task_id="fetch_odds",   python_callable=fetch_odds_task)
    t_scores = PythonOperator(task_id="fetch_scores", python_callable=fetch_scores_task)

    # All three tasks run independently — no inter-task dependencies
    [t_events, t_odds, t_scores]
```

**Step 4: Run test to verify it passes**

```bash
pytest tests/unit/test_ingest_dag.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add dags/ingest_dag.py tests/unit/test_ingest_dag.py
git commit -m "feat: add nba_ingest DAG"
```

---

## Task 8: Transform DAG

**Files:**
- Create: `dags/transform_dag.py`
- Create: `tests/unit/test_transform_dag.py`

**Step 1: Write failing test**

```python
# tests/unit/test_transform_dag.py

def test_transform_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_transform" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_transform_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_transform"]
    task_ids = [t.task_id for t in dag.tasks]
    assert "transform_events"      in task_ids
    assert "transform_odds"        in task_ids
    assert "transform_scores"      in task_ids
    assert "transform_player_props" in task_ids
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_transform_dag.py -v
```

Expected: FAIL

**Step 3: Create `dags/transform_dag.py`**

```python
# dags/transform_dag.py
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, "/opt/airflow")

from plugins.db_client import get_data_db_conn
from plugins.transformers.events import transform_events
from plugins.transformers.odds import transform_odds
from plugins.transformers.scores import transform_scores
from plugins.transformers.player_props import transform_player_props


def _get_latest_raw(conn, endpoint):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT response FROM raw_api_responses
            WHERE endpoint = %s AND status = 'success'
            ORDER BY fetched_at DESC LIMIT 1
            """,
            (endpoint,),
        )
        row = cur.fetchone()
    return row[0] if row else []


def run_transform_events():
    conn = get_data_db_conn()
    try:
        transform_events(conn, _get_latest_raw(conn, "events"))
    finally:
        conn.close()


def run_transform_odds():
    conn = get_data_db_conn()
    try:
        transform_odds(conn, _get_latest_raw(conn, "odds"))
    finally:
        conn.close()


def run_transform_scores():
    conn = get_data_db_conn()
    try:
        transform_scores(conn, _get_latest_raw(conn, "scores"))
    finally:
        conn.close()


def run_transform_player_props():
    conn = get_data_db_conn()
    try:
        transform_player_props(conn, _get_latest_raw(conn, "odds"))
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nba_transform",
    default_args=default_args,
    description="Normalize raw NBA Odds-API data into structured tables",
    schedule_interval="15 8,20 * * *",  # 15 min after ingest
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "transform"],
) as dag:
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="nba_ingest",
        external_task_id=None,  # wait for full DAG completion
        timeout=600,
        mode="reschedule",
    )

    t_events = PythonOperator(task_id="transform_events",       python_callable=run_transform_events)
    t_odds   = PythonOperator(task_id="transform_odds",         python_callable=run_transform_odds)
    t_scores = PythonOperator(task_id="transform_scores",       python_callable=run_transform_scores)
    t_props  = PythonOperator(task_id="transform_player_props", python_callable=run_transform_player_props)

    wait_for_ingest >> [t_events, t_odds, t_scores]
    t_odds >> t_props
```

**Step 4: Run test to verify it passes**

```bash
pytest tests/unit/test_transform_dag.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add dags/transform_dag.py tests/unit/test_transform_dag.py
git commit -m "feat: add nba_transform DAG"
```

---

## Task 9: Backfill DAG

**Files:**
- Create: `dags/backfill_dag.py`
- Create: `tests/unit/test_backfill_dag.py`

**Step 1: Write failing test**

```python
# tests/unit/test_backfill_dag.py

def test_backfill_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "nba_backfill" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_backfill_dag_has_no_schedule():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="dags/", include_examples=False).dags["nba_backfill"]
    assert dag.schedule_interval is None
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_backfill_dag.py -v
```

Expected: FAIL

**Step 3: Create `dags/backfill_dag.py`**

```python
# dags/backfill_dag.py
import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from config.settings import SPORT, REGIONS, MARKETS, BOOKMAKERS, ODDS_FORMAT
from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores
from plugins.transformers.events import transform_events
from plugins.transformers.odds import transform_odds
from plugins.transformers.scores import transform_scores
from plugins.transformers.player_props import transform_player_props

BACKFILL_SLEEP_SECONDS = 2  # rate limit between API calls


def run_backfill(**context):
    api_key = os.environ["ODDS_API_KEY"]
    params = context["params"]
    date_from = params.get("date_from")
    date_to = params.get("date_to")

    event_params = {"sport": SPORT}
    if date_from:
        event_params["commenceTimeFrom"] = f"{date_from}T00:00:00Z"
    if date_to:
        event_params["commenceTimeTo"] = f"{date_to}T23:59:59Z"

    conn = get_data_db_conn()
    try:
        events = fetch_events(api_key=api_key, sport=SPORT)
        store_raw_response(conn, "events", event_params, events)
        transform_events(conn, events)
        time.sleep(BACKFILL_SLEEP_SECONDS)

        odds = fetch_odds(api_key=api_key, sport=SPORT, regions=REGIONS,
                          markets=MARKETS, bookmakers=BOOKMAKERS)
        store_raw_response(conn, "odds", {"sport": SPORT}, odds)
        transform_odds(conn, odds)
        transform_player_props(conn, odds)
        time.sleep(BACKFILL_SLEEP_SECONDS)

        scores = fetch_scores(api_key=api_key, sport=SPORT, days_from=30)
        store_raw_response(conn, "scores", {"sport": SPORT, "days_from": 30}, scores)
        transform_scores(conn, scores)
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nba_backfill",
    default_args=default_args,
    description="On-demand historical NBA data backfill",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "backfill"],
    params={
        "date_from": Param(None, type=["null", "string"], description="Start date YYYY-MM-DD"),
        "date_to":   Param(None, type=["null", "string"], description="End date YYYY-MM-DD"),
    },
) as dag:
    PythonOperator(task_id="run_backfill", python_callable=run_backfill)
```

**Step 4: Run test to verify it passes**

```bash
pytest tests/unit/test_backfill_dag.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add dags/backfill_dag.py tests/unit/test_backfill_dag.py
git commit -m "feat: add nba_backfill DAG for on-demand historical seeding"
```

---

## Task 10: Integration Tests

**Files:**
- Create: `tests/integration/test_ingest_to_raw.py`
- Create: `tests/integration/test_transform_to_normalized.py`

> Requires `docker compose up data-postgres -d` before running.

**Step 1: Create `tests/integration/test_ingest_to_raw.py`**

```python
# tests/integration/test_ingest_to_raw.py
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
    from plugins.db_client import store_raw_response

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
```

**Step 2: Create `tests/integration/test_transform_to_normalized.py`**

```python
# tests/integration/test_transform_to_normalized.py
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


@pytest.fixture
def db_conn():
    conn = psycopg2.connect(**CONN_PARAMS)
    yield conn
    conn.rollback()
    conn.close()


def test_transform_events_writes_to_games_table(db_conn):
    from plugins.transformers.events import transform_events

    transform_events(conn=db_conn, raw_events=SAMPLE_EVENTS)

    cur = db_conn.cursor()
    cur.execute("SELECT game_id FROM games WHERE game_id = 'intg_game_001'")
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "intg_game_001"


def test_transform_events_is_idempotent(db_conn):
    from plugins.transformers.events import transform_events

    transform_events(conn=db_conn, raw_events=SAMPLE_EVENTS)
    transform_events(conn=db_conn, raw_events=SAMPLE_EVENTS)

    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM games WHERE game_id = 'intg_game_001'")
    assert cur.fetchone()[0] == 1
```

**Step 3: Run integration tests**

```bash
docker compose up data-postgres -d
pytest tests/integration/ -v
```

Expected: PASS

**Step 4: Commit**

```bash
git add tests/integration/
git commit -m "feat: add integration tests for ingest and transform"
```

---

## Task 11: Smoke Test — Full Stack

**Step 1: Start the full stack**

```bash
docker compose up -d
```

Wait ~30 seconds for Airflow to initialize.

**Step 2: Verify DAGs load in Airflow UI**

Open `http://localhost:8080` (admin / admin). Confirm all three DAGs appear with no import errors:
- `nba_ingest`
- `nba_transform`
- `nba_backfill`

**Step 3: Trigger `nba_ingest` manually**

Click `nba_ingest` → Trigger DAG. Confirm `fetch_events`, `fetch_odds`, `fetch_scores` all succeed.

**Step 4: Trigger `nba_transform` manually**

Click `nba_transform` → Trigger DAG. Confirm all transform tasks succeed.

**Step 5: Verify data in pgAdmin**

Open `http://localhost:5050` (admin@admin.com / admin). Connect to `data-postgres` (host: `data-postgres`, port: `5432`, db: `odds_db`). Spot-check:
- `raw_api_responses` — rows for events, odds, scores
- `games` — NBA game rows
- `player_props` — prop rows with player names and lines

**Step 6: Write README and commit**

```markdown
# Odds Pipeline

NBA betting data ingestion pipeline using Airflow, Odds-API, and Postgres.

## Setup

1. Copy `.env.example` to `.env` and fill in your `ODDS_API_KEY`
2. `docker compose up -d`
3. Airflow UI: http://localhost:8080 (admin / admin)
4. pgAdmin: http://localhost:5050 (admin@admin.com / admin)

## DAGs

- **nba_ingest** — fetches events, odds, scores from Odds-API (8am + 8pm daily)
- **nba_transform** — normalizes raw data into structured tables (15 min after ingest)
- **nba_backfill** — on-demand historical seeding (manual trigger only)

## Running Tests

```bash
pip install -r requirements-dev.txt
pytest tests/unit/ -v

# Integration tests (requires data-postgres running)
docker compose up data-postgres -d
pytest tests/integration/ -v
```

```bash
git add README.md
git commit -m "docs: add README with setup and usage instructions"
```
