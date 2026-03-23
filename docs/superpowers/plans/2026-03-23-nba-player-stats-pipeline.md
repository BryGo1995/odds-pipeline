# NBA Player Stats Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pipeline that ingests NBA player and team stats from nba_api into Postgres to support feature engineering for player prop predictions.

**Architecture:** Three Airflow DAGs (ingest, transform, backfill) follow the existing medallion pattern — raw JSON stored in `raw_api_responses`, then normalized into five new tables (`players`, `player_game_logs`, `team_game_logs`, `team_season_stats`, `player_name_mappings`). A fuzzy-matching entity resolution step links Odds API player names to nba_api player IDs. Two existing tables (`games`, `player_props`) gain new FK columns populated by the transform DAG.

**Tech Stack:** Python 3.11, Apache Airflow 2.9, Postgres 15, psycopg2, nba_api, rapidfuzz, unicodedata (stdlib)

**Note on test helpers:** Each transformer test module defines its own `_make_mock_conn()` helper, following the existing pattern in the codebase (`test_player_props.py`). A future refactor could promote this to a shared `conftest.py` fixture, but for now keep it consistent with the existing style.

**Spec:** `docs/superpowers/specs/2026-03-23-nba-player-stats-design.md`

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `sql/migrations/001_add_player_stats_tables.sql` | All new tables + ALTER TABLE for existing ones |
| Modify | `sql/init_schema.sql` | Add new tables + columns for fresh deployments |
| Modify | `requirements.txt` | Add nba_api, rapidfuzz |
| Create | `plugins/nba_api_client.py` | Wraps nba_api endpoints; returns lists of dicts; handles rate-limit retry |
| Create | `plugins/transformers/players.py` | Upserts player dimension from raw response |
| Create | `plugins/transformers/player_game_logs.py` | Inserts per-game player stats |
| Create | `plugins/transformers/team_game_logs.py` | Inserts per-game team stats |
| Create | `plugins/transformers/team_season_stats.py` | Upserts season-level team advanced stats |
| Create | `plugins/transformers/player_name_resolution.py` | Fuzzy name matching + player_props FK population |
| Create | `plugins/transformers/game_id_linker.py` | Links games.nba_game_id by date+team matching |
| Create | `dags/nba_player_stats_ingest_dag.py` | Daily 6am MT ingest DAG (4 parallel tasks) |
| Create | `dags/nba_player_stats_transform_dag.py` | Daily 6:20am MT transform DAG (sequential + parallel) |
| Create | `dags/nba_player_stats_backfill_dag.py` | Manual-trigger historical seed DAG |
| Create | `tests/unit/test_nba_api_client.py` | Unit tests for nba_api_client |
| Create | `tests/unit/transformers/test_players.py` | Unit tests for players transformer |
| Create | `tests/unit/transformers/test_player_game_logs.py` | Unit tests for player_game_logs transformer |
| Create | `tests/unit/transformers/test_team_game_logs.py` | Unit tests for team_game_logs transformer |
| Create | `tests/unit/transformers/test_team_season_stats.py` | Unit tests for team_season_stats transformer |
| Create | `tests/unit/test_player_name_resolution.py` | Unit tests for name normalization + fuzzy matching |
| Create | `tests/unit/test_game_id_linker.py` | Unit tests for game ID linking |
| Create | `tests/integration/test_player_stats_pipeline.py` | Integration test: ingest → transform → verify rows |

---

## Task 1: Add Dependencies and Migration SQL

**Files:**
- Modify: `requirements.txt`
- Create: `sql/migrations/001_add_player_stats_tables.sql`
- Modify: `sql/init_schema.sql`

- [ ] **Step 1: Add nba_api and rapidfuzz to requirements.txt**

```
apache-airflow==2.9.0
apache-airflow-providers-postgres==5.10.0
requests==2.31.0
psycopg2-binary==2.9.9
python-dotenv==1.0.0
nba_api==1.4.1
rapidfuzz==3.6.1
```

- [ ] **Step 2: Create the migration file**

Create `sql/migrations/001_add_player_stats_tables.sql`:

```sql
-- Migration 001: Add NBA player stats tables
-- Idempotent: safe to run multiple times

CREATE TABLE IF NOT EXISTS players (
    player_id         INT PRIMARY KEY,
    full_name         TEXT NOT NULL,
    position          TEXT,
    team_id           INT,
    team_abbreviation TEXT,
    is_active         BOOLEAN,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_players_full_name ON players (full_name);

CREATE TABLE IF NOT EXISTS player_game_logs (
    id               SERIAL PRIMARY KEY,
    player_id        INT REFERENCES players(player_id),
    nba_game_id      TEXT NOT NULL,
    season           TEXT NOT NULL,
    game_date        DATE NOT NULL,
    matchup          TEXT,
    team_id          INT,
    opponent_team_id INT,
    wl               TEXT,
    min              FLOAT,
    fga              INT,
    fta              INT,
    usg_pct          FLOAT,
    pts              INT,
    reb              INT,
    ast              INT,
    blk              INT,
    stl              INT,
    plus_minus       INT,
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, nba_game_id)
);

CREATE INDEX IF NOT EXISTS idx_pgl_player_id   ON player_game_logs (player_id);
CREATE INDEX IF NOT EXISTS idx_pgl_game_date   ON player_game_logs (game_date);
CREATE INDEX IF NOT EXISTS idx_pgl_season      ON player_game_logs (season);

CREATE TABLE IF NOT EXISTS team_game_logs (
    id                SERIAL PRIMARY KEY,
    team_id           INT NOT NULL,
    team_abbreviation TEXT,
    nba_game_id       TEXT NOT NULL,
    season            TEXT NOT NULL,
    game_date         DATE NOT NULL,
    matchup           TEXT,
    wl                TEXT,
    pts               INT,
    plus_minus        INT,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, nba_game_id)
);

CREATE INDEX IF NOT EXISTS idx_tgl_team_id   ON team_game_logs (team_id);
CREATE INDEX IF NOT EXISTS idx_tgl_game_date ON team_game_logs (game_date);

CREATE TABLE IF NOT EXISTS team_season_stats (
    id                SERIAL PRIMARY KEY,
    team_id           INT NOT NULL,
    team_abbreviation TEXT,
    season            TEXT NOT NULL,
    pace              FLOAT,
    off_rating        FLOAT,
    def_rating        FLOAT,
    opp_pts_paint_pg  FLOAT,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, season)
);

CREATE TABLE IF NOT EXISTS player_name_mappings (
    odds_api_name TEXT PRIMARY KEY,
    nba_player_id INT REFERENCES players(player_id),
    confidence    FLOAT,
    verified      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Modify existing tables
ALTER TABLE games ADD COLUMN IF NOT EXISTS nba_game_id TEXT;
ALTER TABLE player_props ADD COLUMN IF NOT EXISTS nba_player_id INT REFERENCES players(player_id);

CREATE INDEX IF NOT EXISTS idx_games_nba_game_id       ON games (nba_game_id);
CREATE INDEX IF NOT EXISTS idx_player_props_nba_player ON player_props (nba_player_id);
```

- [ ] **Step 3: Add the same tables/columns to init_schema.sql for fresh deployments**

Append the contents of the migration file (without the `ALTER TABLE` statements — add the new columns directly into the `CREATE TABLE` definitions for `games` and `player_props` instead). Specifically:
- Add `nba_game_id TEXT` column to the `games` CREATE TABLE block
- Add `nba_player_id INT REFERENCES players(player_id)` column to `player_props` CREATE TABLE block
- Add all five new CREATE TABLE blocks and their indexes

- [ ] **Step 4: Update `tests/unit/conftest.py` to stub nba_api**

`nba_api` makes network calls at import time in some versions. Add stubs so unit tests can run without the package installed (e.g. in CI before `pip install`). Open `tests/unit/conftest.py` and add after the existing `_stub_airflow()` call:

```python
def _stub_nba_api():
    """Stub nba_api so unit tests don't require the package to be installed."""
    if "nba_api" in sys.modules:
        return
    nba_api_stub = MagicMock()
    sys.modules.setdefault("nba_api", nba_api_stub)
    sys.modules.setdefault("nba_api.stats", nba_api_stub)
    sys.modules.setdefault("nba_api.stats.endpoints", nba_api_stub)

_stub_nba_api()
```

Note: when `nba_api` IS installed (the normal dev/CI path after `pip install`), this stub is skipped and the real package is used. Tests mock all actual API calls via `patch`, so the real package never makes network requests during tests.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt sql/migrations/001_add_player_stats_tables.sql sql/init_schema.sql tests/unit/conftest.py
git commit -m "feat: add migration SQL, dependencies, and conftest stubs for player stats pipeline"
```

---

## Task 2: nba_api Client

**Files:**
- Create: `plugins/nba_api_client.py`
- Create: `tests/unit/test_nba_api_client.py`

The client wraps five nba_api endpoints and returns plain Python lists of dicts, keeping the same interface style as `odds_api_client.py`. Each function sleeps `delay_seconds` before the API call to respect stats.nba.com rate limits. HTTP 429 responses are retried once after a 30s sleep.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_nba_api_client.py`:

```python
# tests/unit/test_nba_api_client.py
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd


def _mock_endpoint(df):
    """Build a mock nba_api endpoint object that returns `df` from get_data_frames()[0]."""
    mock = MagicMock()
    mock.get_data_frames.return_value = [df]
    return mock


def test_fetch_players_returns_list_of_dicts():
    from plugins.nba_api_client import fetch_players
    df = pd.DataFrame([{
        "PERSON_ID": 2544, "DISPLAY_FIRST_LAST": "LeBron James",
        "TEAM_ID": 1610612747, "TEAM_ABBREVIATION": "LAL",
        "ROSTERSTATUS": 1, "POSITION": "F",
    }])
    with patch("plugins.nba_api_client.CommonAllPlayers") as mock_cls:
        mock_cls.return_value = _mock_endpoint(df)
        result = fetch_players(delay_seconds=0)
    assert len(result) == 1
    assert result[0]["player_id"] == 2544
    assert result[0]["full_name"] == "LeBron James"
    assert result[0]["is_active"] is True


def test_fetch_players_filters_inactive():
    from plugins.nba_api_client import fetch_players
    df = pd.DataFrame([
        {"PERSON_ID": 1, "DISPLAY_FIRST_LAST": "Active Player",
         "TEAM_ID": 1, "TEAM_ABBREVIATION": "LAL", "ROSTERSTATUS": 1, "POSITION": "G"},
        {"PERSON_ID": 2, "DISPLAY_FIRST_LAST": "Inactive Player",
         "TEAM_ID": 0, "TEAM_ABBREVIATION": "", "ROSTERSTATUS": 0, "POSITION": ""},
    ])
    with patch("plugins.nba_api_client.CommonAllPlayers") as mock_cls:
        mock_cls.return_value = _mock_endpoint(df)
        result = fetch_players(delay_seconds=0)
    assert len(result) == 1
    assert result[0]["player_id"] == 1


def test_fetch_player_game_logs_merges_base_and_advanced():
    from plugins.nba_api_client import fetch_player_game_logs
    base_df = pd.DataFrame([{
        "PLAYER_ID": 2544, "PLAYER_NAME": "LeBron James", "GAME_ID": "0022400001",
        "GAME_DATE": "2025-01-15", "MATCHUP": "LAL vs. DEN", "TEAM_ID": 1610612747,
        "WL": "W", "MIN": 34.5, "FGA": 14, "FTA": 6, "PTS": 28,
        "REB": 8, "AST": 10, "BLK": 1, "STL": 2, "PLUS_MINUS": 12,
    }])
    adv_df = pd.DataFrame([{
        "PLAYER_ID": 2544, "GAME_ID": "0022400001", "USG_PCT": 0.312,
    }])
    with patch("plugins.nba_api_client.PlayerGameLogs") as mock_cls:
        mock_cls.side_effect = [_mock_endpoint(base_df), _mock_endpoint(adv_df)]
        result = fetch_player_game_logs(season="2024-25", delay_seconds=0)
    assert len(result) == 1
    row = result[0]
    assert row["player_id"] == 2544
    assert row["nba_game_id"] == "0022400001"
    assert row["min"] == 34.5
    assert row["usg_pct"] == pytest.approx(0.312)


def test_fetch_team_game_logs_returns_list_of_dicts():
    from plugins.nba_api_client import fetch_team_game_logs
    df = pd.DataFrame([{
        "TEAM_ID": 1610612743, "TEAM_ABBREVIATION": "DEN", "GAME_ID": "0022400001",
        "GAME_DATE": "2025-01-15", "MATCHUP": "DEN @ LAL", "WL": "L",
        "PTS": 110, "PLUS_MINUS": -12,
    }])
    with patch("plugins.nba_api_client.LeagueGameLog") as mock_cls:
        mock_cls.return_value = _mock_endpoint(df)
        result = fetch_team_game_logs(season="2024-25", delay_seconds=0)
    assert len(result) == 1
    assert result[0]["team_abbreviation"] == "DEN"
    assert result[0]["plus_minus"] == -12


def test_fetch_team_season_stats_merges_advanced_and_opponent():
    from plugins.nba_api_client import fetch_team_season_stats
    adv_df = pd.DataFrame([{
        "TEAM_ID": 1610612743, "TEAM_ABBREVIATION": "DEN",
        "PACE": 98.5, "OFF_RATING": 115.2, "DEF_RATING": 110.8,
    }])
    opp_df = pd.DataFrame([{
        "TEAM_ID": 1610612743, "TEAM_ABBREVIATION": "DEN", "OPP_PTS_PAINT": 42.1,
    }])
    with patch("plugins.nba_api_client.LeagueDashTeamStats") as mock_cls:
        mock_cls.side_effect = [_mock_endpoint(adv_df), _mock_endpoint(opp_df)]
        result = fetch_team_season_stats(season="2024-25", delay_seconds=0)
    assert len(result) == 1
    assert result[0]["pace"] == pytest.approx(98.5)
    assert result[0]["opp_pts_paint_pg"] == pytest.approx(42.1)


def test_fetch_player_game_logs_retries_on_429():
    import requests
    from plugins.nba_api_client import fetch_player_game_logs
    base_df = pd.DataFrame([{
        "PLAYER_ID": 1, "PLAYER_NAME": "P", "GAME_ID": "G1",
        "GAME_DATE": "2025-01-01", "MATCHUP": "A vs. B", "TEAM_ID": 1,
        "WL": "W", "MIN": 30.0, "FGA": 10, "FTA": 3, "PTS": 20,
        "REB": 5, "AST": 5, "BLK": 0, "STL": 1, "PLUS_MINUS": 5,
    }])
    adv_df = pd.DataFrame([{"PLAYER_ID": 1, "GAME_ID": "G1", "USG_PCT": 0.25}])
    err = requests.exceptions.HTTPError(response=MagicMock(status_code=429))
    with patch("plugins.nba_api_client.PlayerGameLogs") as mock_cls, \
         patch("plugins.nba_api_client.time.sleep") as mock_sleep:
        mock_cls.side_effect = [err, _mock_endpoint(base_df), _mock_endpoint(adv_df)]
        result = fetch_player_game_logs(season="2024-25", delay_seconds=0)
    mock_sleep.assert_any_call(30)
    assert len(result) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
python -m pytest tests/unit/test_nba_api_client.py -v
```
Expected: `ModuleNotFoundError` or `ImportError` — `plugins.nba_api_client` does not exist yet.

- [ ] **Step 3: Install new dependencies**

```bash
pip install nba_api==1.4.1 rapidfuzz==3.6.1
```

- [ ] **Step 4: Implement nba_api_client.py**

Create `plugins/nba_api_client.py`:

```python
# plugins/nba_api_client.py
import time

import requests
from nba_api.stats.endpoints import (
    CommonAllPlayers,
    PlayerGameLogs,
    LeagueGameLog,
    LeagueDashTeamStats,
)

_DEFAULT_TIMEOUT = 60


def _call_with_retry(endpoint_cls, delay_seconds, **kwargs):
    """Call an nba_api endpoint class, retrying once on HTTP 429."""
    time.sleep(delay_seconds)
    try:
        return endpoint_cls(timeout=_DEFAULT_TIMEOUT, **kwargs)
    except requests.exceptions.HTTPError as e:
        if hasattr(e, "response") and e.response is not None and e.response.status_code == 429:
            time.sleep(30)
            return endpoint_cls(timeout=_DEFAULT_TIMEOUT, **kwargs)
        raise


def fetch_players(delay_seconds=1):
    """Fetch all active NBA players. Returns list of dicts."""
    result = _call_with_retry(CommonAllPlayers, delay_seconds, is_only_current_season=1)
    df = result.get_data_frames()[0]
    active = df[df["ROSTERSTATUS"] == 1]
    return [
        {
            "player_id": int(row["PERSON_ID"]),
            "full_name": row["DISPLAY_FIRST_LAST"],
            "team_id": int(row["TEAM_ID"]) if row["TEAM_ID"] else None,
            "team_abbreviation": row["TEAM_ABBREVIATION"] or None,
            "position": row["POSITION"] or None,
            "is_active": True,
        }
        for _, row in active.iterrows()
    ]


def fetch_player_game_logs(season, delay_seconds=1):
    """
    Fetch all player game logs for a season (basic + advanced merged).
    Returns list of dicts. season format: '2024-25'.
    """
    base = _call_with_retry(
        PlayerGameLogs, delay_seconds,
        season_nullable=season,
        measure_type_player_game_logs_nullable="Base",
    )
    adv = _call_with_retry(
        PlayerGameLogs, delay_seconds,
        season_nullable=season,
        measure_type_player_game_logs_nullable="Advanced",
    )
    base_df = base.get_data_frames()[0]
    adv_df = adv.get_data_frames()[0][["PLAYER_ID", "GAME_ID", "USG_PCT"]]
    merged = base_df.merge(adv_df, on=["PLAYER_ID", "GAME_ID"], how="left")
    rows = []
    for _, row in merged.iterrows():
        rows.append({
            "player_id": int(row["PLAYER_ID"]),
            "nba_game_id": row["GAME_ID"],
            "season": season,
            "game_date": row["GAME_DATE"],
            "matchup": row["MATCHUP"],
            "team_id": int(row["TEAM_ID"]) if row.get("TEAM_ID") else None,
            "wl": row.get("WL"),
            "min": float(row["MIN"]) if row.get("MIN") is not None else None,
            "fga": int(row["FGA"]) if row.get("FGA") is not None else None,
            "fta": int(row["FTA"]) if row.get("FTA") is not None else None,
            "usg_pct": float(row["USG_PCT"]) if row.get("USG_PCT") is not None else None,
            "pts": int(row["PTS"]) if row.get("PTS") is not None else None,
            "reb": int(row["REB"]) if row.get("REB") is not None else None,
            "ast": int(row["AST"]) if row.get("AST") is not None else None,
            "blk": int(row["BLK"]) if row.get("BLK") is not None else None,
            "stl": int(row["STL"]) if row.get("STL") is not None else None,
            "plus_minus": int(row["PLUS_MINUS"]) if row.get("PLUS_MINUS") is not None else None,
        })
    return rows


def fetch_team_game_logs(season, delay_seconds=1):
    """Fetch all team game logs for a season. Returns list of dicts."""
    result = _call_with_retry(
        LeagueGameLog, delay_seconds,
        season=season,
        player_or_team_abbreviation="T",
    )
    df = result.get_data_frames()[0]
    return [
        {
            "team_id": int(row["TEAM_ID"]),
            "team_abbreviation": row["TEAM_ABBREVIATION"],
            "nba_game_id": row["GAME_ID"],
            "season": season,
            "game_date": row["GAME_DATE"],
            "matchup": row["MATCHUP"],
            "wl": row.get("WL"),
            "pts": int(row["PTS"]) if row.get("PTS") is not None else None,
            "plus_minus": int(row["PLUS_MINUS"]) if row.get("PLUS_MINUS") is not None else None,
        }
        for _, row in df.iterrows()
    ]


def fetch_team_season_stats(season, delay_seconds=1):
    """
    Fetch season-level team stats (pace, ratings, paint defense).
    Merges Advanced + Opponent measure types. Returns list of dicts.
    """
    adv_result = _call_with_retry(
        LeagueDashTeamStats, delay_seconds,
        season=season,
        measure_type_simple="Advanced",
        per_mode_simple="PerGame",
    )
    opp_result = _call_with_retry(
        LeagueDashTeamStats, delay_seconds,
        season=season,
        measure_type_simple="Opponent",
        per_mode_simple="PerGame",
    )
    adv_df = adv_result.get_data_frames()[0][["TEAM_ID", "TEAM_ABBREVIATION", "PACE", "OFF_RATING", "DEF_RATING"]]
    opp_df = opp_result.get_data_frames()[0][["TEAM_ID", "OPP_PTS_PAINT"]]
    merged = adv_df.merge(opp_df, on="TEAM_ID", how="left")
    return [
        {
            "team_id": int(row["TEAM_ID"]),
            "team_abbreviation": row["TEAM_ABBREVIATION"],
            "season": season,
            "pace": float(row["PACE"]) if row.get("PACE") is not None else None,
            "off_rating": float(row["OFF_RATING"]) if row.get("OFF_RATING") is not None else None,
            "def_rating": float(row["DEF_RATING"]) if row.get("DEF_RATING") is not None else None,
            "opp_pts_paint_pg": float(row["OPP_PTS_PAINT"]) if row.get("OPP_PTS_PAINT") is not None else None,
        }
        for _, row in merged.iterrows()
    ]
```

- [ ] **Step 5: Run tests and verify they pass**

```bash
python -m pytest tests/unit/test_nba_api_client.py -v
```
Expected: All 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add plugins/nba_api_client.py tests/unit/test_nba_api_client.py
git commit -m "feat: add nba_api_client wrapper with rate-limit retry"
```

---

## Task 3: Players Transformer

**Files:**
- Create: `plugins/transformers/players.py`
- Create: `tests/unit/transformers/test_players.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/transformers/test_players.py`:

```python
# tests/unit/transformers/test_players.py
from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_PLAYERS = [
    {
        "player_id": 2544,
        "full_name": "LeBron James",
        "position": "F",
        "team_id": 1610612747,
        "team_abbreviation": "LAL",
        "is_active": True,
    }
]


def test_transform_players_upserts_one_row():
    from plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn()
    transform_players(mock_conn, SAMPLE_PLAYERS)
    assert mock_cursor.execute.call_count == 1
    args = mock_cursor.execute.call_args[0][1]
    assert args[0] == 2544         # player_id
    assert args[1] == "LeBron James"  # full_name
    assert args[2] == "F"          # position
    assert args[4] == "LAL"        # team_abbreviation
    assert args[5] is True         # is_active
    mock_conn.commit.assert_called_once()


def test_transform_players_skips_empty():
    from plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn()
    transform_players(mock_conn, [])
    mock_cursor.execute.assert_not_called()


def test_transform_players_idempotent_on_conflict():
    """Verify ON CONFLICT clause is present in the SQL (upsert not insert)."""
    from plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn()
    transform_players(mock_conn, SAMPLE_PLAYERS)
    sql = mock_cursor.execute.call_args[0][0]
    assert "ON CONFLICT" in sql.upper()
    assert "DO UPDATE" in sql.upper()
```

- [ ] **Step 2: Run and verify failure**

```bash
python -m pytest tests/unit/transformers/test_players.py -v
```
Expected: `ImportError` — module does not exist.

- [ ] **Step 3: Implement players transformer**

Create `plugins/transformers/players.py`:

```python
# plugins/transformers/players.py

def transform_players(conn, raw_players):
    if not raw_players:
        return
    with conn.cursor() as cur:
        for p in raw_players:
            cur.execute(
                """
                INSERT INTO players
                    (player_id, full_name, position, team_id, team_abbreviation, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name         = EXCLUDED.full_name,
                    position          = EXCLUDED.position,
                    team_id           = EXCLUDED.team_id,
                    team_abbreviation = EXCLUDED.team_abbreviation,
                    is_active         = EXCLUDED.is_active,
                    fetched_at        = NOW()
                """,
                (
                    p["player_id"],
                    p["full_name"],
                    p.get("position"),
                    p.get("team_id"),
                    p.get("team_abbreviation"),
                    p.get("is_active", True),
                ),
            )
    conn.commit()
```

- [ ] **Step 4: Run and verify tests pass**

```bash
python -m pytest tests/unit/transformers/test_players.py -v
```
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/transformers/players.py tests/unit/transformers/test_players.py
git commit -m "feat: add players transformer"
```

---

## Task 4: Player Game Logs Transformer

**Files:**
- Create: `plugins/transformers/player_game_logs.py`
- Create: `tests/unit/transformers/test_player_game_logs.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/transformers/test_player_game_logs.py`:

```python
# tests/unit/transformers/test_player_game_logs.py
from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_LOG = {
    "player_id": 2544,
    "nba_game_id": "0022400001",
    "season": "2024-25",
    "game_date": "2025-01-15",
    "matchup": "LAL vs. DEN",
    "team_id": 1610612747,
    "wl": "W",
    "min": 34.5,
    "fga": 14,
    "fta": 6,
    "usg_pct": 0.312,
    "pts": 28,
    "reb": 8,
    "ast": 10,
    "blk": 1,
    "stl": 2,
    "plus_minus": 12,
}


def test_transform_player_game_logs_inserts_one_row():
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [SAMPLE_LOG])
    assert mock_cursor.execute.call_count == 1
    args = mock_cursor.execute.call_args[0][1]
    assert args[0] == 2544             # player_id
    assert args[1] == "0022400001"     # nba_game_id
    assert args[4] == "LAL vs. DEN"   # matchup
    assert args[8] == 34.5            # min
    assert args[10] == 0.312          # usg_pct


def test_transform_player_game_logs_skips_empty():
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [])
    mock_cursor.execute.assert_not_called()


def test_transform_player_game_logs_uses_on_conflict():
    """Append-only: ON CONFLICT DO NOTHING ensures idempotency."""
    from plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [SAMPLE_LOG])
    sql = mock_cursor.execute.call_args[0][0]
    assert "ON CONFLICT" in sql.upper()
```

- [ ] **Step 2: Run and verify failure**

```bash
python -m pytest tests/unit/transformers/test_player_game_logs.py -v
```

- [ ] **Step 3: Implement**

Create `plugins/transformers/player_game_logs.py`:

```python
# plugins/transformers/player_game_logs.py

def transform_player_game_logs(conn, raw_logs):
    if not raw_logs:
        return
    with conn.cursor() as cur:
        for log in raw_logs:
            cur.execute(
                """
                INSERT INTO player_game_logs
                    (player_id, nba_game_id, season, game_date, matchup,
                     team_id, wl, min, fga, fta, usg_pct,
                     pts, reb, ast, blk, stl, plus_minus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, nba_game_id) DO NOTHING
                """,
                (
                    log["player_id"],
                    log["nba_game_id"],
                    log["season"],
                    log["game_date"],
                    log.get("matchup"),
                    log.get("team_id"),
                    log.get("wl"),
                    log.get("min"),
                    log.get("fga"),
                    log.get("fta"),
                    log.get("usg_pct"),
                    log.get("pts"),
                    log.get("reb"),
                    log.get("ast"),
                    log.get("blk"),
                    log.get("stl"),
                    log.get("plus_minus"),
                ),
            )
    conn.commit()
```

- [ ] **Step 4: Run and verify tests pass**

```bash
python -m pytest tests/unit/transformers/test_player_game_logs.py -v
```
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/transformers/player_game_logs.py tests/unit/transformers/test_player_game_logs.py
git commit -m "feat: add player_game_logs transformer"
```

---

## Task 5: Team Game Logs Transformer

**Files:**
- Create: `plugins/transformers/team_game_logs.py`
- Create: `tests/unit/transformers/test_team_game_logs.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/transformers/test_team_game_logs.py`:

```python
# tests/unit/transformers/test_team_game_logs.py
from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_LOG = {
    "team_id": 1610612743,
    "team_abbreviation": "DEN",
    "nba_game_id": "0022400001",
    "season": "2024-25",
    "game_date": "2025-01-15",
    "matchup": "DEN @ LAL",
    "wl": "L",
    "pts": 110,
    "plus_minus": -12,
}


def test_transform_team_game_logs_inserts_one_row():
    from plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [SAMPLE_LOG])
    assert mock_cursor.execute.call_count == 1
    args = mock_cursor.execute.call_args[0][1]
    assert args[0] == 1610612743   # team_id
    assert args[1] == "DEN"        # team_abbreviation
    assert args[8] == -12          # plus_minus


def test_transform_team_game_logs_skips_empty():
    from plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [])
    mock_cursor.execute.assert_not_called()


def test_transform_team_game_logs_uses_on_conflict():
    from plugins.transformers.team_game_logs import transform_team_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_game_logs(mock_conn, [SAMPLE_LOG])
    sql = mock_cursor.execute.call_args[0][0]
    assert "ON CONFLICT" in sql.upper()
```

- [ ] **Step 2: Run and verify failure**

```bash
python -m pytest tests/unit/transformers/test_team_game_logs.py -v
```

- [ ] **Step 3: Implement**

Create `plugins/transformers/team_game_logs.py`:

```python
# plugins/transformers/team_game_logs.py

def transform_team_game_logs(conn, raw_logs):
    if not raw_logs:
        return
    with conn.cursor() as cur:
        for log in raw_logs:
            cur.execute(
                """
                INSERT INTO team_game_logs
                    (team_id, team_abbreviation, nba_game_id, season,
                     game_date, matchup, wl, pts, plus_minus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_id, nba_game_id) DO NOTHING
                """,
                (
                    log["team_id"],
                    log.get("team_abbreviation"),
                    log["nba_game_id"],
                    log["season"],
                    log["game_date"],
                    log.get("matchup"),
                    log.get("wl"),
                    log.get("pts"),
                    log.get("plus_minus"),
                ),
            )
    conn.commit()
```

- [ ] **Step 4: Run and verify tests pass**

```bash
python -m pytest tests/unit/transformers/test_team_game_logs.py -v
```

- [ ] **Step 5: Commit**

```bash
git add plugins/transformers/team_game_logs.py tests/unit/transformers/test_team_game_logs.py
git commit -m "feat: add team_game_logs transformer"
```

---

## Task 6: Team Season Stats Transformer

**Files:**
- Create: `plugins/transformers/team_season_stats.py`
- Create: `tests/unit/transformers/test_team_season_stats.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/transformers/test_team_season_stats.py`:

```python
# tests/unit/transformers/test_team_season_stats.py
from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_STATS = {
    "team_id": 1610612743,
    "team_abbreviation": "DEN",
    "season": "2024-25",
    "pace": 98.5,
    "off_rating": 115.2,
    "def_rating": 110.8,
    "opp_pts_paint_pg": 42.1,
}


def test_transform_team_season_stats_upserts():
    from plugins.transformers.team_season_stats import transform_team_season_stats
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_season_stats(mock_conn, [SAMPLE_STATS])
    assert mock_cursor.execute.call_count == 1
    args = mock_cursor.execute.call_args[0][1]
    assert args[0] == 1610612743
    assert args[2] == "2024-25"
    assert args[3] == 98.5    # pace
    assert args[6] == 42.1   # opp_pts_paint_pg
    sql = mock_cursor.execute.call_args[0][0]
    assert "ON CONFLICT" in sql.upper()
    assert "DO UPDATE" in sql.upper()


def test_transform_team_season_stats_skips_empty():
    from plugins.transformers.team_season_stats import transform_team_season_stats
    mock_conn, mock_cursor = _make_mock_conn()
    transform_team_season_stats(mock_conn, [])
    mock_cursor.execute.assert_not_called()
```

- [ ] **Step 2: Run and verify failure**

```bash
python -m pytest tests/unit/transformers/test_team_season_stats.py -v
```

- [ ] **Step 3: Implement**

Create `plugins/transformers/team_season_stats.py`:

```python
# plugins/transformers/team_season_stats.py

def transform_team_season_stats(conn, raw_stats):
    if not raw_stats:
        return
    with conn.cursor() as cur:
        for s in raw_stats:
            cur.execute(
                """
                INSERT INTO team_season_stats
                    (team_id, team_abbreviation, season, pace, off_rating, def_rating, opp_pts_paint_pg)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_id, season) DO UPDATE SET
                    pace             = EXCLUDED.pace,
                    off_rating       = EXCLUDED.off_rating,
                    def_rating       = EXCLUDED.def_rating,
                    opp_pts_paint_pg = EXCLUDED.opp_pts_paint_pg,
                    fetched_at       = NOW()
                """,
                (
                    s["team_id"],
                    s.get("team_abbreviation"),
                    s["season"],
                    s.get("pace"),
                    s.get("off_rating"),
                    s.get("def_rating"),
                    s.get("opp_pts_paint_pg"),
                ),
            )
    conn.commit()
```

- [ ] **Step 4: Run and verify tests pass**

```bash
python -m pytest tests/unit/transformers/test_team_season_stats.py -v
```

- [ ] **Step 5: Commit**

```bash
git add plugins/transformers/team_season_stats.py tests/unit/transformers/test_team_season_stats.py
git commit -m "feat: add team_season_stats transformer"
```

---

## Task 7: Player Name Resolution

**Files:**
- Create: `plugins/transformers/player_name_resolution.py`
- Create: `tests/unit/test_player_name_resolution.py`

This module normalizes player name strings (accent stripping, suffix removal) and fuzzy-matches Odds API player names against the `players` table to populate `player_name_mappings` and `player_props.nba_player_id`.

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_player_name_resolution.py`:

```python
# tests/unit/test_player_name_resolution.py
from unittest.mock import MagicMock, patch


def test_normalize_name_strips_accents():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("Nikola Jokić") == "nikola jokic"


def test_normalize_name_removes_jr_suffix():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("Gary Trent Jr.") == "gary trent"
    assert normalize_name("Gary Trent Jr") == "gary trent"


def test_normalize_name_removes_sr_suffix():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("Marcus Morris Sr.") == "marcus morris"


def test_normalize_name_removes_roman_numeral_suffixes():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("Otto Porter III") == "otto porter"
    assert normalize_name("Gary Payton II") == "gary payton"


def test_normalize_name_lowercases_and_strips_whitespace():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("  LeBron James  ") == "lebron james"


def test_resolve_player_ids_inserts_high_confidence_match():
    from plugins.transformers.player_name_resolution import resolve_player_ids

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Simulate: 1 unresolved name in player_props, 1 player in players table
    mock_cursor.fetchall.side_effect = [
        [("Nikola Jokic",)],           # unresolved names from player_props
        [(2544, "Nikola Jokić")],       # players in players table
    ]

    with patch("plugins.transformers.player_name_resolution.send_slack_message") as mock_slack:
        resolve_player_ids(mock_conn, slack_webhook_url="http://example.com")

    # Should insert 1 mapping (high confidence)
    insert_calls = [
        c for c in mock_cursor.execute.call_args_list
        if "INSERT INTO player_name_mappings" in str(c)
    ]
    assert len(insert_calls) == 1
    args = insert_calls[0][0][1]
    assert args[0] == "Nikola Jokic"  # odds_api_name
    assert args[1] == 2544            # nba_player_id
    assert args[2] >= 95.0            # confidence

    # No Slack alert for high confidence match
    mock_slack.assert_not_called()


def test_resolve_player_ids_sends_slack_for_low_confidence():
    from plugins.transformers.player_name_resolution import resolve_player_ids

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.side_effect = [
        [("Zyx Unkown Player",)],      # name with no good match
        [(9999, "Totally Different")],  # only player in DB
    ]

    with patch("plugins.transformers.player_name_resolution.send_slack_message") as mock_slack:
        resolve_player_ids(mock_conn, slack_webhook_url="http://example.com")

    mock_slack.assert_called_once()
    # Should NOT insert a mapping for low confidence
    insert_calls = [
        c for c in mock_cursor.execute.call_args_list
        if "INSERT INTO player_name_mappings" in str(c)
    ]
    assert len(insert_calls) == 0
```

- [ ] **Step 2: Run and verify failure**

```bash
python -m pytest tests/unit/test_player_name_resolution.py -v
```

- [ ] **Step 3: Implement**

Create `plugins/transformers/player_name_resolution.py`:

```python
# plugins/transformers/player_name_resolution.py
import logging
import unicodedata

from rapidfuzz import fuzz

from plugins.slack_notifier import send_slack_message

CONFIDENCE_THRESHOLD = 95.0

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Lowercase, strip accents, remove Jr./Sr./II/III/IV suffixes."""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().strip()
    for suffix in [" jr.", " jr", " sr.", " sr", " iii", " ii", " iv"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
            break
    return name


def resolve_player_ids(conn, slack_webhook_url=None):
    """
    Find all Odds API player names in player_props not yet in player_name_mappings.
    Fuzzy-match against players table. Insert high-confidence matches (>=95%).
    Send Slack alert for unresolved names (<95%).
    Then populate player_props.nba_player_id for all resolved names.
    """
    with conn.cursor() as cur:
        # Unresolved names: in player_props but not yet in player_name_mappings
        cur.execute(
            """
            SELECT DISTINCT pp.player_name
            FROM player_props pp
            WHERE pp.player_name IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM player_name_mappings m
                WHERE m.odds_api_name = pp.player_name
              )
            """
        )
        unresolved = [row[0] for row in cur.fetchall()]

        if not unresolved:
            return

        # Load all known players for matching
        cur.execute("SELECT player_id, full_name FROM players")
        known_players = cur.fetchall()  # list of (player_id, full_name)

    unresolved_names = []

    with conn.cursor() as cur:
        for odds_name in unresolved:
            norm_odds = normalize_name(odds_name)
            best_score = 0.0
            best_player_id = None

            for player_id, full_name in known_players:
                score = fuzz.ratio(norm_odds, normalize_name(full_name))
                if score > best_score:
                    best_score = score
                    best_player_id = player_id

            if best_score >= CONFIDENCE_THRESHOLD:
                cur.execute(
                    """
                    INSERT INTO player_name_mappings (odds_api_name, nba_player_id, confidence)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (odds_api_name) DO NOTHING
                    """,
                    (odds_name, best_player_id, best_score),
                )
            else:
                logger.warning("Low confidence match for '%s': score=%.1f", odds_name, best_score)
                unresolved_names.append((odds_name, best_score))

        # Backfill player_props.nba_player_id for all mapped names
        cur.execute(
            """
            UPDATE player_props pp
            SET nba_player_id = m.nba_player_id
            FROM player_name_mappings m
            WHERE pp.player_name = m.odds_api_name
              AND pp.nba_player_id IS NULL
            """
        )

    conn.commit()

    if unresolved_names and slack_webhook_url:
        lines = "\n".join(
            f"  • {name} (best score: {score:.1f})"
            for name, score in unresolved_names
        )
        send_slack_message(
            slack_webhook_url,
            f":warning: *Player name resolution* — {len(unresolved_names)} unresolved name(s) need manual review:\n{lines}",
        )
```

- [ ] **Step 4: Check that send_slack_message exists in slack_notifier.py**

Open `plugins/slack_notifier.py` and verify it exports a `send_slack_message(webhook_url, text)` function. If it only has `notify_success` / `notify_failure` callbacks, add the helper:

```python
def send_slack_message(webhook_url, text):
    import requests
    if not webhook_url:
        return
    requests.post(webhook_url, json={"text": text})
```

- [ ] **Step 5: Run and verify tests pass**

```bash
python -m pytest tests/unit/test_player_name_resolution.py -v
```
Expected: 7 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add plugins/transformers/player_name_resolution.py tests/unit/test_player_name_resolution.py plugins/slack_notifier.py
git commit -m "feat: add player name resolution with fuzzy matching"
```

---

## Task 8: Game ID Linker

**Files:**
- Create: `plugins/transformers/game_id_linker.py`
- Create: `tests/unit/test_game_id_linker.py`

Links `games.nba_game_id` by matching `DATE(commence_time AT TIME ZONE 'America/New_York')` with `game_date` from `team_game_logs`, using team abbreviations parsed from the nba_api `MATCHUP` string.

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_game_id_linker.py`:

```python
# tests/unit/test_game_id_linker.py
from unittest.mock import MagicMock, call


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_parse_matchup_home_game():
    from plugins.transformers.game_id_linker import parse_matchup_teams
    home, away = parse_matchup_teams("DEN vs. LAL")
    assert home == "DEN"
    assert away == "LAL"


def test_parse_matchup_away_game():
    from plugins.transformers.game_id_linker import parse_matchup_teams
    # "DEN @ LAL" means DEN is away, LAL is home
    home, away = parse_matchup_teams("DEN @ LAL")
    assert home == "LAL"
    assert away == "DEN"


def test_link_nba_game_ids_updates_games_table():
    from plugins.transformers.game_id_linker import link_nba_game_ids
    mock_conn, mock_cursor = _make_mock_conn()

    # team_game_logs rows: one game
    mock_cursor.fetchall.return_value = [
        ("0022400001", "2025-01-15", "DEN vs. LAL"),
    ]

    link_nba_game_ids(mock_conn)

    update_calls = [
        c for c in mock_cursor.execute.call_args_list
        if "UPDATE games" in str(c)
    ]
    assert len(update_calls) >= 1
    mock_conn.commit.assert_called()


def test_link_nba_game_ids_no_rows_is_noop():
    from plugins.transformers.game_id_linker import link_nba_game_ids
    mock_conn, mock_cursor = _make_mock_conn()
    mock_cursor.fetchall.return_value = []
    link_nba_game_ids(mock_conn)
    update_calls = [
        c for c in mock_cursor.execute.call_args_list
        if "UPDATE games" in str(c)
    ]
    assert len(update_calls) == 0
```

- [ ] **Step 2: Run and verify failure**

```bash
python -m pytest tests/unit/test_game_id_linker.py -v
```

- [ ] **Step 3: Implement**

Create `plugins/transformers/game_id_linker.py`:

```python
# plugins/transformers/game_id_linker.py
import logging

logger = logging.getLogger(__name__)


def parse_matchup_teams(matchup: str):
    """
    Parse home and away team abbreviations from nba_api MATCHUP string.
    'DEN vs. LAL' → home='DEN', away='LAL'  (DEN is home)
    'DEN @ LAL'   → home='LAL', away='DEN'  (DEN is away, LAL is home)
    Returns (home_abbrev, away_abbrev).
    """
    if " vs. " in matchup:
        home, away = matchup.split(" vs. ")
        return home.strip(), away.strip()
    elif " @ " in matchup:
        away, home = matchup.split(" @ ")
        return home.strip(), away.strip()
    raise ValueError(f"Unrecognised matchup format: {matchup!r}")


def link_nba_game_ids(conn):
    """
    For each unique game in team_game_logs, find the matching game in the games table
    by date (ET-converted) and team abbreviations, then set games.nba_game_id.
    Unmatched games are logged as warnings.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT nba_game_id, game_date, matchup
            FROM team_game_logs
            WHERE nba_game_id IS NOT NULL
            """
        )
        rows = cur.fetchall()

    if not rows:
        return

    updated = 0
    unmatched = []

    with conn.cursor() as cur:
        for nba_game_id, game_date, matchup in rows:
            try:
                home_abbrev, away_abbrev = parse_matchup_teams(matchup)
            except ValueError as e:
                logger.warning("Skipping unparseable matchup: %s", e)
                continue

            # Primary match: exact date (ET) + team abbreviations
            cur.execute(
                """
                UPDATE games
                SET nba_game_id = %s
                WHERE nba_game_id IS NULL
                  AND DATE(commence_time AT TIME ZONE 'America/New_York') = %s
                  AND home_team ILIKE %s
                  AND away_team ILIKE %s
                """,
                (nba_game_id, game_date, f"%{home_abbrev}%", f"%{away_abbrev}%"),
            )
            if cur.rowcount > 0:
                updated += cur.rowcount
                continue

            # Fallback: ±1 day window
            cur.execute(
                """
                UPDATE games
                SET nba_game_id = %s
                WHERE nba_game_id IS NULL
                  AND DATE(commence_time AT TIME ZONE 'America/New_York')
                      BETWEEN %s::date - INTERVAL '1 day' AND %s::date + INTERVAL '1 day'
                  AND home_team ILIKE %s
                  AND away_team ILIKE %s
                """,
                (nba_game_id, game_date, game_date, f"%{home_abbrev}%", f"%{away_abbrev}%"),
            )
            if cur.rowcount > 0:
                updated += cur.rowcount
            else:
                unmatched.append(nba_game_id)

    conn.commit()

    if unmatched:
        logger.warning(
            "link_nba_game_ids: %d game(s) could not be matched to games table: %s",
            len(unmatched), unmatched,
        )
    logger.info("link_nba_game_ids: updated %d game(s)", updated)
```

- [ ] **Step 4: Run and verify tests pass**

```bash
python -m pytest tests/unit/test_game_id_linker.py -v
```
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/transformers/game_id_linker.py tests/unit/test_game_id_linker.py
git commit -m "feat: add game ID linker for games.nba_game_id"
```

---

## Task 9: Ingest DAG

**Files:**
- Create: `dags/nba_player_stats_ingest_dag.py`

No TDD here — DAG structure tests come in a follow-up. Follow the exact pattern of `dags/ingest_dag.py`.

- [ ] **Step 1: Implement the ingest DAG**

Create `dags/nba_player_stats_ingest_dag.py`:

```python
# dags/nba_player_stats_ingest_dag.py
import json
import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.nba_api_client import (
    fetch_players,
    fetch_player_game_logs,
    fetch_team_game_logs,
    fetch_team_season_stats,
)
from plugins.slack_notifier import notify_success, notify_failure

CURRENT_SEASON = "2024-25"  # Update manually each season; programmatic derivation is a future improvement
INGEST_DELAY_SECONDS = 1


def _get_current_season():
    """Return the current NBA season string e.g. '2024-25'."""
    return CURRENT_SEASON


def fetch_players_task(**context):
    conn = get_data_db_conn()
    try:
        data = fetch_players(delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, "nba_api/players", {}, data)
    except Exception:
        store_raw_response(conn, "nba_api/players", {}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_player_game_logs_task(**context):
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        # Build props-driven player filter
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT m.nba_player_id
                FROM player_props pp
                JOIN player_name_mappings m ON m.odds_api_name = pp.player_name
                WHERE m.nba_player_id IS NOT NULL
                """
            )
            prop_player_ids = {row[0] for row in cur.fetchall()}

            # Minutes fallback: players averaging >20 min/game this season
            cur.execute(
                """
                SELECT DISTINCT player_id
                FROM player_game_logs
                WHERE season = %s
                GROUP BY player_id
                HAVING AVG(min) > 20
                """,
                (season,),
            )
            mins_player_ids = {row[0] for row in cur.fetchall()}

        player_ids = prop_player_ids | mins_player_ids
        # Bootstrap: if no data yet, fetch all (backfill must run first)
        if not player_ids:
            import logging
            logging.getLogger(__name__).info(
                "No player filter available — fetching all (bootstrap mode). "
                "Run nba_player_stats_backfill first."
            )

        # nba_api's PlayerGameLogs endpoint does not support server-side player-ID filtering,
        # so we always fetch the full league dataset (~450 players) and filter client-side.
        # This is intentional: the API call is cheap (one HTTP request per season) and
        # filtering after the fact is simpler than iterating per-player.
        data = fetch_player_game_logs(season=season, delay_seconds=INGEST_DELAY_SECONDS)

        # Filter to relevant players if we have a list
        if player_ids:
            data = [row for row in data if row["player_id"] in player_ids]

        store_raw_response(conn, "nba_api/player_game_logs", {"season": season}, data)
    except Exception:
        store_raw_response(conn, "nba_api/player_game_logs", {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_team_game_logs_task(**context):
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        data = fetch_team_game_logs(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, "nba_api/team_game_logs", {"season": season}, data)
    except Exception:
        store_raw_response(conn, "nba_api/team_game_logs", {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_team_season_stats_task(**context):
    conn = get_data_db_conn()
    season = _get_current_season()
    try:
        data = fetch_team_season_stats(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, "nba_api/team_season_stats", {"season": season}, data)
    except Exception:
        store_raw_response(conn, "nba_api/team_season_stats", {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nba_player_stats_ingest",
    default_args=default_args,
    description="Fetch NBA player and team stats from nba_api",
    schedule_interval="0 13 * * *",  # 6am MT = 1pm UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "player_stats", "ingest"],
    # Note: notify_success reads XCom from task_id="fetch_odds" which does not exist in this DAG.
    # The callback will degrade gracefully to a plain success message (no quota info).
    # Decoupling notify_success from the fetch_odds XCom key is a future improvement.
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
) as dag:
    t_players      = PythonOperator(task_id="fetch_players",          python_callable=fetch_players_task)
    t_player_logs  = PythonOperator(task_id="fetch_player_game_logs", python_callable=fetch_player_game_logs_task)
    t_team_logs    = PythonOperator(task_id="fetch_team_game_logs",   python_callable=fetch_team_game_logs_task)
    t_team_stats   = PythonOperator(task_id="fetch_team_season_stats",python_callable=fetch_team_season_stats_task)

    # All four tasks run independently in parallel
    [t_players, t_player_logs, t_team_logs, t_team_stats]
```

- [ ] **Step 2: Verify DAG loads in Airflow (no import errors)**

```bash
python -c "import sys; sys.path.insert(0, '.'); import dags.nba_player_stats_ingest_dag"
```
Expected: No output (clean import).

- [ ] **Step 3: Commit**

```bash
git add dags/nba_player_stats_ingest_dag.py
git commit -m "feat: add nba_player_stats_ingest DAG"
```

---

## Task 10: Transform DAG

**Files:**
- Create: `dags/nba_player_stats_transform_dag.py`

- [ ] **Step 1: Implement the transform DAG**

Create `dags/nba_player_stats_transform_dag.py`:

```python
# dags/nba_player_stats_transform_dag.py
import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, "/opt/airflow")

from plugins.db_client import get_data_db_conn
from plugins.transformers.players import transform_players
from plugins.transformers.player_game_logs import transform_player_game_logs
from plugins.transformers.team_game_logs import transform_team_game_logs
from plugins.transformers.team_season_stats import transform_team_season_stats
from plugins.transformers.player_name_resolution import resolve_player_ids
from plugins.transformers.game_id_linker import link_nba_game_ids


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
    if row is None:
        raise ValueError(f"No successful raw data for endpoint '{endpoint}'. Run ingest first.")
    # psycopg2 deserializes JSONB columns to Python dicts/lists automatically.
    # row[0] is already a Python list — transformers receive it directly.
    return row[0]


def run_transform_players():
    conn = get_data_db_conn()
    try:
        transform_players(conn, _get_latest_raw(conn, "nba_api/players"))
    finally:
        conn.close()


def run_transform_player_game_logs():
    conn = get_data_db_conn()
    try:
        transform_player_game_logs(conn, _get_latest_raw(conn, "nba_api/player_game_logs"))
    finally:
        conn.close()


def run_transform_team_game_logs():
    conn = get_data_db_conn()
    try:
        transform_team_game_logs(conn, _get_latest_raw(conn, "nba_api/team_game_logs"))
    finally:
        conn.close()


def run_transform_team_season_stats():
    conn = get_data_db_conn()
    try:
        transform_team_season_stats(conn, _get_latest_raw(conn, "nba_api/team_season_stats"))
    finally:
        conn.close()


def run_resolve_player_ids():
    conn = get_data_db_conn()
    try:
        slack_url = os.environ.get("SLACK_WEBHOOK_URL")
        resolve_player_ids(conn, slack_webhook_url=slack_url)
    finally:
        conn.close()


def run_link_nba_game_ids():
    conn = get_data_db_conn()
    try:
        link_nba_game_ids(conn)
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nba_player_stats_transform",
    default_args=default_args,
    description="Normalize raw nba_api data into structured player stats tables",
    schedule_interval="20 13 * * *",  # 6:20am MT = 1:20pm UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["nba", "player_stats", "transform"],
) as dag:
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="nba_player_stats_ingest",
        external_task_id=None,
        execution_delta=timedelta(minutes=20),
        timeout=600,
        mode="reschedule",
    )

    t_players      = PythonOperator(task_id="transform_players",          python_callable=run_transform_players)
    t_player_logs  = PythonOperator(task_id="transform_player_game_logs", python_callable=run_transform_player_game_logs)
    t_team_logs    = PythonOperator(task_id="transform_team_game_logs",   python_callable=run_transform_team_game_logs)
    t_team_stats   = PythonOperator(task_id="transform_team_season_stats",python_callable=run_transform_team_season_stats)
    t_resolve      = PythonOperator(task_id="resolve_player_ids",         python_callable=run_resolve_player_ids)
    t_link_games   = PythonOperator(task_id="link_nba_game_ids",          python_callable=run_link_nba_game_ids)

    wait_for_ingest >> t_players >> [t_player_logs, t_team_logs, t_team_stats] >> t_resolve >> t_link_games
```

- [ ] **Step 2: Verify DAG imports cleanly**

```bash
python -c "import sys; sys.path.insert(0, '.'); import dags.nba_player_stats_transform_dag"
```
Expected: No output.

- [ ] **Step 3: Commit**

```bash
git add dags/nba_player_stats_transform_dag.py
git commit -m "feat: add nba_player_stats_transform DAG"
```

---

## Task 11: Backfill DAG

**Files:**
- Create: `dags/nba_player_stats_backfill_dag.py`

- [ ] **Step 1: Implement the backfill DAG**

Create `dags/nba_player_stats_backfill_dag.py`:

```python
# dags/nba_player_stats_backfill_dag.py
import re
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from plugins.db_client import get_data_db_conn, store_raw_response
from plugins.nba_api_client import (
    fetch_players,
    fetch_player_game_logs,
    fetch_team_game_logs,
    fetch_team_season_stats,
)
from plugins.transformers.players import transform_players
from plugins.transformers.player_game_logs import transform_player_game_logs
from plugins.transformers.team_game_logs import transform_team_game_logs
from plugins.transformers.team_season_stats import transform_team_season_stats
from plugins.transformers.game_id_linker import link_nba_game_ids

BACKFILL_DELAY_SECONDS = 2.5
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
        # Players only need to be fetched once
        players = fetch_players(delay_seconds=BACKFILL_DELAY_SECONDS)
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
    dag_id="nba_player_stats_backfill",
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
```

- [ ] **Step 2: Verify DAG imports cleanly**

```bash
python -c "import sys; sys.path.insert(0, '.'); import dags.nba_player_stats_backfill_dag"
```
Expected: No output.

- [ ] **Step 3: Commit**

```bash
git add dags/nba_player_stats_backfill_dag.py
git commit -m "feat: add nba_player_stats_backfill DAG"
```

---

## Task 12: Run Full Unit Test Suite

- [ ] **Step 1: Run all unit tests**

```bash
python -m pytest tests/unit/ -v
```
Expected: All existing tests PASS, all new tests PASS. No regressions.

- [ ] **Step 2: Fix any failures before proceeding**

If a test fails, fix it now. Do not proceed to integration tests with a broken unit suite.

- [ ] **Step 3: Commit any fixes**

```bash
git add -p
git commit -m "fix: correct unit test failures"
```

---

## Task 13: Integration Test

**Files:**
- Create: `tests/integration/test_player_stats_pipeline.py`

The integration test requires the `data-postgres` container running. Start it with `docker compose up -d data-postgres` and apply the migration before running.

- [ ] **Step 1: Apply migration to the running database**

```bash
docker compose up -d data-postgres
sleep 3
docker compose exec data-postgres psql -U odds -d odds_db -f /docker-entrypoint-initdb.d/init_schema.sql
# Also apply migration for existing deployments:
cat sql/migrations/001_add_player_stats_tables.sql | docker compose exec -T data-postgres psql -U odds -d odds_db
```

- [ ] **Step 2: Write the integration test**

Create `tests/integration/test_player_stats_pipeline.py`:

```python
# tests/integration/test_player_stats_pipeline.py
"""
Integration tests for the player stats pipeline.
Requires: data-postgres container running (docker compose up -d data-postgres).
Run with: python -m pytest tests/integration/test_player_stats_pipeline.py -v
"""
import pytest
from plugins.db_client import get_data_db_conn
from plugins.transformers.players import transform_players
from plugins.transformers.player_game_logs import transform_player_game_logs
from plugins.transformers.team_game_logs import transform_team_game_logs
from plugins.transformers.team_season_stats import transform_team_season_stats
from plugins.transformers.player_name_resolution import resolve_player_ids
from plugins.transformers.game_id_linker import link_nba_game_ids


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
    stats = [{"team_id": 9999, "team_abbreviation": "TST", "season": "2024-25",
              "pace": 98.0, "off_rating": 110.0, "def_rating": 108.0, "opp_pts_paint_pg": 40.0}]
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
```

- [ ] **Step 3: Add a unit test for the bootstrap path**

Add to `tests/unit/test_nba_api_client.py` (or a new `tests/unit/test_ingest_dag_tasks.py`):

```python
def test_fetch_player_game_logs_task_bootstrap_fetches_all_players():
    """When player_name_mappings and player_game_logs are both empty, all players are fetched."""
    import sys
    sys.path.insert(0, ".")
    from unittest.mock import MagicMock, patch

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    # Both queries return empty — bootstrap mode
    mock_cursor.fetchall.side_effect = [[], []]

    fake_logs = [
        {"player_id": 1, "nba_game_id": "G1", "season": "2024-25", "game_date": "2025-01-01",
         "matchup": "A vs. B", "team_id": 1, "wl": "W", "min": 30.0, "fga": 8, "fta": 2,
         "usg_pct": 0.22, "pts": 15, "reb": 4, "ast": 3, "blk": 0, "stl": 1, "plus_minus": 5},
    ]

    with patch("dags.nba_player_stats_ingest_dag.get_data_db_conn", return_value=mock_conn), \
         patch("dags.nba_player_stats_ingest_dag.fetch_player_game_logs", return_value=fake_logs) as mock_fetch, \
         patch("dags.nba_player_stats_ingest_dag.store_raw_response"):
        from dags.nba_player_stats_ingest_dag import fetch_player_game_logs_task
        fetch_player_game_logs_task(ti=MagicMock())

    # In bootstrap mode (player_ids is empty set), no filtering occurs — all rows are stored
    mock_fetch.assert_called_once()
```

- [ ] **Step 4: Run the integration tests**

```bash
python -m pytest tests/integration/test_player_stats_pipeline.py -v
```
Expected: 4 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_player_stats_pipeline.py
git commit -m "test: add integration tests for player stats pipeline"
```

---

## Task 14: Final Verification

- [ ] **Step 1: Run full test suite**

```bash
python -m pytest tests/unit/ -v
```
Expected: All tests PASS.

- [ ] **Step 2: Verify all three DAG IDs appear in Airflow**

With Airflow running (`docker compose up -d`), open the Airflow UI at http://localhost:8080 and confirm:
- `nba_player_stats_ingest` is present and paused
- `nba_player_stats_transform` is present and paused
- `nba_player_stats_backfill` is present and paused

- [ ] **Step 3: Trigger the backfill manually to seed historical data**

In the Airflow UI, trigger `nba_player_stats_backfill` with default params (`season_start=2022-23`, `season_end=2024-25`). Monitor logs. After completion, verify row counts:

```sql
SELECT COUNT(*) FROM players;             -- expect ~450
SELECT COUNT(*) FROM player_game_logs;    -- expect ~75k+
SELECT COUNT(*) FROM team_game_logs;      -- expect ~7k+
SELECT COUNT(*) FROM team_season_stats;   -- expect ~90 (30 teams × 3 seasons)
```

- [ ] **Step 4: Commit any final cleanup**

```bash
git add .
git commit -m "feat: complete NBA player stats pipeline"
```
