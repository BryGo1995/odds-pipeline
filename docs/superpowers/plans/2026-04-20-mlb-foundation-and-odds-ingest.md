# MLB Foundation & Odds Ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lay the foundation for MLB integration (schema, config, shared helpers, NBA sport-scoping) and ship the MLB odds ingestion path end-to-end: Odds-API → `games`/`odds`/`scores`/`player_props` rows with `sport='MLB'`.

**Architecture:** Sibling `mlb/` vertical mirroring `nba/`. Sport-agnostic transformers (`events`, `odds`, `scores`, `player_props`) are promoted to `shared/` during this plan — they already contain zero sport-specific logic, so moving them removes imminent duplication in the MLB DAG. NBA DAGs are updated to import from the new location. The `recommendations` and `player_props` tables grow a `sport` column / `mlb_player_id` column respectively; NBA's `score.py` and `settle.py` get sport filters so both sports can coexist.

**Tech Stack:** Python 3, Apache Airflow, PostgreSQL, psycopg2, pytest. Follows existing repo conventions (functional transformers, TDD for unit tests, DAG-structure tests, commits per task).

**Spec:** `docs/superpowers/specs/2026-04-20-mlb-integration-design.md`

**Milestone after this plan:** Trigger `mlb_odds_pipeline` in Airflow → MLB events, odds (h2h/spreads/totals), scores, and batter_hits/batter_total_bases/batter_home_runs player props land in Postgres with `sport='MLB'`. NBA pipeline continues to run unchanged in parallel.

---

## File Map

**New files:**
- `shared/plugins/odds_math.py` — pure odds conversion helpers
- `shared/plugins/transformers/__init__.py`
- `shared/plugins/transformers/events.py` — (moved from `nba/plugins/transformers/events.py`)
- `shared/plugins/transformers/odds.py` — (moved + refactored)
- `shared/plugins/transformers/scores.py` — (moved)
- `shared/plugins/transformers/player_props.py` — (moved + refactored)
- `shared/tests/__init__.py`
- `shared/tests/unit/__init__.py`
- `shared/tests/unit/test_odds_math.py`
- `shared/tests/unit/transformers/__init__.py`
- `shared/tests/unit/transformers/test_events.py` — (moved from `nba/tests/unit/transformers/test_events.py`)
- `shared/tests/unit/transformers/test_odds.py` — (moved)
- `shared/tests/unit/transformers/test_scores.py` — (moved)
- `shared/tests/unit/transformers/test_player_props.py` — (moved)
- `sql/migrations/006_mlb_tables.sql`
- `nba/config.py`
- `mlb/__init__.py`
- `mlb/config.py`
- `mlb/dags/__init__.py`
- `mlb/dags/mlb_odds_pipeline_dag.py`
- `mlb/dags/mlb_odds_backfill_dag.py`
- `mlb/tests/__init__.py`
- `mlb/tests/unit/__init__.py`
- `mlb/tests/unit/test_mlb_odds_pipeline_dag.py`
- `mlb/tests/unit/test_mlb_odds_backfill_dag.py`
- `mlb/plugins/__init__.py`

**Modified files:**
- `nba/plugins/transformers/features.py` — import `american_odds_to_implied_prob` from `shared.plugins.odds_math`
- `nba/dags/nba_odds_pipeline_dag.py` — update imports (transformers moved to `shared`, config moved to `nba.config`)
- `nba/dags/nba_odds_backfill_dag.py` — same import updates
- `nba/plugins/ml/score.py` — filter `DELETE`/`INSERT` by `sport='NBA'`
- `nba/plugins/ml/settle.py` — filter `SELECT` by `sport='NBA'`
- `nba/tests/unit/ml/test_score.py` — assert sport column in writes
- `nba/tests/unit/ml/test_settle.py` — assert sport filter in reads
- `pytest.ini` — add `shared/tests` and `mlb/tests` to `testpaths`

**Deleted files:**
- `config/settings.py` — contents split into `nba/config.py` and `mlb/config.py`
- `config/__init__.py` — empty after settings moves
- `nba/plugins/transformers/events.py` — moved to `shared/`
- `nba/plugins/transformers/odds.py` — moved
- `nba/plugins/transformers/scores.py` — moved
- `nba/plugins/transformers/player_props.py` — moved
- `nba/tests/unit/transformers/test_events.py` — moved
- `nba/tests/unit/transformers/test_odds.py` — moved
- `nba/tests/unit/transformers/test_scores.py` — moved
- `nba/tests/unit/transformers/test_player_props.py` — moved

**Note on spec deviation:** The spec said "copy rather than share" for MVP and promoted only `american_odds_to_implied_prob`. On implementation we found that `events.py`, `odds.py`, `scores.py`, and `player_props.py` already contain zero sport-specific logic — they read `sport_key` directly off the Odds-API payload. The only thing preventing them from being shared is two hardcoded market-prefix checks (`startswith("player_")`) that assume NBA prop naming. We replace those checks with a constant `GAME_LEVEL_MARKETS = {"h2h", "spreads", "totals"}` that works for any sport, then promote the files. This is a net simplification (eliminates imminent duplication in the MLB DAG) rather than a factoring exercise, so it respects the spec's intent even while deviating from the letter.

---

## Task 1: Promote `american_odds_to_implied_prob` to `shared/plugins/odds_math.py`

**Files:**
- Create: `shared/plugins/odds_math.py`
- Create: `shared/tests/__init__.py`
- Create: `shared/tests/unit/__init__.py`
- Create: `shared/tests/unit/test_odds_math.py`
- Modify: `nba/plugins/transformers/features.py` (line 24–29, import + remove local definition)
- Modify: `pytest.ini` (add `shared/tests` to `testpaths`)

- [ ] **Step 1.1: Create empty test-package init files**

```bash
mkdir -p shared/tests/unit
touch shared/tests/__init__.py shared/tests/unit/__init__.py
```

- [ ] **Step 1.2: Write the failing test**

Create `shared/tests/unit/test_odds_math.py`:

```python
import math

import pytest

from shared.plugins.odds_math import american_odds_to_implied_prob


def test_negative_odds_favorite():
    # -110 → 110 / (110 + 100) = 0.5238...
    assert math.isclose(american_odds_to_implied_prob(-110), 110 / 210, rel_tol=1e-9)


def test_positive_odds_underdog():
    # +200 → 100 / (200 + 100) = 0.3333...
    assert math.isclose(american_odds_to_implied_prob(200), 100 / 300, rel_tol=1e-9)


def test_plus_one_hundred_even_money():
    assert math.isclose(american_odds_to_implied_prob(100), 0.5, rel_tol=1e-9)


def test_string_input_is_coerced():
    assert math.isclose(american_odds_to_implied_prob("-110"), 110 / 210, rel_tol=1e-9)


@pytest.mark.parametrize("price,expected", [
    (-200, 200 / 300),
    (-150, 150 / 250),
    (+150, 100 / 250),
    (+500, 100 / 600),
])
def test_known_pairs(price, expected):
    assert math.isclose(american_odds_to_implied_prob(price), expected, rel_tol=1e-9)
```

- [ ] **Step 1.3: Update `pytest.ini` to discover the new test path**

Replace the contents of `pytest.ini` with:

```ini
[pytest]
testpaths = nba/tests shared/tests
pythonpath = .
```

- [ ] **Step 1.4: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_odds_math.py -v`
Expected: all tests FAIL with `ModuleNotFoundError: No module named 'shared.plugins.odds_math'`

- [ ] **Step 1.5: Create the shared module**

Create `shared/plugins/odds_math.py`:

```python
"""Pure math helpers for converting between odds formats and probabilities.

Sport-agnostic — no imports from sport-specific packages.
"""


def american_odds_to_implied_prob(price) -> float:
    """Convert American odds (e.g. -110, +200) to implied probability in [0, 1]."""
    price = float(price)
    if price > 0:
        return 100.0 / (price + 100.0)
    return abs(price) / (abs(price) + 100.0)
```

- [ ] **Step 1.6: Run test to verify it passes**

Run: `pytest shared/tests/unit/test_odds_math.py -v`
Expected: all 7 tests (4 named + 3 parametrize combos = actually 4 + 4 = 8; adjust count after run) PASS

- [ ] **Step 1.7: Update NBA features.py to import from shared**

In `nba/plugins/transformers/features.py`:

Replace:
```python
def american_odds_to_implied_prob(price) -> float:
    """Convert American odds (e.g. -110, +200) to implied probability."""
    price = float(price)
    if price > 0:
        return 100.0 / (price + 100.0)
    return abs(price) / (abs(price) + 100.0)
```

With:
```python
from shared.plugins.odds_math import american_odds_to_implied_prob
```

Place the new import at the top of the file with the other imports (after `import pandas as pd`).

- [ ] **Step 1.8: Run the existing NBA features tests**

Run: `pytest nba/tests/unit/transformers/test_features.py -v`
Expected: all tests PASS (behavior is unchanged; only the import source moved)

- [ ] **Step 1.9: Commit**

```bash
git add shared/plugins/odds_math.py \
        shared/tests/__init__.py \
        shared/tests/unit/__init__.py \
        shared/tests/unit/test_odds_math.py \
        pytest.ini \
        nba/plugins/transformers/features.py
git commit -m "refactor: promote american_odds_to_implied_prob to shared.odds_math"
```

---

## Task 2: Move `events.py` and `scores.py` transformers to `shared/`

These are already sport-agnostic (no MLB/NBA-specific branches). We move them as-is.

**Files:**
- Create: `shared/plugins/transformers/__init__.py`
- Create: `shared/plugins/transformers/events.py` (content moved from `nba/plugins/transformers/events.py`)
- Create: `shared/plugins/transformers/scores.py` (content moved from `nba/plugins/transformers/scores.py`)
- Create: `shared/tests/unit/transformers/__init__.py`
- Create: `shared/tests/unit/transformers/test_events.py` (content moved)
- Create: `shared/tests/unit/transformers/test_scores.py` (content moved)
- Delete: `nba/plugins/transformers/events.py`
- Delete: `nba/plugins/transformers/scores.py`
- Delete: `nba/tests/unit/transformers/test_events.py`
- Delete: `nba/tests/unit/transformers/test_scores.py`
- Modify: `nba/dags/nba_odds_pipeline_dag.py` (imports)
- Modify: `nba/dags/nba_odds_backfill_dag.py` (imports)

- [ ] **Step 2.1: Create shared transformers package**

```bash
mkdir -p shared/plugins/transformers shared/tests/unit/transformers
touch shared/plugins/transformers/__init__.py shared/tests/unit/transformers/__init__.py
```

- [ ] **Step 2.2: Move events.py and scores.py using git mv**

```bash
git mv nba/plugins/transformers/events.py shared/plugins/transformers/events.py
git mv nba/plugins/transformers/scores.py shared/plugins/transformers/scores.py
git mv nba/tests/unit/transformers/test_events.py shared/tests/unit/transformers/test_events.py
git mv nba/tests/unit/transformers/test_scores.py shared/tests/unit/transformers/test_scores.py
```

- [ ] **Step 2.3: Update test imports in moved test files**

Both `shared/tests/unit/transformers/test_events.py` and `shared/tests/unit/transformers/test_scores.py` currently import from `nba.plugins.transformers`. Change each:

In `shared/tests/unit/transformers/test_events.py`, replace:
```python
from nba.plugins.transformers.events import transform_events
```
with:
```python
from shared.plugins.transformers.events import transform_events
```

In `shared/tests/unit/transformers/test_scores.py`, replace:
```python
from nba.plugins.transformers.scores import transform_scores
```
with:
```python
from shared.plugins.transformers.scores import transform_scores
```

- [ ] **Step 2.4: Update NBA DAG imports**

In `nba/dags/nba_odds_pipeline_dag.py`, replace:
```python
from nba.plugins.transformers.events import transform_events
from nba.plugins.transformers.odds import transform_odds
from nba.plugins.transformers.player_props import transform_player_props
from nba.plugins.transformers.scores import transform_scores
```
with:
```python
from shared.plugins.transformers.events import transform_events
from shared.plugins.transformers.scores import transform_scores
from nba.plugins.transformers.odds import transform_odds
from nba.plugins.transformers.player_props import transform_player_props
```

(The `odds` and `player_props` transformers move in Task 3; don't touch their imports yet.)

In `nba/dags/nba_odds_backfill_dag.py`, replace:
```python
from nba.plugins.transformers.events import transform_events
from nba.plugins.transformers.odds import transform_odds
from nba.plugins.transformers.scores import transform_scores
from nba.plugins.transformers.player_props import transform_player_props
```
with:
```python
from shared.plugins.transformers.events import transform_events
from shared.plugins.transformers.scores import transform_scores
from nba.plugins.transformers.odds import transform_odds
from nba.plugins.transformers.player_props import transform_player_props
```

- [ ] **Step 2.5: Run the moved tests and the NBA DAG tests**

Run: `pytest shared/tests/unit/transformers/test_events.py shared/tests/unit/transformers/test_scores.py nba/tests/unit/test_nba_odds_pipeline_dag.py nba/tests/unit/test_nba_odds_backfill_dag.py -v`
Expected: all PASS

- [ ] **Step 2.6: Commit**

```bash
git add -A  # includes deletes, moves, and DAG import updates
git commit -m "refactor: promote events and scores transformers to shared"
```

---

## Task 3: Move `odds.py` and `player_props.py` to `shared/` with market-prefix refactor

These two transformers currently hardcode `startswith("player_")` — an NBA-specific assumption that would break MLB (MLB prop markets start with `batter_`). Replace with a positive-list constant of game-level markets.

**Files:**
- Create: `shared/plugins/transformers/odds.py`
- Create: `shared/plugins/transformers/player_props.py`
- Create: `shared/tests/unit/transformers/test_odds.py`
- Create: `shared/tests/unit/transformers/test_player_props.py`
- Delete: `nba/plugins/transformers/odds.py`
- Delete: `nba/plugins/transformers/player_props.py`
- Delete: `nba/tests/unit/transformers/test_odds.py`
- Delete: `nba/tests/unit/transformers/test_player_props.py`
- Modify: `nba/dags/nba_odds_pipeline_dag.py` (imports)
- Modify: `nba/dags/nba_odds_backfill_dag.py` (imports)

- [ ] **Step 3.1: git mv the source and test files**

```bash
git mv nba/plugins/transformers/odds.py shared/plugins/transformers/odds.py
git mv nba/plugins/transformers/player_props.py shared/plugins/transformers/player_props.py
git mv nba/tests/unit/transformers/test_odds.py shared/tests/unit/transformers/test_odds.py
git mv nba/tests/unit/transformers/test_player_props.py shared/tests/unit/transformers/test_player_props.py
```

- [ ] **Step 3.2: Update test imports in moved test files**

In `shared/tests/unit/transformers/test_odds.py`, replace:
```python
from nba.plugins.transformers.odds import transform_odds
```
with:
```python
from shared.plugins.transformers.odds import transform_odds
```

In `shared/tests/unit/transformers/test_player_props.py`, replace:
```python
from nba.plugins.transformers.player_props import transform_player_props
```
with:
```python
from shared.plugins.transformers.player_props import transform_player_props
```

- [ ] **Step 3.3: Run existing tests to confirm the move didn't break anything**

Run: `pytest shared/tests/unit/transformers/test_odds.py shared/tests/unit/transformers/test_player_props.py -v`
Expected: all PASS

- [ ] **Step 3.4: Add a failing test for MLB market routing**

Append to `shared/tests/unit/transformers/test_odds.py`:

```python
def test_odds_skips_mlb_batter_prop_markets(mock_conn):
    """batter_* markets should be skipped by transform_odds (they're player-level, not game-level)."""
    raw = [{
        "id": "mlb-game-1",
        "bookmakers": [{
            "key": "draftkings",
            "markets": [
                {
                    "key": "h2h",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [
                        {"name": "Home", "price": -120},
                        {"name": "Away", "price": +100},
                    ],
                },
                {
                    "key": "batter_hits",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [
                        {"description": "Mookie Betts", "name": "Over", "price": -110, "point": 1.5},
                    ],
                },
            ],
        }],
    }]

    transform_odds(mock_conn, raw)

    # Only h2h should be inserted into odds; batter_hits should be skipped
    inserts = [
        call.args for call in mock_conn.cursor.return_value.__enter__.return_value.execute.call_args_list
        if "INSERT INTO odds" in call.args[0]
    ]
    assert len(inserts) == 2  # 2 h2h outcomes
    for _, params in inserts:
        assert params[2] == "h2h"  # market_type column
```

Append to `shared/tests/unit/transformers/test_player_props.py`:

```python
def test_player_props_includes_mlb_batter_markets(mock_conn):
    """batter_* markets should be inserted into player_props."""
    raw = [{
        "id": "mlb-game-1",
        "bookmakers": [{
            "key": "draftkings",
            "markets": [
                {
                    "key": "h2h",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [{"name": "Home", "price": -120}],
                },
                {
                    "key": "batter_hits",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [
                        {"description": "Mookie Betts", "name": "Over", "price": -110, "point": 1.5},
                    ],
                },
                {
                    "key": "batter_home_runs",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [
                        {"description": "Aaron Judge", "name": "Over", "price": +300, "point": 0.5},
                    ],
                },
            ],
        }],
    }]

    transform_player_props(mock_conn, raw)

    inserts = [
        call.args for call in mock_conn.cursor.return_value.__enter__.return_value.execute.call_args_list
        if "INSERT INTO player_props" in call.args[0]
    ]
    # h2h is skipped; 1 batter_hits outcome + 1 batter_home_runs outcome
    assert len(inserts) == 2
    prop_types = {params[3] for _, params in inserts}
    assert prop_types == {"batter_hits", "batter_home_runs"}
```

Note: if the existing tests in those files do not use a `mock_conn` fixture, check the top of each test file. If they build their own `MagicMock` inline, mirror the same pattern in the new tests (copy the setup from an existing test in the same file). The point of this step is adding one behavior-change test per transformer — adjust the mock construction to match the file's existing style.

- [ ] **Step 3.5: Run the new tests to verify they fail**

Run: `pytest shared/tests/unit/transformers/test_odds.py::test_odds_skips_mlb_batter_prop_markets shared/tests/unit/transformers/test_player_props.py::test_player_props_includes_mlb_batter_markets -v`
Expected: FAIL — `transform_player_props` currently requires `market_key.startswith("player_")`, so MLB `batter_*` markets are skipped. `transform_odds` currently skips only `player_*`, so MLB `batter_*` markets would be treated as game-level (asserted count mismatch).

- [ ] **Step 3.6: Refactor odds.py with GAME_LEVEL_MARKETS**

Replace the full contents of `shared/plugins/transformers/odds.py` with:

```python
"""
Transforms raw odds data into the normalized `odds` table.

Note: This transformer uses plain INSERT (no ON CONFLICT) because the odds table
is an append-only historical log — each fetch represents a point-in-time snapshot
of lines, enabling line movement tracking. The transform DAG is designed to run
once per ingest cycle; running it multiple times against the same raw data will
create duplicate rows.
"""

# Game-level markets go into `odds`. Anything not in this set is treated as a
# player-level / prop market and routed to `player_props` by a separate transformer.
GAME_LEVEL_MARKETS = {"h2h", "spreads", "totals"}


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
                    if market_key not in GAME_LEVEL_MARKETS:
                        continue  # player/prop markets handled by transform_player_props
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

- [ ] **Step 3.7: Refactor player_props.py to use the same constant**

Replace the full contents of `shared/plugins/transformers/player_props.py` with:

```python
"""
Transforms raw odds data into the normalized `player_props` table.

Note: This transformer uses plain INSERT (no ON CONFLICT) because player_props
is an append-only historical log for tracking line movement over time. The
transform DAG should run once per ingest cycle to avoid duplicate rows.
"""
from shared.plugins.transformers.odds import GAME_LEVEL_MARKETS


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
                    if market_key in GAME_LEVEL_MARKETS:
                        continue  # game-level markets handled by transform_odds
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
                                outcome.get("description"),  # player name is in description field
                                market_key,
                                outcome["name"],             # Over / Under
                                outcome.get("price"),
                                outcome.get("point"),
                                last_update,
                            ),
                        )
    conn.commit()
```

- [ ] **Step 3.8: Run all transformer tests**

Run: `pytest shared/tests/unit/transformers/ -v`
Expected: all PASS, including the two new tests from Step 3.4.

- [ ] **Step 3.9: Update remaining NBA DAG imports**

In `nba/dags/nba_odds_pipeline_dag.py`, replace:
```python
from nba.plugins.transformers.odds import transform_odds
from nba.plugins.transformers.player_props import transform_player_props
```
with:
```python
from shared.plugins.transformers.odds import transform_odds
from shared.plugins.transformers.player_props import transform_player_props
```

In `nba/dags/nba_odds_backfill_dag.py`, replace:
```python
from nba.plugins.transformers.odds import transform_odds
from nba.plugins.transformers.player_props import transform_player_props
```
with:
```python
from shared.plugins.transformers.odds import transform_odds
from shared.plugins.transformers.player_props import transform_player_props
```

- [ ] **Step 3.10: Run NBA DAG tests**

Run: `pytest nba/tests/unit/test_nba_odds_pipeline_dag.py nba/tests/unit/test_nba_odds_backfill_dag.py -v`
Expected: PASS

- [ ] **Step 3.11: Commit**

```bash
git add -A
git commit -m "refactor: promote odds and player_props transformers to shared; use GAME_LEVEL_MARKETS set for sport-agnostic routing"
```

---

## Task 4: Database migration `006_mlb_tables.sql`

Adds MLB stats tables, widens `player_props` with `mlb_player_id`, and adds `sport` column to `recommendations`.

**Files:**
- Create: `sql/migrations/006_mlb_tables.sql`

- [ ] **Step 4.1: Write the migration file**

Create `sql/migrations/006_mlb_tables.sql`:

```sql
-- Migration 006: MLB foundation tables + sport-scoping of shared tables
-- Idempotent: safe to run multiple times.

-- ---------------------------------------------------------------------------
-- MLB-specific tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mlb_players (
    player_id         INT PRIMARY KEY,
    full_name         TEXT NOT NULL,
    position          TEXT,
    bats              TEXT,
    throws            TEXT,
    team_id           INT,
    team_abbreviation TEXT,
    is_active         BOOLEAN,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mlb_players_full_name ON mlb_players (full_name);

CREATE TABLE IF NOT EXISTS mlb_player_game_logs (
    id                SERIAL PRIMARY KEY,
    player_id         INT REFERENCES mlb_players(player_id),
    mlb_game_pk       TEXT NOT NULL,
    season            TEXT NOT NULL,
    game_date         DATE NOT NULL,
    matchup           TEXT,
    team_id           INT,
    opponent_team_id  INT,
    plate_appearances INT,
    at_bats           INT,
    hits              INT,
    doubles           INT,
    triples           INT,
    home_runs         INT,
    rbi               INT,
    runs              INT,
    walks             INT,
    strikeouts        INT,
    stolen_bases      INT,
    total_bases       INT,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, mlb_game_pk)
);

CREATE INDEX IF NOT EXISTS idx_mlb_pgl_player_id ON mlb_player_game_logs (player_id);
CREATE INDEX IF NOT EXISTS idx_mlb_pgl_game_date ON mlb_player_game_logs (game_date);
CREATE INDEX IF NOT EXISTS idx_mlb_pgl_season    ON mlb_player_game_logs (season);

CREATE TABLE IF NOT EXISTS mlb_teams (
    team_id      INT PRIMARY KEY,
    full_name    TEXT NOT NULL,
    abbreviation TEXT NOT NULL,
    league       TEXT,
    division     TEXT,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mlb_player_name_mappings (
    odds_api_name TEXT PRIMARY KEY,
    mlb_player_id INT REFERENCES mlb_players(player_id),
    confidence    FLOAT,
    verified      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Widen player_props with mlb_player_id (one of {nba_player_id, mlb_player_id}
-- is non-null per row; games.sport tells queries which to read)
-- ---------------------------------------------------------------------------

ALTER TABLE player_props
  ADD COLUMN IF NOT EXISTS mlb_player_id INT REFERENCES mlb_players(player_id);

CREATE INDEX IF NOT EXISTS idx_player_props_mlb_player ON player_props (mlb_player_id);

-- ---------------------------------------------------------------------------
-- Scope recommendations by sport
-- Add column with DEFAULT 'NBA' (backfills existing rows), then drop the
-- default so future inserts must specify sport explicitly.
-- ---------------------------------------------------------------------------

ALTER TABLE recommendations
  ADD COLUMN IF NOT EXISTS sport VARCHAR(20) NOT NULL DEFAULT 'NBA';

ALTER TABLE recommendations
  ALTER COLUMN sport DROP DEFAULT;

CREATE INDEX IF NOT EXISTS idx_recs_sport_date ON recommendations (sport, game_date);
```

- [ ] **Step 4.2: Apply the migration to the dev Postgres**

Start the data database if not already running:

```bash
docker compose up data-postgres -d
```

Apply the migration. Use the same DB connection values as `.env`:

```bash
docker exec -i $(docker compose ps -q data-postgres) psql -U odds -d odds_db < sql/migrations/006_mlb_tables.sql
```

Expected output (trailing whitespace may differ):

```
CREATE TABLE
CREATE INDEX
CREATE TABLE
CREATE INDEX
CREATE INDEX
CREATE INDEX
CREATE TABLE
CREATE TABLE
ALTER TABLE
CREATE INDEX
ALTER TABLE
ALTER TABLE
CREATE INDEX
```

(Second run will show `NOTICE` messages about existing objects; that's the idempotence working.)

- [ ] **Step 4.3: Verify the schema**

```bash
docker exec -i $(docker compose ps -q data-postgres) psql -U odds -d odds_db -c "\d mlb_players"
docker exec -i $(docker compose ps -q data-postgres) psql -U odds -d odds_db -c "\d mlb_player_game_logs"
docker exec -i $(docker compose ps -q data-postgres) psql -U odds -d odds_db -c "\d recommendations" | grep -E "sport|idx_recs"
docker exec -i $(docker compose ps -q data-postgres) psql -U odds -d odds_db -c "\d player_props" | grep -E "mlb_player_id|idx_player_props_mlb"
```

Expected: `mlb_players` has columns `player_id`, `full_name`, `position`, `bats`, `throws`, `team_id`, `team_abbreviation`, `is_active`, `fetched_at`; `mlb_player_game_logs` has the batting stat columns; `recommendations` shows the `sport` column and the `idx_recs_sport_date` index; `player_props` shows `mlb_player_id` and the associated index.

- [ ] **Step 4.4: Run the schema test suite (exercises real Postgres)**

Run: `pytest nba/tests/unit/test_schema.py -v`
Expected: PASS — the schema test inspects init_schema.sql structure and should be unaffected by the new migration. If a test fails because it asserts an exact column count on `recommendations` or `player_props`, update that assertion.

- [ ] **Step 4.5: Commit**

```bash
git add sql/migrations/006_mlb_tables.sql nba/tests/unit/test_schema.py
git commit -m "feat: add migration 006 for MLB tables and sport scoping"
```

(If `test_schema.py` didn't need changes, omit it from `git add`.)

---

## Task 5: Sport-scope NBA `score.py` and `settle.py`

The `recommendations` table now has a `sport` column. NBA's writes and reads need to be filtered to `sport='NBA'` so they don't wipe or read MLB rows.

**Files:**
- Modify: `nba/plugins/ml/score.py` (the DELETE and INSERT statements)
- Modify: `nba/plugins/ml/settle.py` (the SELECT in `settle_recommendations`)
- Modify: `nba/tests/unit/ml/test_score.py` (update assertions)
- Modify: `nba/tests/unit/test_slack_notifier_settle.py` and/or `nba/tests/unit/ml/test_settle.py` (update assertions)

- [ ] **Step 5.1: Update a test in `test_score.py` to assert the sport filter**

Open `nba/tests/unit/ml/test_score.py`. Find the test that verifies the `DELETE FROM recommendations` call (search for `DELETE FROM recommendations`). Update the assertion so it now expects the query to contain both `game_date = %s` and `sport = 'NBA'`. Also update the test that asserts the INSERT column list to expect a `sport` column.

Concretely, where the existing test has (example — actual wording may vary slightly):
```python
cur.execute.assert_any_call(
    "DELETE FROM recommendations WHERE game_date = %s", (game_date,)
)
```
change it to:
```python
cur.execute.assert_any_call(
    "DELETE FROM recommendations WHERE game_date = %s AND sport = 'NBA'", (game_date,)
)
```

And where the INSERT is asserted, the column list should now include `sport` as the final column and the row tuple should end in `'NBA'`.

If the existing test uses a looser assertion (e.g. just checks that `execute` was called N times), add one new test:

```python
def test_score_writes_sport_nba(tmp_path, ...):
    """Every row written to recommendations must carry sport='NBA'."""
    # (Use the same setup helpers as the existing tests in this file.)
    # After calling score(), inspect the execute calls:
    insert_calls = [
        c for c in mock_conn.cursor.return_value.__enter__.return_value.execute.call_args_list
        if "INSERT INTO recommendations" in c.args[0]
    ]
    assert insert_calls, "expected at least one INSERT"
    for call in insert_calls:
        sql, params = call.args
        assert "sport" in sql.lower()
        assert params[-1] == "NBA"
```

(The exact fixture/setup code depends on what the file already has — mirror the style of nearby tests.)

- [ ] **Step 5.2: Run the updated test to verify it fails**

Run: `pytest nba/tests/unit/ml/test_score.py -v`
Expected: the updated/new assertion FAILS because `score.py` still emits the old SQL without sport.

- [ ] **Step 5.3: Patch `nba/plugins/ml/score.py`**

Open `nba/plugins/ml/score.py`. Locate the `DELETE FROM recommendations` and the subsequent `INSERT INTO recommendations` in the `score()` function.

Change:
```python
cur.execute("DELETE FROM recommendations WHERE game_date = %s", (game_date,))
```
to:
```python
cur.execute("DELETE FROM recommendations WHERE game_date = %s AND sport = 'NBA'", (game_date,))
```

Change the INSERT from:
```python
cur.execute(
    """
    INSERT INTO recommendations
        (player_name, prop_type, bookmaker, line, outcome,
         model_prob, implied_prob, edge, rank, model_version, game_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (
        row["player_name"],
        row["prop_type"],
        row["bookmaker"],
        float(row["line"]),
        row["outcome"],
        float(row["model_prob"]),
        float(row["implied_prob"]),
        float(row["edge"]),
        int(row["rank"]),
        str(row["model_version"]),
        game_date,
    ),
)
```
to:
```python
cur.execute(
    """
    INSERT INTO recommendations
        (player_name, prop_type, bookmaker, line, outcome,
         model_prob, implied_prob, edge, rank, model_version, game_date, sport)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (
        row["player_name"],
        row["prop_type"],
        row["bookmaker"],
        float(row["line"]),
        row["outcome"],
        float(row["model_prob"]),
        float(row["implied_prob"]),
        float(row["edge"]),
        int(row["rank"]),
        str(row["model_version"]),
        game_date,
        "NBA",
    ),
)
```

- [ ] **Step 5.4: Run score tests to verify they pass**

Run: `pytest nba/tests/unit/ml/test_score.py -v`
Expected: PASS

- [ ] **Step 5.5: Update `nba/plugins/ml/settle.py` with sport filter**

In `nba/plugins/ml/settle.py`, locate the main SELECT in `settle_recommendations`:

```sql
SELECT
    r.id,
    r.player_name,
    r.prop_type,
    r.line,
    r.game_date,
    pgl.pts,
    pgl.reb,
    pgl.ast,
    pgl.fg3m,
    pgl.fg3a
FROM recommendations r
JOIN player_name_mappings m ON m.odds_api_name = r.player_name
JOIN player_game_logs pgl
    ON pgl.player_id = m.nba_player_id
   AND pgl.game_date = r.game_date
WHERE r.settled_at IS NULL
  AND r.game_date < CURRENT_DATE
```

Add one line to the WHERE clause: `AND r.sport = 'NBA'`. Final WHERE:

```sql
WHERE r.settled_at IS NULL
  AND r.game_date < CURRENT_DATE
  AND r.sport = 'NBA'
```

Also update the stale-recs SELECT in the same function:

```sql
SELECT id FROM recommendations
WHERE settled_at IS NULL
  AND game_date < CURRENT_DATE - INTERVAL '7 days'
```

becomes:

```sql
SELECT id FROM recommendations
WHERE settled_at IS NULL
  AND game_date < CURRENT_DATE - INTERVAL '7 days'
  AND sport = 'NBA'
```

Also update the `_notify_completed_dates` SELECT so it filters by sport:

```sql
SELECT DISTINCT game_date
FROM recommendations
WHERE game_date = ANY(%s)
  AND rank <= 10
GROUP BY game_date
HAVING COUNT(*) FILTER (WHERE settled_at IS NULL) = 0
```

becomes:

```sql
SELECT DISTINCT game_date
FROM recommendations
WHERE game_date = ANY(%s)
  AND rank <= 10
  AND sport = 'NBA'
GROUP BY game_date
HAVING COUNT(*) FILTER (WHERE settled_at IS NULL) = 0
```

And `_send_recap`'s SELECT:

```sql
SELECT player_name, prop_type, line, outcome, actual_result,
       actual_stat_value, edge
FROM recommendations
WHERE game_date = %s AND rank <= 10
ORDER BY rank
```

becomes:

```sql
SELECT player_name, prop_type, line, outcome, actual_result,
       actual_stat_value, edge
FROM recommendations
WHERE game_date = %s AND rank <= 10 AND sport = 'NBA'
ORDER BY rank
```

- [ ] **Step 5.6: Update settle tests**

In `nba/tests/unit/ml/test_settle.py` (and `test_slack_notifier_settle.py` if it asserts SELECT text), any test that asserts on the query text needs to include the `sport = 'NBA'` filter. Use `grep`-style assertions to avoid over-specifying:

```python
# Instead of exact-match assertions, use substring checks
assert "r.sport = 'NBA'" in emitted_sql  # for the main settle SELECT
assert "sport = 'NBA'" in stale_sql      # for the stale-recs SELECT
```

If the current tests use exact-match `assert_called_with` on SQL strings, update them to match the new SQL verbatim.

- [ ] **Step 5.7: Run settle tests**

Run: `pytest nba/tests/unit/ml/test_settle.py nba/tests/unit/test_slack_notifier_settle.py -v`
Expected: PASS

- [ ] **Step 5.8: Run the full NBA test suite as a regression check**

Run: `pytest nba/tests/ -v --ignore=nba/tests/integration`
Expected: all PASS

- [ ] **Step 5.9: Commit**

```bash
git add nba/plugins/ml/score.py nba/plugins/ml/settle.py nba/tests/unit/ml/ nba/tests/unit/test_slack_notifier_settle.py
git commit -m "feat: sport-scope NBA score and settle for MLB coexistence"
```

---

## Task 6: Config split — `nba/config.py` + `mlb/config.py`; remove `config/settings.py`

**Files:**
- Create: `nba/config.py`
- Create: `mlb/__init__.py`
- Create: `mlb/config.py`
- Modify: `nba/dags/nba_odds_pipeline_dag.py` (import)
- Modify: `nba/dags/nba_odds_backfill_dag.py` (import)
- Modify: any other file in `nba/` that imports from `config.settings` (sweep step below)
- Delete: `config/settings.py`
- Delete: `config/__init__.py` (if empty after move)

- [ ] **Step 6.1: Create `nba/config.py`**

Create `nba/config.py`:

```python
# nba/config.py
# NBA-specific configuration for odds-pipeline.

SPORT = "basketball_nba"

REGIONS = ["us"]

# Markets to fetch from Odds-API.
# Add or remove to control API quota usage.
# Options: h2h, spreads, totals
MARKETS = [
    "h2h",
    "spreads",
    "totals",
]

PLAYER_PROP_MARKETS = [
    "player_points",
    "player_rebounds",
    "player_assists",
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

- [ ] **Step 6.2: Create the `mlb/` package skeleton**

```bash
mkdir -p mlb
touch mlb/__init__.py
```

- [ ] **Step 6.3: Create `mlb/config.py`**

Create `mlb/config.py`:

```python
# mlb/config.py
# MLB-specific configuration for odds-pipeline.

SPORT = "baseball_mlb"

REGIONS = ["us"]

# Game-level markets to fetch from Odds-API.
MARKETS = [
    "h2h",
    "spreads",
    "totals",
]

# Batter player-prop markets — MVP targets these three.
# Post-MVP: batter_rbis, batter_runs_scored, batter_stolen_bases,
#           batter_hits_runs_rbis; plus pitcher props.
PLAYER_PROP_MARKETS = [
    "batter_hits",
    "batter_total_bases",
    "batter_home_runs",
]

BOOKMAKERS = [
    "draftkings",
    "fanduel",
    "betmgm",
]

ODDS_FORMAT = "american"

# How many days back to fetch scores for
SCORES_DAYS_FROM = 3
```

- [ ] **Step 6.4: Find every file that imports from `config.settings`**

Run: `grep -rn "from config.settings" nba shared mlb 2>/dev/null`

Expected output (verify before step 6.5):
```
nba/dags/nba_odds_pipeline_dag.py:11:from config.settings import (
nba/dags/nba_odds_backfill_dag.py:10:from config.settings import SPORT, REGIONS, MARKETS, BOOKMAKERS
```

If other NBA DAGs appear in the grep, include them in Step 6.5. The stats/feature/train/score DAGs currently don't import from `config.settings`, so only the two odds DAGs should need updating.

- [ ] **Step 6.5: Update NBA DAG imports**

In `nba/dags/nba_odds_pipeline_dag.py`, replace:
```python
from config.settings import (
    SPORT, REGIONS, MARKETS, PLAYER_PROP_MARKETS, BOOKMAKERS, ODDS_FORMAT, SCORES_DAYS_FROM,
)
```
with:
```python
from nba.config import (
    SPORT, REGIONS, MARKETS, PLAYER_PROP_MARKETS, BOOKMAKERS, ODDS_FORMAT, SCORES_DAYS_FROM,
)
```

In `nba/dags/nba_odds_backfill_dag.py`, replace:
```python
from config.settings import SPORT, REGIONS, MARKETS, BOOKMAKERS
```
with:
```python
from nba.config import SPORT, REGIONS, MARKETS, BOOKMAKERS
```

- [ ] **Step 6.6: Delete old config directory**

```bash
git rm config/settings.py
# If config/__init__.py exists and is empty, remove it too:
[ -f config/__init__.py ] && [ ! -s config/__init__.py ] && git rm config/__init__.py || true
rmdir config 2>/dev/null || true
```

- [ ] **Step 6.7: Add a unit test that both configs load**

Create `nba/tests/unit/test_config.py`:

```python
def test_nba_config_imports():
    from nba.config import (
        SPORT, REGIONS, MARKETS, PLAYER_PROP_MARKETS, BOOKMAKERS,
        ODDS_FORMAT, SCORES_DAYS_FROM,
    )
    assert SPORT == "basketball_nba"
    assert "player_points" in PLAYER_PROP_MARKETS
    assert "h2h" in MARKETS
```

Create `mlb/tests/__init__.py`, `mlb/tests/unit/__init__.py`, and `mlb/tests/unit/test_config.py`:

```bash
mkdir -p mlb/tests/unit
touch mlb/tests/__init__.py mlb/tests/unit/__init__.py
```

`mlb/tests/unit/test_config.py`:

```python
def test_mlb_config_imports():
    from mlb.config import (
        SPORT, REGIONS, MARKETS, PLAYER_PROP_MARKETS, BOOKMAKERS,
        ODDS_FORMAT, SCORES_DAYS_FROM,
    )
    assert SPORT == "baseball_mlb"
    assert PLAYER_PROP_MARKETS == ["batter_hits", "batter_total_bases", "batter_home_runs"]
    assert "h2h" in MARKETS
```

- [ ] **Step 6.8: Update `pytest.ini` to include `mlb/tests`**

Replace `pytest.ini` contents with:

```ini
[pytest]
testpaths = nba/tests shared/tests mlb/tests
pythonpath = .
```

- [ ] **Step 6.9: Run all tests**

Run: `pytest nba/tests/unit shared/tests mlb/tests -v --ignore=nba/tests/integration`
Expected: all PASS (including the two new config tests)

- [ ] **Step 6.10: Commit**

```bash
git add -A
git commit -m "refactor: split config into nba/config.py and mlb/config.py"
```

---

## Task 7: `mlb_odds_pipeline_dag.py`

Mirror of `nba_odds_pipeline_dag.py` but uses `mlb.config` and writes MLB rows. Reuses shared transformers.

**Files:**
- Create: `mlb/dags/__init__.py`
- Create: `mlb/dags/mlb_odds_pipeline_dag.py`
- Create: `mlb/tests/unit/test_mlb_odds_pipeline_dag.py`

- [ ] **Step 7.1: Create `mlb/dags/` package**

```bash
mkdir -p mlb/dags
touch mlb/dags/__init__.py
```

- [ ] **Step 7.2: Write the failing DAG-structure test**

Create `mlb/tests/unit/test_mlb_odds_pipeline_dag.py`:

```python
import importlib
import pytest


@pytest.fixture(scope="module")
def dag():
    module = importlib.import_module("mlb.dags.mlb_odds_pipeline_dag")
    return module.dag


def test_dag_id(dag):
    assert dag.dag_id == "mlb_odds_pipeline"


def test_dag_schedule_and_tags(dag):
    assert dag.schedule_interval == "0 15 * * *"  # 8am MT
    assert "mlb" in dag.tags
    assert "odds" in dag.tags


def test_expected_task_ids(dag):
    expected = {
        "fetch_events",
        "fetch_odds",
        "fetch_scores",
        "transform_events",
        "transform_odds",
        "transform_scores",
        "fetch_player_props",
        "transform_player_props",
    }
    assert set(dag.task_dict.keys()) == expected


def test_fetch_props_depends_on_transform_events(dag):
    fetch_props = dag.get_task("fetch_player_props")
    upstream_ids = {t.task_id for t in fetch_props.upstream_list}
    assert "transform_events" in upstream_ids


def test_transform_props_depends_on_fetch_props(dag):
    transform_props = dag.get_task("transform_player_props")
    upstream_ids = {t.task_id for t in transform_props.upstream_list}
    assert "fetch_player_props" in upstream_ids
```

- [ ] **Step 7.3: Run to verify the test fails**

Run: `pytest mlb/tests/unit/test_mlb_odds_pipeline_dag.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mlb.dags.mlb_odds_pipeline_dag'`

- [ ] **Step 7.4: Create the DAG**

Create `mlb/dags/mlb_odds_pipeline_dag.py`:

```python
# mlb/dags/mlb_odds_pipeline_dag.py
import logging
import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from mlb.config import (
    SPORT, REGIONS, MARKETS, PLAYER_PROP_MARKETS, BOOKMAKERS, ODDS_FORMAT, SCORES_DAYS_FROM,
)
from shared.plugins.db_client import get_data_db_conn, store_raw_response
from shared.plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores, fetch_player_props
from shared.plugins.slack_notifier import notify_failure
from shared.plugins.transformers.events import transform_events
from shared.plugins.transformers.odds import transform_odds
from shared.plugins.transformers.player_props import transform_player_props
from shared.plugins.transformers.scores import transform_scores


# Namespace raw_api_responses.endpoint values so NBA and MLB rows don't collide.
EP_EVENTS = "mlb_events"
EP_ODDS = "mlb_odds"
EP_SCORES = "mlb_scores"
EP_PLAYER_PROPS = "mlb_player_props"


# ---------------------------------------------------------------------------
# Shared ingest helper
# ---------------------------------------------------------------------------

def _fetch_and_store(endpoint_name, fetch_fn, fetch_kwargs, ti):
    api_key = os.environ["ODDS_API_KEY"]
    conn = get_data_db_conn()
    try:
        data, remaining = fetch_fn(api_key=api_key, **fetch_kwargs)
        store_raw_response(conn, endpoint=endpoint_name, params=fetch_kwargs, response=data, status="success")
        ti.xcom_push(key="api_remaining", value=remaining)
    except Exception:
        store_raw_response(conn, endpoint=endpoint_name, params=fetch_kwargs, response=None, status="error")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Ingest tasks
# ---------------------------------------------------------------------------

def fetch_events_task(**context):
    _fetch_and_store(EP_EVENTS, fetch_events, {"sport": SPORT}, context["ti"])


def fetch_odds_task(**context):
    _fetch_and_store(EP_ODDS, fetch_odds, {
        "sport": SPORT,
        "regions": REGIONS,
        "markets": MARKETS,
        "bookmakers": BOOKMAKERS,
        "odds_format": ODDS_FORMAT,
    }, context["ti"])


def fetch_scores_task(**context):
    _fetch_and_store(EP_SCORES, fetch_scores, {"sport": SPORT, "days_from": SCORES_DAYS_FROM}, context["ti"])


# ---------------------------------------------------------------------------
# Shared transform helper
# ---------------------------------------------------------------------------

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
        raise ValueError(f"No successful raw data found for endpoint: '{endpoint}'.")
    return row[0]


# ---------------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------------

def transform_events_task(**context):
    conn = get_data_db_conn()
    try:
        transform_events(conn, _get_latest_raw(conn, EP_EVENTS))
    finally:
        conn.close()


def transform_odds_task(**context):
    conn = get_data_db_conn()
    try:
        transform_odds(conn, _get_latest_raw(conn, EP_ODDS))
    finally:
        conn.close()


def transform_scores_task(**context):
    conn = get_data_db_conn()
    try:
        transform_scores(conn, _get_latest_raw(conn, EP_SCORES))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Player props tasks (ingest + transform)
# ---------------------------------------------------------------------------

def fetch_player_props_task(**context):
    ti = context["ti"]
    api_key = os.environ["ODDS_API_KEY"]
    conn = get_data_db_conn()
    try:
        with conn.cursor() as cur:
            # Only MLB games — filter by games.sport
            cur.execute(
                """
                SELECT game_id FROM games
                WHERE commence_time >= CURRENT_DATE
                  AND sport = %s
                ORDER BY commence_time
                """,
                (SPORT,),
            )
            game_ids = [row[0] for row in cur.fetchall()]

        if not game_ids:
            logging.warning("No today/upcoming MLB games found — skipping player props fetch.")
            ti.xcom_push(key="skipped", value=True)
            return

        fetch_kwargs = {
            "sport": SPORT,
            "regions": REGIONS,
            "markets": PLAYER_PROP_MARKETS,
            "bookmakers": BOOKMAKERS,
            "odds_format": ODDS_FORMAT,
        }
        all_props = []
        remaining = 0
        for game_id in game_ids:
            try:
                data, remaining = fetch_player_props(
                    api_key=api_key,
                    event_id=game_id,
                    **fetch_kwargs,
                )
                all_props.append(data)
            except Exception as e:
                logging.warning("Failed to fetch MLB player props for event %s: %s", game_id, e)

        store_raw_response(
            conn,
            endpoint=EP_PLAYER_PROPS,
            params=fetch_kwargs,
            response=all_props,
            status="success" if all_props else "error",
        )
        ti.xcom_push(key="api_remaining", value=remaining)

        if not all_props:
            raise ValueError("All MLB player props fetches failed. Check API key and event IDs.")
    finally:
        conn.close()


def transform_player_props_task(**context):
    ti = context["ti"]
    if ti.xcom_pull(task_ids="fetch_player_props", key="skipped") is True:
        logging.info("MLB player props fetch was skipped (no games) — nothing to transform.")
        return
    conn = get_data_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT response FROM raw_api_responses
                WHERE endpoint = %s AND status = 'success'
                ORDER BY fetched_at DESC LIMIT 1
                """,
                (EP_PLAYER_PROPS,),
            )
            row = cur.fetchone()
        if row is None:
            raise ValueError("No successful mlb_player_props raw data found.")
        transform_player_props(conn, row[0])
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="mlb_odds_pipeline",
    default_args=default_args,
    description="Fetch and transform MLB odds, scores, and batter player props from The-Odds-API",
    schedule_interval="0 15 * * *",  # 8am MT (3pm UTC)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["mlb", "odds"],
    on_failure_callback=notify_failure,
) as dag:
    t_fetch_events  = PythonOperator(task_id="fetch_events",           python_callable=fetch_events_task)
    t_fetch_odds    = PythonOperator(task_id="fetch_odds",             python_callable=fetch_odds_task)
    t_fetch_scores  = PythonOperator(task_id="fetch_scores",           python_callable=fetch_scores_task)
    t_xform_events  = PythonOperator(task_id="transform_events",       python_callable=transform_events_task)
    t_xform_odds    = PythonOperator(task_id="transform_odds",         python_callable=transform_odds_task)
    t_xform_scores  = PythonOperator(task_id="transform_scores",       python_callable=transform_scores_task)
    t_fetch_props   = PythonOperator(task_id="fetch_player_props",     python_callable=fetch_player_props_task)
    t_xform_props   = PythonOperator(task_id="transform_player_props", python_callable=transform_player_props_task)

    [t_fetch_events, t_fetch_odds, t_fetch_scores] >> t_xform_events
    t_xform_events >> [t_xform_odds, t_xform_scores, t_fetch_props]
    t_fetch_props >> t_xform_props
```

- [ ] **Step 7.5: Run tests to verify they pass**

Run: `pytest mlb/tests/unit/test_mlb_odds_pipeline_dag.py -v`
Expected: all 5 tests PASS

- [ ] **Step 7.6: Commit**

```bash
git add mlb/dags/__init__.py mlb/dags/mlb_odds_pipeline_dag.py mlb/tests/unit/test_mlb_odds_pipeline_dag.py
git commit -m "feat: add mlb_odds_pipeline DAG"
```

---

## Task 8: `mlb_odds_backfill_dag.py`

Mirror of `nba_odds_backfill_dag.py` using `mlb.config`. Manual-trigger only.

**Files:**
- Create: `mlb/dags/mlb_odds_backfill_dag.py`
- Create: `mlb/tests/unit/test_mlb_odds_backfill_dag.py`

- [ ] **Step 8.1: Write the failing DAG-structure test**

Create `mlb/tests/unit/test_mlb_odds_backfill_dag.py`:

```python
import importlib
import pytest


@pytest.fixture(scope="module")
def dag():
    module = importlib.import_module("mlb.dags.mlb_odds_backfill_dag")
    return module.dag


def test_dag_id(dag):
    assert dag.dag_id == "mlb_odds_backfill"


def test_manual_schedule_only(dag):
    assert dag.schedule_interval is None


def test_tags(dag):
    assert "mlb" in dag.tags
    assert "backfill" in dag.tags


def test_params_shape(dag):
    assert "date_from" in dag.params
    assert "date_to" in dag.params


def test_single_task(dag):
    assert list(dag.task_dict.keys()) == ["run_backfill"]
```

- [ ] **Step 8.2: Run to verify the test fails**

Run: `pytest mlb/tests/unit/test_mlb_odds_backfill_dag.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 8.3: Create the DAG**

Create `mlb/dags/mlb_odds_backfill_dag.py`:

```python
# mlb/dags/mlb_odds_backfill_dag.py
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from mlb.config import SPORT, REGIONS, MARKETS, BOOKMAKERS
from shared.plugins.db_client import get_data_db_conn, store_raw_response
from shared.plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores
from shared.plugins.transformers.events import transform_events
from shared.plugins.transformers.odds import transform_odds
from shared.plugins.transformers.player_props import transform_player_props
from shared.plugins.transformers.scores import transform_scores

EP_EVENTS = "mlb_events"
EP_ODDS = "mlb_odds"
EP_SCORES = "mlb_scores"

# Seconds to sleep between API calls during backfill to protect quota
BACKFILL_SLEEP_SECONDS = 2


def run_backfill(**context):
    api_key = os.environ["ODDS_API_KEY"]
    params = context["params"]
    date_from = params.get("date_from")
    date_to = params.get("date_to")

    extra_event_params = {}
    if date_from:
        extra_event_params["commenceTimeFrom"] = f"{date_from}T00:00:00Z"
    if date_to:
        extra_event_params["commenceTimeTo"] = f"{date_to}T23:59:59Z"

    conn = get_data_db_conn()
    try:
        events, _ = fetch_events(api_key=api_key, sport=SPORT, **extra_event_params)
        store_raw_response(conn, EP_EVENTS, {"sport": SPORT, **extra_event_params}, events)
        transform_events(conn, events)
        time.sleep(BACKFILL_SLEEP_SECONDS)

        odds, _ = fetch_odds(
            api_key=api_key,
            sport=SPORT,
            regions=REGIONS,
            markets=MARKETS,
            bookmakers=BOOKMAKERS,
        )
        store_raw_response(conn, EP_ODDS, {"sport": SPORT}, odds)
        transform_odds(conn, odds)
        transform_player_props(conn, odds)
        time.sleep(BACKFILL_SLEEP_SECONDS)

        scores, _ = fetch_scores(api_key=api_key, sport=SPORT, days_from=30)
        store_raw_response(conn, EP_SCORES, {"sport": SPORT, "days_from": 30}, scores)
        transform_scores(conn, scores)
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mlb_odds_backfill",
    default_args=default_args,
    description="On-demand historical MLB odds backfill",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlb", "backfill"],
    params={
        "date_from": Param(None, type=["null", "string"], description="Start date YYYY-MM-DD"),
        "date_to":   Param(None, type=["null", "string"], description="End date YYYY-MM-DD"),
    },
) as dag:
    PythonOperator(task_id="run_backfill", python_callable=run_backfill)
```

Note: unlike the existing NBA backfill which treats `fetch_events(...)` as a single value, this DAG unpacks the `(data, remaining)` tuple returned by the shared `odds_api_client.py` functions. That's deliberate — the NBA backfill has a latent bug we don't inherit.

- [ ] **Step 8.4: Run tests to verify they pass**

Run: `pytest mlb/tests/unit/test_mlb_odds_backfill_dag.py -v`
Expected: all 5 tests PASS

- [ ] **Step 8.5: Commit**

```bash
git add mlb/dags/mlb_odds_backfill_dag.py mlb/tests/unit/test_mlb_odds_backfill_dag.py
git commit -m "feat: add mlb_odds_backfill DAG"
```

---

## Task 9: End-to-end verification

A wiring check — exercise the new MLB DAG against the running Airflow stack without burning real API quota.

**Files:** none

- [ ] **Step 9.1: Run the full unit test suite**

Run: `pytest nba/tests/unit shared/tests mlb/tests -v --ignore=nba/tests/integration`
Expected: all PASS.

- [ ] **Step 9.2: Start the stack and let Airflow parse all DAGs**

```bash
docker compose up -d
sleep 45
docker compose logs airflow-scheduler 2>&1 | tail -n 50
```

Expected: scheduler log shows no import errors for `mlb_odds_pipeline` or `mlb_odds_backfill`. If a DAG fails to import, the scheduler log will say so with a Python traceback.

- [ ] **Step 9.3: Confirm the DAGs are visible in Airflow**

```bash
docker compose exec airflow-scheduler airflow dags list | grep -E "mlb_odds|nba_odds"
```

Expected:
```
mlb_odds_pipeline
mlb_odds_backfill
nba_odds_pipeline
nba_odds_backfill
```

- [ ] **Step 9.4: Manually trigger `mlb_odds_pipeline` with a small quota impact**

Set `PLAYER_PROP_MARKETS` temporarily in `mlb/config.py` to a single market (`["batter_hits"]`) for the trigger run, OR set a `DRY_RUN` env var if you prefer — pick whichever minimizes quota. Then:

```bash
docker compose exec airflow-scheduler airflow dags trigger mlb_odds_pipeline
sleep 120
docker compose exec airflow-scheduler airflow dags list-runs --dag-id mlb_odds_pipeline
```

Expected: at least one run with state `success`. If it failed, inspect with:
```bash
docker compose exec airflow-scheduler airflow tasks list mlb_odds_pipeline --tree
docker compose logs airflow-scheduler 2>&1 | grep -A20 "mlb_odds_pipeline"
```

- [ ] **Step 9.5: Query Postgres to confirm MLB rows landed**

```bash
docker exec -i $(docker compose ps -q data-postgres) psql -U odds -d odds_db -c \
  "SELECT sport, COUNT(*) FROM games GROUP BY sport;"
docker exec -i $(docker compose ps -q data-postgres) psql -U odds -d odds_db -c \
  "SELECT prop_type, COUNT(*) FROM player_props
   WHERE game_id IN (SELECT game_id FROM games WHERE sport = 'baseball_mlb')
   GROUP BY prop_type ORDER BY prop_type;"
```

Expected: `games` table contains at least one `sport = 'baseball_mlb'` row (during MLB season). `player_props` contains rows with `prop_type IN ('batter_hits', 'batter_total_bases', 'batter_home_runs')`.

- [ ] **Step 9.6: Revert any temporary single-market change**

If you set `PLAYER_PROP_MARKETS` to a smaller list for Step 9.4, restore the full list.

- [ ] **Step 9.7: Final commit**

If any changes happened during verification (unlikely — this task shouldn't edit code), commit them. Otherwise skip. No commit needed if working tree is clean.

---

## Self-Review Notes

**Spec coverage check (ran inline):**

| Spec requirement | Task(s) |
|---|---|
| `shared/plugins/odds_math.py` with `american_odds_to_implied_prob` | Task 1 |
| Migration 006: MLB tables + widen player_props + sport on recommendations | Task 4 |
| `mlb/config.py` + `nba/config.py` + delete `config/settings.py` | Task 6 |
| NBA `score.py` / `settle.py` sport-scoping | Task 5 |
| MLB odds pipeline DAG | Task 7 |
| MLB odds backfill DAG | Task 8 |
| Promotion of events/odds/scores/player_props transformers | Tasks 2–3 (spec deviation documented at top) |
| Market-prefix fix (MLB `batter_*` routing) | Task 3 |

**Deferred to later plans (intentional):**
- MLB stats pipeline (API client, players/game_logs/teams/name_resolution transformers, stats DAGs) → Plan 2
- MLB feature engineering, training, scoring, settlement, + their DAGs → Plan 3
- Slack `[MLB]` message prefix — covered in the MLB ML DAGs (Plan 3)

**Type/naming consistency:**
- `SPORT` constant: `"basketball_nba"` (NBA) / `"baseball_mlb"` (MLB) — these are the Odds-API sport keys, used consistently in DAG queries (Step 7.4 filters `games.sport = %s` with SPORT).
- `GAME_LEVEL_MARKETS = {"h2h", "spreads", "totals"}` — defined once in `shared/plugins/transformers/odds.py`, imported from there by `player_props.py`.
- Raw endpoint names namespaced per sport (`mlb_events`, `mlb_odds`, etc.) to prevent collision in `raw_api_responses`.

---

## Execution Choice

Plan complete and saved to `docs/superpowers/plans/2026-04-20-mlb-foundation-and-odds-ingest.md`. Two execution options:

1. **Subagent-Driven (recommended)** — a fresh subagent per task with two-stage review between tasks. Best for a plan this size.
2. **Inline Execution** — execute tasks in this session using `executing-plans`, with checkpoints between tasks.

Which approach?
