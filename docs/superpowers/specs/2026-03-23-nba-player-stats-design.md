# NBA Player Stats Pipeline — Design Spec

**Date:** 2026-03-23
**Status:** Approved

---

## Overview

Add an NBA player stats pipeline to the odds-pipeline project to support feature engineering for player prop predictions. The pipeline ingests per-game and season-level player/team stats from the `nba_api` Python package (unofficial wrapper for stats.nba.com) and normalizes them into Postgres tables that can be joined with existing `player_props` and `games` data.

---

## Goals

- Provide per-game player stats (pts, reb, ast, min, fga, fta, usg_pct, blk, stl, plus_minus) for feature engineering
- Provide team-level stats (pace, paint defense allowed, point differential) for contextual features
- Establish a stable player ID system that links Odds API player names to nba_api player IDs
- Seed 3 seasons of historical data (2022-23, 2023-24, 2024-25)

---

## Target Features Supported

| Feature | Source |
|---|---|
| Opponent defense vs position | Derived from `player_game_logs` + opponent team grouping |
| Usage rate / shot attempts | `player_game_logs.usg_pct`, `fga`, `fta` |
| Pace | `team_season_stats.pace` |
| Rest / fatigue | Calculated from `team_game_logs.game_date` |
| Minutes played | `player_game_logs.min` |
| Blowout / spread | `team_game_logs.plus_minus` |
| Opponent rim protector | `player_game_logs.blk` (player-level) + `team_season_stats.opp_pts_paint_pg` (team-level) |
| Teammate injuries | Separate data source (out of scope for this pipeline) |

---

## Architecture

### New Files

```
plugins/
  nba_api_client.py                    # Wrapper around nba_api package with rate-limit handling
  transformers/
    players.py                         # Player dimension transformer
    player_game_logs.py                # Per-game player stats transformer
    team_game_logs.py                  # Per-game team stats transformer
    team_season_stats.py               # Season-level advanced stats transformer

dags/
  nba_player_stats_ingest_dag.py       # Fetches raw responses from nba_api
  nba_player_stats_transform_dag.py    # Normalizes raw → structured tables
  nba_player_stats_backfill_dag.py     # Historical 3-season seed (manual trigger)

sql/
  migrations/
    001_add_player_stats_tables.sql    # New tables + ALTER TABLE statements (see Migration Strategy)
```

### nba_api Endpoints Used

| Endpoint | raw_api_responses endpoint tag | Purpose |
|---|---|---|
| `CommonAllPlayers` | `nba_api/players` (16 chars) | Player dimension: name, position, team |
| `PlayerGameLogs` (advanced) | `nba_api/player_game_logs` (25 chars) | Per-game player stats including usg_pct |
| `TeamGameLogs` | `nba_api/team_game_logs` (23 chars) | Per-game team stats: pts, plus_minus, game_date |
| `LeagueDashTeamStats` (advanced) | `nba_api/team_season_stats` (25 chars) | Season-level pace, off_rating, def_rating |
| `LeagueDashPtStats` (defense) | `nba_api/team_paint_defense` (27 chars) | Paint points allowed per team per season |

All tags are under 50 characters and are compatible with the existing `raw_api_responses.endpoint VARCHAR(50)` column.

---

## Player Selection Strategy

Rather than pulling all ~450 active NBA players, the ingest task filters to:

1. **Props-driven**: Players with at least one row in `player_props` (resolved via `player_name_mappings`)
2. **Minutes fallback**: Any player averaging >20 minutes/game this season, derived from `player_game_logs`

**Bootstrap note**: On the very first run (before any historical backfill), `player_name_mappings` will be empty and `player_game_logs` will have no data. In this case the ingest falls back to fetching all active players from `CommonAllPlayers`. The backfill DAG must be run first to seed `player_game_logs` before the daily ingest player filter becomes effective.

---

## Entity Resolution: Player ID Mapping

**Problem**: Odds API returns player names as raw strings. nba_api uses stable integer player IDs. Names can differ by accents, suffixes (Jr./Sr./III), or abbreviations.

**Solution**: A `player_name_mappings` table maps Odds API name strings to canonical nba_api player IDs.

**Resolution process** (`resolve_player_ids` transform task):
1. Query all distinct player names in `player_props` not yet in `player_name_mappings`
2. Normalize names: lowercase, strip accents, remove Jr./Sr./III suffixes
3. Fuzzy-match against `players.full_name`
4. Auto-insert matches with confidence ≥95% as unverified (`verified = FALSE`)
5. Send Slack alert listing all names with confidence <95% (including zero-confidence / no match found) for manual review — these are not inserted automatically
6. Populate `player_props.nba_player_id` FK column for all players present in `player_name_mappings`

---

## Database Schema

### Migration Strategy

The existing project uses a single `sql/init_schema.sql` applied on container startup (safe for fresh deployments). To handle the two `ALTER TABLE` statements on existing deployments, a separate migration file is introduced:

**`sql/migrations/001_add_player_stats_tables.sql`**

This file contains:
- All five new `CREATE TABLE IF NOT EXISTS` statements
- `ALTER TABLE games ADD COLUMN IF NOT EXISTS nba_game_id TEXT`
- `ALTER TABLE player_props ADD COLUMN IF NOT EXISTS nba_player_id INT REFERENCES players(player_id)`

`init_schema.sql` is also updated to include the new tables and columns for clean fresh deployments. The migration file is idempotent (`IF NOT EXISTS` throughout) and safe to run multiple times.

---

### `players` (dimension, upserted)
```sql
player_id         INT PRIMARY KEY,   -- nba_api canonical ID
full_name         TEXT NOT NULL,
position          TEXT,              -- G, F, C, G-F, F-C, etc.
team_id           INT,
team_abbreviation TEXT,
is_active         BOOLEAN,
fetched_at        TIMESTAMPTZ NOT NULL
```
Index: `CREATE INDEX ON players (full_name)` — used by `resolve_player_ids` fuzzy matching.

---

### `player_game_logs` (append-only, unique per player+game)
```sql
id               SERIAL PRIMARY KEY,
player_id        INT REFERENCES players(player_id),
nba_game_id      TEXT NOT NULL,
season           TEXT NOT NULL,      -- e.g. '2024-25'
game_date        DATE NOT NULL,
matchup          TEXT,               -- e.g. 'DEN vs. LAL'
team_id          INT,
opponent_team_id INT,                -- derived: parse opponent abbreviation from matchup string,
                                     -- reverse-lookup team_id via team_game_logs
wl               TEXT,
min              FLOAT,
fga              INT,
fta              INT,
usg_pct          FLOAT,              -- usage rate %
pts              INT,
reb              INT,
ast              INT,
blk              INT,
stl              INT,
plus_minus       INT,
fetched_at       TIMESTAMPTZ NOT NULL,
UNIQUE (player_id, nba_game_id)
```

**Derivation of `opponent_team_id`**: The `PlayerGameLogs` endpoint returns a `MATCHUP` string in the format `"DEN vs. LAL"` (home) or `"DEN @ LAL"` (away). The transformer parses the opponent team abbreviation from this string, then looks up the `team_id` from the `team_game_logs` table using `(nba_game_id, team_abbreviation != player_team_abbreviation)`. If `team_game_logs` is not yet populated when `transform_player_game_logs` runs, the column is set to NULL and can be backfilled in a follow-up run. The task dependency ordering (team_game_logs transforms in parallel with player_game_logs) means a second-pass resolution may be needed on the first run — this is acceptable given the append-only + idempotent upsert pattern.

---

### `team_game_logs` (append-only, unique per team+game)
```sql
id                SERIAL PRIMARY KEY,
team_id           INT NOT NULL,
team_abbreviation TEXT,
nba_game_id       TEXT NOT NULL,
season            TEXT NOT NULL,
game_date         DATE NOT NULL,
matchup           TEXT,
wl                TEXT,
pts               INT,
plus_minus        INT,               -- point differential → blowout feature
fetched_at        TIMESTAMPTZ NOT NULL,
UNIQUE (team_id, nba_game_id)
```

---

### `team_season_stats` (upserted per team+season, current season only on daily ingest)
```sql
id                SERIAL PRIMARY KEY,
team_id           INT NOT NULL,
team_abbreviation TEXT,
season            TEXT NOT NULL,
pace              FLOAT,
off_rating        FLOAT,
def_rating        FLOAT,
opp_pts_paint_pg  FLOAT,             -- opponent paint points allowed per game
fetched_at        TIMESTAMPTZ NOT NULL,
UNIQUE (team_id, season)
```

**Daily ingest scope**: The daily ingest fetches `team_season_stats` for the current season only (e.g., `2024-25`). Prior seasons are seeded by the backfill DAG and are not re-fetched on daily runs.

---

### `player_name_mappings` (entity resolution)
```sql
odds_api_name  TEXT PRIMARY KEY,
nba_player_id  INT REFERENCES players(player_id),
confidence     FLOAT,
verified       BOOLEAN DEFAULT FALSE,
created_at     TIMESTAMPTZ NOT NULL
```

---

### Modifications to existing tables

```sql
-- games: enables joins between odds/props data and player game logs
ALTER TABLE games ADD COLUMN IF NOT EXISTS nba_game_id TEXT;

-- player_props: canonical player identifier across both pipelines
ALTER TABLE player_props ADD COLUMN IF NOT EXISTS nba_player_id INT REFERENCES players(player_id);
```

`games.nba_game_id` is populated by the `link_nba_game_ids` transform task using:
```sql
DATE(games.commence_time AT TIME ZONE 'America/New_York') = player_game_logs.game_date
AND (games.home_team = <home_abbrev> AND games.away_team = <away_abbrev>)
```
where the home/away abbreviations are parsed from the nba_api `MATCHUP` string.

**Timezone note**: The Odds API stores `commence_time` as UTC. nba_api returns `game_date` in Eastern Time (ET). A game tipping off at 10:30 PM ET has a UTC `commence_time` of 03:30 the following day — a naive `DATE(commence_time)` cast would return the wrong date for late-night East Coast games. The join converts `commence_time` to ET before extracting the date. If the direct match still fails (e.g., team abbreviation mismatch between data sources), a ±1 day window is used as a fallback before logging the game as unmatched.

---

## DAG Structure

### `nba_player_stats_ingest_dag`
**Schedule**: Daily at 6:00am MT (America/Denver) — before existing 8am ingest

```
fetch_players ──────────────────────────────────────────┐
fetch_player_game_logs (props-driven + >20min fallback) ─┤→ all store to raw_api_responses
fetch_team_game_logs ────────────────────────────────────┤
fetch_team_season_stats (current season only) ───────────┘
```

All four tasks run in parallel. Each stores the full API response JSON to `raw_api_responses` using the endpoint tags defined in the Architecture section. The client applies a configurable inter-request delay (default 1s) between nba_api calls within each task to avoid triggering stats.nba.com rate limits.

`fetch_player_game_logs` queries `player_props` and `player_name_mappings` at task start to build the filtered player list. This is a read-only dependency on tables populated by the existing `nba_player_props` DAG — no cross-DAG sensor is required since these tables persist across runs.

---

### `nba_player_stats_transform_dag`
**Schedule**: Daily at 6:20am MT (America/Denver), waits via `ExternalTaskSensor` with `execution_delta=timedelta(minutes=20)`

```
wait_for_ingest
      │
      ▼
transform_players
      │
      ▼
┌──────────────────────────────────────┐
│ transform_player_game_logs           │
│ transform_team_game_logs             │  (parallel)
│ transform_team_season_stats          │
└──────────────────────────────────────┘
      │
      ▼
resolve_player_ids    ← fuzzy-matches new Odds API names → nba_player_id; Slack alert for confidence <95%
      │
      ▼
link_nba_game_ids     ← matches DATE(games.commence_time) + team abbreviations → games.nba_game_id
```

`transform_players` runs first since `resolve_player_ids` depends on the `players` table being populated. The three mid-level transforms run in parallel. `resolve_player_ids` and `link_nba_game_ids` run last as they depend on all normalized tables being up to date.

---

### `nba_player_stats_backfill_dag`
**Schedule**: Manual trigger only

**Parameters** (Airflow `Param` objects, consistent with existing `nba_backfill` DAG pattern):

| Param | Type | Default | Validation |
|---|---|---|---|
| `season_start` | `str` | `"2022-23"` | Must match regex `^\d{4}-\d{2}$` |
| `season_end` | `str` | `"2024-25"` | Must match regex `^\d{4}-\d{2}$`; must be ≥ `season_start` (enforced at runtime in the task function, not at the Airflow `Param` level) |

**Fetch strategy**: The `PlayerGameLogs` endpoint accepts a `season` parameter (e.g., `"2022-23"`) and returns all game logs for that season in a single call. The backfill task iterates season-by-season from `season_start` to `season_end`, fetching all four endpoint types per season with a 2-3s inter-request delay between API calls.

---

## Error Handling

- All ingest tasks: 3 retries with exponential backoff (consistent with existing DAGs)
- HTTP 429 (throttling) from stats.nba.com: caught explicitly, retried with a longer delay (30s) before failing
- `resolve_player_ids`: names with confidence <95% (including zero-confidence / no match) do not fail the task — they are logged as warnings and trigger a Slack alert listing all unresolved names for manual review
- `link_nba_game_ids`: unmatched games (no Odds API `game_id` found for a given nba_game_id) are logged as warnings, not errors

---

## Testing

```
tests/
  unit/
    transformers/
      test_players.py
      test_player_game_logs.py
      test_team_game_logs.py
      test_team_season_stats.py
    test_nba_api_client.py           # mocked nba_api responses, rate-limit retry logic
    test_player_name_resolution.py   # fuzzy matching: accented chars, Jr./Sr. suffixes, team changes
  integration/
    test_player_stats_pipeline.py    # full ingest → transform against live Postgres
```

**Key test cases:**
- Idempotency: re-running transform produces same result (enforced by unique constraints)
- Name resolution: Jokić/Jokic accent handling, Trent Jr./Trent suffix stripping, players who changed teams mid-season
- Game ID linking: `games.nba_game_id` correctly populated by `DATE(commence_time)` + team abbreviation matching
- Bootstrap path: first run with empty `player_name_mappings` and `player_game_logs` falls back to full `CommonAllPlayers` fetch
- Backfill params: invalid season strings rejected with clear error message

---

## Historical Data

- **Seasons**: 2022-23, 2023-24, 2024-25
- **Rationale**: 3 seasons provides sufficient ML training data while keeping stats relevant to current player roles, systems, and pace of play. Older data introduces noise from trades, coaching changes, and aging curves.
