# MLB Odds Integration — Design

**Date:** 2026-04-20
**Status:** Approved (pending spec review)
**Scope:** Add MLB batter-prop support to the odds-pipeline repo as a sibling vertical to `nba/`, with full end-to-end parity (ingest → stats → features → train → score → recommendations + Slack). Keeps NBA code untouched. Built so pitcher props and NFL can be added later with minimal rework.

## Goals

1. Generate daily MLB batter-prop recommendations where the model identifies an edge vs bookmaker-implied probability.
2. Follow the existing NBA pipeline pattern so the codebase stays consistent and the lessons from NBA (per-prop-type models, isotonic calibration, top-N slot allocation, Slack alerting) carry over.
3. Do not modify working NBA code beyond the minimum required (sport-scoping a couple of DB writes).
4. Position `shared/` for progressive promotion so NFL (planned next) can be added without copying a third time.

## Non-Goals (MVP)

- Pitcher props (`pitcher_strikeouts`, `pitcher_outs`, `pitcher_earned_runs`). Schema leaves room for them; DAGs and ML do not handle them in v1.
- Additional batter markets beyond the three MVP markets (see below). `batter_rbis`, `batter_runs_scored`, `batter_stolen_bases`, `batter_hits_runs_rbis` are post-MVP.
- Statcast / pybaseball advanced metrics (exit velocity, xwOBA, barrel rate).
- Opposing-pitcher matchup features (batter-vs-throws handedness splits, opponent SP K/9 / OPS-against). Flagged as the highest-lift post-MVP feature work.
- Park-factor and lineup-spot features.
- Doubleheader-specific handling beyond what naturally works.
- Refactoring NBA code into `shared/` beyond one trivial duplication (`american_odds_to_implied_prob`). Shared-core work happens during NFL planning, once we have two concrete implementations to factor from.

## Summary of Decisions

| Decision | Choice |
|---|---|
| Prop markets (MVP) | `batter_hits`, `batter_total_bases`, `batter_home_runs` |
| MLB stats source | MLB Stats API (via `MLB-StatsAPI` or raw HTTP) |
| Code layout | Sibling `mlb/` directory mirroring `nba/`; opportunistic shared-core promotion only |
| Schema approach | Parallel sport-specific stats tables (`mlb_players`, `mlb_player_game_logs`, …); widen `player_props` with `mlb_player_id`; add `sport` column to `recommendations` |
| DAG cadence | Same as NBA (daily 8am MT through 9am MT; weekly train Mon 3am MT) |
| Feature set (MVP) | 1:1 with NBA (implied prob, line movement, rolling 5/10/20g, rolling std 10g, is_home, rest_days) |
| ML approach | Per-prop-type XGBoost + isotonic calibration; MLflow registry names prefixed `mlb_prop_model_*` |
| Config | `mlb/config.py` next to code; NBA settings migrated into `nba/config.py` |
| Slack | Same webhook, `[MLB]` prefix on messages |

## Architecture

### Directory layout

```
odds-pipeline/
├── nba/                              # unchanged except for 2 one-line sport filters
├── mlb/                              # NEW — mirrors nba/ shape
│   ├── config.py
│   ├── dags/
│   │   ├── mlb_odds_pipeline_dag.py
│   │   ├── mlb_odds_backfill_dag.py
│   │   ├── mlb_stats_pipeline_dag.py
│   │   ├── mlb_stats_backfill_dag.py
│   │   ├── mlb_feature_dag.py
│   │   ├── mlb_train_dag.py
│   │   └── mlb_score_dag.py
│   ├── plugins/
│   │   ├── mlb_api_client.py
│   │   ├── transformers/
│   │   │   ├── players.py
│   │   │   ├── player_game_logs.py
│   │   │   ├── teams.py
│   │   │   ├── player_props.py
│   │   │   ├── player_name_resolution.py
│   │   │   └── features.py
│   │   └── ml/
│   │       ├── train.py
│   │       ├── score.py
│   │       └── settle.py
│   └── tests/
│       ├── unit/
│       └── integration/
├── shared/
│   └── plugins/
│       ├── odds_api_client.py        # already generic
│       ├── db_client.py
│       ├── slack_notifier.py
│       └── odds_math.py              # NEW — houses american_odds_to_implied_prob
├── nba/config.py                     # NBA settings move here from config/settings.py
├── config/                           # slimmed or removed once NBA settings migrate
└── sql/migrations/
    └── 006_mlb_tables.sql            # single migration adds all MLB schema changes
```

### Data flow (MLB)

```
Odds-API ──► mlb_odds_pipeline ──► games / odds / scores / player_props   (rows with sport='MLB')
                 │
                 │ (ExternalTaskSensor on mlb_odds_pipeline completion)
                 ▼
MLB Stats API ─► mlb_stats_pipeline ──► mlb_players / mlb_player_game_logs / mlb_teams
                                     └─► mlb_player_name_mappings (fuzzy-match)
                                     └─► backfill player_props.mlb_player_id
                 │
                 │ (ExternalTaskSensor on mlb_stats_pipeline completion)
                 ▼
            mlb_feature_dag ────► features/mlb_props_features_<date>.parquet
                 │
                 │ (ExternalTaskSensor on mlb_feature_dag completion)
                 ▼
            mlb_score_dag ─────► recommendations (sport='MLB')

mlb_train_dag (weekly) ─────────► MLflow registry (mlb_prop_model_{prop_type})
```

MLB and NBA pipelines run concurrently each morning with no contention — they use sport-specific stats tables and filter the shared tables (`games`, `player_props`, `recommendations`) by a `sport` column.

## Data Model

### New MLB-specific tables

```sql
CREATE TABLE mlb_players (
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

CREATE INDEX idx_mlb_players_full_name ON mlb_players (full_name);

CREATE TABLE mlb_player_game_logs (
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

CREATE INDEX idx_mlb_pgl_player_id ON mlb_player_game_logs (player_id);
CREATE INDEX idx_mlb_pgl_game_date ON mlb_player_game_logs (game_date);
CREATE INDEX idx_mlb_pgl_season    ON mlb_player_game_logs (season);

CREATE TABLE mlb_teams (
    team_id      INT PRIMARY KEY,
    full_name    TEXT NOT NULL,
    abbreviation TEXT NOT NULL,
    league       TEXT,
    division     TEXT,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE mlb_player_name_mappings (
    odds_api_name TEXT PRIMARY KEY,
    mlb_player_id INT REFERENCES mlb_players(player_id),
    confidence    FLOAT,
    verified      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Notes:
- `mlb_game_pk` is the MLB Stats API primary-key for a game (string for forward-compat with postseason game IDs).
- `total_bases` is stored on the row (1×hits-singles + 2×doubles + 3×triples + 4×home_runs). Stored rather than computed to match how NBA features join — the transformer that writes game logs computes and inserts it.
- Pitcher columns are intentionally absent for MVP. Pitcher props in v2 will add a separate `mlb_pitcher_game_logs` table (or extend this one — decided at that time).

### `mlb_team_game_logs` / `mlb_team_season_stats` not created

NBA uses team-level stats for pace and opponent defense (points props are team-shape-driven). MLB batter props are overwhelmingly driven by opposing *pitcher*, not opposing team. Team-aggregate features would add noise without lift. The post-MVP "opposing-pitcher features" work replaces what team stats do for NBA.

### Widen `player_props`

```sql
ALTER TABLE player_props
  ADD COLUMN mlb_player_id INT REFERENCES mlb_players(player_id);

CREATE INDEX idx_player_props_mlb_player ON player_props (mlb_player_id);
```

One of `nba_player_id` / `mlb_player_id` is non-null per row. `games.sport` tells queries which to read. When NFL lands, `nfl_player_id` slots in the same way.

### Scope `recommendations` by sport

```sql
ALTER TABLE recommendations
  ADD COLUMN sport VARCHAR(20) NOT NULL DEFAULT 'NBA';

ALTER TABLE recommendations ALTER COLUMN sport DROP DEFAULT;

CREATE INDEX idx_recs_sport_date ON recommendations (sport, game_date);
```

The one-shot `DEFAULT 'NBA'` backfills existing rows; dropping the default forces every future insert to specify sport explicitly.

### NBA code changes required (minimal)

Changes are confined to the shared `recommendations` table writes/reads; no NBA business logic is touched:

- `nba/plugins/ml/score.py` — `DELETE FROM recommendations WHERE game_date = %s` becomes `… WHERE game_date = %s AND sport = 'NBA'`; `INSERT INTO recommendations` adds a `sport` column set to `'NBA'`.
- `nba/plugins/ml/settle.py` — the SELECT over `recommendations` adds a `sport = 'NBA'` filter.

Both changes are mechanical and covered by existing NBA unit tests with minor updates. No transformer, DAG, training, or feature logic changes.

## Pipelines

### DAG schedule

| DAG | Schedule (UTC) | MT time | Waits on | Purpose |
|---|---|---|---|---|
| `mlb_odds_pipeline` | `0 15 * * *` | 8:00 am | — | Odds-API → games/odds/scores/player_props |
| `mlb_stats_pipeline` | `20 15 * * *` | 8:20 am | `mlb_odds_pipeline` | MLB Stats API → players/game_logs/teams + name resolution |
| `mlb_feature_dag` | `40 15 * * *` | 8:40 am | `mlb_stats_pipeline` | Build today's features → Parquet |
| `mlb_score_dag` | `0 16 * * *` | 9:00 am | `mlb_feature_dag` | Score + write recommendations |
| `mlb_train_dag` | `0 10 * * 1` | Mon 3:00 am | — | Weekly per-prop-type training |
| `mlb_odds_backfill` | Manual | — | — | Historical odds seeding |
| `mlb_stats_backfill` | Manual | — | — | Historical stats seeding |

Season awareness: MLB regular season is roughly Apr–Oct. DAGs run year-round and early-exit when Odds-API returns no events — matches NBA's offseason handling.

### MLB Stats API client (`mlb/plugins/mlb_api_client.py`)

Thin functional wrapper using `requests` directly against `https://statsapi.mlb.com/api/v1/…` — matches the pattern of `shared/plugins/odds_api_client.py` and avoids pulling in a wrapper library we don't need. Endpoints needed for MVP:

- `fetch_teams()` — 30 MLB teams (MLB Stats API `/teams?sportId=1`)
- `fetch_players(season)` — active rosters across all 30 teams (`/teams/{team_id}/roster` per team, or `/sports/1/players?season=YYYY`)
- `fetch_player_game_logs(player_id, season)` — batter season log (`/people/{player_id}/stats?stats=gameLog&group=hitting&season=YYYY`)
- `fetch_schedule(date_from, date_to)` — games + probable pitchers (`/schedule?sportId=1&startDate=…&endDate=…`)

No auth required. No rate limiter for MVP (MLB Stats API is permissive); add throttling if we hit 429s. Errors surface via `response.raise_for_status()`, same as the Odds-API client.

### Transformers

- `transformers/teams.py` — upsert into `mlb_teams`
- `transformers/players.py` — upsert into `mlb_players`
- `transformers/player_game_logs.py` — upsert into `mlb_player_game_logs`, computing `total_bases` at insert time
- `transformers/player_props.py` — normalize MLB prop payload (`batter_hits` / `batter_total_bases` / `batter_home_runs` outcomes) into existing `player_props` table; keep `nba_player_id` NULL. Also responsible for inserting the parent row into `games` with `sport='MLB'` set explicitly (mirrors how NBA's odds transformer sets `sport` today).
- `transformers/player_name_resolution.py` — fuzzy-match Odds-API names → `mlb_player_id` via `mlb_player_name_mappings` with ≥95 rapidfuzz threshold, identical logic to NBA's version

### Feature engineering (`mlb/plugins/transformers/features.py`)

```python
MLB_PROP_STAT_MAP = {
    "batter_hits":        "hits",
    "batter_total_bases": "total_bases",
    "batter_home_runs":   "home_runs",
}
```

Features produced per `(player, prop_type, bookmaker)` row for `game_date`:

| Feature | Source |
|---|---|
| `implied_prob_over` | American odds → implied probability (`shared/plugins/odds_math.py`) |
| `line_movement` | `line − opening_line` across bookmaker's snapshots |
| `rolling_avg_5g` / `_10g` / `_20g` | Rolling mean of mapped stat from `mlb_player_game_logs` |
| `rolling_std_10g` | Rolling std (sample) of mapped stat over last 10 games |
| `is_home` | Parsed from `matchup` (" vs. " → home, " @ " → away) |
| `rest_days` | Days between `game_date` and player's previous game |
| `actual_result` | 1 if stat > line, 0 if ≤ line, NULL until game played (labels only) |
| `actual_stat_value` | Raw stat value for settlement (NULL until game played) |

Output: `features/mlb_props_features_<date>.parquet`. Empty dataframe if no props on `game_date`.

### ML training (`mlb/plugins/ml/train.py`)

Per-prop-type XGBoost + isotonic calibration, MLflow-registered. Mirrors NBA's `train.py` in structure.

- **Registry names:** `mlb_prop_model_batter_hits`, `mlb_prop_model_batter_total_bases`, `mlb_prop_model_batter_home_runs`
- **Features:** same 6 numeric features as NBA's `PER_PROP_FEATURES` (no `prop_type_encoded` — per-type models don't need it)
- **Train/val split:** time-based, last 2 days as validation (same as NBA `VALIDATION_DAYS=2`)
- **Model:** `xgb.XGBClassifier(n_estimators=300, max_depth=5, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8)` — same hyperparameters as NBA starting point; tuned after first evaluation
- **Calibration:** isotonic regression on validation predictions, wrapped in `_CalibratedModel` (duplicated from NBA for MVP; promoted to shared in NFL planning)
- **Promotion tagging:** tag MLflow run `promotion_candidate=true` if val ROC-AUC > current production model for that prop-type
- **Min-rows threshold:**
  - `batter_hits` / `batter_total_bases`: 50 rows (same as NBA)
  - `batter_home_runs`: 100 rows (sparser positive class; avoid overfitting early-season)

Skips a prop-type gracefully if under threshold (matches `train_all_models` behavior in NBA).

### ML scoring (`mlb/plugins/ml/score.py`)

Same flow as NBA's `score.py`:

1. Load today's `mlb_props_features_<date>.parquet`
2. For each prop-type in `MLB_PROP_STAT_MAP`: load production model from MLflow, score, compute `edge = model_prob − implied_prob`
3. Allocate top-10 slots evenly across active prop-types using `_allocate_slots` (extra slots to highest top-pick edge)
4. Remaining picks are ranked by edge below the top 10
5. `DELETE FROM recommendations WHERE game_date = %s AND sport = 'MLB'` then INSERT with `sport='MLB'`

### Settlement (`mlb/plugins/ml/settle.py`)

Runs as a task inside `mlb_score_dag` after the scoring task completes. Settles any still-unsettled rows from prior run-days (yesterday's picks get settled in today's run, once yesterday's game logs have been ingested by `mlb_stats_pipeline`).

For each `recommendations` row where `sport='MLB'` and `settled_at IS NULL`:

- Look up actual stat value in `mlb_player_game_logs` using `mlb_player_id` + `game_date` + the mapped stat column from `MLB_PROP_STAT_MAP`
- Skip if no matching game log row yet (stays unsettled until next run)
- Compute `actual_result = 1 if actual > line else 0`
- Update `actual_result`, `actual_stat_value`, `settled_at = NOW()`

### Slack

Same webhook as NBA. Each pipeline DAG calls `send_slack_message` on success/failure with messages prefixed `[MLB]`:

- `[MLB] :white_check_mark: mlb_odds_pipeline — 15 events, 447 player_props rows`
- `[MLB] :white_check_mark: mlb_score_dag — top 10 picks (hits: 4, TB: 4, HR: 2)`
- `[MLB] :warning: player name resolution — 3 unresolved names need manual review: …`

## Config

- `mlb/config.py` holds `SPORT`, `MARKETS`, `PLAYER_PROP_MARKETS`, `BOOKMAKERS`, `ODDS_FORMAT`, `SCORES_DAYS_FROM`.
- `nba/config.py` is created from the existing `config/settings.py` contents.
- `config/settings.py` is deleted once both sports own their config (or kept for a single file of truly cross-sport constants if any emerge — unlikely for MVP).

## Shared-core strategy

For the MVP, **copy rather than share** — MLB gets its own `train.py`, `score.py`, `features.py`, `player_name_resolution.py`. The following are promoted to `shared/` with NFL in mind, once we have two real implementations to factor from:

| From | To | Notes |
|---|---|---|
| `nba/.../features.py::american_odds_to_implied_prob` | `shared/plugins/odds_math.py` | **Promoted during MVP** — zero risk, trivial pure function |
| `nba/.../train.py::_CalibratedModel` | `shared/plugins/ml/calibration.py` | Promote during NFL planning |
| `nba/.../score.py::_allocate_slots` | `shared/plugins/ml/allocation.py` | Promote during NFL planning |
| `nba/.../player_name_resolution.py::normalize_name` | `shared/plugins/name_matching.py` | Promote during NFL planning |
| Generic XGBoost + isotonic training loop | `shared/plugins/ml/train_base.py` | Biggest promotion candidate; needs both NBA + MLB written to design the interface right |

Rule: resist promoting during MLB MVP. Two copies is a signal; one copy "might be reused" is a trap.

## Testing

```
mlb/tests/
├── unit/
│   ├── test_mlb_api_client.py
│   ├── test_mlb_odds_pipeline_dag.py
│   ├── test_mlb_stats_pipeline_dag.py
│   ├── test_mlb_feature_dag.py
│   ├── test_mlb_train_dag.py
│   ├── test_mlb_score_dag.py
│   ├── transformers/
│   │   ├── test_player_game_logs.py
│   │   ├── test_features.py
│   │   └── test_player_name_resolution.py
│   └── ml/
│       ├── test_train.py
│       └── test_score.py
└── integration/
    └── test_transform_to_normalized.py
```

- Unit tests: no Docker, use fixtures for MLB Stats API responses (mock with recorded JSON).
- Integration tests: require `data-postgres` container, exercise full ingest → normalize path against real Postgres with `sport='MLB'` rows.
- Follow `tests/` project pattern — existing `pytest.ini` and `requirements-dev.txt` cover both sports.

## Risk & Open Questions

- **Home runs sparse positives:** The MVP min-rows threshold (100) reduces overfitting risk, but HR model may skip the first few weeks of the season until enough labeled data accumulates. Acceptable — top-10 slot allocator gracefully downshifts when a prop-type has no production model.
- **Doubleheaders:** Same-day DHs exist. `mlb_player_game_logs` uses `(player_id, mlb_game_pk)` as the UNIQUE key so both games are stored; `game_date` is not unique per player. Feature builder joins on `player_id + game_date` — needs to handle the case of two game rows per player per date. Decision: for MVP, if a player has 2 game logs on `game_date`, use the first (chronologically earliest `mlb_game_pk`) for settlement and sum both for actual_stat_value. Revisit if this causes material bugs.
- **MLB Stats API rate limits:** Not documented publicly; assumed permissive. Add throttling if 429s appear.
- **`total_bases` stored at write time:** If we decide to add `batter_strikeouts` (batter, not pitcher) or other markets later, we only need to map to an already-fetched column. No additional stat storage decisions needed for MVP.

## Out of Scope / Post-MVP Ordering

Highest expected lift at the top — this is the order we should tackle next:

1. **Opposing-pitcher features** (handedness matchup, opp SP K/9, opp SP OPS-against). Requires probable-pitcher ingestion (MLB Stats API schedule endpoint returns it). Most important MLB-specific feature.
2. **Pitcher props** (`pitcher_strikeouts`, `pitcher_outs`, `pitcher_earned_runs`). New `mlb_pitcher_game_logs` table + distinct feature set.
3. **Statcast / pybaseball advanced metrics** (exit velocity, xwOBA, barrel rate).
4. **Park factor** feature.
5. **Batting-order lineup-spot** feature.
6. **Doubleheader edge cases** if MVP behavior proves inadequate.
7. **Shared-core refactor** — wait until NFL planning is underway.
8. **Additional batter markets** — `batter_rbis`, `batter_runs_scored`, `batter_stolen_bases`, `batter_hits_runs_rbis`.
