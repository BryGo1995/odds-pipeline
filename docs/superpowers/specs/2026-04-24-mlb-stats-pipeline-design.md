# MLB Stats Pipeline — Design

**Date:** 2026-04-24
**Status:** Approved (pending spec review)
**Scope:** Wire the step-2 `mlb_stats_client` into a working daily + backfill ingest pipeline. Adds four transformers (`teams`, `players`, `player_game_logs`, `player_name_resolution`) and two DAGs (`mlb_stats_pipeline_dag`, `mlb_stats_backfill_dag`) to produce populated `mlb_teams`, `mlb_players`, `mlb_player_game_logs`, and `mlb_player_name_mappings` tables. Step 3 of issue #6.

## Relationship to prior specs

- `2026-04-20-mlb-integration-design.md` (umbrella): describes the full MLB vertical. This spec narrows to the stats-ingest vertical (§Transformers + §Pipelines sections of the umbrella).
- `2026-04-23-mlb-stats-client-design.md` (step 2): the client this pipeline consumes. Already landed at commit `ce32a6e`.

Features, training, scoring, and settlement (umbrella §Features/ML/Settlement) are **out of scope** for this step and belong to later DAGs (`mlb_feature_dag`, `mlb_train_dag`, `mlb_score_dag`).

## Goals

1. Populate `mlb_teams`, `mlb_players`, `mlb_player_game_logs` daily via an Airflow DAG that waits on `mlb_odds_pipeline`.
2. Resolve Odds-API player names to `mlb_player_id` and backfill `player_props.mlb_player_id` for MLB games (gating for later feature / score DAGs).
3. Provide a manual-trigger backfill DAG that seeds one or more historical MLB seasons.
4. Mirror the NBA pattern exactly where applicable; resist shared-core refactors that only have one callsite.

## Non-Goals

- **Game-ID linking** (`mlb_game_pk` ↔ `games.game_id`). Migration 006 does not add `mlb_game_pk` to `games`, and umbrella §Settlement joins on `(mlb_player_id, game_date)`. No linker needed.
- **Team game logs / team season stats.** Intentionally omitted per umbrella (MLB batter props are pitcher-driven, not team-shape-driven).
- **Features, ML training, scoring, settlement.** Separate umbrella steps / separate specs.
- **Schema changes.** Migration 006 already defines all tables.
- **New Python dependencies.** `rapidfuzz` already in `requirements.txt`.
- **Shared-core refactor of `normalize_name` or DAG helpers.** Deferred until NFL planning per umbrella §"Shared-core strategy".

## Summary of Decisions

| Decision | Choice |
|---|---|
| Transformer shape | Pure functions `(conn, raw_rows)` + `INSERT … ON CONFLICT` upsert + `conn.commit()`. Mirrors NBA. |
| `team_abbreviation` in `mlb_players` | Resolved by `transform_players` via `mlb_teams` join (client leaves `None`). |
| `normalize_name` in `player_name_resolution` | Duplicate NBA's implementation verbatim. Promotion to `shared/` deferred to NFL planning. |
| Fuzzy-match threshold | ≥95.0 rapidfuzz `ratio`, identical to NBA. |
| Sport filter | `baseball_mlb` (matches `mlb/config.py::SPORT` and `games.sport`). |
| Daily game-log window | 3-day lookback: `start = today - 3 days`, `end = yesterday`. Idempotent re-ingest via `ON CONFLICT DO NOTHING`. |
| Current season (daily) | Auto-derived: `str(date.today().year)`. |
| Backfill parameterization | `season_start` + `season_end` as year strings (e.g. `"2023"`, `"2025"`). |
| Backfill date window per season | Fixed `{YEAR}-03-15` to `{YEAR}-11-15` (spring training through World Series). |
| Backfill `fetch_players` | `active_only=False` — ingest historical rosters before game logs so the FK on `mlb_player_game_logs.player_id` is satisfied. |
| DAG schedule (daily) | `20 15 * * *` UTC (8:20am MT), waits on `mlb_odds_pipeline` via `ExternalTaskSensor`. |
| DAG schedule (backfill) | `None` (manual trigger only). |
| Settlement task in stats DAG | **No.** Lives in the future `mlb_score_dag` per umbrella. |
| `raw_api_responses.endpoint` namespacing | `mlb_api/teams`, `mlb_api/players`, `mlb_api/player_game_logs`. |
| Test strategy | `MagicMock`-based `conn`/`cursor`. No real DB. Mirrors NBA transformer tests. |
| Airflow stubbing | New `mlb/tests/unit/conftest.py` stubs `airflow.*` in `sys.modules` (copied from NBA's conftest). Shared conftest deferred to NFL planning. |

## Architecture

### File layout

**New files (10 source + 6 test):**

```
mlb/
├── plugins/
│   └── transformers/
│       ├── __init__.py
│       ├── teams.py
│       ├── players.py
│       ├── player_game_logs.py
│       └── player_name_resolution.py
├── dags/
│   ├── mlb_stats_pipeline_dag.py
│   └── mlb_stats_backfill_dag.py
└── tests/
    └── unit/
        ├── conftest.py                        # airflow stubs (copy of nba conftest)
        ├── transformers/
        │   ├── __init__.py
        │   ├── test_teams.py
        │   ├── test_players.py
        │   └── test_player_game_logs.py
        ├── test_player_name_resolution.py     # top-level, matches NBA placement
        ├── test_mlb_stats_pipeline_dag.py
        └── test_mlb_stats_backfill_dag.py
```

Existing `mlb/plugins/mlb_stats_client.py` (step 2) is imported but not modified.

### Data flow

```
(mlb_odds_pipeline completes)
         │
         ▼
ExternalTaskSensor "wait_for_mlb_odds_pipeline"
         │
         ▼
┌────────────────┬─────────────────┬─────────────────────────┐
│   fetch_teams  │  fetch_players  │  fetch_batter_game_logs │
└────────────────┴─────────────────┴─────────────────────────┘
         │              │                    │
         ▼              ▼                    ▼
  transform_teams    transform_players    transform_player_game_logs
         │              │                    │
         └──────────────┤                    │
                        ▼                    │
                  (mlb_teams is populated,   │
                   so transform_players can  │
                   resolve team_abbreviation)│
                        │                    │
                        └────────────────────┤
                                             ▼
                                (mlb_players is populated,
                                 so transform_player_game_logs
                                 won't FK-violate)
                                             │
                                             ▼
                                   resolve_player_ids
                                             │
                                             ▼
                                          (done)
```

Task order enforces FK readiness: teams → players → game_logs. The parallel-fetch then serialized-transform shape mirrors `nba_stats_pipeline_dag.py`.

## Transformers

### `transformers/teams.py` (~25 lines)

Pure function. Upserts each dict from `fetch_teams` output into `mlb_teams`:

```python
def transform_teams(conn, raw_teams):
    if not raw_teams:
        return
    with conn.cursor() as cur:
        for t in raw_teams:
            cur.execute(
                """
                INSERT INTO mlb_teams (team_id, full_name, abbreviation, league, division)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (team_id) DO UPDATE SET
                    full_name    = EXCLUDED.full_name,
                    abbreviation = EXCLUDED.abbreviation,
                    league       = EXCLUDED.league,
                    division     = EXCLUDED.division,
                    fetched_at   = NOW()
                """,
                (t["team_id"], t["full_name"], t["abbreviation"],
                 t.get("league"), t.get("division")),
            )
    conn.commit()
```

### `transformers/players.py` (~35 lines)

Upserts into `mlb_players`. Resolves `team_abbreviation` via a single `SELECT team_id, abbreviation FROM mlb_teams` lookup built at function entry.

```python
def transform_players(conn, raw_players):
    if not raw_players:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT team_id, abbreviation FROM mlb_teams")
        team_abbr = dict(cur.fetchall())  # {team_id: abbreviation}

        for p in raw_players:
            tid = p.get("team_id")
            cur.execute(
                """
                INSERT INTO mlb_players
                    (player_id, full_name, position, bats, throws,
                     team_id, team_abbreviation, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name         = EXCLUDED.full_name,
                    position          = EXCLUDED.position,
                    bats              = EXCLUDED.bats,
                    throws            = EXCLUDED.throws,
                    team_id           = EXCLUDED.team_id,
                    team_abbreviation = EXCLUDED.team_abbreviation,
                    is_active         = EXCLUDED.is_active,
                    fetched_at        = NOW()
                """,
                (p["player_id"], p["full_name"],
                 p.get("position"), p.get("bats"), p.get("throws"),
                 tid, team_abbr.get(tid), p.get("is_active", True)),
            )
    conn.commit()
```

If `mlb_teams` is empty at invocation time (shouldn't happen given DAG ordering), `team_abbreviation` falls back to `None`. Safe.

### `transformers/player_game_logs.py` (~75 lines)

Upserts into `mlb_player_game_logs` with per-row `SAVEPOINT` to isolate FK violations (mirrors NBA's pattern — rarely triggers in steady state but prevents one bad row from killing the whole batch):

```python
def transform_player_game_logs(conn, raw_logs):
    if not raw_logs:
        return
    with conn.cursor() as cur:
        for log in raw_logs:
            cur.execute("SAVEPOINT pgl_row")
            try:
                cur.execute(
                    """
                    INSERT INTO mlb_player_game_logs
                        (player_id, mlb_game_pk, season, game_date, matchup,
                         team_id, opponent_team_id,
                         plate_appearances, at_bats, hits, doubles, triples,
                         home_runs, rbi, runs, walks, strikeouts,
                         stolen_bases, total_bases)
                    VALUES (%s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s)
                    ON CONFLICT (player_id, mlb_game_pk) DO NOTHING
                    """,
                    (log["player_id"], log["mlb_game_pk"], log["season"],
                     log["game_date"], log.get("matchup"),
                     log.get("team_id"), log.get("opponent_team_id"),
                     log.get("plate_appearances"), log.get("at_bats"),
                     log.get("hits"), log.get("doubles"), log.get("triples"),
                     log.get("home_runs"), log.get("rbi"), log.get("runs"),
                     log.get("walks"), log.get("strikeouts"),
                     log.get("stolen_bases"), log.get("total_bases")),
                )
                cur.execute("RELEASE SAVEPOINT pgl_row")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT pgl_row")
                cur.execute("RELEASE SAVEPOINT pgl_row")
    conn.commit()
```

`ON CONFLICT (player_id, mlb_game_pk) DO NOTHING` makes the 3-day lookback idempotent.

### `transformers/player_name_resolution.py` (~115 lines)

Near-duplicate of `nba/plugins/transformers/player_name_resolution.py` with three changes:

1. **Sport filter in unresolved-names query**: `AND g.sport = 'baseball_mlb'` (instead of `'basketball_nba'`).
2. **Known-players query**: `SELECT player_id, full_name FROM mlb_players` (instead of `players`).
3. **Insert target + backfill query**:
   ```sql
   INSERT INTO mlb_player_name_mappings (odds_api_name, mlb_player_id, confidence) …
   UPDATE player_props pp SET mlb_player_id = m.mlb_player_id
     FROM mlb_player_name_mappings m, games g
     WHERE pp.player_name = m.odds_api_name
       AND g.game_id = pp.game_id
       AND g.sport = 'baseball_mlb'
       AND pp.mlb_player_id IS NULL
   ```

The `normalize_name` function (lowercase, strip accents, drop Jr./Sr./II/III/IV suffixes) is duplicated verbatim from NBA. Same `CONFIDENCE_THRESHOLD = 95.0`. Same Slack alert structure with a `[MLB]` prefix on the `send_slack_message` body.

## DAGs

### `mlb_stats_pipeline_dag.py` (~280 lines, mirrors NBA)

**Schedule**: `"20 15 * * *"` UTC (8:20am MT). `start_date = pendulum.datetime(2024, 1, 1, tz="America/Denver")`. `catchup=False`. `tags=["mlb", "stats"]`. `on_failure_callback=notify_failure`.

**default_args**: `retries=3`, `retry_delay=timedelta(minutes=5)`, `retry_exponential_backoff=True`.

**Sensor**:
```python
wait_for_odds = ExternalTaskSensor(
    task_id="wait_for_mlb_odds_pipeline",
    external_dag_id="mlb_odds_pipeline",
    external_task_id=None,
    mode="reschedule", poke_interval=60, timeout=3600,
    execution_delta=timedelta(minutes=20),
)
```

**Helpers** (module-level):
- `_current_season() -> str`: `str(date.today().year)`
- `_game_log_window() -> tuple[date, date]`: `start = date.today() - timedelta(days=3)`, `end = date.today() - timedelta(days=1)`
- `_get_latest_raw(conn, endpoint) -> list[dict]`: identical to the helper in `mlb_odds_pipeline_dag.py` — `SELECT response FROM raw_api_responses WHERE endpoint = %s AND status = 'success' ORDER BY fetched_at DESC LIMIT 1`; raises `ValueError` if none.

**Ingest task callables** (each stores raw via `store_raw_response`):

```python
def fetch_teams_task(**ctx):
    conn = get_data_db_conn()
    try:
        data = fetch_teams(delay_seconds=1)
        store_raw_response(conn, "mlb_api/teams", {}, data)
    except Exception:
        store_raw_response(conn, "mlb_api/teams", {}, None, status="error")
        raise
    finally:
        conn.close()

def fetch_players_task(**ctx): ...        # fetch_players(season=_current_season())
def fetch_player_game_logs_task(**ctx):   # fetch_batter_game_logs(start, end) over window
    ...
```

**Transform task callables**: each grabs latest raw via `_get_latest_raw`, calls corresponding transformer.

**Task wiring**:
```python
wait_for_odds >> [t_fetch_teams, t_fetch_players, t_fetch_pgl]
t_fetch_teams >> t_xform_teams
t_fetch_players >> t_xform_players
t_fetch_pgl    >> t_xform_pgl
t_xform_teams >> t_xform_players          # FK: mlb_teams must exist for abbrev lookup
t_xform_players >> t_xform_pgl            # FK: mlb_players must exist for game_logs
t_xform_pgl >> t_resolve_player_ids
```

### `mlb_stats_backfill_dag.py` (~110 lines)

**Schedule**: `None` (manual trigger). `default_args`: `retries=1`, `retry_delay=timedelta(minutes=10)`. `catchup=False`. `tags=["mlb", "stats", "backfill"]`.

**Airflow Params**:
- `season_start`: `Param("2023", type="string", description="Start season YYYY (e.g. 2023)")`
- `season_end`: `Param("2025", type="string", description="End season YYYY (e.g. 2025)")`

**Constants**:
- `BACKFILL_DELAY_SECONDS = 1.0`
- `SEASON_START_MONTH_DAY = "03-15"`
- `SEASON_END_MONTH_DAY = "11-15"`
- `SEASON_PATTERN = re.compile(r"^\d{4}$")`

**Helpers**:
- `_season_range(start, end)`: yields year strings from `int(start)` through `int(end)` inclusive.

**Single `run_backfill` task** (sequential, keeps rate-limit story simple and failure resumable via param re-entry):

1. Validate params (pattern + ordering; raise `ValueError` on bad input).
2. Open connection.
3. Ingest teams once (MLB team set is stable across seasons).
4. For each season in range:
   - `fetch_players(season=season, active_only=False, delay_seconds=1)` → store → `transform_players`. `active_only=False` is critical — historical game logs reference retired/traded players who aren't on current rosters.
   - `fetch_batter_game_logs(f"{season}-03-15", f"{season}-11-15", delay_seconds=1)` → store → `transform_player_game_logs`.
   - `time.sleep(BACKFILL_DELAY_SECONDS)` between seasons.
5. `resolve_player_ids(conn, slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL"))` at the end.

**What this DAG does not do**:
- No game-ID linker call (not needed for MLB).
- No retroactive population of `player_props.mlb_player_id` for props that didn't exist at backfill time — `resolve_player_ids` handles what's in the DB when it runs, and the daily DAG picks up the rest going forward.

## Rate-limit posture

MLB Stats API is unauthenticated and cooperative. Posture matches the umbrella's general guidance:

| DAG | `delay_seconds` | Per-run calls (approx) | Wall time |
|---|---|---|---|
| Daily | `1.0` | 1 teams + 1 players + ~45 boxscores | ~55s |
| Backfill (per season) | `1.0` | 1 players + ~200 schedule-days + ~2,430 boxscores | ~45 min |
| Backfill (3 seasons) | `1.0` | Proportional | ~2.5 hours |

Delays are intentionally higher than the client's 0.2s default — cheap extra headroom against any 429 behavior we haven't seen. Still well within polite limits.

## Testing

### Transformer tests (mock-based, no DB)

Pattern copied from `nba/tests/unit/transformers/test_players.py`:

```python
def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor
```

Per-transformer test surface:

- **`test_teams.py`** (3 tests): empty list is a no-op; upsert inserts expected bind params; `commit` called once.
- **`test_players.py`** (4 tests): empty list no-op; `team_abbreviation` resolved from `mlb_teams` lookup; missing team returns `None` abbrev; `commit` called once. Lookup is mocked by setting `mock_cursor.fetchall.return_value` for the teams `SELECT`.
- **`test_player_game_logs.py`** (4 tests): empty list no-op; upsert writes 19 bind params in order; SAVEPOINT branch — one row raises, other rows still land and `commit` is called; doubleheader — two rows with same `(player_id, game_date)` but different `mlb_game_pk` both inserted.
- **`test_player_name_resolution.py`** (top-level, 6 tests):
  - Exact match → inserted with confidence 100.
  - Fuzzy match ≥ 95 → inserted.
  - Fuzzy match < 95 → not inserted, Slack alert with unresolved name.
  - Sport filter — verified by asserting `"g.sport = 'baseball_mlb'"` appears in the SQL string passed to `cur.execute` (NBA's resolver embeds sport as a literal, not a bind param; MLB's must too).
  - Backfill `UPDATE player_props` SQL string contains `g.sport = 'baseball_mlb'`.
  - No unresolved → no Slack call.

### DAG tests

- **`test_mlb_stats_pipeline_dag.py`** (~6 tests): DAG loads without exception (Airflow stubbed via conftest); expected task IDs present; dependency graph matches spec; each ingest-task callable with mocked `get_data_db_conn` + mocked client function calls `store_raw_response` correctly; error path sets `status="error"` on raw response.
- **`test_mlb_stats_backfill_dag.py`** (~5 tests): DAG loads; bad `season_start` format raises `ValueError`; `season_end < season_start` raises `ValueError`; iterating 3 seasons calls `fetch_players` 3× and `fetch_batter_game_logs` 3× with correct year-stamped date windows; single-task structure has `task_id="run_backfill"`.

### Conftest

New `mlb/tests/unit/conftest.py` (copy of `nba/tests/unit/conftest.py`'s Airflow-stubbing part, minus the `nba_api` stub which MLB doesn't need). Pseudocode:

```python
import sys
from unittest.mock import MagicMock

def _stub_airflow():
    if "airflow" in sys.modules:
        return
    sys.modules.setdefault("airflow", MagicMock())
    sys.modules.setdefault("airflow.models", MagicMock())
    sys.modules.setdefault("airflow.models.param", MagicMock())
    sys.modules.setdefault("airflow.operators", MagicMock())
    sys.modules.setdefault("airflow.operators.python", MagicMock())
    sys.modules.setdefault("airflow.sensors", MagicMock())
    sys.modules.setdefault("airflow.sensors.external_task", MagicMock())

_stub_airflow()
```

### What we don't test

- Live API calls. Mocked everywhere.
- Integration tests against a real Postgres container. `shared/tests/integration/` is separate; MLB integration coverage is out of scope for this step.
- End-to-end DAG execution. Structural + per-task-callable coverage is sufficient and matches NBA's depth.

## Follow-ups

These land in later steps, not in this plan:

- **`mlb_feature_dag`** + `mlb/plugins/transformers/features.py` — reads from `mlb_player_game_logs`, writes `features/mlb_props_features_<date>.parquet`.
- **`mlb_train_dag`** + `mlb/plugins/ml/train.py` — per-prop-type XGBoost + isotonic calibration.
- **`mlb_score_dag`** + `mlb/plugins/ml/{score,settle}.py` — writes recommendations + settles prior runs.
- **Shared-core promotion** of `normalize_name` and the Airflow-stubbing conftest — during NFL planning.
- **Opposing-pitcher features** — highest post-MVP lift per umbrella §"Out of Scope / Post-MVP Ordering".
