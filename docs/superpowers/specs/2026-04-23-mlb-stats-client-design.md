# MLB Stats Client — Design

**Date:** 2026-04-23
**Status:** Approved (pending spec review)
**Scope:** Implement `mlb/plugins/mlb_stats_client.py` — a thin HTTP client over the public MLB Stats API (`statsapi.mlb.com`) that fetches teams, players, and per-game batter logs for downstream ingestion into the MLB medallion tables. This is step 2 of issue #6.

## Relationship to prior spec

The umbrella spec `2026-04-20-mlb-integration-design.md` describes the full MLB vertical (ingest → stats → features → train → score). This spec narrows to the stats client module only. It adopts `mlb_stats_client.py` as the filename (per issue #6) rather than `mlb_api_client.py` used in the umbrella spec — the explicit `stats` qualifier disambiguates from `shared/plugins/odds_api_client.py`.

## Goals

1. Provide pure-function fetchers for MLB teams, players, and batter game logs that return `list[dict]` keyed to the columns of `mlb_teams`, `mlb_players`, and `mlb_player_game_logs` (defined in `sql/migrations/006_mlb_tables.sql`).
2. Match the division-of-labor pattern of `nba/plugins/nba_api_client.py`: client performs raw fetch; transformers handle DB writes, FK resolution, and cross-entity joins.
3. Add zero new Python dependencies (uses `requests`, already in `requirements.txt`).

## Non-Goals

- **DAG wiring** (`mlb_stats_pipeline_dag.py`, `mlb_stats_backfill_dag.py`) — step 3 of issue #6.
- **Transformers** (`mlb/plugins/transformers/players.py`, etc.) — step 3.
- **Pitcher game logs.** `mlb_player_game_logs` schema has batter columns only. Pitcher-stat support requires a schema change and is deferred to post-MVP per the umbrella spec.
- **Team-level stats** (team game logs, team season stats). Not in the MLB schema; the NBA-side `team_game_logs` / `team_season_stats` support pace-based features that are NBA-specific.
- **Statcast / pybaseball metrics** (exit velocity, xwOBA, barrel rate). Deferred per the umbrella spec.
- **Live API calls in tests.** All tests mock `requests.get`.

## Summary of Decisions

| Decision | Choice |
|---|---|
| Data source | Direct HTTP against `statsapi.mlb.com` (the official MLB Stats API) |
| Python dependency | `requests` only (no new deps, no `pybaseball`, no `MLB-StatsAPI` package) |
| Module shape | Single file, pure functions (no class), returning `list[dict]` |
| Game-log fetch pattern | Per-game via schedule + boxscore endpoints (not per-player) |
| `fetch_players` team_abbreviation | Leave `None` at client level; transformer resolves via `mlb_teams` join |
| Default inter-call sleep | `delay_seconds=0.2` (parameter overridable) |
| Retry budget | 3 attempts, 30s/60s/120s backoffs on 429 / 5xx / Timeout / ConnectionError |
| Test location | `mlb/tests/unit/test_mlb_stats_client.py` |
| `pytest.ini` change | None needed — `mlb/tests` already in `testpaths` |

## Architecture

### Module layout

Single file: `mlb/plugins/mlb_stats_client.py`. Mirrors the shape of `nba/plugins/nba_api_client.py`:

- Module-level constants for base URL, sport ID, timeout, User-Agent, retry backoffs.
- One private helper `_get(path, params, delay_seconds)` for HTTP + retry.
- Three public functions, each mapping to one endpoint family.
- One private boxscore-parsing helper used by the game-logs function.

No class, no `requests.Session`, no threading. Serial calls with sleep between them.

### Public interface

```python
def fetch_teams(delay_seconds: float = 0.2) -> list[dict]: ...
def fetch_players(season: str, active_only: bool = True, delay_seconds: float = 0.2) -> list[dict]: ...
def fetch_batter_game_logs(start_date, end_date, delay_seconds: float = 0.2) -> list[dict]: ...
```

`start_date` / `end_date` accept `date` or `"YYYY-MM-DD"` strings (normalized internally). `season` is a year string like `"2026"`.

### Endpoint mapping

| Function | Endpoint(s) | Notes |
|---|---|---|
| `fetch_teams` | `GET /teams?sportId=1&activeStatus=Y` | One call. |
| `fetch_players` | `GET /sports/1/players?season={season}` (plus `&activeStatus=Y` when `active_only=True`) | One call. |
| `fetch_batter_game_logs` | `GET /schedule?sportId=1&startDate={start}&endDate={end}` → then `GET /game/{gamePk}/boxscore` per Final-status game | Skips Postponed / In Progress. |

Base URL: `https://statsapi.mlb.com/api/v1`.

### Return shapes

Keyed to match DB columns directly, so transformer code reads like `dict[key]` with no remapping.

**`fetch_teams` row**:
```python
{"team_id", "full_name", "abbreviation", "league", "division"}
```

**`fetch_players` row**:
```python
{"player_id", "full_name", "position", "bats", "throws",
 "team_id", "team_abbreviation", "is_active"}
```
`team_abbreviation` is always `None` from the client. `team_id` is nullable (free agents). `bats` / `throws` come from `batSide.code` / `pitchHand.code`.

**`fetch_batter_game_logs` row**:
```python
{"player_id", "mlb_game_pk", "season", "game_date", "matchup",
 "team_id", "opponent_team_id",
 "plate_appearances", "at_bats", "hits", "doubles", "triples",
 "home_runs", "rbi", "runs", "walks", "strikeouts",
 "stolen_bases", "total_bases"}
```
`mlb_game_pk` is a string (matches schema `TEXT`). `season` is derived from `game_date.year`. `matchup` is `"{away_abbr} @ {home_abbr}"`. Only batters with `plateAppearances > 0` are emitted — bench players are filtered out.

### `_get` helper

Responsibilities: prepend base URL, attach `User-Agent`, apply request timeout, sleep `delay_seconds` before the first attempt, retry on transient failures.

Retry matrix:

| Response / error | Retry? |
|---|---|
| 200–299 | No — return `r.json()` |
| 429 | Yes |
| 500–599 | Yes |
| `requests.Timeout` | Yes |
| `requests.ConnectionError` | Yes |
| 4xx other than 429 | No — `raise_for_status()` (programmer error: bad gamePk, bad params) |

Backoffs: `[30, 60, 120]` seconds. Raise after exhausting all three.

Unlike `nba_api_client.py`, there are no browser-mimicking headers (`Sec-Fetch-*`, `Origin`, `Referer`) and no `JSONDecodeError`-as-silent-drop handling. `statsapi.mlb.com` is a cooperative JSON API.

### Loop-level resilience in `fetch_batter_game_logs`

The schedule call is authoritative — if it fails after retries, the whole function raises (nothing to recover from).

Per-game boxscore calls are wrapped individually. A single boxscore failure logs a warning and continues to the next game. If every boxscore in the range fails, the function raises at the end (matches the "all fetches failed" pattern in `mlb_odds_pipeline_dag.fetch_player_props_task`).

## Rate Limiting Posture

MLB Stats API publishes no official rate limit. Community evidence shows it handles thousands of requests/hour without issue but will start throttling under zero-delay high-concurrency hammering. The chosen posture:

- 0.2s default sleep between calls (overridable per-call).
- Serial, not concurrent.
- Polite `User-Agent` identifying the project.
- Exponential backoff on 429 / 5xx.

Expected workload under this posture:

| Workload | Calls | Wall time @ 0.2s |
|---|---|---|
| Daily stats DAG | ~15 boxscores + 1 schedule + 1 players + 1 teams | ~4s |
| Full-season backfill | ~2,430 boxscores + a handful of schedule calls (chunked by month to avoid truncation) + 1 players + 1 teams | ~8 min |

Both are well within polite limits.

## Testing

**Location:** `mlb/tests/unit/test_mlb_stats_client.py`

**Strategy:** Patch `requests.get` to return `MagicMock` responses. Inline dict literals for fixtures (no external JSON files). Patch `time.sleep` in retry tests for speed.

**Test surface:**

1. `test_fetch_teams_maps_fields` — happy-path field mapping.
2. `test_fetch_teams_handles_missing_division` — minor-league-shaped payloads omit `division`; ensure `None`, not `KeyError`.
3. `test_fetch_players_active_only_appends_param` — `activeStatus=Y` present when `active_only=True`, absent when `False`.
4. `test_fetch_players_maps_fields` — one batter + one pitcher fixture, assert `bats` / `throws` / `position` populated.
5. `test_fetch_players_free_agent_has_null_team` — missing `currentTeam` → `team_id: None`.
6. `test_fetch_batter_game_logs_single_game` — 1 gamePk → 2 batter rows (home + away) with correct `opponent_team_id` and `matchup`.
7. `test_fetch_batter_game_logs_skips_non_final_games` — Postponed and In Progress games in the schedule are not fetched.
8. `test_fetch_batter_game_logs_excludes_bench_players` — `plateAppearances: 0` filtered out.
9. `test_fetch_batter_game_logs_skips_failed_boxscore` — one boxscore returns 500 after retries; warning logged, other game's rows still returned.
10. `test_get_retries_on_429` — `_get` retries with backoff on 429, then succeeds. Verify sleep was called with each backoff value.

## Follow-ups (not in scope)

- **Step 3 of issue #6**: transformers (`mlb/plugins/transformers/{players,teams,player_game_logs,player_name_resolution}.py`), stats pipeline DAG, backfill DAG. Separate spec + plan.
- **`mlb_game_id_linker`** transformer analogous to NBA's `link_nba_game_ids` — also step 3.
- **Pitcher stats** — post-MVP, requires `mlb_pitcher_game_logs` table.
- **Statcast integration** — post-MVP, likely a separate client module (`mlb_statcast_client.py` or adoption of `pybaseball`).
