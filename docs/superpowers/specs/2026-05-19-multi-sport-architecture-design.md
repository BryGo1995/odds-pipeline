# Multi-Sport Architecture Refactor — Design

**Status:** Draft
**Date:** 2026-05-19
**Author:** bryanj.gonzales95@gmail.com (with Claude)

## Goal

Refactor the NBA and MLB pipelines so that adding an NFL pipeline becomes a **config + one plugin module** exercise, not a third copy-paste of ~1200 lines of DAGs and ~1200 lines of ML modules.

**Non-goals:** This refactor does not add NFL itself. NFL will be added in a follow-up effort, on top of the architecture established here.

## Constraints

- **Refactor only.** No NFL onboarding in this work. NFL is the forcing function shaping the design, not a deliverable.
- **Incremental.** Five phases, each its own PR, each independently revertable.
- **Looser-than-strict behavior preservation.** Neither pipeline is mission-critical yet — minor output shifts during cleanup are acceptable, but we won't be cavalier. Golden tests cover the risky ML phase.
- **Address DAG duplication first.** This is the user's #1 stated pain point; Phase 1 attacks it directly for immediate visible win.

## Current State

The repo has two sister packages, `nba/` and `mlb/`, with parallel directory structures and substantial duplication that has already drifted in places:

### DAG duplication

| DAG | NBA LOC | MLB LOC | Status |
|---|---|---|---|
| `odds_pipeline_dag.py` | 228 | 236 | ~95% identical |
| `odds_backfill_dag.py` | 79 | 83 | ~95% identical |
| `feature_dag.py` | 63 | 64 | ~98% identical |
| `feature_backfill_dag.py` | 65 | 66 | ~98% identical |
| `train_dag.py` | 36 | 36 | 100% identical except import path |
| `score_dag.py` | 54 | 54 | 100% identical except import path |
| `stats_pipeline_dag.py` | 276 | 200 | Genuinely different shape per sport |
| `stats_backfill_dag.py` | 104 | 117 | Genuinely different shape per sport |

NFL would inherit a third copy of all of this — ~1200+ duplicated LOC.

### ML module duplication

| Module | NBA LOC | MLB LOC | Notes |
|---|---|---|---|
| `ml/train.py` | 276 | 253 | Drifted: NBA has unused "general model" path with `prop_type_encoded`; MLB doesn't. NBA's `prepare_features` returns a 3-tuple; MLB's returns 2-tuple. |
| `ml/score.py` | 163 | 160 | ~95% identical |
| `ml/settle.py` | 172 | 180 | ~90% identical; differ in JOIN tables and the MLB doubleheader CTE |

### Data layer inconsistencies

- `raw_api_responses.endpoint`: NBA uses bare `"events"`/`"odds"`/`"scores"`/`"player_props"`; MLB uses `"mlb_events"`/etc. NFL would be a third convention.
- Parquet filenames: NBA writes `props_features_*.parquet`; MLB writes `mlb_props_features_*.parquet`.
- Stats tables: NBA tables are unprefixed (`players`, `player_game_logs`); MLB tables are prefixed (`mlb_player_game_logs`, `mlb_player_name_mappings`).

## Design

### Architecture overview

```
shared/
  sport_config.py            # SportConfig dataclass — the contract every sport implements
  dag_factories/
    odds_pipeline.py         # build_odds_pipeline_dag(sport) -> DAG
    odds_backfill.py
    feature.py
    feature_backfill.py
    train.py
    score.py
    stats_helpers.py         # shared helpers used by per-sport stats DAGs
    stats_components.py      # task-builder functions used by per-sport stats DAGs
  ml/
    train.py                 # sport-agnostic; accepts SportConfig
    score.py
    settle.py                # accepts SportConfig + sport-supplied fetch_resolvable_rows callable
  plugins/                   # unchanged
nba/
  sport.py                   # exports NBA_SPORT = SportConfig(...)
  dags/                      # 6 thin entrypoints (~5 LOC each) + 2 sport-shaped stats DAGs
  plugins/
    stats_client.py          # NBA stats source wrapper
    transformers/            # NBA-specific feature/stats transformers
    ml/settle_lookup.py      # NBA-specific settle JOIN
mlb/                         # symmetric to nba/
nfl/                         # FUTURE: same shape; existing factories pick up DAGs with no changes
```

### Section 1 — The `SportConfig` dataclass

A single immutable dataclass captures every sport-specific knob:

```python
# shared/sport_config.py
from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class SportConfig:
    # --- Identity ----------------------------------------------------------
    sport_id: str                       # Odds-API sport key, e.g. "basketball_nba"
    sport_label: str                    # Display label, e.g. "NBA" — written to recommendations.sport
    sport_slug: str                     # Lowercase slug, e.g. "nba" — used in DAG IDs,
                                        # endpoint prefixes, parquet filenames, MLflow model names

    # --- Odds-API ----------------------------------------------------------
    markets: tuple[str, ...]            # ("h2h", "spreads", "totals")
    prop_markets: tuple[str, ...]       # ("player_points", "player_rebounds", ...)
    bookmakers: tuple[str, ...]
    regions: tuple[str, ...]            # ("us",)
    odds_format: str                    # "american"
    scores_days_from: int               # 3

    # --- ML ----------------------------------------------------------------
    prop_stat_map: dict[str, str]       # {"player_points": "pts", ...}
    per_prop_features: tuple[str, ...]  # XGBoost feature columns
    min_training_rows: int              # 50 by default

    # --- Scheduling --------------------------------------------------------
    start_date: datetime
    odds_pipeline_cron: str
    stats_pipeline_cron: str
    feature_dag_cron: str
    score_dag_cron: str
    train_dag_cron: str

    # --- Derived ----------------------------------------------------------
    @property
    def model_name(self) -> str:
        return f"{self.sport_slug}_prop_model"

    @property
    def features_filename_prefix(self) -> str:
        return f"{self.sport_slug}_props_features"

    def raw_endpoint(self, name: str) -> str:
        """Compose a namespaced raw_api_responses.endpoint value."""
        return f"{self.sport_slug}_{name}"
```

The dataclass is **pure data**. Sport-specific *behavior* (e.g., the settle JOIN query) lives in sport-specific modules and is passed as a callable to shared code.

Example NBA instantiation:

```python
# nba/sport.py
from shared.sport_config import SportConfig
import pendulum
from nba.plugins.transformers.features import PROP_STAT_MAP

NBA_SPORT = SportConfig(
    sport_id="basketball_nba",
    sport_label="NBA",
    sport_slug="nba",
    markets=("h2h", "spreads", "totals"),
    prop_markets=("player_points", "player_rebounds", "player_assists"),
    bookmakers=("draftkings", "fanduel", "betmgm"),
    regions=("us",),
    odds_format="american",
    scores_days_from=3,
    prop_stat_map=PROP_STAT_MAP,
    per_prop_features=(
        "implied_prob_over", "line_movement",
        "rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g", "rolling_std_10g",
        "is_home", "rest_days",
    ),
    min_training_rows=50,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    odds_pipeline_cron="0 15 * * *",
    stats_pipeline_cron="20 15 * * *",
    feature_dag_cron="40 15 * * *",
    score_dag_cron="0 16 * * *",
    train_dag_cron="0 10 * * 1",
)
```

### Section 2 — Shared DAG factories (the 6 identical-shape DAGs)

Six DAGs become true factories. Each factory accepts a `SportConfig` and returns an `airflow.DAG`. Per-sport DAG files shrink to 3-5 line entrypoints.

| Factory | Per-sport entrypoint |
|---|---|
| `shared/dag_factories/odds_pipeline.py` | `nba/dags/odds_pipeline_dag.py` |
| `shared/dag_factories/odds_backfill.py` | `nba/dags/odds_backfill_dag.py` |
| `shared/dag_factories/feature.py` | `nba/dags/feature_dag.py` |
| `shared/dag_factories/feature_backfill.py` | `nba/dags/feature_backfill_dag.py` |
| `shared/dag_factories/train.py` | `nba/dags/train_dag.py` |
| `shared/dag_factories/score.py` | `nba/dags/score_dag.py` |

Factory signature (example — score):

```python
# shared/dag_factories/score.py
def build_score_dag(sport: SportConfig) -> DAG:
    with DAG(
        dag_id=f"{sport.sport_slug}_score_dag",
        default_args=_DEFAULT_ARGS,
        description=f"Score today's {sport.sport_label} props and write ranked recommendations",
        schedule_interval=sport.score_dag_cron,
        start_date=sport.start_date,
        catchup=False,
        tags=[sport.sport_slug, "ml"],
        on_success_callback=notify_score_ready,
        on_failure_callback=notify_failure,
    ) as dag:
        wait_for_features = ExternalTaskSensor(
            task_id=f"wait_for_{sport.sport_slug}_feature_dag",
            external_dag_id=f"{sport.sport_slug}_feature_dag",
            external_task_id=None, mode="reschedule",
            poke_interval=60, timeout=3600,
            execution_delta=timedelta(minutes=20),
        )
        t_score = PythonOperator(
            task_id="score",
            python_callable=lambda **ctx: _run_score(sport, ctx["ds"]),
        )
        wait_for_features >> t_score
    return dag
```

Per-sport DAG file:

```python
# nba/dags/score_dag.py
from nba.sport import NBA_SPORT
from shared.dag_factories.score import build_score_dag

dag = build_score_dag(NBA_SPORT)
```

**DAG IDs are preserved** — the factory builds them from `sport.sport_slug`, so existing IDs (`nba_score_dag`, `mlb_odds_pipeline`, etc.) continue to work and the Airflow UI history is uninterrupted.

### Section 3 — Stats pipeline: per-sport DAGs + shared helpers (Option C)

The stats pipeline is **not** a factory because the dependency wiring genuinely differs per sport:

- NBA: 5 fetches → fan-in to `transform_teams` → fan-out to player/team/season transforms → `link_nba_game_ids` → settle.
- MLB: 3 fetches → individual fetch→transform chains → linear FK ordering → settle.
- NFL: TBD; weekly cadence, bye weeks, different stats source shape.

Trying to abstract the dep wiring with N=2 examples would require inventing a mini DAG-edge DSL and would still need escape hatches. Instead:

- `shared/dag_factories/stats_helpers.py` — shared `_fetch_and_store`, `_get_latest_raw`, `_DEFAULT_ARGS`, `notify_failure` wiring.
- `shared/dag_factories/stats_components.py` — task builders: `make_wait_for_odds_sensor(sport)`, `make_settle_tail_task(sport, lookup_fn)`, `make_fetch_task(endpoint, fetch_fn)`, `make_transform_task(endpoint, transform_fn)`.

Each sport's `stats_pipeline_dag.py` shrinks to ~80-100 LOC: a `with DAG(...)` block that calls the task builders and writes the dependency wiring as plain Airflow `>>` operators. Adding NFL = copy MLB's template, edit fetches/transforms/edges.

### Section 4 — Shared ML modules

```
shared/ml/
  train.py       # train_model(sport, prop_type) + train_all_models(sport)
  score.py       # score(conn, sport, game_date)
  settle.py      # settle_recommendations(conn, sport, fetch_resolvable_rows)
```

**`shared/ml/train.py`** — accepts `SportConfig`. Reads `sport.prop_stat_map`, `sport.per_prop_features`, `sport.model_name`, and uses `sport.features_filename_prefix` to glob `{features_dir}/{prefix}_*.parquet`. The `prepare_features()` signature unifies on `(X, y)` — matching MLB's current shape; NBA's unused "general model" path with `LabelEncoder` and `prop_type_encoded` is deleted (dead code since per-prop models replaced it in March).

**`shared/ml/score.py`** — same parameterization. Slot allocation, edge computation, sort+rank, and the recommendations INSERT collapse to one function. Sport label written from `sport.sport_label`.

**`shared/ml/settle.py`** — needs sport-specific behavior because the SQL JOIN differs:

```python
# shared/ml/settle.py
def settle_recommendations(
    conn,
    sport: SportConfig,
    fetch_resolvable_rows: Callable[[Any, SportConfig], list[tuple]],
) -> None:
    """
    fetch_resolvable_rows must return rows of
    (rec_id, player_name, prop_type, line, game_date, stat_value).

    Shared orchestration: mark settled, mark stale, send Slack recap.
    """
```

```python
# nba/plugins/ml/settle_lookup.py
def fetch_resolvable_rows(conn, sport):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.id, r.player_name, r.prop_type, r.line, r.game_date,
                   CASE r.prop_type
                       WHEN 'player_points'   THEN pgl.pts
                       WHEN 'player_rebounds' THEN pgl.reb
                       WHEN 'player_assists'  THEN pgl.ast
                   END
            FROM recommendations r
            JOIN player_name_mappings m ON m.odds_api_name = r.player_name
            JOIN player_game_logs pgl
                ON pgl.player_id = m.nba_player_id
               AND pgl.game_date = r.game_date
            WHERE r.settled_at IS NULL
              AND r.game_date < CURRENT_DATE
              AND r.sport = %s
            """,
            (sport.sport_label,),
        )
        return cur.fetchall()
```

MLB's `settle_lookup.py` carries the doubleheader CTE. NFL's will carry its own join logic.

### Section 5 — Per-sport package contract

A sport package is "complete" when it provides:

**Required:**
1. `sport.py` — exports a `SportConfig` instance.
2. `plugins/stats_client.py` — duck-typed wrapper for the sport's stats source.
3. `plugins/transformers/features.py` — exports `PROP_STAT_MAP: dict[str, str]` and `build_features(conn, game_date) -> pd.DataFrame`.
4. `plugins/transformers/players.py` — `transform_players(conn, raw_data)`.
5. `plugins/transformers/teams.py` — `transform_teams(conn, raw_data)`.
6. `plugins/transformers/player_game_logs.py` — `transform_player_game_logs(conn, raw_data)`.
7. `plugins/transformers/player_name_resolution.py` — `resolve_player_ids(conn, slack_webhook_url=None)`.
8. `plugins/ml/settle_lookup.py` — `fetch_resolvable_rows(conn, sport)`.
9. `dags/stats_pipeline_dag.py` — sport-shaped, built from shared task-builder helpers.
10. `dags/stats_backfill_dag.py` — sport-shaped.
11. A SQL migration adding `{slug}_player_game_logs`, `{slug}_player_name_mappings`, etc.

**Optional sport-specific transformers** (added if the sport needs them):
- `transformers/team_game_logs.py`
- `transformers/team_season_stats.py`
- `transformers/game_id_linker.py`

**Generated for free** — six thin DAG entrypoint files (~30 LOC total), one for each shared factory.

**Total NFL-specific code estimate:** ~500–800 LOC of real logic + ~30 LOC of thin DAG entrypoints.

### Data layer (schema) — keep sport-prefixed stats tables

The existing pattern is correct:

- **Shared tables** (`games`, `odds`, `player_props`, `scores`, `recommendations`) discriminate by a `sport` column.
- **Sport-specific stats tables** are prefixed (`mlb_player_game_logs`, future `nfl_player_game_logs`).

NBA's existing tables remain unprefixed (`players`, `player_game_logs`) as legacy — migrating them would be a heavy data move on a non-live system. The forward convention is `{slug}_*` for all new sports.

### Data layer cleanup (one-time migrations)

Two small migrations land in Phase 4:

**1. `raw_api_responses.endpoint` namespace unification:**
```sql
UPDATE raw_api_responses SET endpoint = 'nba_' || endpoint
WHERE endpoint IN ('events','odds','scores','player_props');
```
NBA's `nba_api/*` rows are already prefixed and left alone. Migration is naturally idempotent — the WHERE clause won't match after the first run.

**2. NBA parquet rename:**
```bash
for f in /data/features/props_features_*.parquet; do
  mv "$f" "${f/props_features_/nba_props_features_}"
done
```

## Migration phasing

Five phases. Each is its own PR; each independently revertable.

### Phase 0 — Foundations (no behavior change)
- Add `shared/sport_config.py`, `nba/sport.py`, `mlb/sport.py`.
- No existing code wired to them yet.
- **Test:** dataclass instantiates; values match `nba/config.py` / `mlb/config.py`.

### Phase 1 — Six DAG factories (the big visible win)
- Add `shared/dag_factories/{odds_pipeline,odds_backfill,feature,feature_backfill,train,score}.py`.
- Replace per-sport DAG files with thin entrypoints (rename: drop redundant `nba_`/`mlb_` prefix from filenames; DAG IDs preserved — factory builds them as `f"{sport.sport_slug}_<type>"`, which matches existing IDs like `nba_score_dag`).
- **Bridging Phase 1 → Phase 3:** Until shared ML exists (Phase 3), factories accept the ML callable as a parameter from the per-sport DAG file. Example:
  ```python
  # Phase 1 — nba/dags/score_dag.py
  from nba.sport import NBA_SPORT
  from nba.plugins.ml.score import score as score_fn
  from shared.dag_factories.score import build_score_dag

  dag = build_score_dag(NBA_SPORT, score_fn=score_fn)
  ```
  In Phase 3, the factory imports `from shared.ml.score import score` internally and the `score_fn` parameter is removed. This keeps factories clean and avoids if/elif dispatch on `sport_slug`.
- **Test:** DagBag parse test + factory unit tests asserting expected `dag_id`, schedule, task list, dependencies, `default_args`.
- **LOC impact:** ~1200 deleted, ~400 added. Net −800.

### Phase 2 — Stats pipeline helpers
- Add `shared/dag_factories/stats_helpers.py` + `stats_components.py`.
- Refactor `nba/dags/stats_pipeline_dag.py` and `mlb/dags/stats_pipeline_dag.py` to use helpers.
- Same for `stats_backfill_dag.py` × 2.
- Sport-specific dependency wiring stays inline as `>>` operators.
- **LOC impact:** ~−300.

### Phase 3 — Shared ML modules (the riskiest phase)
- Add `shared/ml/{train,score,settle}.py`.
- Add `{nba,mlb}/plugins/ml/settle_lookup.py`.
- Update Phase-1 factory imports to redirect to `shared/ml`.
- Delete `{nba,mlb}/plugins/ml/{train,score,settle}.py`.
- **Drift fixes:** NBA "general model" path deleted; `prepare_features` unifies on `(X, y)`; `min_training_rows` becomes a SportConfig field.
- **Test:** golden test — train + score a fixture dataset with `random_state=42` before Phase 0, diff after Phase 3.
- **LOC impact:** ~−300.

### Phase 4 — Data layer cleanup
- One-shot SQL migration: `raw_api_responses` rename.
- One-shot shell: NBA parquet rename.
- Grep audit: confirm no hardcoded endpoint strings outside `sport.raw_endpoint()` callers.

### Phase 5 — Docs
- Update `README.md` architecture section.
- Add `docs/adding_a_new_sport.md` — checklist for NFL implementer.
- Update `CLAUDE.md` if relevant.

### Sequencing notes
- Phase 0 must come first — everything depends on `SportConfig`.
- Phases 1 and 2 are independent; sequential for review simplicity.
- Phase 3 must come after Phase 1 (factories need shared ML to redirect to).
- Phase 4 can land any time after Phase 0; recommend late to avoid coordinating with in-flight changes.

## Testing strategy

### Per-phase tests

**Phase 0:** `tests/test_sport_config.py` asserts `SportConfig` instances match pre-refactor constants.

**Phase 1:**
- *Factory unit tests* — `shared/tests/test_dag_factories.py`. Call each factory with `NBA_SPORT` and `MLB_SPORT`; assert returned `DAG` has expected `dag_id`, `schedule_interval`, task IDs, dependency edges, `default_args`, and sensor config. Catches accidental drops of `retries`, `execution_delta`, etc.
- *DagBag parse test* — extend existing coverage.

**Phase 2:** Existing per-sport stats unit tests (transformers, etc.) keep passing. Dependency wiring stays in per-sport files, so dependency tests unchanged.

**Phase 3 — golden behavior tests:**
- Capture baseline before Phase 0 begins: small fixed feature parquet → `train_model()` with `random_state=42` → `score()` → save recommendations to `tests/golden/{sport}_score_baseline.csv`.
- After Phase 3: re-run through shared ML; assert resulting rows match the golden on `(player_name, prop_type, line, rank)` tuples (MLflow run IDs and timestamps excluded).
- Add `tests/integration/test_settle.py` exercising shared `settle_recommendations()` with both NBA and MLB `settle_lookup` callables against `data-postgres`.

**Phase 4 integration tests:**
- `raw_api_responses` migration runs on populated table; pre-existing `'events'`/`'odds'` rows become `'nba_events'`/`'nba_odds'`; re-running is a no-op.
- After parquet rename, NBA score DAG reads new filenames and produces non-empty recommendations.

## Risks and rollback

| Risk | Mitigation |
|---|---|
| DAG factory churn breaks Airflow parse | DagBag test in CI; manual trigger of one DAG per sport before merging |
| Shared ML produces different scoring outputs | Golden test captured pre-Phase-0; diffed after Phase 3 |
| Hardcoded endpoint strings missed in Phase 4 | Grep audit: `grep -rE '"(events\|odds\|scores\|player_props)"'`; expect zero hits outside `sport.raw_endpoint()` callers |
| `raw_api_responses` migration run twice corrupts data | WHERE clause naturally idempotent — after first run no rows match |
| `execution_delta`, `retries`, callbacks silently dropped | Factory unit tests assert constructed DAG matches expected attrs |
| "Looser preservation" interpreted too liberally | Spot-check top-10 recommendations for one game date manually before/after each phase |

### Rollback

Each phase is its own PR; revert the PR to undo. Phase 4's data migrations have explicit reverses:

```sql
-- Reverse raw_api_responses rename
UPDATE raw_api_responses SET endpoint = REPLACE(endpoint, 'nba_', '')
WHERE endpoint IN ('nba_events','nba_odds','nba_scores','nba_player_props');
```
```bash
# Reverse parquet rename
for f in /data/features/nba_props_features_*.parquet; do
  mv "$f" "${f/nba_props_features_/props_features_}"
done
```

MLflow registry is unaffected by any phase (model names use `sport.model_name`, which equals existing strings).

## Open questions / future considerations

- **Contract enforcement via `Protocol` classes.** Deferred. Could add type-checked contracts (e.g., `SportStatsClient` Protocol) if we have N>3 contributors writing new sports.
- **NBA legacy table prefix.** `players` and `player_game_logs` remain unprefixed. Migrating to `nba_players` and `nba_player_game_logs` would be a heavier data move; deferred unless NFL or another sport forces a conflict.
- **NFL cadence specifics.** Weekly games may justify a cron that runs only on game days (e.g., `0 15 * * 0,1,4,5`) rather than daily. This is a `SportConfig` field today; NFL just sets it appropriately.
- **`make new-sport` scaffolding script.** A nice-to-have to generate the per-sport file skeleton. Not required for the refactor.

## Estimated impact

- **Net LOC change across all phases:** approximately −1400 (factoring out test additions).
- **Files added:** ~15 in `shared/`; 2 in each `{nba,mlb}/` for `sport.py` and `settle_lookup.py`.
- **Files deleted:** the 12 per-sport ML modules (6 per sport).
- **Files renamed/shrunk:** 16 per-sport DAG files become thin entrypoints.
- **One-time migrations:** one SQL UPDATE + one bash `mv` loop.
