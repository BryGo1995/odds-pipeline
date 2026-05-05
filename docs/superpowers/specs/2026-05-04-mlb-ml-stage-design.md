# MLB ML Stage — Design

**Date:** 2026-05-04
**Status:** Approved (pending spec review)
**Scope:** Implement the train / score / settle stage of the MLB batter-prop pipeline. Closes the MVP loop opened by `2026-04-20-mlb-integration-design.md` (umbrella spec) — features land in Parquet, models train into MLflow, daily scoring writes recommendations, settlement resolves them against game logs.

## Goals

1. Produce daily MLB batter-prop recommendations matching the NBA pipeline's behavior shape, with `sport='MLB'` cleanly partitioning the shared `recommendations` table.
2. Mirror NBA's per-prop-type XGBoost + isotonic calibration architecture so lessons and bug-fixes carry across sports without divergence.
3. Make `shared/plugins/slack_notifier.py` sport-aware so both sports use the same callbacks. Touch NBA business logic only via the one-line callsite kwarg required by that refactor.
4. Keep the path open for NFL: copy small ML primitives into MLB rather than promote, so the eventual three-sport refactor has three concrete implementations to factor from.

## Non-Goals

- Opposing-pitcher / handedness / Statcast features. (Umbrella's post-MVP item #1.)
- Pitcher props. (Umbrella's post-MVP item #2.)
- `mlb_train_backfill_dag` or `mlb_score_backfill_dag`. Bootstrap is a manual ritual using existing `mlb_feature_backfill_dag`.
- Auto-promotion of `promotion_candidate` to `@production`. Manual MLflow gate-keeping, matching NBA.
- Promoting `_CalibratedModel`, `_allocate_slots`, `_make_label_encoder`, or `prepare_features` to `shared/`. Deferred to NFL planning.
- New database migrations. The schema (recommendations.sport, mlb_player_game_logs, mlb_player_name_mappings) already exists.

## Summary of Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Settle DAG location | Inside `mlb_stats_pipeline_dag` as a tail task | Matches NBA's actual pattern (umbrella's "settle in score DAG" was inconsistent with NBA — chosen consistency over the umbrella) |
| Slack notifier handling | Refactor `slack_notifier.py` to be sport-aware | Three callbacks, contained refactor; NFL will need it anyway |
| Cold-start strategy | Mirror NBA — score raises if no models load; bootstrap is manual ritual | Bootstrap is a one-time act; NBA's pattern is well-understood |
| Doubleheader settlement | Day-aggregate (`SUM(stat) GROUP BY player_id, game_date`) | Cleanest SQL; non-DH days collapse to one-row sum (no behavioral difference) |
| ML primitive sharing | Copy verbatim into `mlb/plugins/ml/`, no `shared/` promotion | Umbrella's "two copies is a signal" rule; NFL forces the right interface |
| Hyperparameters | Identical to NBA (n=300, depth=5, lr=0.05, subsample=0.8, colsample=0.8) | Same starting point; tune after first eval |
| Min-rows thresholds | 50 / 50 / 100 (hits / TB / HR) | HR has sparser positive class; matches umbrella |
| Registry naming | `mlb_prop_model_batter_hits`, `…_batter_total_bases`, `…_batter_home_runs` | Matches NBA's `nba_prop_model_<prop>` convention |
| Schedule | Train Mon 3am MT (`0 10 * * 1`); Score daily 9am MT (`0 16 * * *`) | Matches umbrella |
| TOP_N | 10 picks/day, even allocation across active prop types | Matches NBA |

## Architecture

### New files

```
mlb/
├── plugins/ml/
│   ├── __init__.py
│   ├── train.py       # per-prop-type XGBoost + isotonic; MLflow registry
│   ├── score.py       # daily scoring → recommendations (sport='MLB')
│   └── settle.py      # day-aggregated settlement
├── dags/
│   ├── mlb_train_dag.py    # weekly Mon 3am MT
│   └── mlb_score_dag.py    # daily 9am MT, sensors mlb_feature_dag
└── tests/unit/ml/
    ├── __init__.py
    ├── test_train.py
    ├── test_score.py
    └── test_settle.py
```

### Modified files

- `mlb/dags/mlb_stats_pipeline_dag.py` — appends a `settle_recommendations` task downstream of the existing terminal task (`resolve_player_ids`), mirroring NBA's stats-pipeline tail.
- `shared/plugins/slack_notifier.py` — sport-aware refactor (see Sport-aware Slack notifier).
- `nba/plugins/ml/settle.py` — single-line callsite update: `notify_picks_settled(game_date, results, sport="nba")`.
- NBA tests touched by the slack refactor get updated alongside; NBA business logic untouched.

### DAG topology

```
mlb_feature_dag (8:40am MT)
        │ ExternalTaskSensor (execution_delta=20m)
        ▼
mlb_score_dag (9:00am MT)
        ├─ wait_for_features
        └─ score              → INSERT recommendations (sport='MLB')

mlb_stats_pipeline_dag (8:20am MT, existing)
        ├─ … existing ingest + transforms + resolve_player_ids …
        └─ settle_recommendations   ← NEW tail task

mlb_train_dag (Mon 3:00am MT, no sensor)
        └─ train_model → train_all_models() over backfilled features
```

`recommendations.sport` already exists; both `score.py` writes and `settle.py` SELECTs filter on `sport='MLB'`. No schema changes.

## Components

### `mlb/plugins/ml/train.py`

Mirrors `nba/plugins/ml/train.py` with these substitutions:

- Imports `MLB_PROP_STAT_MAP` from `mlb.plugins.transformers.features` (3 prop types).
- `MODEL_NAME = "mlb_prop_model"` → registered as `mlb_prop_model_batter_hits`, `…_batter_total_bases`, `…_batter_home_runs`.
- `min_rows`: 50 for `batter_hits` and `batter_total_bases`, **100** for `batter_home_runs` (sparser positive class).
- `PER_PROP_FEATURES`: identical 8 features as NBA (no `prop_type_encoded` for per-type models).
- `train_all_models()` iterates `MLB_PROP_STAT_MAP.keys()`, returns `{prop_type: run_id}`, swallows `ValueError` from under-threshold prop types.
- `_CalibratedModel` and `_make_label_encoder` copied verbatim (umbrella-mandated copy).
- `prepare_features` copied verbatim — same numeric NA-fill behavior, same `is_home` cast.
- Reads `FEATURES_DIR` env (same path NBA uses; sport split happens at parquet level — `mlb_props_features_<date>.parquet` vs `props_features_<date>.parquet`).
- `_get_production_model_auc(model_name)` queries the MLflow registry for the current `@production` alias and returns its run's `roc_auc` metric, or `None` if no production version exists.
- Calibration degenerates gracefully: if validation has only one class or fewer than 2 samples per class, log warning and use raw model probabilities (NBA's existing fallback).

### `mlb/plugins/ml/score.py`

Mirrors NBA's:

- Loads `{features_dir}/mlb_props_features_{game_date}.parquet`.
- For each prop type in `MLB_PROP_STAT_MAP`: load `models:/mlb_prop_model_<type>@production`, score, compute `edge = model_prob - implied_prob_over`.
- Cold-start: if `mlflow.sklearn.load_model` fails, log WARNING and `continue` (skip prop type). If zero prop types load → raise `"No prop types could be scored for {game_date}"`. Matches NBA, intentional fail-loud per Cold-start decision.
- `_allocate_slots(active_prop_types, top_edges, total=10)` copied verbatim.
- Writes `recommendations` with `sport='MLB'` after `DELETE WHERE game_date=%s AND sport='MLB'`.
- `model_version` is read via `client.get_model_version_by_alias(model_name, "production")`.

### `mlb/plugins/ml/settle.py`

Same overall shape as NBA's `settle.py`. Two MLB-specific deviations:

1. **Day-aggregated stats** (Doubleheader handling).
2. **Mapping table swap**: joins `mlb_player_name_mappings` on `m.odds_api_name = r.player_name`, using `m.mlb_player_id`.

The unsettled-rec join SQL:

```sql
WITH agg AS (
    SELECT player_id, game_date,
           SUM(hits)        AS hits,
           SUM(total_bases) AS total_bases,
           SUM(home_runs)   AS home_runs
    FROM mlb_player_game_logs
    GROUP BY player_id, game_date
)
SELECT r.id, r.player_name, r.prop_type, r.line, r.game_date,
       agg.hits, agg.total_bases, agg.home_runs
  FROM recommendations r
  JOIN mlb_player_name_mappings m ON m.odds_api_name = r.player_name
  JOIN agg                          ON agg.player_id = m.mlb_player_id
                                   AND agg.game_date = r.game_date
 WHERE r.settled_at IS NULL
   AND r.game_date < CURRENT_DATE
   AND r.sport = 'MLB'
```

The column-tuple shape returned matches NBA's pattern, so the rest of `settle_recommendations` (stats lookup, UPDATE, `newly_settled_dates`, recap notification) is structurally identical to NBA. `_STAT_COLS = list(MLB_PROP_STAT_MAP.values())` provides the column-index lookup.

Stale-rec fallback (>7 days unsettled → mark `settled_at = NOW()`) and `_notify_completed_dates` flow are copied unchanged. Recap call: `notify_picks_settled(game_date, results, sport="mlb")`.

### DAGs

**`mlb_train_dag.py`** — `PythonOperator` calling `train_all_models()`, schedule `0 10 * * 1` (Mon 3am MT), no sensor. `tags=["mlb", "ml"]`. `on_success_callback=notify_model_ready`, `on_failure_callback=notify_failure`. Pushes `{prop_type: run_id}` dict to XCom under key `mlflow_run_ids` for the success callback to consume.

**`mlb_score_dag.py`** — `ExternalTaskSensor` on `mlb_feature_dag` (`execution_delta=timedelta(minutes=20)`, `mode="reschedule"`, `poke_interval=60`, `timeout=3600`), then `PythonOperator` on `score()`. `tags=["mlb", "ml"]`. `on_success_callback=notify_score_ready`, `on_failure_callback=notify_failure`. Schedule `0 16 * * *` (9am MT).

**`mlb_stats_pipeline_dag.py`** (existing, modified) — append `t_settle = PythonOperator(task_id="settle_recommendations", python_callable=run_settle_recommendations)` chained downstream of `t_resolve` (the existing `resolve_player_ids` task). The new task imports `settle_recommendations` from `mlb.plugins.ml.settle` and follows NBA's `nba_stats_pipeline_dag.run_settle_recommendations` shape.

## Data flow

### Daily lifecycle (steady state, post-bootstrap)

```
05:00 UTC (8:00am MT)  mlb_odds_pipeline           → games / player_props (sport='baseball_mlb')
05:20 UTC (8:20am MT)  mlb_stats_pipeline          → mlb_player_game_logs / resolve / SETTLE yesterday's recs
05:40 UTC (8:40am MT)  mlb_feature_dag             → mlb_props_features_<today>.parquet
06:00 UTC (9:00am MT)  mlb_score_dag               → recommendations (sport='MLB')
                                                   → notify_score_ready Slack post
Mon 10:00 UTC (3:00am) mlb_train_dag (weekly)      → MLflow runs, candidate tags, notify_model_ready
```

Settlement runs ~24h behind: today's `mlb_stats_pipeline` settles yesterday's recommendations, since yesterday's game logs are now ingested.

### Bootstrap ritual (one-time)

1. Run `mlb_odds_backfill_dag` and `mlb_stats_backfill_dag` for the prior season (2025) — both already exist.
2. Run `mlb_feature_backfill_dag` over the same range — already exists. Produces a season's worth of `mlb_props_features_*.parquet` with `actual_result` populated.
3. Trigger `mlb_train_dag` manually. `train_all_models()` registers candidate versions for whichever prop types clear `min_rows`.
4. Inspect MLflow UI; promote each acceptable candidate to `@production` alias (manual gate-keeping — NBA pattern).
5. From this point `mlb_score_dag` produces recommendations and `mlb_stats_pipeline.settle_recommendations` resolves them.

Until step 4 completes for a prop type, that prop type is silently skipped in scoring (NBA's `continue` on `load_model` failure). Once at least one prop type has a `@production` alias, `mlb_score_dag` succeeds.

## Sport-aware Slack notifier

### Sport detection

Each DAG already declares `tags=["nba", "ml"]` or `tags=["mlb", "ml"]`. Callbacks resolve sport from `context["dag"].tags`, taking the first tag in `{"nba", "mlb"}`. `notify_picks_settled` is called from `settle.py` (no Airflow context) so it gains an explicit `sport` keyword argument.

```python
def _resolve_sport(context) -> str:
    tags = set(context["dag"].tags or [])
    for s in ("nba", "mlb"):
        if s in tags:
            return s
    raise ValueError(f"DAG {context['dag'].dag_id} has no nba/mlb tag")
```

### Module-level config

Replaces today's hardcoded constants:

```python
_DAILY_PIPELINE_DAGS_BY_SPORT = {
    "nba": ["nba_odds_pipeline", "nba_stats_pipeline", "nba_feature_dag", "nba_score_dag"],
    "mlb": ["mlb_odds_pipeline", "mlb_stats_pipeline", "mlb_feature_dag", "mlb_score_dag"],
}

_PROP_LABELS = {
    # NBA
    "player_points":          "Points",
    "player_rebounds":        "Rebounds",
    "player_assists":         "Assists",
    "player_threes":          "3-Pointers",
    "player_threes_attempts": "3PA",
    # MLB
    "batter_hits":        "Hits",
    "batter_total_bases": "Total Bases",
    "batter_home_runs":   "Home Runs",
}

_SPORT_DISPLAY = {
    "nba": {"prefix": "[NBA]", "emoji": "🏀", "filter": "NBA"},
    "mlb": {"prefix": "[MLB]", "emoji": "⚾", "filter": "MLB"},
}
```

### Callback changes

- `notify_failure(context)` — prepends `_SPORT_DISPLAY[sport]["prefix"]` to the existing message.
- `notify_score_ready(context)` — reads `sport` from tags; SQL becomes `WHERE … AND sport = %s` parameterized with `_SPORT_DISPLAY[sport]["filter"]`; checklist iterates `_DAILY_PIPELINE_DAGS_BY_SPORT[sport]`; header becomes `f"{prefix} {emoji} Recommendations ready — {date_str}"`.
- `notify_model_ready(context)` — adds prefix; prop-label lookup uses the merged `_PROP_LABELS`.
- `notify_picks_settled(game_date, results, *, sport)` — adds prefix; merged `_PROP_LABELS`. Caller passes sport explicitly.

### Why a single merged label dict?

(1) `prop_type` strings are already disjoint between sports (`player_*` vs `batter_*`), so no conflict; (2) callers pass `prop_type` they already have, no extra plumbing; (3) when NFL ships, adding more keys to one dict is a smaller surface than threading another lookup level.

## Edge cases

**Empty feature parquet (no MLB props on date).** `load_todays_features` returns empty `DataFrame`. `score()` raises `"No feature file found for {game_date}"` (NBA pattern). Acceptable for MVP — early-season days with no MLB schedule will produce a feature-DAG warning upstream and the score DAG will fail loudly until the season starts.

**Partial model availability.** If 2 of 3 prop types have a `@production` alias, `score()` skips the third with a WARNING, `_allocate_slots` distributes 10 picks across the 2 active types (5/5, ties broken by top edge). Recommendations land normally with `prop_type` reflecting only the active types.

**Stale recommendations (>7 days unsettled).** Same as NBA's settle: `UPDATE … SET settled_at = NOW()` for rows older than 7 days that never resolved. Logs at WARNING. Possible MLB-specific cause: rainout-postponed games rescheduled outside the 7-day window. For MVP, accept the lossy mark; revisit if rainout frequency is material.

**Player name mapping miss.** `mlb_player_name_resolution` runs in `mlb_stats_pipeline_dag` and posts `[MLB] :warning: …unresolved names…` to Slack. Settlement only joins resolved mappings; unmapped recs stay unsettled until either name resolution catches up or the 7-day stale fallback fires.

**Concurrent NBA + MLB settle.** Both `nba_stats_pipeline` and `mlb_stats_pipeline` run their settle tasks within ~20 minutes of each other every morning. Both write to `recommendations` but filter `sport='NBA'` / `sport='MLB'` exclusively, so there's no row contention.

## NBA touch points

| File | Change | Reason |
|---|---|---|
| `nba/plugins/ml/settle.py` | One-line: `notify_picks_settled(game_date, results)` → `notify_picks_settled(game_date, results, sport="nba")` | Slack refactor requires explicit `sport` kwarg |
| `nba/tests/unit/test_slack_notifier_settle.py` | Update mock-call assertions to include `sport="nba"` | Test-side mirror |
| `nba/tests/unit/ml/test_settle.py` | Update mock-call assertions if they peek at `notify_picks_settled` args | Test-side mirror |
| `shared/plugins/slack_notifier.py` | Sport-aware refactor | Section above |
| `shared/tests/...` (any covering the notifier) | Updated to assert `[NBA]`/`[MLB]` prefix and tag-resolved sport | Refactor test mirror |

NBA business logic — DAGs, transformers, training, scoring, settle internals — is not touched.

## Testing

### New unit test files

```
mlb/tests/unit/ml/
├── __init__.py
├── test_train.py            # ~10 tests, mirrors nba/tests/unit/ml/test_train.py
├── test_score.py            # ~8 tests, mirrors nba/tests/unit/ml/test_score.py
└── test_settle.py           # ~10 tests, mirrors nba/tests/unit/ml/test_settle.py

mlb/tests/unit/
├── test_mlb_train_dag.py    # DagBag structure + tags + schedule + callbacks
└── test_mlb_score_dag.py    # DagBag + ExternalTaskSensor + score wiring
```

### Test parity targets

**`test_train.py`**

- Happy path: `train_all_models()` registers one MLflow version per prop type that clears `min_rows`.
- `min_rows` skip: prop type with < threshold rows raises `ValueError`, `train_all_models` swallows it.
- Empty validation set raises with informative message ("Run the feature backfill for older dates …").
- Calibration-degenerate validation set (one class only, or < 2 per class) falls back to raw model probabilities and warns.
- `promotion_candidate=true` tag is set when val ROC-AUC > production (and on baseline / no-production case).
- Registry name is `mlb_prop_model_<prop_type>` (not `nba_prop_model_*`).

**`test_score.py`**

- Happy path: writes 10 ranked rows to `recommendations` with `sport='MLB'`, evenly allocated across active prop types.
- Partial model availability: 2 of 3 prop types have `@production`, 10 picks split 5/5 (ties broken by top edge).
- Zero models: raises `"No prop types could be scored"`.
- DELETE-then-INSERT scopes to `sport='MLB'` (a separate-sport row in the same `game_date` is not deleted).
- `model_version` recorded on each row matches the MLflow alias.

**`test_settle.py`**

- Happy path: settles a single-game day's recs, sets `actual_result`, `actual_stat_value`, `settled_at`.
- **Doubleheader**: two `mlb_player_game_logs` rows for the same `(player_id, game_date)` aggregate — `actual_stat_value` is the sum, `actual_result = (sum > line)` — explicit DH fixture.
- Stale-rec fallback: rec older than 7 days with no matching log gets `settled_at = NOW()`, logs at WARNING.
- Mapping miss: rec whose `player_name` has no `mlb_player_name_mappings` row stays unsettled.
- Sport scoping: every SELECT/UPDATE filters `sport='MLB'` (assert via SQL string match in mock cursor).
- Recap notification: when a game date's top-10 are fully settled, `notify_picks_settled(game_date, results, sport="mlb")` is called once.

### Test fixtures

- `mlb/tests/unit/conftest.py` (already exists from stats-pipeline slice) extended with MLflow stubs (`mlflow.set_tracking_uri`, `mlflow.tracking.MlflowClient`, `mlflow.sklearn.log_model`/`load_model`, `register_model`) following NBA's existing conftest pattern.
- Synthetic `mlb_props_features_<date>.parquet` written to `tmp_path` — 60+ labeled rows covering all 3 prop types so `train_all_models` runs end-to-end with real XGBoost.
- DB mocks: `unittest.mock.MagicMock` for psycopg2 connection/cursor — same idiom NBA's settle/score tests use.

### DagBag tests

The existing Python 3.14 / SQLAlchemy DagBag-test failure (flagged in issue #6 comments and applied to all NBA + MLB DagBag tests) applies here too. New DagBag tests for `mlb_train_dag` and `mlb_score_dag` will be written following the same pattern but expected to fail collection on the current interpreter — green once the runtime issue is resolved. They do NOT block the slice.

### Slack-notifier tests

- Existing NBA notifier tests retargeted to assert `[NBA]` prefix and tag-based sport resolution.
- New MLB notifier tests: `[MLB]` prefix + MLB prop labels + `sport='MLB'` SQL filter in `notify_score_ready` query.
- `_resolve_sport` helper: tests for nba tag, mlb tag, no-sport-tag-raises.

### Acceptance criteria

- `pytest mlb/tests/unit/ml/` is green.
- `pytest nba/tests/unit/ml/` remains green.
- `pytest` of any updated shared notifier tests is green for both sports.
- `mlb_train_dag` and `mlb_score_dag` import successfully via `python -c "import …"` even if DagBag tests are blocked by the Python 3.14 issue.

## Risk & Open Questions

- **Bootstrap data sufficiency.** Today is 2026-05-04 — about a month into the 2026 season. We will rely on the 2025 season backfill for initial training. If 2025 stats backfill has not been run or has gaps, training a `batter_home_runs` model (min 100 rows) may fail on first attempt. Mitigation: train DAG returns `{}` and `notify_model_ready` posts a "no models trained" message — graceful failure, not a pager event.
- **Day-aggregate vs per-game prop semantics.** Some sportsbooks post DH props per-game rather than as day-aggregates. Day-aggregate settlement is a known approximation. If this distorts settled outcomes materially, revisit by linking `player_props.game_id` → specific `mlb_game_pk` (requires schema change tracked separately).
- **`_DAILY_PIPELINE_DAGS_BY_SPORT` drift.** When new daily DAGs are added per sport, this dict must be updated. Acceptable short-term; flagged as a small maintenance cost.
- **MLflow registry empty on cold start.** `_get_production_model_auc` already swallows the "no production version" exception and returns `None` (NBA pattern). No new behavior needed.

## Out of Scope / Post-MVP Ordering

Per the umbrella spec, the highest-leverage next slices after this one:

1. Opposing-pitcher features (handedness matchup, opp SP K/9, opp SP OPS-against). Most important MLB-specific feature.
2. Pitcher props (`pitcher_strikeouts`, `pitcher_outs`, `pitcher_earned_runs`).
3. Statcast / pybaseball advanced metrics.
4. Park-factor and lineup-spot features.
5. Shared-core refactor — wait until NFL planning is underway.
6. Additional batter markets (`batter_rbis`, `batter_runs_scored`, `batter_stolen_bases`, `batter_hits_runs_rbis`).
