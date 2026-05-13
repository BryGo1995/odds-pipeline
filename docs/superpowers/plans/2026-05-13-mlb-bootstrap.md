# MLB Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take merged MLB code from "tests green" to "produces daily picks in `recommendations` (sport='MLB')" by running the missing backfills, training real models, manually promoting in MLflow, and unpausing the daily DAGs — with explicit verification gates between every phase so a partial bootstrap halts visibly.

**Architecture:** Seven phases executed in order against the live Docker stack: HR-market diagnostic (Phase 0, possibly a code-change branch); manual stats backfill (Phase 1); daily stats pipeline unpause + first run (Phase 2); feature backfill (Phase 3); manual train (Phase 4); MLflow `@production` promotion (Phase 5); daily DAG unpause (Phase 6); T+1 settle verification (Phase 7). Each phase has a SQL-or-MLflow-API verification gate. Code changes (if any) ride on `feature/mlb-bootstrap`; operational steps run against `main`.

**Tech Stack:** Apache Airflow 2.9 (docker-compose), Postgres 15 (data-postgres on port 5433), MLflow 2.14 (host port 5001), psql client, MLflow Python client. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-12-mlb-bootstrap-design.md`

---

## Pre-flight: Verify stack is healthy

- [ ] **Step 1: Confirm all containers are up**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
docker compose ps
```

Expected: `airflow-scheduler`, `airflow-webserver`, `data-postgres`, `airflow-postgres`, `mlflow`, `pgadmin`, `odds-admin` all `Up` and `healthy` where applicable.

If any container is down: `docker compose up -d` and re-check after 60 seconds.

- [ ] **Step 2: Confirm DAG state matches spec's "Observed current state"**

```bash
docker exec odds-pipeline-airflow-postgres-1 psql -U airflow -d airflow -c "
SELECT dag_id, is_paused FROM dag
WHERE dag_id LIKE 'mlb_%'
ORDER BY dag_id;
"
```

Expected: `mlb_odds_pipeline` → `f` (unpaused); all six other MLB DAGs → `t` (paused). If this differs (e.g., someone unpaused things), pause anything other than `mlb_odds_pipeline` before continuing — bootstrap assumes a known starting state.

```bash
docker exec odds-pipeline-airflow-postgres-1 psql -U airflow -d airflow -c "
UPDATE dag SET is_paused = TRUE WHERE dag_id IN (
  'mlb_stats_pipeline','mlb_stats_backfill',
  'mlb_feature_dag','mlb_feature_backfill',
  'mlb_train_dag','mlb_score_dag'
) AND is_paused = FALSE;
"
```

- [ ] **Step 3: Confirm starting data snapshot**

```bash
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
SELECT 'mlb_teams' AS t, COUNT(*) FROM mlb_teams
UNION ALL SELECT 'mlb_players',            COUNT(*) FROM mlb_players
UNION ALL SELECT 'mlb_player_game_logs',   COUNT(*) FROM mlb_player_game_logs
UNION ALL SELECT 'mlb_player_name_mappings', COUNT(*) FROM mlb_player_name_mappings;
"
```

Expected: all four rows show `0`. If non-zero, bootstrap has been partially run — stop and reconcile with the operator before proceeding.

---

## Task 1 (Phase 0): Diagnose `batter_home_runs` market gap

**Files:**
- Read-only inspection: `raw_api_responses` table; live Odds-API response

The goal of Phase 0 is to *decide between Task 1a (transform bug fix) and Task 1b (drop HR from config)*. Run the diagnostic first, then execute exactly one of 1a or 1b based on the outcome.

**Pre-flight intel (2026-05-13):** the diagnostic was run during plan authoring. Across 24 daily `mlb_player_props` responses (Apr 21 → May 12), zero contain `batter_home_runs`. The distinct-market-key walk returns only `batter_hits` and `batter_total_bases`. **Strong prior for Task 1b**, but re-run Steps 1–3 anyway in case API behavior shifted between authoring and execution.

- [ ] **Step 1: Walk the markets actually present in the latest player_props raw response**

(Note: player props are under endpoint `mlb_player_props`, not `mlb_odds`. The `mlb_odds` endpoint only carries game-level markets — h2h/spreads/totals.)

```bash
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
WITH latest AS (
  SELECT response FROM raw_api_responses
  WHERE endpoint = 'mlb_player_props' AND status = 'success'
  ORDER BY fetched_at DESC LIMIT 1
)
SELECT DISTINCT
  jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(response)->'bookmakers')->'markets')->>'key'
  AS market_key
FROM latest
ORDER BY market_key;
"
```

- [ ] **Step 2: Confirm presence/absence of `batter_home_runs` across recent responses**

```bash
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
SELECT fetched_at::date,
       (response::text LIKE '%batter_home_runs%') AS has_hr
FROM raw_api_responses
WHERE endpoint='mlb_player_props' AND status='success'
ORDER BY fetched_at DESC LIMIT 10;
"
```

Possible outcomes:
- Step 1 lists `batter_home_runs` and/or Step 2 shows `has_hr = t` for any row → HR market IS in the API response, but transform drops it. **Branch to Task 1a.**
- Step 1 lists only `batter_hits` and `batter_total_bases` and Step 2 shows `has_hr = f` for all rows → HR market is absent from the API response. **Branch to Task 1b.**

- [ ] **Step 3: Cross-check with a fresh live Odds-API call (only if Steps 1–2 said NOT FOUND)**

```bash
# Pull a single event id to query against
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
SELECT pp.game_id FROM player_props pp
JOIN games g ON g.game_id = pp.game_id
WHERE g.sport = 'baseball_mlb'
  AND g.commence_time > NOW()
ORDER BY g.commence_time
LIMIT 1;
" -t -A
```

Then (replace `<EVENT_ID>` and `<API_KEY>`):

```bash
API_KEY=$(grep '^ODDS_API_KEY=' /home/bryang/Dev_Space/python_projects/odds-pipeline/.env | cut -d= -f2)
EVENT_ID=<from previous query>
curl -s "https://api.the-odds-api.com/v4/sports/baseball_mlb/events/${EVENT_ID}/odds?apiKey=${API_KEY}&regions=us&markets=batter_home_runs&bookmakers=draftkings,fanduel,betmgm&oddsFormat=american" | grep -c 'batter_home_runs' || echo "NOT FOUND"
```

If the live call returns the market: it's a transform bug (Task 1a). If it also says NOT FOUND: Odds-API really doesn't expose it for our 3 books on MLB → Task 1b.

- [ ] **Step 4: Record the decision in the plan execution log**

Open a comment thread on issue #6 (or in the conversation) stating the Phase 0 outcome: `"HR market diagnosis: <bug | not exposed>. Proceeding with Task 1<a|b>."` Then continue.

### Task 1a: HR transform bug fix (TDD)

**Execute ONLY if Phase 0 found `batter_home_runs` in the raw response but not in `player_props`.** Highly unlikely per pre-flight intel.

**Files:**
- Modify: `shared/plugins/transformers/player_props.py`
- Test: `shared/tests/unit/transformers/test_player_props.py`

**Context for the engineer:** as of authoring, `shared/plugins/transformers/player_props.py` already uses a sport-agnostic *negative* filter against `GAME_LEVEL_MARKETS` (the only `continue` in the loop). It does NOT use any prop-prefix filter. So if Phase 0 finds HR markets in `raw_api_responses` rows for `mlb_player_props` but those rows don't land in `player_props`, the bug is **not** in the market-key filter — it's in the outcomes loop, the `INSERT` parameter mapping, or the request construction in `mlb_odds_pipeline_dag.fetch_player_props_task`. Investigate empirically before writing a fix.

- [ ] **Step 1: Create branch**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
git checkout -b feature/mlb-bootstrap
```

- [ ] **Step 2: Read all sites that touch player-props ingest/transform**

```bash
sed -n '1,50p' shared/plugins/transformers/player_props.py
grep -n -B 2 -A 10 'fetch_player_props\|transform_player_props' mlb/dags/mlb_odds_pipeline_dag.py
```

Identify what is actually filtering / silently dropping `batter_home_runs` rows. Common possibilities to rule out:
- `outcome.get("description")` returns `None` for HR markets (different payload shape) → rows insert with NULL `player_name` and are invisible to our downstream SELECT.
- `outcome["name"]` raises KeyError for HR (caught by an outer try/except that swallows it).
- `fetch_player_props_task` passes a stripped `markets` list per event.

- [ ] **Step 3: Write a failing regression test**

In `shared/tests/unit/transformers/test_player_props.py`, append:

```python
def test_transform_player_props_includes_batter_home_runs():
    """Regression: 2026-05 bootstrap audit found batter_home_runs silently
    dropped despite being requested in MLB_PLAYER_PROP_MARKETS."""
    from shared.plugins.transformers.player_props import transform_player_props
    from unittest.mock import MagicMock

    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value

    odds_payload = [{
        "id": "evt-1",
        "sport_key": "baseball_mlb",
        "bookmakers": [{
            "key": "draftkings",
            "markets": [{
                "key": "batter_home_runs",
                "last_update": "2026-05-13T18:00:00Z",
                "outcomes": [
                    {"name": "Over",  "description": "Aaron Judge",
                     "price": 350, "point": 0.5},
                    {"name": "Under", "description": "Aaron Judge",
                     "price": -450, "point": 0.5},
                ],
            }],
        }],
    }]

    transform_player_props(conn, odds_payload)

    inserts = [c for c in cur.execute.call_args_list
               if "player_props" in str(c).lower()
               and "insert" in str(c).lower()]
    assert any("batter_home_runs" in str(c) for c in inserts), (
        "batter_home_runs market should produce a player_props INSERT"
    )
```

- [ ] **Step 4: Run the test to verify it fails**

```bash
pytest shared/tests/unit/transformers/test_player_props.py::test_transform_player_props_includes_batter_home_runs -v
```

Expected: FAIL (assertion error — no `batter_home_runs` INSERT seen).

If it unexpectedly passes, the diagnosis was wrong; stop and re-check Phase 0.

- [ ] **Step 5: Fix the actual root cause surfaced by Step 2**

The fix location depends on what Step 2 surfaced:

- If `outcome.get("description")` is None for HR markets: branch the transform to fall back to another field (e.g., `outcome.get("name")` may carry player identity in some payload variants — verify against a real payload).
- If `outcome["name"]` raises: replace with `outcome.get("name")` + a continue/skip with a WARN log.
- If `fetch_player_props_task` is silently dropping HR before storage: fix the request or its retry path in `mlb/dags/mlb_odds_pipeline_dag.py`.

Whichever is the root cause, also re-confirm the regression test from Step 3 passes — if the bug is upstream (in the DAG, not the transform), extend the test or add a second test that covers the upstream layer.

- [ ] **Step 6: Run the test to verify it passes**

```bash
pytest shared/tests/unit/transformers/test_player_props.py -v
```

Expected: all `test_player_props.py` tests PASS (the new one + every existing one).

- [ ] **Step 7: Run the wider regression suite**

```bash
pytest shared/tests/ nba/tests/unit/ mlb/tests/unit/ -v --ignore=nba/tests/unit/test_schema.py 2>&1 | tail -30
```

Expected: no new failures vs. baseline.

- [ ] **Step 8: Commit**

```bash
git add shared/plugins/transformers/player_props.py shared/tests/unit/transformers/test_player_props.py
git commit -m "fix(shared): include batter_home_runs in player_props transform

The market-prefix filter assumed 'player_' was the only prop-market prefix.
MLB props use 'batter_' / 'pitcher_'. Switch to negative filter against
GAME_LEVEL_MARKETS (the sport-agnostic source of truth) so any sport's
prop markets flow through.

Caught by 2026-05-12 MLB bootstrap audit: 22 days of ingest never wrote
a single batter_home_runs row despite the market being in the request.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 9: Merge to main**

```bash
git checkout main
git merge --no-ff feature/mlb-bootstrap -m "Merge branch 'feature/mlb-bootstrap'"
git branch -d feature/mlb-bootstrap
```

- [ ] **Step 10: Restart Airflow scheduler so it picks up the fix**

```bash
docker compose restart airflow-scheduler airflow-webserver
```

Wait ~60 seconds. The next scheduled `mlb_odds_pipeline` run will produce HR rows going forward (we cannot retroactively fill past days; that's an Odds-API historical-endpoint limitation, tracked in issue #12).

- [ ] **Step 11: Update spec with outcome**

Edit `docs/superpowers/specs/2026-05-12-mlb-bootstrap-design.md` and append a one-line update in the "Risks & open questions" section under `batter_home_runs outcome`:

> **Resolved (2026-05-13):** Transform-bug path. Fixed in commit `<hash>`. HR rows will flow from next `mlb_odds_pipeline` run forward. Historical days (Apr 21–May 12) have no HR data; HR model will not clear `min_rows=100` until ~mid-July at current pace.

```bash
git add docs/superpowers/specs/2026-05-12-mlb-bootstrap-design.md
git commit -m "docs: record Phase 0 outcome (HR transform fix)"
```

**Skip Task 1b. Proceed to Task 2.**

### Task 1b: Drop HR from config

**Execute ONLY if Phase 0 confirmed `batter_home_runs` is not exposed by Odds-API for DK/FD/BetMGM on MLB.**

**Files:**
- Modify: `mlb/config.py`
- Modify: `mlb/plugins/transformers/features.py`
- Modify: `mlb/plugins/ml/train.py`
- Modify: `mlb/tests/unit/test_config.py`
- Modify: `mlb/tests/unit/transformers/test_features.py`
- Modify: `mlb/tests/unit/ml/test_train.py`

- [ ] **Step 1: Create branch**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
git checkout -b feature/mlb-bootstrap
```

- [ ] **Step 2: Update test_config first (TDD — assert the new state)**

Edit `mlb/tests/unit/test_config.py`. Locate the test asserting `PLAYER_PROP_MARKETS` contents and change the expectation:

```python
def test_player_prop_markets_is_two_market_mvp():
    """Phase 0 of 2026-05 bootstrap: batter_home_runs not exposed by
    Odds-API for DK/FD/BetMGM. Drop from MVP scope."""
    from mlb.config import PLAYER_PROP_MARKETS
    assert PLAYER_PROP_MARKETS == ["batter_hits", "batter_total_bases"]
```

(If a similar test already exists, modify it. Don't leave both — one source of truth.)

- [ ] **Step 3: Run the test to verify it fails**

```bash
pytest mlb/tests/unit/test_config.py -v
```

Expected: FAIL (current `PLAYER_PROP_MARKETS` still has 3 items).

- [ ] **Step 4: Edit `mlb/config.py`**

Remove the `"batter_home_runs"` line from `PLAYER_PROP_MARKETS`. Keep the trailing comment about post-MVP markets unchanged.

- [ ] **Step 5: Edit `mlb/plugins/transformers/features.py`**

Remove the `"batter_home_runs": "home_runs"` entry from `MLB_PROP_STAT_MAP`. Confirm no other reference to `batter_home_runs` remains in this file.

```bash
grep -n batter_home_runs mlb/plugins/transformers/features.py
```

Expected: no output (or only comments).

- [ ] **Step 6: Edit `mlb/plugins/ml/train.py`**

Locate the `min_rows` dict / mapping that currently has three entries (50/50/100). Remove the `batter_home_runs` entry. Also remove HR from any iteration list / `PROP_TYPES` constant if duplicated locally.

```bash
grep -n batter_home_runs mlb/plugins/ml/train.py
```

Expected: no output.

- [ ] **Step 7: Update `mlb/tests/unit/transformers/test_features.py`**

Any test that asserts on a 3-prop output (e.g., `assert df["prop_type"].nunique() == 3`) → change to `2`. Any fixture passing HR rows in to `build_features` → drop them, or expect them to be filtered.

```bash
grep -n batter_home_runs mlb/tests/unit/transformers/test_features.py
```

Update each match.

- [ ] **Step 8: Update `mlb/tests/unit/ml/test_train.py`**

Same treatment: any HR-specific test asserts → remove or convert. Any `min_rows=100` assertion → confirm it now refers only to the 2 active markets.

- [ ] **Step 9: Run full MLB test suite**

```bash
pytest mlb/tests/unit/ -v 2>&1 | tail -30
```

Expected: green (modulo the pre-existing Python 3.14 / SQLAlchemy DagBag-collection issue noted in prior plans).

- [ ] **Step 10: Run broader regression**

```bash
pytest shared/tests/ nba/tests/unit/ -v --ignore=nba/tests/unit/test_schema.py 2>&1 | tail -20
```

Expected: no new failures.

- [ ] **Step 11: Commit**

```bash
git add mlb/config.py mlb/plugins/transformers/features.py mlb/plugins/ml/train.py mlb/tests/
git commit -m "feat(mlb): drop batter_home_runs from MVP markets

Odds-API does not expose batter_home_runs for DraftKings / FanDuel /
BetMGM on MLB (verified 2026-05-13 via raw response + direct API call).
22 days of ingest produced zero HR rows for the same reason.

Drop from PLAYER_PROP_MARKETS, MLB_PROP_STAT_MAP, and train.py min_rows.
MVP ships with batter_hits and batter_total_bases. HR re-enters when we
add a book that offers it (covered by issue #12 if pursued).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 12: Merge to main**

```bash
git checkout main
git merge --no-ff feature/mlb-bootstrap -m "Merge branch 'feature/mlb-bootstrap'"
git branch -d feature/mlb-bootstrap
```

- [ ] **Step 13: Restart Airflow scheduler**

```bash
docker compose restart airflow-scheduler airflow-webserver
```

Wait ~60 seconds.

- [ ] **Step 14: Update spec with outcome**

Edit `docs/superpowers/specs/2026-05-12-mlb-bootstrap-design.md`, append in "Risks & open questions" under `batter_home_runs outcome`:

> **Resolved (2026-05-13):** API-availability path. Dropped from MVP in commit `<hash>`. Re-introduce if/when a supported book starts exposing HR markets.

```bash
git add docs/superpowers/specs/2026-05-12-mlb-bootstrap-design.md
git commit -m "docs: record Phase 0 outcome (drop HR from MVP)"
```

**Proceed to Task 2.**

---

## Task 2 (Phase 1): Stats backfill

**Goal:** Populate `mlb_teams`, `mlb_players`, `mlb_player_game_logs`, `mlb_player_name_mappings` for 2025 + 2026-to-date.

**Files:**
- No code changes. Triggers `mlb_stats_backfill` DAG.

- [ ] **Step 1: Unpause `mlb_stats_backfill` so it can be triggered**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags unpause mlb_stats_backfill
```

Expected output: `Dag: mlb_stats_backfill, paused: False`.

- [ ] **Step 2: Trigger the backfill with 2025-2026 params**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags trigger \
  --conf '{"season_start": "2025", "season_end": "2026"}' \
  mlb_stats_backfill
```

Expected output: a `dag_run_id` line confirming the trigger.

- [ ] **Step 3: Tail the task log until the run finishes**

In a new terminal (or background-poll), check progress:

```bash
docker exec odds-pipeline-airflow-postgres-1 psql -U airflow -d airflow -c "
SELECT run_id, state, start_date::timestamp(0), end_date::timestamp(0)
FROM dag_run
WHERE dag_id = 'mlb_stats_backfill'
ORDER BY start_date DESC LIMIT 1;
"
```

Re-run every ~5 minutes. Expected end-state: `state = 'success'` after 60-180 minutes. If `state = 'failed'`, fetch task logs:

```bash
docker exec odds-pipeline-airflow-scheduler-1 \
  airflow tasks logs mlb_stats_backfill run_backfill <run_id>
```

Triage and re-trigger only after the failure cause is fixed.

- [ ] **Step 4: Verification gate — confirm tables populated**

```bash
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
SELECT
  (SELECT COUNT(*) FROM mlb_teams)                                                AS teams,
  (SELECT COUNT(*) FROM mlb_players)                                              AS players,
  (SELECT MIN(game_date) FROM mlb_player_game_logs)                               AS first_log,
  (SELECT MAX(game_date) FROM mlb_player_game_logs)                               AS last_log,
  (SELECT COUNT(DISTINCT game_date) FROM mlb_player_game_logs)                    AS distinct_dates,
  (SELECT COUNT(*) FROM mlb_player_name_mappings)                                 AS name_mappings;
"
```

Pass criteria (all must hold):
- `teams >= 30`
- `players >= 1500`
- `first_log <= 2025-04-01`
- `last_log >= <yesterday>`
- `distinct_dates >= 300`
- `name_mappings > 0`

If any fails: stop; triage logs; re-run after fix. Do **not** proceed to Task 3 with partial stats data.

- [ ] **Step 5: Re-pause `mlb_stats_backfill` (one-shot DAG, shouldn't auto-trigger)**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags pause mlb_stats_backfill
```

---

## Task 3 (Phase 2): Daily stats pipeline first run

**Goal:** Unpause `mlb_stats_pipeline` and confirm one successful daily run that picks up where the backfill ended.

**Files:**
- No code changes. Unpauses + triggers `mlb_stats_pipeline`.

- [ ] **Step 1: Unpause `mlb_stats_pipeline`**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags unpause mlb_stats_pipeline
```

- [ ] **Step 2: Snapshot `mlb_player_game_logs.last` before the run**

```bash
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
SELECT MAX(game_date) FROM mlb_player_game_logs;
"
```

Record the date (call it `LAST_BEFORE`).

- [ ] **Step 3: Trigger one manual run for the current logical date**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags trigger mlb_stats_pipeline
```

The DAG has an `ExternalTaskSensor` waiting on `mlb_odds_pipeline`. If the sensor blocks (because manual trigger execution_date won't naturally align with `mlb_odds_pipeline`'s schedule), open the Graph view at http://localhost:8080 → click the sensor task → **Mark Success** to bypass it. (Same pattern used for NBA per the README.)

- [ ] **Step 4: Wait for run to finish; check status**

```bash
docker exec odds-pipeline-airflow-postgres-1 psql -U airflow -d airflow -c "
SELECT run_id, state, start_date::timestamp(0), end_date::timestamp(0)
FROM dag_run
WHERE dag_id = 'mlb_stats_pipeline'
ORDER BY start_date DESC LIMIT 1;
"
```

Expected end-state: `state = 'success'` within ~10 minutes.

- [ ] **Step 5: Verification gate**

```bash
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
SELECT MAX(game_date) FROM mlb_player_game_logs;
"
```

Pass: result is `>= LAST_BEFORE`. (A no-game day produces equality; a normal day advances.)

Also confirm the `settle_recommendations` task ran:

```bash
docker exec odds-pipeline-airflow-postgres-1 psql -U airflow -d airflow -c "
SELECT task_id, state FROM task_instance
WHERE dag_id = 'mlb_stats_pipeline'
  AND run_id = (SELECT run_id FROM dag_run WHERE dag_id='mlb_stats_pipeline' ORDER BY start_date DESC LIMIT 1)
ORDER BY task_id;
"
```

Pass: `settle_recommendations` exists with `state = 'success'`. (Expected no-op since there are zero MLB recommendations yet.)

---

## Task 4 (Phase 3): Feature backfill

**Goal:** Generate one `mlb_props_features_YYYY-MM-DD.parquet` per day in the window where we have both props and labels.

**Files:**
- No code changes. Triggers `mlb_feature_backfill` DAG.

- [ ] **Step 1: Compute the date range**

```bash
TODAY=$(date -u +%Y-%m-%d)
YESTERDAY=$(date -u -d 'yesterday' +%Y-%m-%d)
echo "Backfill window: 2026-04-21 → $YESTERDAY"
```

- [ ] **Step 2: Unpause `mlb_feature_backfill`**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags unpause mlb_feature_backfill
```

- [ ] **Step 3: Trigger with date range**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags trigger \
  --conf "{\"date_from\": \"2026-04-21\", \"date_to\": \"$YESTERDAY\"}" \
  mlb_feature_backfill
```

- [ ] **Step 4: Wait for the run to finish**

```bash
docker exec odds-pipeline-airflow-postgres-1 psql -U airflow -d airflow -c "
SELECT run_id, state, start_date::timestamp(0), end_date::timestamp(0)
FROM dag_run
WHERE dag_id = 'mlb_feature_backfill'
ORDER BY start_date DESC LIMIT 1;
"
```

Expected end-state: `state = 'success'` within ~5-10 minutes.

- [ ] **Step 5: Verification gate — file count**

```bash
docker exec odds-pipeline-airflow-webserver-1 bash -c 'ls /data/features/mlb_props_features_*.parquet 2>/dev/null | wc -l'
```

Pass: `>= 18` files (allowing for ~1-3 no-MLB days that produce empty / skipped output).

- [ ] **Step 6: Verification gate — schema and label coverage**

```bash
docker exec odds-pipeline-airflow-webserver-1 python -c "
import pandas as pd, glob, os
files = sorted(glob.glob('/data/features/mlb_props_features_*.parquet'))
print(f'files: {len(files)}')
df = pd.read_parquet(files[len(files)//2])  # pick a middle file
print('shape:', df.shape)
print('prop_types:', dict(df['prop_type'].value_counts()))
print('label coverage:', df['actual_result'].notna().mean())
print('cols:', sorted(df.columns.tolist()))
"
```

Pass criteria:
- A middle file has `shape` rows in 100-800 range.
- `prop_types` contains the active 2 (or 3 if Task 1a happened): `batter_hits`, `batter_total_bases` (+ `batter_home_runs` if applicable — but HR will only appear for parquets generated AFTER the Task 1a fix landed; old parquet days won't backfill HR).
- `label coverage` ≈ 1.0 (close to all rows have labels — meaning the join to `mlb_player_game_logs` worked).
- `cols` includes `implied_prob_over`, `rolling_avg_5g`, `rolling_avg_10g`, `rolling_avg_20g`, `rolling_std_10g`, `is_home`, `actual_result`, `actual_stat_value`.

If label coverage is much below 1.0: likely `mlb_player_name_mappings` resolution gap. Check:

```bash
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
SELECT COUNT(DISTINCT pp.player_name) AS odds_names,
       COUNT(DISTINCT m.odds_api_name) AS mapped_names
FROM player_props pp
LEFT JOIN mlb_player_name_mappings m ON m.odds_api_name = pp.player_name
JOIN games g ON g.game_id = pp.game_id
WHERE g.sport = 'baseball_mlb';
"
```

Decision: if `mapped_names / odds_names > 0.8`, proceed. If lower, open issue and proceed anyway — training will use what resolved.

- [ ] **Step 7: Re-pause `mlb_feature_backfill`**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags pause mlb_feature_backfill
```

---

## Task 5 (Phase 4): Train

**Goal:** Run `mlb_train_dag` once and inspect the MLflow output to decide which markets clear the promotion threshold.

**Files:**
- No code changes. Triggers `mlb_train_dag` + MLflow inspection.

- [ ] **Step 1: Unpause `mlb_train_dag`**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags unpause mlb_train_dag
```

- [ ] **Step 2: Trigger the train DAG**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags trigger mlb_train_dag
```

- [ ] **Step 3: Wait for completion**

```bash
docker exec odds-pipeline-airflow-postgres-1 psql -U airflow -d airflow -c "
SELECT run_id, state, start_date::timestamp(0), end_date::timestamp(0)
FROM dag_run
WHERE dag_id = 'mlb_train_dag'
ORDER BY start_date DESC LIMIT 1;
"
```

Expected: `state = 'success'` within ~15 minutes (training XGBoost on a few thousand rows is fast).

- [ ] **Step 4: Inspect MLflow runs**

```bash
docker exec odds-pipeline-airflow-webserver-1 python -c "
from mlflow.tracking import MlflowClient
import os
c = MlflowClient(tracking_uri=os.environ.get('MLFLOW_TRACKING_URI','http://mlflow:5000'))
names = ['mlb_prop_model_batter_hits','mlb_prop_model_batter_total_bases','mlb_prop_model_batter_home_runs']
for name in names:
    try:
        vs = c.search_model_versions(f\"name='{name}'\")
        for v in vs:
            run = c.get_run(v.run_id)
            m = run.data.metrics
            print(f'{name} v{v.version}: tags={dict(v.tags)} '
                  f'auc={m.get(\"roc_auc\")} log_loss={m.get(\"log_loss\")} '
                  f'n_train={m.get(\"n_train_rows\")} n_val={m.get(\"n_val_rows\")}')
    except Exception as e:
        print(f'{name}: ERROR — {e}')
"
```

- [ ] **Step 5: Record the decision matrix**

For each of `batter_hits`, `batter_total_bases` (and `batter_home_runs` if applicable), record:

| Market | `roc_auc` | `log_loss` | `n_train` | `n_val` | Pass threshold? |
|---|---|---|---|---|---|
| `batter_hits` | … | … | … | … | yes/no |
| `batter_total_bases` | … | … | … | … | yes/no |

Threshold: `roc_auc > 0.55` AND `log_loss < 0.69`.

- [ ] **Step 6: Verification gate**

At least one market must pass threshold. If none pass:
- Open an issue documenting the failure mode (low data, miscalibration, etc.).
- Stop the bootstrap. The decision becomes: ship anyway with low-confidence picks (resume Task 6 / Task 7 selectively), or pause until more data accrues.
- Do not silently proceed.

If at least one passes: proceed to Task 6.

---

## Task 6 (Phase 5): Manual MLflow promotion

**Goal:** Set the `@production` alias on each model version that cleared threshold.

**Files:**
- No code changes. MLflow Python client.

- [ ] **Step 1: For each passing market, get the candidate version**

```bash
docker exec odds-pipeline-airflow-webserver-1 python -c "
from mlflow.tracking import MlflowClient
import os
c = MlflowClient(tracking_uri=os.environ.get('MLFLOW_TRACKING_URI','http://mlflow:5000'))
# Replace with the names you decided in Task 5 Step 5
PASSING = ['mlb_prop_model_batter_hits','mlb_prop_model_batter_total_bases']
for name in PASSING:
    vs = sorted(c.search_model_versions(f\"name='{name}'\"),
                key=lambda v: int(v.version), reverse=True)
    candidates = [v for v in vs if v.tags.get('promotion_candidate') == 'true']
    if not candidates:
        print(f'{name}: NO promotion_candidate=true version — re-check Task 5')
    else:
        latest = candidates[0]
        print(f'{name}: candidate version {latest.version}, run {latest.run_id}')
"
```

- [ ] **Step 2: Set `@production` alias for each passing model**

```bash
docker exec odds-pipeline-airflow-webserver-1 python -c "
from mlflow.tracking import MlflowClient
import os
c = MlflowClient(tracking_uri=os.environ.get('MLFLOW_TRACKING_URI','http://mlflow:5000'))
PROMOTIONS = {
    'mlb_prop_model_batter_hits':        '<VERSION>',
    'mlb_prop_model_batter_total_bases': '<VERSION>',
}
for name, version in PROMOTIONS.items():
    c.set_registered_model_alias(name, 'production', version)
    print(f'{name} @production -> v{version}')
"
```

(Replace `<VERSION>` with the version numbers from Step 1.)

- [ ] **Step 3: Verification gate**

```bash
docker exec odds-pipeline-airflow-webserver-1 python -c "
from mlflow.tracking import MlflowClient
import os
c = MlflowClient(tracking_uri=os.environ.get('MLFLOW_TRACKING_URI','http://mlflow:5000'))
for name in ['mlb_prop_model_batter_hits','mlb_prop_model_batter_total_bases','mlb_prop_model_batter_home_runs']:
    try:
        v = c.get_model_version_by_alias(name, 'production')
        print(f'{name} @production -> v{v.version}')
    except Exception:
        print(f'{name}: no production alias (will be skipped in scoring)')
"
```

Pass: at least one model prints `@production -> vN`. The remaining (HR likely) prints `no production alias` — `mlb_score_dag` already handles this via WARNING-and-`continue` per spec.

---

## Task 7 (Phase 6): Unpause daily DAGs

**Goal:** Light up `mlb_feature_dag` + `mlb_score_dag` so tomorrow's morning runs produce real picks. Re-pause `mlb_train_dag` so it only fires on its weekly schedule.

**Files:**
- No code changes.

- [ ] **Step 1: Unpause `mlb_feature_dag`**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags unpause mlb_feature_dag
```

- [ ] **Step 2: Unpause `mlb_score_dag`**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags unpause mlb_score_dag
```

- [ ] **Step 3: Re-pause `mlb_train_dag`**

```bash
docker exec odds-pipeline-airflow-scheduler-1 airflow dags pause mlb_train_dag
```

(Weekly schedule `0 10 * * 1` — next Monday 3am MT it will auto-unpause-trigger? No — `is_paused` and `schedule_interval` are independent. The schedule only fires when unpaused. Re-pausing means we'll need to manually unpause next Monday morning, or leave unpaused and let it fire on Monday. **Decision:** leave it unpaused if you trust Monday's auto-trigger; pause it if you'd rather promote new versions manually after each retrain. Default per spec: pause and let manual promotion remain the gate.)

- [ ] **Step 4: Verify final DAG state matrix**

```bash
docker exec odds-pipeline-airflow-postgres-1 psql -U airflow -d airflow -c "
SELECT dag_id, is_paused FROM dag
WHERE dag_id LIKE 'mlb_%'
ORDER BY dag_id;
"
```

Expected end state:

| DAG | `is_paused` |
|---|---|
| `mlb_odds_pipeline` | `f` |
| `mlb_stats_pipeline` | `f` |
| `mlb_feature_dag` | `f` |
| `mlb_score_dag` | `f` |
| `mlb_train_dag` | `t` (per Step 3 decision) |
| `mlb_odds_backfill` | `t` |
| `mlb_stats_backfill` | `t` |
| `mlb_feature_backfill` | `t` |

---

## Task 8 (Phase 7): T+1 verification (next morning)

**Goal:** After overnight, confirm the daily pipeline produced real recommendations and that yesterday's recs got settled.

**Files:**
- No code changes. Verification queries.

⚠️ **This task runs the morning *after* Task 7 completes (typically next day after ~9:30am MT, once `mlb_score_dag`'s 9am MT run has finished).**

- [ ] **Step 1: Verify today's picks landed**

```bash
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
SELECT prop_type, COUNT(*), ROUND(AVG(edge)::numeric, 4) AS avg_edge,
       MIN(model_version), MAX(model_version)
FROM recommendations
WHERE sport = 'MLB' AND game_date = CURRENT_DATE
GROUP BY prop_type
ORDER BY prop_type;
"
```

Pass:
- At least one row.
- Total row count across prop_types ≈ 10 (TOP_N).
- Per-prop allocation matches `_allocate_slots` (even split for 2 active markets, ties broken by edge).

- [ ] **Step 2: Verify `mlb_score_dag` ran successfully**

```bash
docker exec odds-pipeline-airflow-postgres-1 psql -U airflow -d airflow -c "
SELECT run_id, state, start_date::timestamp(0), end_date::timestamp(0)
FROM dag_run
WHERE dag_id = 'mlb_score_dag'
ORDER BY start_date DESC LIMIT 1;
"
```

Pass: `state = 'success'`. Also confirm Slack got a `[MLB] ⚾ Recommendations ready` post (visual check in Slack).

- [ ] **Step 3: Verify yesterday's recs were settled**

(If Task 7 was completed on day D, this check runs on day D+2 morning — we need two daily cycles before there are settle-able recs from "yesterday".)

```bash
docker exec odds-pipeline-data-postgres-1 psql -U odds -d odds_db -c "
SELECT
  prop_type,
  COUNT(*) AS total,
  SUM(CASE WHEN settled_at IS NOT NULL THEN 1 ELSE 0 END) AS settled,
  SUM(CASE WHEN actual_result IS NOT NULL THEN 1 ELSE 0 END) AS labeled
FROM recommendations
WHERE sport = 'MLB' AND game_date = CURRENT_DATE - 1
GROUP BY prop_type;
"
```

Pass: `settled = total` and `labeled = total` (or close — at minimum, > 50%; stragglers fall to the 7-day stale fallback).

- [ ] **Step 4: Close out the bootstrap**

Update issue #6 or open a new issue (e.g., "MLB MVP live"):

> 2026-05-13 bootstrap complete. `mlb_prop_model_*` @production: `<list>`. First daily recs landed `<date>`. Next slice: opposing-pitcher features (umbrella post-MVP #1).

Commit the spec with final outcomes:

```bash
git add docs/superpowers/specs/2026-05-12-mlb-bootstrap-design.md
git commit -m "docs: mark MLB bootstrap complete"
```

---

## Self-review notes

This plan covers every phase in `docs/superpowers/specs/2026-05-12-mlb-bootstrap-design.md`:

| Spec section | Plan task |
|---|---|
| Phase 0 diagnostic | Task 1 (Steps 1–4) |
| Code change Option A (HR fix) | Task 1a |
| Code change Option B (HR drop) | Task 1b |
| Phase 1 stats backfill | Task 2 |
| Phase 2 daily stats unpause + first run | Task 3 |
| Phase 3 feature backfill | Task 4 |
| Phase 4 train | Task 5 |
| Phase 5 manual MLflow promotion | Task 6 |
| Phase 6 daily DAG unpause | Task 7 |
| Phase 7 T+1 settle verification | Task 8 |
| Acceptance criteria | Distributed across Tasks 2–8 verification gates |
| Promotion threshold | Task 5 Step 5 decision matrix |

### Known characteristics that depart from a standard TDD plan

- Tasks 2, 3, 4, 5 contain long-running steps (waits of minutes to hours). The "snapshot → action → snapshot-again" verification pattern replaces the "failing test → passing test" cycle for these operational phases.
- Tasks 6 and 7 are MLflow-CLI + Airflow-CLI commands with no associated test code.
- Task 1 is genuinely branching: 1a XOR 1b based on Phase 0 outcome. Only one set of substeps executes.
- The plan crosses day boundaries: Task 8 must run on the morning after Task 7 finishes.

### Files touched (summary)

If Task 1a fires:
- `shared/plugins/transformers/player_props.py`
- `shared/tests/unit/transformers/test_player_props.py`
- `docs/superpowers/specs/2026-05-12-mlb-bootstrap-design.md`

If Task 1b fires:
- `mlb/config.py`
- `mlb/plugins/transformers/features.py`
- `mlb/plugins/ml/train.py`
- `mlb/tests/unit/test_config.py`
- `mlb/tests/unit/transformers/test_features.py`
- `mlb/tests/unit/ml/test_train.py`
- `docs/superpowers/specs/2026-05-12-mlb-bootstrap-design.md`

Everything else is operational against the running Docker stack.
