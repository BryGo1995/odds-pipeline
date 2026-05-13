# MLB Bootstrap — Design

**Date:** 2026-05-12
**Status:** Draft (pending user review)
**Scope:** One-time operational rollout that takes the merged MLB code (foundation → odds → stats → features → ML stage, all green) from "tests pass" to "produces daily recommendations with `sport='MLB'` in Postgres." Closes the gap that prior plans never covered: every MLB plan exited at "git clean / tests green," none included a runbook for actually operating the pipeline.

## Goals

1. At least one `mlb_prop_model_<prop_type>` exists at `@production` in MLflow.
2. `mlb_score_dag` runs successfully and writes ranked recommendations to the `recommendations` table with `sport='MLB'` daily, sensors-fed by `mlb_feature_dag`.
3. `mlb_stats_pipeline.settle_recommendations` runs daily and resolves yesterday's picks against game logs.
4. Each step has an explicit verification gate so a partial bootstrap halts visibly rather than silently leaving the pipeline broken.
5. `batter_home_runs` market gap is either fixed forward or removed from MVP scope with a written rationale.

## Non-Goals

- Opposing-pitcher / handedness / Statcast features (umbrella's post-MVP item #1; tackled in the next slice).
- Pitcher props (`pitcher_strikeouts`, etc.) (umbrella's post-MVP item #2).
- Renaming or rewriting `mlb_odds_backfill_dag` despite the "backfill" misnomer (it only fetches *current* odds with optional time filters; it cannot fetch historical odds because the Odds-API historical endpoint isn't wired into our client). Documented separately if it becomes a problem.
- ROI/calibration dashboards or a customer-facing UI (issues #5, #13).
- CI/CD (issue #7).
- New schema migrations (all MLB tables already exist per migration 006).

## Observed current state (as of 2026-05-12)

| Component | State |
|---|---|
| `mlb_odds_pipeline` | ✅ Unpaused. 22 daily runs (Apr 21 → May 12), all success. |
| `player_props` rows (MLB) | 29,248 across 22 distinct game dates. Two `prop_type` values only: `batter_hits` (17,234) and `batter_total_bases` (12,014). `batter_home_runs` **never appears** despite being in `mlb/config.py:21`. |
| `mlb_teams` / `mlb_players` / `mlb_player_game_logs` / `mlb_player_name_mappings` | All zero rows. `mlb_stats_pipeline` has never run; `mlb_stats_backfill` has never been triggered. |
| `mlb_stats_pipeline` | ⏸ Paused. Never executed. |
| `mlb_feature_dag` / `mlb_feature_backfill` | ⏸ Paused. No parquet files in `/data/features/mlb_*`. |
| `mlb_train_dag` | ⏸ Paused. MLflow has zero `mlb_prop_model_*` runs. |
| `mlb_score_dag` | ⏸ Paused. `recommendations` has no `sport='MLB'` rows. |
| Docker stack | ✅ Healthy. Up 6 days. data-postgres, airflow-postgres, airflow-scheduler, airflow-webserver, mlflow, pgadmin, odds-admin. |

## Summary of Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Sequence | HR diagnostic → stats backfill → daily stats → feature backfill → train → manual promote → unpause daily | Each step is gated; failure halts the chain visibly |
| Stats backfill seasons | `season_start=2025`, `season_end=2026` | 20-game rolling lookback in features needs prior season. 2024 adds ~zero lift for current-season features and ~doubles runtime |
| HR market handling | Diagnose first (15-min raw-response inspection). Fix forward if it's a transform bug; drop from `mlb/config.py` if Odds-API doesn't expose it for our 3 books | Concrete decision rule, not "investigate forever" |
| Feature backfill range | `2026-04-21` → `<yesterday>` | Earliest props ingest is Apr 21; later dates fed by daily DAG once unpaused |
| Promotion threshold | ROC-AUC > 0.55 **and** validation log-loss < 0.69 (Brier as sanity check) | AUC alone can mask miscalibration; log-loss < 0.69 ≈ "better than uniform 0.5 prediction" |
| Per-prop-type promotion | Each market promoted independently; `mlb_score_dag` already tolerates partial availability (writes WARNING + `continue`) | Don't block hits/TB on HR's situation |
| Unpause cadence | Sequential with verification gate between each, not bulk-unpause | Matches the spec's "halt visibly on partial bootstrap" goal |
| `mlb_train_dag` post-bootstrap | Leave **paused** after manual run until next Monday's scheduled retrain or until we re-eval | No reason to retrain minutes after bootstrap |
| Bootstrap location | Performed on `main` directly (or short-lived branch `feature/mlb-bootstrap` if any code changes are required, e.g. HR drop) | Mostly operational; only code touches are pre-bootstrap config changes |

## Bootstrap procedure

The runbook is a sequence of phases, each with an explicit **verification gate** that must pass before the next phase begins. If any gate fails, the bootstrap halts and we triage rather than proceeding optimistically.

### Phase 0: Pre-bootstrap diagnostic — `batter_home_runs` market

**Action:** SQL-inspect the most recent `raw_responses` row for `endpoint='mlb_odds'`. Parse the JSON and check whether any bookmaker returned a `batter_home_runs` market for any event.

```sql
SELECT response_json -> 0 -> 'bookmakers'
FROM raw_responses
WHERE endpoint = 'mlb_odds'
ORDER BY fetched_at DESC LIMIT 1;
```

(Or fetch a single MLB event's odds endpoint live with the `batter_home_runs` market explicitly requested and inspect the raw return.)

**Decision rule:**
- If `batter_home_runs` market is present in the API response: this is a transform bug in `shared/plugins/transformers/player_props.py`. Fix it inline (likely a market-name filter that's too narrow), add a regression test, ship the fix to `main`, and **all 3 markets stay in scope**.
- If `batter_home_runs` is absent from the API response (DK/FD/BetMGM don't offer it on the Odds-API for MLB): remove it from `mlb/config.py:PLAYER_PROP_MARKETS`, update `MLB_PROP_STAT_MAP` and `mlb/plugins/ml/train.py:min_rows`, update tests, ship. **MVP becomes 2 markets** (`batter_hits`, `batter_total_bases`).

**Gate:** A diagnostic note is written into the implementation plan output ("Outcome: bug fixed / market dropped"). If neither outcome is clear (e.g. mixed signal across books), open a follow-up issue and proceed with the 2-market path; don't block bootstrap.

### Phase 1: Stats backfill (`mlb_stats_backfill`)

**Action:** Manually trigger `mlb_stats_backfill` with params:

```json
{"season_start": "2025", "season_end": "2026"}
```

Estimated runtime: 60–180 min (~180 days × 2 seasons × ~1s/day baseline + variable boxscore-fetch time).

**Gate:**

```sql
SELECT
  (SELECT COUNT(*) FROM mlb_teams)                      AS teams,
  (SELECT COUNT(*) FROM mlb_players)                    AS players,
  (SELECT MIN(game_date) FROM mlb_player_game_logs)     AS first_log,
  (SELECT MAX(game_date) FROM mlb_player_game_logs)     AS last_log,
  (SELECT COUNT(DISTINCT game_date) FROM mlb_player_game_logs) AS distinct_dates,
  (SELECT COUNT(*) FROM mlb_player_name_mappings)       AS name_mappings;
```

Pass criteria:
- `teams ≥ 30`
- `players ≥ 1500` (typical MLB active+inactive across 2 seasons)
- `first_log ≤ 2025-04-01` and `last_log ≥ <yesterday>`
- `distinct_dates ≥ 300` (most days of both seasons)
- `name_mappings > 0` (resolver ran successfully)

If gate fails: triage `mlb_stats_backfill` logs, fix, re-run.

### Phase 2: Daily stats pipeline (`mlb_stats_pipeline`)

**Action:**
1. Unpause `mlb_stats_pipeline` in Airflow.
2. Manually trigger one run for the current logical date (the `wait_for_mlb_odds_pipeline` ExternalTaskSensor must find a successful upstream — verify via Graph view, mark-success only if needed).
3. Let it complete.

**Gate:** Same query as Phase 1 — confirm `last_log = <today_or_yesterday>` advances vs. Phase 1's snapshot. Also confirm `settle_recommendations` task ran and exited cleanly (no MLB recs to settle yet, which is the expected no-op).

If gate fails: triage logs. Common failure modes: missing player FK (resolver lag), MLB Stats API rate limit, schedule endpoint returning unexpected shape for off-day.

### Phase 3: Feature backfill (`mlb_feature_backfill`)

**Action:** Manually trigger `mlb_feature_backfill` with params:

```json
{"date_from": "2026-04-21", "date_to": "<yesterday>"}
```

(`yesterday` = the day before the bootstrap is being run; today's parquet will be generated by the daily `mlb_feature_dag` once unpaused in Phase 5.)

**Gate:**

```sql
-- Container-side: list parquet files
ls /data/features/mlb_props_features_*.parquet | wc -l
```

```python
# Inside one of the parquet files (verify schema and label coverage):
import pandas as pd
df = pd.read_parquet("/data/features/mlb_props_features_2026-04-25.parquet")
print(df.shape)
print(df["prop_type"].value_counts())
print(df["actual_result"].notna().mean())  # should be ~1.0 for old dates
```

Pass criteria:
- One parquet per game date in range (allowing ~1-3 empty/skipped days for no-MLB days; ~22 files expected).
- Each parquet has the expected feature columns (`implied_prob_over`, `rolling_avg_5g`, etc.) per `features.py` output schema.
- `actual_result` is populated (non-null) for dates ≤ yesterday — confirms the join to `mlb_player_game_logs` worked.
- Row count per parquet is in the 100–800 range (typical day's props × 2 outcomes / filtered to mapped players).

If gate fails: most likely `mlb_player_name_mappings` join miss (props for players we couldn't resolve). Acceptable up to a threshold; we ship with whatever resolved.

### Phase 4: Train (`mlb_train_dag`)

**Action:** Unpause `mlb_train_dag` (one-off — we'll repause after promotion) and trigger one manual run.

**Gate:**

1. MLflow UI / API check:
   ```python
   from mlflow.tracking import MlflowClient
   c = MlflowClient()
   for name in ["mlb_prop_model_batter_hits",
                "mlb_prop_model_batter_total_bases",
                "mlb_prop_model_batter_home_runs"]:
       try:
           versions = c.search_model_versions(f"name='{name}'")
           print(name, [(v.version, v.tags) for v in versions])
       except Exception as e:
           print(name, "—", e)
   ```
2. For each registered version, record from the run:
   - `roc_auc`
   - `log_loss`
   - `brier_score` (if logged)
   - `n_train_rows`, `n_val_rows`

Pass criteria:
- At least one model registered with `promotion_candidate=true` tag.
- `n_train_rows ≥ min_rows` for the prop type (50 hits / 50 TB / 100 HR-if-applicable).
- At least one model meets the promotion threshold: `roc_auc > 0.55` **and** `log_loss < 0.69`.

If zero models clear threshold: this is the most likely partial-success path. Decide between (a) proceeding with whatever models *do* clear (even if poor), (b) accepting the data-thin reality and pausing bootstrap until more days of props have accumulated, or (c) opening a follow-up issue to debug feature quality. Default action: ship whatever clears, open issue for the rest.

### Phase 5: Manual promotion in MLflow

**Action:** For each model that clears the promotion threshold, in MLflow UI:

1. Open the candidate version's run.
2. Under "Registered Models" → `mlb_prop_model_<prop_type>`, set the `@production` alias to the candidate version.
3. Confirm alias points to the new version (`client.get_model_version_by_alias(name, "production").version`).

**Gate:**

```python
from mlflow.tracking import MlflowClient
c = MlflowClient()
for name in ["mlb_prop_model_batter_hits",
             "mlb_prop_model_batter_total_bases",
             "mlb_prop_model_batter_home_runs"]:
    try:
        v = c.get_model_version_by_alias(name, "production")
        print(name, "→ version", v.version)
    except Exception:
        print(name, "—", "no production alias (skip in scoring)")
```

Pass criteria: at least one `@production` alias is set.

### Phase 6: Unpause daily DAGs

**Action:** In order:

1. Unpause `mlb_feature_dag`.
2. Unpause `mlb_score_dag`.
3. Re-pause `mlb_train_dag` if it was unpaused for the manual run (next scheduled retrain Mon 3am MT — no reason to retrain immediately).

**Gate (deferred to tomorrow morning):** Tomorrow's daily runs land and produce:

```sql
SELECT game_date, prop_type, COUNT(*), AVG(edge) AS avg_edge
FROM recommendations
WHERE sport = 'MLB' AND game_date = CURRENT_DATE
GROUP BY game_date, prop_type;
```

Expect: ~10 total rows (`TOP_N=10`) split across active prop types. Also expect a `[MLB]` Slack post from `notify_score_ready`.

### Phase 7: Tomorrow's settlement loop verification

**Action (T+1 day):** After tomorrow's `mlb_stats_pipeline` runs, verify yesterday's recommendations are being settled:

```sql
SELECT settled_at IS NOT NULL AS settled, COUNT(*)
FROM recommendations
WHERE sport = 'MLB' AND game_date = CURRENT_DATE - 1
GROUP BY 1;
```

Expect: all rows have `settled_at` set, with `actual_result` and `actual_stat_value` populated. A `[MLB]` Slack recap post should have fired from `notify_picks_settled`.

If recs stay unsettled past one day: likely a `mlb_player_name_mappings` join miss. Acceptable for stragglers up to the 7-day stale-fallback; investigate if pervasive.

## Code changes required

The bootstrap is operational, but Phase 0's outcome may force one of two small code changes:

**Option A — HR transform bug (most likely fix-forward path):** edit `shared/plugins/transformers/player_props.py` to widen the market filter, add a unit test asserting `batter_home_runs` outcomes survive transform. Commit on `main` (or a short branch).

**Option B — HR not exposed by API:** edit `mlb/config.py` to remove `"batter_home_runs"` from `PLAYER_PROP_MARKETS`; edit `mlb/plugins/transformers/features.py` to remove the key from `MLB_PROP_STAT_MAP`; edit `mlb/plugins/ml/train.py` to remove the `batter_home_runs` `min_rows` entry; update tests that reference the 3-market config. Commit on `main` (or short branch).

**No new files. No new tests beyond a transform regression test under Option A.** No DB migrations.

## Acceptance criteria

Bootstrap is "complete" when all of the following are true at the same moment:

1. **Stats data present.** `mlb_player_game_logs` covers 2025 + 2026-to-date, `mlb_player_name_mappings` has > 0 rows, `mlb_stats_pipeline` runs daily on schedule.
2. **Features generated.** ≥ 18 parquet files in `/data/features/mlb_props_features_*.parquet` covering Apr 21+, plus a fresh file every day from the daily DAG.
3. **At least one model at `@production`.** `MlflowClient.get_model_version_by_alias("mlb_prop_model_<X>", "production")` returns a version object for at least one `X`.
4. **Daily picks landing.** `recommendations` gains ~10 new rows per day with `sport='MLB'`, distributed across active prop types per `_allocate_slots`.
5. **Settle loop verified.** Yesterday's MLB recs have `settled_at IS NOT NULL` and `actual_result` populated.
6. **Slack notifications working.** `[MLB]` prefix appears on `notify_score_ready` (today's recs) and `notify_picks_settled` (yesterday's recap).
7. **HR market mystery resolved.** Either a regression test asserts `batter_home_runs` round-trips, or `mlb/config.py` no longer lists it and a follow-up issue documents the decision.

## Risks & open questions

- **Stats backfill runtime.** Estimated 1–3 hours but could blow up if MLB Stats API throttles harder than the 1-req/sec baseline assumed. Mitigation: run during low-priority window; if it stalls past 4 hours, kill and re-trigger with reduced delay or chunk by half-season.
- **Player name resolution miss rate.** Unknown until run. If > 20% of Odds-API names fail to resolve, the feature backfill will produce thin parquets and training rows will be capped artificially. Mitigation: `[MLB]` Slack alert from resolver surfaces the list; manually correct via DB update or extend `normalize_name`. If pervasive, open an issue and proceed with whatever resolves.
- **Models below promotion threshold.** With ~22 days of 2-market data, ~5-12k rows per market, models *should* train but quality is genuinely unknown. If AUC < 0.55 across all markets: open issue, accept that we ship low-confidence picks (or no picks) and that the next slice (opposing-pitcher features) is doubly motivated.
- **`batter_home_runs` outcome.** Genuinely 50/50 between fix-forward and drop-from-MVP. Phase 0's diagnostic is the decision point; no need to pre-commit.
- **Doubleheader settlement edge cases on first real settle.** The spec accounts for DH via day-aggregation, but the first time this fires on real data is during Phase 7. Watch for unexpected `actual_stat_value` magnitudes; this is the spec's known approximation.
- **2026 mid-season start.** All 2026 game-log data lands at once via the stats backfill. The daily DAG's `wait_for_mlb_odds_pipeline` sensor depends on yesterday's odds run, which has been running all along, so the daily DAG should fire cleanly once unpaused — no execution-date mismatch worry.

## Out of scope / next slice ordering

After this bootstrap closes, resume the umbrella spec's post-MVP ordering:

1. **Opposing-pitcher features** (handedness matchup, opp SP K/9, opp SP OPS-against). Highest expected lift. Issue/spec to be written.
2. Pitcher props (`pitcher_strikeouts`, `pitcher_outs`, `pitcher_earned_runs`).
3. Statcast / pybaseball advanced metrics.
4. Park factor.
5. Batting-order lineup-spot.
6. Doubleheader edge cases (only if Phase 7 reveals real distortion).
7. Shared-core refactor (deferred to NFL planning).
8. Additional batter markets (`batter_rbis`, `batter_runs_scored`, `batter_stolen_bases`, `batter_hits_runs_rbis`).

Independent of the above, two infrastructure items surfaced by this audit but deferred:

- The "odds backfill" misnomer in `mlb_odds_backfill_dag` / `nba_odds_backfill_dag` — neither DAG actually backfills historical odds, since the Odds-API historical endpoint isn't wired into `shared/plugins/odds_api_client.py`. Renaming or implementing historical fetch is a separate piece of work (covered by open issue #12).
- A bootstrap-style spec template — every prior plan exited at "tests green" without an operational gate. If we ship a second sport (NFL), we should bake bootstrap into the slice plans rather than as an afterthought.
