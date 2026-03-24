# DAG Refactor Design — 2026-03-24

## Overview

Refactor the Airflow DAG structure to:
1. Establish consistent naming conventions by data source (`nba_odds_*` vs `nba_stats_*`)
2. Eliminate `ExternalTaskSensor` fragility by merging ingest and transform tasks into single pipeline DAGs
3. Fold `nba_player_props` into `nba_odds_pipeline` to reduce DAG count and make the dependency on the `games` table explicit within the DAG
4. Fix a latent bug where `fetch_player_props_task` raises `ValueError` on no-game days, which would fail the entire pipeline once folded in

## Approach

Option C (per-pipeline incremental):
- **PR 1**: Build `nba_odds_pipeline` (merge ingest + transform + player props, fix graceful skip, rename backfill)
- **PR 2**: Build `nba_stats_pipeline` (merge ingest + transform, add ExternalTaskSensor, rename backfill)

## File Changes

| Old File | New File | Old `dag_id` | New `dag_id` |
|---|---|---|---|
| `dags/ingest_dag.py` | _(deleted)_ | `nba_ingest` | _(merged)_ |
| `dags/transform_dag.py` | _(deleted)_ | `nba_transform` | _(merged)_ |
| `dags/player_props_dag.py` | _(deleted)_ | `nba_player_props` | _(merged)_ |
| `dags/backfill_dag.py` | `dags/nba_odds_backfill_dag.py` | `nba_backfill` | `nba_odds_backfill` |
| _(new)_ | `dags/nba_odds_pipeline_dag.py` | — | `nba_odds_pipeline` |
| `dags/nba_player_stats_ingest_dag.py` | _(deleted)_ | `nba_player_stats_ingest` | _(merged)_ |
| `dags/nba_player_stats_transform_dag.py` | _(deleted)_ | `nba_player_stats_transform` | _(merged)_ |
| `dags/nba_player_stats_backfill_dag.py` | `dags/nba_stats_backfill_dag.py` | `nba_player_stats_backfill` | `nba_stats_backfill` |
| _(new)_ | `dags/nba_stats_pipeline_dag.py` | — | `nba_stats_pipeline` |

Net result: 7 DAG files → 4 DAG files.

## PR 1: `nba_odds_pipeline`

### Schedule
`0 15 * * *` — 8am MT (3pm UTC), once daily during development to conserve API quota.

### Task Chain

```
fetch_events  ─┐
fetch_odds    ─┼─► transform_events ─► transform_odds
fetch_scores  ─┘                    ├─► transform_scores
                                    └─► fetch_player_props ─► transform_player_props
```

- `fetch_events`, `fetch_odds`, `fetch_scores` run in parallel
- `transform_events` runs after all three complete
- `transform_odds` and `transform_scores` run in parallel after `transform_events`
- `fetch_player_props` runs after `transform_events` (needs `games` table populated)
- `transform_player_props` runs after `fetch_player_props`

### Player Props Graceful Skip

On days with no upcoming games, the pipeline should not fail. Fix:

- `fetch_player_props_task`: if no `game_ids` found, log a warning, push `xcom_push(key="skipped", value=True)`, and return early without storing a raw response
- `transform_player_props_task`: at the start, check `ti.xcom_pull(task_ids="fetch_player_props", key="skipped")`; if `True`, log and return early

### Notifications
DAG-level `on_success_callback=notify_success` and `on_failure_callback=notify_failure` (unchanged from existing DAGs).

### `nba_odds_backfill`
Rename only: `dags/backfill_dag.py` → `dags/nba_odds_backfill_dag.py`, `dag_id` `nba_backfill` → `nba_odds_backfill`. No structural changes.

---

## PR 2: `nba_stats_pipeline`

### Schedule
`20 15 * * *` — 8:20am MT (3:20pm UTC). The `ExternalTaskSensor` handles the actual dependency; this time just determines when polling begins.

### Task Chain

```
ExternalTaskSensor(nba_odds_pipeline)
    └─► fetch_teams
    └─► fetch_players
    └─► fetch_player_game_logs   (parallel)
    └─► fetch_team_game_logs
    └─► fetch_team_season_stats
            └─► transform_teams
                    └─► transform_players
                            └─► transform_player_game_logs  (parallel)
                            └─► transform_team_game_logs
                            └─► transform_team_season_stats
                                    └─► resolve_player_ids
                                            └─► link_nba_game_ids
```

- All five fetch tasks run in parallel (raw storage only, no FK constraints)
- Transform chain enforces FK order: teams → players → logs
- `resolve_player_ids` and `link_nba_game_ids` run last, after all transforms complete

### ExternalTaskSensor
```python
ExternalTaskSensor(
    task_id="wait_for_nba_odds_pipeline",
    external_dag_id="nba_odds_pipeline",
    external_task_id=None,  # wait for full DAG completion
    mode="reschedule",
    timeout=3600,  # 1hr — generous buffer for slow/retried odds runs
)
```

### `nba_stats_backfill`
Rename only: `dags/nba_player_stats_backfill_dag.py` → `dags/nba_stats_backfill_dag.py`, `dag_id` `nba_player_stats_backfill` → `nba_stats_backfill`. No structural changes.

---

## Out of Scope

- `nba_odds_player_props` running at afternoon cadence (deferred until API subscription upgrade)
- Resolving the cross-pipeline dependency where `nba_stats_pipeline` ingest reads from `player_props` / `player_name_mappings` (graceful bootstrap fallback remains as-is)
- Programmatic season derivation (`CURRENT_SEASON` constant stays manual)
