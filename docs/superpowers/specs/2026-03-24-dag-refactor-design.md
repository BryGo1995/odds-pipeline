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

---

## PR 1: `nba_odds_pipeline`

### Schedule
`0 15 * * *` — 8am MT (3pm UTC), once daily during development to conserve API quota.

### DAG Metadata

```python
start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
catchup=False,
tags=["nba", "odds"],
```

`start_date` must be timezone-aware and identical to the value used in `nba_stats_pipeline` so the `ExternalTaskSensor` logical execution dates align.

### Task Chain

```
fetch_events  ─┐
fetch_odds    ─┼─► transform_events ─► transform_odds        (parallel)
fetch_scores  ─┘                    ├─► transform_scores
                                    └─► fetch_player_props ─► transform_player_props
```

- `fetch_events`, `fetch_odds`, `fetch_scores` run in parallel
- `transform_events` runs after all three complete
- `transform_odds` and `transform_scores` run in parallel after `transform_events` — this ordering is a FK requirement: `game_odds` and `scores` both reference `games.game_id`, so `transform_events` must commit first
- `fetch_player_props` runs after `transform_events` (queries `games` table for upcoming game IDs)
- `transform_player_props` runs after `fetch_player_props`

### `default_args`

Use fetch-style retry settings for the whole DAG (player props and ingest tasks are the most failure-prone; transform tasks benefit from the same retry coverage):

```python
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}
```

### Player Props Graceful Skip

On days with no upcoming games, the pipeline should not fail. Fix:

- `fetch_player_props_task`: if no `game_ids` found, log a warning, push `ti.xcom_push(key="skipped", value=True)`, and return early without storing a raw response. The existing `ti.xcom_push(key="api_remaining", value=remaining)` must be retained on the non-skipped path (it is only emitted when props are successfully fetched, not on early return).
- `transform_player_props_task`: at the start, check `ti.xcom_pull(task_ids="fetch_player_props", key="skipped")`; if the result `== True` (strict equality — `xcom_pull` returns `None` when the key was never set, which should be treated as not-skipped), log and return early

### Notifications
DAG-level `on_success_callback=notify_success` and `on_failure_callback=notify_failure` (unchanged from existing DAGs).

### `nba_odds_backfill`
Rename only: `dags/backfill_dag.py` → `dags/nba_odds_backfill_dag.py`, `dag_id` `nba_backfill` → `nba_odds_backfill`. No structural changes.

---

## PR 2: `nba_stats_pipeline`

### Schedule
`20 15 * * *` — 8:20am MT (3:20pm UTC). The `ExternalTaskSensor` handles the actual dependency; this time just determines when polling begins.

### DAG Metadata

```python
start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
catchup=False,
tags=["nba", "stats"],
```

`start_date` must be identical to `nba_odds_pipeline` for the `ExternalTaskSensor` to match logical execution dates correctly.

### Task Chain

```
ExternalTaskSensor(nba_odds_pipeline)
    >> [fetch_teams, fetch_players, fetch_player_game_logs,
        fetch_team_game_logs, fetch_team_season_stats]     (all parallel)
    >> transform_teams
    >> transform_players
    >> [transform_player_game_logs,
        transform_team_game_logs,
        transform_team_season_stats]                       (parallel)
    >> resolve_player_ids
    >> link_nba_game_ids
```

- All five fetch tasks run in parallel after the sensor clears (raw storage only, no FK constraints at ingest time)
- Transform chain enforces FK order: teams → players → logs; this ordering is required because `players.team_id` FKs to `teams.team_id`, and log tables FK to `players`
- `resolve_player_ids` and `link_nba_game_ids` run last, after all three log transforms complete

### `default_args`

```python
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}
```

### ExternalTaskSensor

Both `nba_odds_pipeline` and `nba_stats_pipeline` share the same `start_date` and daily schedule, so their logical execution dates align on the same UTC day. No `execution_delta` is needed.

```python
ExternalTaskSensor(
    task_id="wait_for_nba_odds_pipeline",
    external_dag_id="nba_odds_pipeline",
    external_task_id=None,   # wait for full DAG completion
    mode="reschedule",
    poke_interval=60,        # check every 60s (Airflow default, stated explicitly)
    timeout=3600,            # 1hr — generous buffer for slow/retried odds runs
)
```

### Notifications
`nba_stats_pipeline` adds DAG-level `on_success_callback=notify_success` and `on_failure_callback=notify_failure` (the existing `nba_player_stats_transform_dag.py` had no callbacks; the merged DAG adds them for consistency with all other scheduled DAGs).

### `nba_stats_backfill`
Rename only: `dags/nba_player_stats_backfill_dag.py` → `dags/nba_stats_backfill_dag.py`, `dag_id` `nba_player_stats_backfill` → `nba_stats_backfill`. No structural changes.

---

## Out of Scope

- Running `nba_odds_pipeline` at an afternoon cadence (deferred until API subscription upgrade; at that point player props can be broken back out or the DAG can be triggered twice daily)
- Resolving the cross-pipeline dependency where `nba_stats_pipeline` ingest reads from `player_props` / `player_name_mappings` (graceful bootstrap fallback remains as-is)
- Programmatic season derivation (`CURRENT_SEASON` constant stays manual)
