# Player Props Ingest — Design Spec

**Date:** 2026-03-18

## Overview

Add a dedicated pipeline for fetching and storing NBA player prop odds. Player props (points, rebounds, assists, etc.) are fetched once per day at 3pm MT via a new `nba_player_props` DAG, independent of the existing game odds pipeline.

## Motivation

The Odds API `/odds` endpoint does not accept `player_props` as a generic market name — specific market keys (e.g., `player_points`) are required. Player props also warrant a separate schedule (once daily at 3pm MT vs. twice daily for game odds) and independent failure isolation.

## Configuration

Add `PLAYER_PROP_MARKETS` to `config/settings.py`:

```python
PLAYER_PROP_MARKETS = [
    "player_points",
    "player_rebounds",
    "player_assists",
]
```

Adding a new prop market is a one-line config change. The transformer already filters on `market_key.startswith("player_")` so no transformer changes are needed when new markets are added.

## API Client

Add `fetch_player_props()` to `plugins/odds_api_client.py`. It delegates to `fetch_odds()` internally:

```python
def fetch_player_props(api_key, sport, regions, markets, bookmakers, odds_format="american"):
    return fetch_odds(api_key, sport, regions, markets, bookmakers, odds_format)
```

The distinct function name keeps DAG logs and task naming readable. The raw response is stored under endpoint `"player_props"` in `raw_api_responses`, separate from `"odds"`.

## DAG Structure

New file: `dags/player_props_dag.py`

- **DAG id:** `nba_player_props`
- **Schedule:** `0 22 * * *` (10pm UTC / 3pm MT)
- **Tasks:**

```
fetch_player_props >> transform_player_props
```

- `fetch_player_props` — calls `fetch_player_props()`, stores raw response under endpoint `"player_props"`
- `transform_player_props` — reads latest `"player_props"` raw response, calls existing `transform_player_props()` transformer, writes to `player_props` table

## Cleanup in `nba_transform`

Since player props will no longer be present in the `"odds"` raw response:

- Remove the `transform_player_props(conn, raw)` call from `run_transform_odds()` in `transform_dag.py`
- Remove the no-op `transform_player_props` task and `run_transform_player_props()` function from `transform_dag.py`

## Error Handling

Follows the ingest pipeline pattern: exceptions bubble up, Airflow retries per `default_args` (`retries=3, retry_delay=5min, retry_exponential_backoff=True`), and the raw response is stored with `status="error"` in `raw_api_responses`.

## Slack Notifications

`nba_player_props` wires up `on_success_callback=notify_success` and `on_failure_callback=notify_failure`, matching `nba_ingest`. A failing props fetch should produce a Slack alert.

## DAG Task Detail

The endpoint name `"player_props"` is set by the DAG task when calling `_fetch_and_store` (or equivalent), not by `fetch_player_props()` itself. The function receives `PLAYER_PROP_MARKETS` as a list and delegates to `fetch_odds()`, which joins it into a comma-separated string for the API request.

## Testing

- Add unit test for `fetch_player_props` in `tests/unit/test_odds_api_client.py` — verifies correct endpoint and market params are sent. Also update the existing stale fixture in that file that still references `"player_props"` as a market name — replace with `"player_points"`.
- Update `tests/unit/test_transform_dag.py` — remove the assertion that `"transform_player_props"` is in the `nba_transform` task IDs, since that task is being removed.
- Existing `tests/unit/transformers/test_player_props.py` and `tests/integration/test_transform_to_normalized.py` require no changes (transformer logic is unchanged).

## Files Changed

| File | Change |
|---|---|
| `config/settings.py` | Add `PLAYER_PROP_MARKETS` list |
| `plugins/odds_api_client.py` | Add `fetch_player_props()` |
| `dags/player_props_dag.py` | New DAG |
| `dags/transform_dag.py` | Remove player props processing and no-op task |
| `tests/unit/test_odds_api_client.py` | Add `fetch_player_props` test, update stale fixture |
| `tests/unit/test_transform_dag.py` | Remove `transform_player_props` task assertion |
