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

Follows the existing pipeline pattern: exceptions bubble up, Airflow retries per `default_args`, and the raw response is stored with `status="error"` in `raw_api_responses`.

## Testing

- Add unit test for `fetch_player_props` in `tests/unit/test_odds_api_client.py` — verifies correct endpoint and market params are sent
- Existing `tests/unit/transformers/test_player_props.py` and `tests/integration/test_transform_to_normalized.py` require no changes (transformer logic is unchanged)

## Files Changed

| File | Change |
|---|---|
| `config/settings.py` | Add `PLAYER_PROP_MARKETS` list |
| `plugins/odds_api_client.py` | Add `fetch_player_props()` |
| `dags/player_props_dag.py` | New DAG |
| `dags/transform_dag.py` | Remove player props processing and no-op task |
| `tests/unit/test_odds_api_client.py` | Add unit test for `fetch_player_props` |
