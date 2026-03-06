# Odds Pipeline — Design Document
**Date:** 2026-03-06
**Scope:** NBA data ingestion pipeline (first iteration)

---

## Overview

A daily data ingestion pipeline that fetches NBA betting data from the Odds-API and stores it in a Postgres database. The pipeline is orchestrated by Apache Airflow, runs 1-2x per day to minimize API usage, and follows a medallion architecture (raw → normalized). The long-term goal is to support ML model training for player prop pick recommendations.

---

## Architecture & Infrastructure

### Docker Compose Stack (local)
| Service | Purpose |
|---|---|
| `airflow-webserver` | Airflow UI |
| `airflow-scheduler` | DAG execution engine |
| `airflow-postgres` | Airflow internal metadata DB |
| `data-postgres` | Data warehouse (separate from Airflow DB) |
| `pgadmin` | Web UI for inspecting data (optional) |

- **Executor:** LocalExecutor (no Celery/Redis overhead, sufficient for 1-2x daily)
- **Local first:** Docker Compose for development; designed to migrate to cloud later

### Secret Management
- `.env` file holds `ODDS_API_KEY` and DB credentials — **gitignored**
- `.env.example` committed with placeholder values
- API key loaded from environment variable at runtime — never logged or hardcoded

### Project Structure
```
odds-pipeline/
├── dags/
│   ├── ingest_dag.py         # daily API fetch
│   ├── transform_dag.py      # raw → normalized
│   └── backfill_dag.py       # on-demand historical seeding
├── plugins/
│   ├── odds_api_client.py    # configurable Odds-API client
│   ├── db_client.py          # Postgres connection utilities
│   └── transformers/         # raw JSON → normalized table logic
├── config/
│   └── settings.py           # markets, bookmakers, regions, sport
├── sql/
│   └── init_schema.sql       # DB schema initialization
├── docker-compose.yml
├── .env                      # gitignored
├── .env.example              # committed
└── .gitignore
```

---

## Data Flow

### Daily Ingest DAG (cron: 1-2x/day)
```
fetch_events → fetch_odds → fetch_scores → store_raw_json
```
- Each task fetches one Odds-API endpoint independently
- Full JSON response stored in raw layer with timestamp
- Task-level retries — a single endpoint failure does not re-fetch others

### Transform DAG (triggered after Ingest DAG succeeds)
```
transform_events → transform_odds → transform_scores → transform_player_props
```
- Reads from raw tables, parses JSON, upserts into normalized tables
- Idempotent — safe to re-run without touching the API

### Backfill DAG (on-demand, manually triggered)
```
fetch_historical_events → fetch_historical_odds → fetch_historical_scores → store_raw → transform
```
- Loops over a configurable date range
- Includes rate limiting (configurable sleep between requests) to protect API quota
- Run once to seed historical data

### Configuration (`config/settings.py`)
```python
SPORT = "basketball_nba"
REGIONS = ["us"]
MARKETS = ["h2h", "spreads", "totals", "player_props"]
BOOKMAKERS = ["draftkings", "fanduel", "betmgm"]
```
Adding/removing endpoints or markets requires only a change to this file — no DAG modifications needed.

---

## Data Schema

### Raw Layer
```sql
raw_api_responses (
    id          SERIAL PRIMARY KEY,
    endpoint    VARCHAR,        -- 'events', 'odds', 'scores'
    params      JSONB,          -- query params used (markets, bookmakers, date)
    response    JSONB,          -- full API response
    fetched_at  TIMESTAMP,
    status      VARCHAR         -- 'success' or 'error'
)
```

### Normalized Layer
```sql
games (
    game_id         VARCHAR PRIMARY KEY,
    home_team       VARCHAR,
    away_team       VARCHAR,
    commence_time   TIMESTAMP,
    season          VARCHAR,
    sport           VARCHAR
)

odds (
    id              SERIAL PRIMARY KEY,
    game_id         VARCHAR REFERENCES games,
    bookmaker       VARCHAR,
    market_type     VARCHAR,    -- 'h2h', 'spreads', 'totals'
    outcome_name    VARCHAR,
    price           NUMERIC,
    point           NUMERIC,    -- spread/total value, null for h2h
    last_update     TIMESTAMP,
    fetched_at      TIMESTAMP
)

player_props (
    id              SERIAL PRIMARY KEY,
    game_id         VARCHAR REFERENCES games,
    bookmaker       VARCHAR,
    player_name     VARCHAR,
    prop_type       VARCHAR,    -- 'player_points', 'player_rebounds', etc.
    outcome         VARCHAR,    -- 'Over' or 'Under'
    price           NUMERIC,
    point           NUMERIC,    -- the line (e.g., 24.5 points)
    last_update     TIMESTAMP,
    fetched_at      TIMESTAMP
)

scores (
    id              SERIAL PRIMARY KEY,
    game_id         VARCHAR REFERENCES games,
    home_score      INTEGER,
    away_score      INTEGER,
    completed       BOOLEAN,
    last_update     TIMESTAMP,
    fetched_at      TIMESTAMP
)
```

> `player_props` is the primary ML training target. `fetched_at` on every row enables line movement tracking over time.

---

## Error Handling & Reliability

| Concern | Approach |
|---|---|
| API failures | Per-task retries (3x, exponential backoff) |
| Failed responses | Stored in `raw_api_responses` with `status = 'error'` |
| Quota protection | Transforms never re-trigger API fetches; backfill has rate limiting |
| Duplicate data | Upserts (`INSERT ... ON CONFLICT DO UPDATE`) throughout |
| Secrets | API key from env var only — never logged or committed |
| Monitoring | Airflow UI alerts on task failure |

---

## Testing

| Layer | Approach |
|---|---|
| Unit | Mock API responses; test transform logic against JSON fixtures |
| Integration | Test ingest → raw storage and transform → normalized against local test Postgres |
| Idempotency | Run transforms twice, verify no duplicates |
| Manual | Spot-check `raw_api_responses` and `player_props` rows against Odds-API website |

End-to-end Airflow DAG testing in CI is deferred — manual Airflow UI verification is sufficient for the first iteration.

---

## Pipeline Diagram

```
                        +---------------------+
                        |   Odds-API          |
                        |  (NBA endpoints)    |
                        +----------+----------+
                                   |
                    +--------------+--------------+
                    |              |              |
              fetch_events   fetch_odds    fetch_scores
                    |              |              |
                    +--------------+--------------+
                                   |
                           store_raw_json
                                   |
                     +-------------+-------------+
                     |                           |
              raw_api_responses            [trigger]
              (Postgres - raw layer)            |
                                      Transform DAG
                                           |
                    +----------+-----------+-----------+
                    |          |           |           |
             transform   transform    transform   transform
              _events     _odds       _scores   _player_props
                    |          |           |           |
                    +----------+-----------+-----------+
                                   |
                    +--------------+--------------+
                    |         |         |         |
                  games      odds    scores  player_props
                         (Postgres - normalized layer)


  On-demand:
  +----------------+
  |  Backfill DAG  |  -----> same flow as above, loops over date range
  +----------------+         with rate limiting between requests
```

---

## Future Considerations
- Migrate Docker Compose to cloud (AWS ECS + RDS, or GCP)
- Add dbt for transformation layer as complexity grows
- Expand beyond NBA to other sports
- Add additional bookmakers and markets as needed
- Build ML feature store on top of normalized layer
