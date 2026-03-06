# Odds Pipeline

NBA betting data ingestion pipeline using Apache Airflow, the Odds-API, and PostgreSQL.

## Overview

Fetches NBA events, odds (including player props), and scores from the [Odds-API](https://the-odds-api.com) and stores them in Postgres using a medallion architecture (raw JSON → normalized tables). Designed for ML model training and analysis — the long-term goal is generating daily player prop pick recommendations.

## Architecture

```
Odds-API → nba_ingest DAG → raw_api_responses (Postgres)
                                      ↓
                          nba_transform DAG → games / odds / player_props / scores
```

- **nba_ingest**: Fetches events, odds, and scores; stores full JSON responses (8am + 8pm daily)
- **nba_transform**: Normalizes raw data into structured tables (runs 15 min after ingest)
- **nba_backfill**: On-demand historical data seeding (manual trigger only)

## Setup

### 1. Prerequisites
- Docker + Docker Compose
- An [Odds-API](https://the-odds-api.com) key

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and set your `ODDS_API_KEY`. Generate a Fernet key for Airflow:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the output as `AIRFLOW_FERNET_KEY` in `.env`.

### 3. Start the stack

```bash
docker compose up -d
```

Wait ~60 seconds for Airflow to initialize.

### 4. Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |

In pgAdmin, connect to `data-postgres`:
- Host: `data-postgres`
- Port: `5432`
- Database: `odds_db`
- Username: `odds` (or value from `.env`)

## DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `nba_ingest` | `0 8,20 * * *` | Fetch from Odds-API, store raw JSON |
| `nba_transform` | `15 8,20 * * *` | Normalize raw → structured tables |
| `nba_backfill` | Manual only | Seed historical data on demand |

To trigger the backfill with a date range, click **Trigger DAG w/ config** in the Airflow UI and pass:
```json
{"date_from": "2024-01-01", "date_to": "2024-03-31"}
```

## Configuration

Edit `config/settings.py` to control what data is fetched:

```python
MARKETS = ["h2h", "spreads", "totals", "player_props"]  # add/remove freely
BOOKMAKERS = ["draftkings", "fanduel", "betmgm"]        # fewer = less API quota used
```

No DAG changes needed — just edit the config file.

## Running Tests

**Unit tests** (no Docker required):

```bash
pip install -r requirements-dev.txt
pytest tests/unit/ -v --ignore=tests/unit/test_schema.py
```

**Schema + integration tests** (requires `data-postgres` running):

```bash
docker compose up data-postgres -d
pytest tests/unit/test_schema.py tests/integration/ -v
```

## Project Structure

```
odds-pipeline/
├── dags/
│   ├── ingest_dag.py       # daily API fetch
│   ├── transform_dag.py    # raw → normalized
│   └── backfill_dag.py     # on-demand historical seeding
├── plugins/
│   ├── odds_api_client.py  # Odds-API HTTP client
│   ├── db_client.py        # Postgres utilities
│   └── transformers/       # raw JSON → normalized table logic
├── config/
│   └── settings.py         # markets, bookmakers, regions (edit here)
├── sql/
│   └── init_schema.sql     # DB schema (auto-applied on first run)
├── tests/
│   ├── unit/               # fast tests, no Docker
│   └── integration/        # requires data-postgres container
├── docker-compose.yml
├── .env.example
└── docs/plans/             # design and implementation docs
```
