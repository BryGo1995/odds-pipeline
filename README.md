# Odds Pipeline

NBA betting data ingestion pipeline using Apache Airflow, the Odds-API, and PostgreSQL.

## Overview

Fetches NBA events, odds (including player props), and scores from the [Odds-API](https://the-odds-api.com) and stores them in Postgres using a medallion architecture (raw JSON → normalized tables). Designed for ML model training and analysis — the long-term goal is generating daily player prop pick recommendations.

## Architecture

```
Odds-API ──► nba_odds_pipeline ──► games / odds / scores / player_props (Postgres)
                    │
                    │ (ExternalTaskSensor)
                    ▼
nba_api ────► nba_stats_pipeline ──► teams / players / game_logs / season_stats
```

- **nba_odds_pipeline**: Fetches events, odds, scores, and player props from the Odds-API; transforms into structured tables in a single daily run (8am MT)
- **nba_stats_pipeline**: Fetches and transforms NBA player/team stats from nba_api; waits for `nba_odds_pipeline` to complete before running (8:20am MT)
- **nba_odds_backfill**: On-demand historical odds data seeding (manual trigger only)
- **nba_stats_backfill**: On-demand historical stats data seeding (manual trigger only)

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
| `nba_odds_pipeline` | `0 15 * * *` (8am MT) | Fetch + transform odds, scores, and player props |
| `nba_stats_pipeline` | `20 15 * * *` (8:20am MT) | Fetch + transform player/team stats; waits on odds pipeline |
| `nba_odds_backfill` | Manual only | Seed historical odds data on demand |
| `nba_stats_backfill` | Manual only | Seed historical stats data on demand |

To trigger a backfill with a date range, click **Trigger DAG w/ config** in the Airflow UI and pass:
```json
{"date_from": "2024-01-01", "date_to": "2024-03-31"}
```

### Manual runs

Each pipeline DAG handles its own ingest and transform tasks internally — no inter-DAG coordination is needed for manual triggers. Just trigger `nba_odds_pipeline` or `nba_stats_pipeline` directly.

Note: `nba_stats_pipeline` has an `ExternalTaskSensor` (`wait_for_nba_odds_pipeline`) that checks for a completed `nba_odds_pipeline` run. For manual triggers where execution dates won't align, click the sensor task in the Graph view → **Mark Success** to bypass it and run the stats pipeline independently.

## Configuration

Edit `config/settings.py` to control what data is fetched:

```python
MARKETS = ["h2h", "spreads", "totals"]  # add/remove freely
BOOKMAKERS = ["draftkings", "fanduel", "betmgm"]        # fewer = less API quota used
```

No DAG changes needed — just edit the config file.

## Slack Notifications

DAG run results (success and failure) can be posted to a Slack channel via an [incoming webhook](https://api.slack.com/messaging/webhooks).

### Setup

1. Create an incoming webhook in Slack (**Apps → Incoming WebHooks → Add to Slack**) and select the target channel.
2. Copy the webhook URL and add it to `.env`:

```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

That's it — all pipeline DAGs will automatically post a message on each successful or failed run.

If `SLACK_WEBHOOK_URL` is not set, notifications are silently skipped.

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
│   ├── nba_odds_pipeline_dag.py   # fetch + transform odds/scores/player props
│   ├── nba_odds_backfill_dag.py   # on-demand historical odds seeding
│   ├── nba_stats_pipeline_dag.py  # fetch + transform player/team stats
│   └── nba_stats_backfill_dag.py  # on-demand historical stats seeding
├── plugins/
│   ├── odds_api_client.py         # Odds-API HTTP client
│   ├── nba_api_client.py          # nba_api client
│   ├── db_client.py               # Postgres utilities
│   └── transformers/              # raw JSON → normalized table logic
├── config/
│   └── settings.py                # markets, bookmakers, regions (edit here)
├── sql/
│   └── init_schema.sql            # DB schema (auto-applied on first run)
├── tests/
│   ├── unit/                      # fast tests, no Docker
│   └── integration/               # requires data-postgres container
├── docker-compose.yml
├── .env.example
└── docs/                          # design specs and implementation plans
```
