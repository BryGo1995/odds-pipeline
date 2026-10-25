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
                    │
                    │ (ExternalTaskSensor)
                    ▼
             nba_feature_dag ──────► features/props_features_<date>.parquet
                    │
                    │ (ExternalTaskSensor)
                    ▼
             nba_score_dag ─────────► recommendations (Postgres)

nba_train_dag (weekly) ────────────► MLflow model registry
```

- **nba_odds_pipeline**: Fetches events, odds, scores, and player props from the Odds-API; transforms into structured tables in a single daily run (3pm MT)
- **nba_stats_pipeline**: Fetches and transforms NBA player/team stats from nba_api; waits for `nba_odds_pipeline` to complete before running (3:20pm MT)
- **nba_feature_dag**: Builds player prop features from Postgres and writes daily Parquet files; waits for `nba_stats_pipeline` (3:40pm MT)
- **nba_score_dag**: Loads the production XGBoost model from MLflow and scores today's props; writes ranked recommendations to Postgres (4pm MT)
- **nba_train_dag**: Trains the XGBoost model weekly on historical features/labels and promotes the best model in MLflow (Mondays 10am MT)
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
| MLflow | http://localhost:5001 | — |

In pgAdmin, connect to `data-postgres`:
- Host: `data-postgres`
- Port: `5432`
- Database: `odds_db`
- Username: `odds` (or value from `.env`)

## DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `nba_odds_pipeline` | `0 15 * * *` (3pm MT) | Fetch + transform odds, scores, and player props |
| `nba_stats_pipeline` | `20 15 * * *` (3:20pm MT) | Fetch + transform player/team stats; waits on odds pipeline |
| `nba_feature_dag` | `40 15 * * *` (3:40pm MT) | Build player prop features → Parquet; waits on stats pipeline |
| `nba_score_dag` | `0 16 * * *` (4pm MT) | Score today's props with ML model → recommendations; waits on feature dag |
| `nba_train_dag` | `0 10 * * 1` (Mon 10am MT) | Train XGBoost model on historical data; log + promote via MLflow |
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

Each sport has its own config module — edit the appropriate one to control what data is fetched:

- `nba/config.py` — NBA markets, prop markets, bookmakers
- `mlb/config.py` — MLB markets, prop markets, bookmakers

```python
# Example (nba/config.py)
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

## Development environment

Experiments, notebooks and tests run in a separate compose project (`odds-dev`), not in the production stack. Nothing in it can write to production:

| Service | What it is | Host port |
|---|---|---|
| `dev` | Workspace container (production image + pytest + JupyterLab), repo mounted at `/workspace`, capped to `DEV_CPUS` (default 4) | `127.0.0.1:8888` (JupyterLab) |
| `dev-postgres` | Copy of production `odds_db`, restored from the latest backup | `127.0.0.1:5435` |
| `dev-mlflow` | Experiment tracking, separate from the production registry | `127.0.0.1:5002` |

Production feature files are mounted read-only at `/data/features`. The dev stack can't reach the production database, and it doesn't receive `ODDS_API_KEY`.

```bash
scripts/backup_db.sh                                     # dump prod odds_db to ~/odds-backups
DEV_UID=$(id -u) docker compose -f docker-compose.dev.yml up -d --build
scripts/dev_refresh_db.sh                                # restore the newest dump into dev-postgres
docker compose -f docker-compose.dev.yml logs dev | grep token=   # JupyterLab URL + token
```

**VS Code over SSH:** connect with the Remote-SSH extension, open the repo folder, then run **Dev Containers: Reopen in Container**. Notebooks, the debugger and the test explorer all run inside `dev`. After changing the Dockerfile, requirements or compose file, use **Dev Containers: Rebuild Container**; otherwise **Reopen in Container** attaches to the running one. (`initializeCommand` builds the image on the host, because Compose v5's Bake builder rejects the Dockerfile VS Code generates in `/tmp`.)

**Browser over SSH:** `ssh -L 8888:localhost:8888 -L 5002:localhost:5002 <host>`, then open the JupyterLab URL from the logs and MLflow at http://localhost:5002.

Start with `notebooks/01_getting_started.ipynb`.

### Backups

`scripts/backup_db.sh` runs `pg_dump` on the production `odds_db`, checks the dump, and keeps the newest 14 in `~/odds-backups` (about 250 MB each). To run it nightly, add it to `crontab -e`:

```
0 2 * * * /path/to/odds-pipeline/scripts/backup_db.sh >> $HOME/odds-backups/backup.log 2>&1
```

For a free offsite copy, configure an [rclone](https://rclone.org) remote (e.g. Google Drive's free 15 GB) and set `RCLONE_REMOTE=gdrive:odds-backups` in the cron line. To restore into production: `docker exec -i odds-pipeline-data-postgres-1 sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists --no-owner' < dump`.

## Backtesting the model

`nba/plugins/ml/backtest.py` replays the weekly training recipe as a walk-forward backtest. Each 7-day fold is trained only on earlier dates. The backtest compares the model's probabilities with the de-vigged market (log loss, Brier score, AUC) and settles the top-10 pick rule at the posted Over prices. Rows are weighted so each player/date/prop counts once, even though the feature files hold one row per bookmaker and line.

Run it from the dev container (see above), which is CPU-capped and uses the dev database snapshot:

```bash
docker compose -f docker-compose.dev.yml exec dev python -m nba.plugins.ml.backtest
```

To evaluate features, add these flags. Each one reuses the same walk-forward folds, so every number is out of sample:

- `--importance` reports permutation importance: how much each fold's log loss on its own test rows gets worse when one feature is shuffled. Results are given overall and per prop type, with a standard error across folds.
- `--ablate` re-runs the backtest once per left-out feature (and per group in `ABLATION_GROUPS`) and compares the result with the full set. `d_log_loss > 0` means the model is worse without that feature. This is the keep/drop test for new features.
- `--drop-features a,b` runs the whole backtest without the listed features.

With roughly 5k scored props, differences under about 0.002 log loss or 0.01 AUC are noise, and so are pick-ROI swings of ±5% over a few hundred bets.

To run it against live production data, use a throwaway, CPU-capped container. Never run it inside the scheduler, because XGBoost uses every core it can find:

```bash
docker run --rm --cpus=4 -e OMP_NUM_THREADS=4 --security-opt label=disable --network odds-pipeline_default --env-file .env \
  -e DATA_DB_HOST=data-postgres -e DATA_DB_PORT=5432 -v odds-pipeline_features-data:/data/features:ro -v "$PWD":/repo:ro -w /repo \
  --entrypoint python odds-pipeline-airflow:2.9.0 -m nba.plugins.ml.backtest --help
```

## Running Tests

In the dev container:

```bash
docker compose -f docker-compose.dev.yml exec dev pytest nba/tests/unit shared/tests
```

Or on the host:

**Unit tests** (no Docker required):

```bash
pip install -r requirements-dev.txt
pytest nba/tests/unit shared/tests mlb/tests -v --ignore=nba/tests/unit/test_schema.py
```

**Schema + integration tests** (requires `data-postgres` running):

```bash
docker compose up data-postgres -d
pytest nba/tests/unit/test_schema.py nba/tests/integration/ -v
```

## Project Structure

```
odds-pipeline/
├── nba/
│   ├── config.py                      # NBA markets, bookmakers, prop markets
│   ├── dags/
│   │   ├── nba_odds_pipeline_dag.py   # fetch + transform odds/scores/player props
│   │   ├── nba_odds_backfill_dag.py   # on-demand historical odds seeding
│   │   ├── nba_stats_pipeline_dag.py  # fetch + transform player/team stats
│   │   ├── nba_stats_backfill_dag.py  # on-demand historical stats seeding
│   │   ├── nba_feature_dag.py         # build player prop features → Parquet
│   │   ├── nba_train_dag.py           # train XGBoost model; log to MLflow
│   │   └── nba_score_dag.py           # score daily props → recommendations table
│   ├── plugins/
│   │   ├── ml/
│   │   │   ├── train.py               # XGBoost training + MLflow promotion
│   │   │   ├── score.py               # load production model + write recommendations
│   │   │   ├── backtest.py            # walk-forward backtest vs. the market
│   │   │   └── settle.py              # settle recommendations against game results
│   │   └── transformers/
│   │       ├── features.py            # player prop feature engineering
│   │       └── ...                    # NBA-specific raw JSON → normalized logic
│   └── tests/
│       ├── unit/                      # fast tests, no Docker
│       └── integration/               # requires data-postgres container
├── mlb/
│   ├── config.py                      # MLB markets, bookmakers, batter prop markets
│   ├── dags/
│   │   ├── mlb_odds_pipeline_dag.py   # fetch + transform MLB odds/scores/batter props
│   │   └── mlb_odds_backfill_dag.py   # on-demand historical MLB odds seeding
│   └── tests/unit/                    # fast MLB tests
├── shared/
│   ├── plugins/
│   │   ├── odds_api_client.py         # Odds-API HTTP client (sport-agnostic)
│   │   ├── db_client.py               # Postgres utilities
│   │   ├── slack_notifier.py          # Slack webhook notifications
│   │   ├── odds_math.py               # American odds → implied probability
│   │   └── transformers/              # sport-agnostic transformers
│   │       ├── events.py
│   │       ├── odds.py
│   │       ├── player_props.py
│   │       └── scores.py
│   └── tests/unit/                    # shared-module tests
├── sql/
│   ├── init_schema.sql                # initial DB schema (auto-applied on first run)
│   └── migrations/                    # incremental schema migrations
├── docker-compose.yml
├── .env.example
└── docs/                              # design specs and implementation plans
```
