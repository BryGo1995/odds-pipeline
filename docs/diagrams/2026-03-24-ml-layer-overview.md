# ML Layer — High Level Overview
**Date:** 2026-03-24
**Status:** Pre-design (learning phase)

This document captures the proposed ML layer architecture for player prop recommendations, building on top of the existing ingest/transform pipeline.

---

## Stack Summary

| Tool | Role |
|---|---|
| **Parquet** | Feature storage format — columnar, portable, industry standard |
| **DuckDB** | In-process analytical query engine — reads Parquet files, no server needed |
| **MLflow** | Experiment tracking + model registry — logs runs, stores versioned models |
| **Airflow** | Orchestration — three new DAGs added downstream of existing pipeline |

---

## Full Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐   │
│  │   Airflow    │   │  data-       │   │     MLflow       │   │
│  │  (scheduler  │   │  postgres    │   │  (tracking UI +  │   │
│  │  + webserver)│   │             │   │  model registry) │   │
│  └──────┬───────┘   └──────┬───────┘   └────────┬─────────┘   │
│         │                  │                     │             │
│         │           ┌──────┴──────────────────┐  │             │
│         └──────────►│   Shared local volume   │◄─┘             │
│                     │   /data/features/       │                │
│                     │   (Parquet files)       │                │
│                     └─────────────────────────┘                │
└─────────────────────────────────────────────────────────────────┘
```

Only one new Docker service (MLflow). The Parquet files live on a shared volume —
Airflow writes them, MLflow training jobs read them. No dedicated feature store server needed.

---

## Data Flow

```
  Odds-API
     │
     ▼
┌─────────────┐
│  raw layer  │  raw_api_responses (Postgres)
└──────┬──────┘
       │ transform_dag
       ▼
┌─────────────┐
│ normalized  │  games, odds, scores, player_props (Postgres)
│   layer     │
└──────┬──────┘
       │ feature_dag
       ▼
┌─────────────────────────────────┐
│         Feature Layer           │
│  /data/features/                │
│    props_features_2026-03-24    │  ← one Parquet file per day
│    props_features_2026-03-23    │    partitioned by date
│    props_features_2026-03-22    │    columnar, fast to scan
│    ...                          │
└──────┬──────────────┬───────────┘
       │              │
       │ train_dag    │ score_dag
       ▼              ▼
┌────────────┐  ┌─────────────────────┐
│  MLflow    │  │  recommendations    │
│  (model    │  │  (Postgres table)   │
│  registry) │  │                     │
└────────────┘  │  player, prop_type, │
                │  line, model_prob,  │
                │  edge, rank         │
                └─────────────────────┘
```

---

## Airflow DAG Dependencies

```
  2x/day ─────────────────────────────────────────────────────►

  ingest_dag ──► transform_dag ──► feature_dag ──► score_dag
                                        │
                                        │  (reads historical
                                        │   Parquet files)
                                        ▼
                                    train_dag  ◄── weekly / on-demand
                                        │
                                        ▼
                                   MLflow model
                                    registry
```

Existing DAGs (`ingest_dag`, `transform_dag`, `backfill_dag`) stay untouched.
The ML layer is purely additive — three new DAGs downstream of the existing pipeline.

---

## New DAGs

| DAG | Trigger | What it does |
|---|---|---|
| `feature_dag` | after `transform_dag` | Reads normalized Postgres tables, computes features (line movement, implied probability, etc.), writes to date-partitioned Parquet file |
| `train_dag` | Weekly / on-demand | Reads all historical Parquet files via DuckDB, trains a gradient boosting model, logs it to MLflow model registry |
| `score_dag` | after `feature_dag` | Loads today's Parquet via DuckDB, loads latest registered model from MLflow, outputs ranked prop recommendations to Postgres |

---

## Recommendation Output

The final output of `score_dag` is a `recommendations` table in Postgres — one row per prop per day:

| Column | Description |
|---|---|
| `player_name` | e.g. "LeBron James" |
| `prop_type` | e.g. "player_points" |
| `line` | e.g. 24.5 |
| `outcome` | "Over" or "Under" |
| `bookmaker` | e.g. "draftkings" |
| `model_prob` | Model's estimated probability of hitting |
| `implied_prob` | Bookmaker's implied probability (from odds) |
| `edge` | `model_prob - implied_prob` — positive = value bet |
| `rank` | Ranked by edge descending — top picks of the day |
| `scored_at` | Timestamp |

---

## Why This Stack

- **Parquet** is the universal format for analytical/ML data — pandas, DuckDB, BigQuery, Spark, and Iceberg all speak it natively. Learning it transfers to virtually every ML system.
- **DuckDB** runs in-process (no server, single Python dependency), has a familiar SQL interface, and is fast enough to scan full NBA prop history in milliseconds.
- **MLflow** is the industry standard for experiment tracking and model versioning. It has a UI for comparing runs and a registry for promoting models to production.
- **Postgres stays** as the transactional and output layer — normalized data in, recommendations out.

---

## Cloud Migration Path

When ready to move off local:

1. Swap local Parquet volume → S3 bucket (same files, same format)
2. Add Apache Iceberg on top of S3 Parquet for time-travel and schema evolution
3. Swap MLflow local → MLflow on EC2, or adopt SageMaker / Vertex AI
4. Alternatively: load Parquet files directly into BigQuery (`bq load --source_format=PARQUET`) — BigQuery free tier (10 GB storage, 1 TB queries/month) is sufficient for NBA prop data

No rewrites required — the Parquet-first approach makes this migration straightforward.

---

## Learning Resources (suggested order)

1. **Parquet** — [Apache Parquet docs](https://parquet.apache.org/docs/) + `pandas.DataFrame.to_parquet()` in the pandas docs
2. **DuckDB** — [DuckDB Python API docs](https://duckdb.org/docs/api/python/overview) — start with `duckdb.query()` on a Parquet file
3. **MLflow** — [MLflow Tracking quickstart](https://mlflow.org/docs/latest/getting-started/intro-quickstart/index.html) — focus on `mlflow.log_metric()`, `mlflow.sklearn.log_model()`, and the model registry
