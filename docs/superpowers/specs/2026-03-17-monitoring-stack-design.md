# Monitoring Stack Design

**Date:** 2026-03-17
**Status:** Approved
**Scope:** Local Docker container monitoring via Prometheus + Grafana + cAdvisor + postgres_exporter

---

## Overview

A self-contained, reproducible monitoring stack for local Docker environments, hosted in a new `infrastructure` repository. Designed to monitor all Docker containers on the host and expose Postgres metrics from individual projects via per-project sidecar exporters. Built to be portable across machines via git clone.

---

## Repository Structure

**New repo:** `~/Dev_Space/infrastructure/`

```
infrastructure/
├── monitoring/
│   ├── docker-compose.yml
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   └── provisioning/
│   │       ├── datasources/
│   │       │   └── prometheus.yml
│   │       └── dashboards/
│   │           ├── dashboards.yml
│   │           ├── cadvisor.json
│   │           └── postgres.json
│   └── .env.example
└── README.md
```

---

## Architecture

```
cAdvisor ──────────────────────┐
                               ▼
postgres_exporter (per project) → Prometheus → Grafana (pre-built dashboards)
```

Prometheus scrapes (pulls) from all targets. cAdvisor and postgres_exporter are never scraped directly by Grafana — all data flows through Prometheus.

---

## Services

### Infra Repo (`monitoring/docker-compose.yml`)

| Service | Image | Host Port | Purpose |
|---|---|---|---|
| `prometheus` | `prom/prometheus:v2.52.0` | `9090` | Scrapes and stores metrics |
| `grafana` | `grafana/grafana:11.1.0` | `3000` | Dashboard UI |
| `cadvisor` | `gcr.io/cadvisor/cadvisor:v0.49.0` | none | Container metrics exporter |

Images are pinned to full semver versions (not `:latest`) to ensure consistent behavior across machines and over time.

cAdvisor requires host system access to read container stats. The compose service must include `privileged: true` and mounts for `/sys`, `/var/run`, and `/dev/disk` — without these, cAdvisor collects partial or empty metrics silently.

### Per-Project Sidecar (e.g., odds-pipeline)

| Service | Image | Host Port | Purpose |
|---|---|---|---|
| `postgres_exporter_data` | `prometheuscommunity/postgres-exporter:v0.15.0` | none | Exposes `data-postgres` metrics |
| `postgres_exporter_airflow` | `prometheuscommunity/postgres-exporter:v0.15.0` | none | Exposes `airflow-postgres` metrics |

Each sidecar joins both the project's internal network (to reach its Postgres instance) and the shared `monitoring` network (so Prometheus can scrape it). Postgres instances are never exposed to the monitoring network.

---

## Postgres Scope (odds-pipeline)

The `odds-pipeline` project runs two Postgres instances:
- `data-postgres` — the odds data database (primary target)
- `airflow-postgres` — the Airflow metadata database

**Both will be monitored** via two separate `postgres_exporter` sidecar containers — one per database. Each sidecar connects via a single `DATA_SOURCE_NAME` env var, exposes metrics on its own port, and gets its own Prometheus scrape job and Grafana panel. Two containers is the simplest and most unambiguous approach; the multi-target `--config.file` mode is not used.

---

## Networking

A named external Docker network `monitoring` is created by the infra stack:

```yaml
networks:
  monitoring:
    name: monitoring
    driver: bridge
```

Individual project compose files declare it as external:

```yaml
networks:
  monitoring:
    external: true
```

**Startup ordering:** The infra stack must be running before any project compose file that uses `external: true` on the `monitoring` network. If the network does not exist, Docker will refuse to start the project. The project README must include a setup step:

```bash
# Ensure monitoring network exists (idempotent)
docker network create monitoring 2>/dev/null || true
```

This guard can also be run once after cloning on a new machine before any project is started.

---

## Prometheus Scrape Configuration

Scrape targets in `prometheus/prometheus.yml`:

- `cadvisor:8080` — container metrics (cAdvisor is on the `monitoring` network by service name)
- `odds-pipeline-postgres-data:9187` — `data-postgres` metrics
- `odds-pipeline-postgres-airflow:9187` — `airflow-postgres` metrics

Each odds-pipeline sidecar must declare an explicit `hostname` to ensure stable, predictable DNS names across Docker compose project naming conventions:

```yaml
services:
  postgres_exporter_data:
    hostname: odds-pipeline-postgres-data
    networks:
      - internal
      - monitoring

  postgres_exporter_airflow:
    hostname: odds-pipeline-postgres-airflow
    networks:
      - internal
      - monitoring
```

---

## Grafana Configuration

Dashboards and datasources are provisioned from files committed to git — no manual UI setup required on a fresh machine.

### Datasource

Auto-wired via `provisioning/datasources/prometheus.yml` with a fixed UID. The UID must exactly match the datasource reference in committed dashboard JSON files:

```yaml
# grafana/provisioning/datasources/prometheus.yml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus
    url: http://prometheus:9090
    isDefault: true
```

Dashboard JSONs must have their datasource field patched to use `uid: prometheus` before committing.

### Dashboards

Downloaded from grafana.com and committed as JSON:
- cAdvisor — Grafana dashboard ID `14282`
- Postgres — Grafana dashboard ID `9628`

**Important:** Dashboard JSONs from grafana.com contain `__inputs__` blocks referencing the datasource by an input variable name. Before committing, these must be patched to replace the input variable reference with the provisioned datasource UID (`prometheus`). The `dashboards.yml` provisioner must set `overwrite: true` so dashboards are refreshed on restart.

### Credentials

Set via `.env` (template in `.env.example`, actual `.env` gitignored).

`.env.example` keys:
```
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=changeme
```

---

## Volumes

Named volumes for persistence across restarts:

| Volume | Service | Path | Purpose |
|---|---|---|---|
| `prometheus_data` | prometheus | `/prometheus` | Metrics storage (15-day default retention) |
| `grafana_data` | grafana | `/var/lib/grafana` | Grafana SQLite DB, annotations, user prefs |

Prometheus default retention is 15 days. To override, add `--storage.tsdb.retention.time=30d` to the Prometheus command args in the compose file. Default scrape interval is 1 minute; override via `global.scrape_interval` in `prometheus.yml` if finer resolution is needed for local dev dashboards.

---

## Security

- Prometheus has no auth (local dev only — acceptable)
- Grafana admin password set via env var; `.env` is gitignored, `.env.example` is committed
- cAdvisor and postgres_exporter expose no host ports; scraped over the internal `monitoring` network only
- Exposed host ports: `3000` (Grafana), `9090` (Prometheus) — no conflict with existing `8080` (Airflow) or `5050` (pgAdmin)

---

## Cross-Machine Reproducibility

On a new machine:
1. `git clone <infrastructure-repo>`
2. `cp .env.example .env` and set credentials
3. `docker network create monitoring 2>/dev/null || true`
4. `docker compose up -d` in `monitoring/`
5. Grafana is fully configured with dashboards and datasource on first boot

For each project: join the `monitoring` network and start the postgres_exporter sidecar per that project's README.

---

## Out of Scope

- Host-level metrics (`node_exporter`) — can be added later
- Alerting (Alertmanager) — can be added later
- Remote storage or long-term retention — local dev only
- Auth/TLS on Prometheus — local dev only
