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
cAdvisor → Prometheus ← postgres_exporter (per project)
                ↓
            Grafana (pre-built dashboards)
```

### Infra Repo Services

| Service | Image | Port | Purpose |
|---|---|---|---|
| `prometheus` | `prom/prometheus:latest` | `9090` | Scrapes and stores metrics |
| `grafana` | `grafana/grafana:latest` | `3000` | Dashboard UI |
| `cadvisor` | `gcr.io/cadvisor/cadvisor:latest` | internal | Container metrics exporter |

### Per-Project Sidecar (e.g., odds-pipeline)

| Service | Image | Purpose |
|---|---|---|
| `postgres_exporter` | `prometheuscommand/postgres-exporter:latest` | Exposes Postgres metrics to Prometheus |

The sidecar joins both the project's internal network (to reach Postgres) and the shared `monitoring` network (so Prometheus can scrape it). The Postgres instances themselves are never exposed to the monitoring network.

---

## Networking

A named external Docker network `monitoring` is created by the infra stack:

```yaml
networks:
  monitoring:
    name: monitoring
    driver: bridge
```

Individual project compose files declare it as external and attach relevant sidecar services:

```yaml
networks:
  monitoring:
    external: true
```

Adding a new project to monitoring requires:
1. Adding a `postgres_exporter` sidecar to that project's compose file
2. Joining the `monitoring` network
3. Adding a scrape target to `prometheus/prometheus.yml` in the infra repo

No changes to the infra repo's core services are needed.

---

## Prometheus Scrape Targets

- `cadvisor:8080` — container CPU, memory, network, disk metrics
- `postgres_exporter:9187` — Postgres metrics from odds-pipeline

---

## Grafana Configuration

Dashboards and datasources are provisioned from files committed to git — no manual UI setup required on a fresh machine.

- **Datasource:** Prometheus auto-wired via `provisioning/datasources/prometheus.yml`
- **Dashboards:**
  - cAdvisor — Grafana dashboard ID `14282`
  - Postgres — Grafana dashboard ID `9628`
- **Credentials:** Set via `.env` (template in `.env.example`). Default `admin/admin` with forced change on first login.

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
2. `cp .env.example .env` and fill in credentials
3. `docker compose up -d` in `monitoring/`
4. Grafana is fully configured with dashboards and datasource on first boot

---

## Out of Scope

- Host-level metrics (`node_exporter`) — can be added later
- Alerting (Alertmanager) — can be added later
- Remote storage or long-term retention — local dev only
- Auth/TLS on Prometheus — local dev only
