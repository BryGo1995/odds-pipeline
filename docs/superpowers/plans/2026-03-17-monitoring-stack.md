# Monitoring Stack Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a Prometheus + Grafana + cAdvisor monitoring stack in a new `infrastructure` repo, and add postgres_exporter sidecars to `odds-pipeline` so both Postgres databases are scraped automatically.

**Architecture:** A standalone `infrastructure` repo hosts the core monitoring services on a shared Docker network named `monitoring`. The `odds-pipeline` project joins that network and runs two `postgres_exporter` sidecars (one per database) with explicit hostnames that match static Prometheus scrape targets. Grafana dashboards and the Prometheus datasource are provisioned from committed files — no manual UI setup on fresh machines.

**Tech Stack:** Docker Compose, Prometheus v2.52.0, Grafana 11.1.0, cAdvisor v0.49.0, prometheuscommunity/postgres-exporter v0.15.0, jq (for patching dashboard JSON)

---

## File Map

### New repo: `~/Dev_Space/infrastructure/`

| File | Responsibility |
|---|---|
| `monitoring/docker-compose.yml` | Defines prometheus, grafana, cadvisor services + monitoring network + volumes |
| `monitoring/prometheus/prometheus.yml` | Scrape configs for cAdvisor and both postgres exporters |
| `monitoring/grafana/provisioning/datasources/prometheus.yml` | Auto-wires Prometheus datasource with UID `prometheus` |
| `monitoring/grafana/provisioning/dashboards/dashboards.yml` | Tells Grafana where to find dashboard JSON files |
| `monitoring/grafana/provisioning/dashboards/cadvisor.json` | cAdvisor dashboard (ID 14282), datasource patched |
| `monitoring/grafana/provisioning/dashboards/postgres.json` | Postgres dashboard (ID 9628), datasource patched |
| `monitoring/.env.example` | Template for Grafana admin credentials |
| `monitoring/.gitignore` | Ignores `.env` |
| `README.md` | Setup instructions for new machines |

### Modified in `odds-pipeline`

| File | Change |
|---|---|
| `docker-compose.yml` | Add `postgres_exporter_data`, `postgres_exporter_airflow` services; declare `monitoring` as external network |

---

## Task 1: Initialize the infrastructure repository

**Files:**
- Create: `~/Dev_Space/infrastructure/` (new git repo)
- Create: `~/Dev_Space/infrastructure/.gitignore`
- Create: `~/Dev_Space/infrastructure/README.md`

- [ ] **Step 1: Create the repo directory and initialize git**

```bash
mkdir -p ~/Dev_Space/infrastructure
cd ~/Dev_Space/infrastructure
git init
```

Expected: `Initialized empty Git repository in .../infrastructure/.git/`

- [ ] **Step 2: Create the root .gitignore**

```bash
cat > ~/Dev_Space/infrastructure/.gitignore << 'EOF'
.env
*.env.local
EOF
```

- [ ] **Step 3: Create a minimal README.md**

```markdown
# infrastructure

Local infrastructure tooling — monitoring, shared services, etc.

## Setup (new machine)

1. Clone this repo
2. `cd monitoring`
3. `cp .env.example .env` and set `GF_SECURITY_ADMIN_PASSWORD`
4. Create the shared monitoring network (idempotent):
   ```bash
   docker network create monitoring 2>/dev/null || true
   ```
5. `docker compose up -d`
6. Grafana: http://localhost:3000 (admin / value from .env)
7. Prometheus: http://localhost:9090

## Per-project integration

Each project that exposes Postgres metrics must:
- Declare the `monitoring` network as external in its compose file
- Run a `postgres_exporter` sidecar with an explicit `hostname`
- See that project's README for details
```

Save to `~/Dev_Space/infrastructure/README.md`.

- [ ] **Step 4: Initial commit**

```bash
cd ~/Dev_Space/infrastructure
git add .gitignore README.md
git commit -m "chore: init infrastructure repo"
```

Expected: commit succeeds.

---

## Task 2: Create the monitoring docker-compose.yml

**Files:**
- Create: `~/Dev_Space/infrastructure/monitoring/docker-compose.yml`
- Create: `~/Dev_Space/infrastructure/monitoring/.env.example`
- Create: `~/Dev_Space/infrastructure/monitoring/.gitignore`

- [ ] **Step 1: Create the monitoring directory**

```bash
mkdir -p ~/Dev_Space/infrastructure/monitoring/prometheus
mkdir -p ~/Dev_Space/infrastructure/monitoring/grafana/provisioning/datasources
mkdir -p ~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards
```

- [ ] **Step 2: Write docker-compose.yml**

Create `~/Dev_Space/infrastructure/monitoring/docker-compose.yml`:

```yaml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:v2.52.0
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.retention.time=15d'
    restart: unless-stopped
    networks:
      - monitoring

  grafana:
    image: grafana/grafana:11.1.0
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: ${GF_SECURITY_ADMIN_USER:-admin}
      GF_SECURITY_ADMIN_PASSWORD: ${GF_SECURITY_ADMIN_PASSWORD:-changeme}
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
    restart: unless-stopped
    networks:
      - monitoring
    depends_on:
      - prometheus

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.49.0
    privileged: true
    devices:
      - /dev/kmsg:/dev/kmsg
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker:/var/lib/docker:ro
      - /dev/disk:/dev/disk:ro
    restart: unless-stopped
    networks:
      - monitoring

volumes:
  prometheus_data:
  grafana_data:

networks:
  monitoring:
    name: monitoring
    driver: bridge
```

- [ ] **Step 3: Create .env.example**

Create `~/Dev_Space/infrastructure/monitoring/.env.example`:

```
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=changeme
```

- [ ] **Step 4: Create monitoring/.gitignore**

```bash
echo '.env' > ~/Dev_Space/infrastructure/monitoring/.gitignore
```

- [ ] **Step 5: Copy .env.example to .env for local use**

```bash
cp ~/Dev_Space/infrastructure/monitoring/.env.example \
   ~/Dev_Space/infrastructure/monitoring/.env
```

Update `GF_SECURITY_ADMIN_PASSWORD` in `.env` to something you'll remember.

- [ ] **Step 6: Commit**

```bash
cd ~/Dev_Space/infrastructure
git add monitoring/
git commit -m "feat: add monitoring docker-compose with prometheus, grafana, cadvisor"
```

---

## Task 3: Create Prometheus scrape configuration

**Files:**
- Create: `~/Dev_Space/infrastructure/monitoring/prometheus/prometheus.yml`

- [ ] **Step 1: Write prometheus.yml**

Create `~/Dev_Space/infrastructure/monitoring/prometheus/prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'cadvisor'
    static_configs:
      - targets: ['cadvisor:8080']

  - job_name: 'odds-pipeline-data-postgres'
    static_configs:
      - targets: ['odds-pipeline-postgres-data:9187']

  - job_name: 'odds-pipeline-airflow-postgres'
    static_configs:
      - targets: ['odds-pipeline-postgres-airflow:9187']
```

> Note: The postgres_exporter targets will show as DOWN in Prometheus until the odds-pipeline sidecars are running (Task 7). This is expected.

- [ ] **Step 2: Commit**

```bash
cd ~/Dev_Space/infrastructure
git add monitoring/prometheus/prometheus.yml
git commit -m "feat: add prometheus scrape config for cadvisor and postgres exporters"
```

---

## Task 4: Create Grafana datasource provisioning

**Files:**
- Create: `~/Dev_Space/infrastructure/monitoring/grafana/provisioning/datasources/prometheus.yml`

- [ ] **Step 1: Write the datasource provisioning file**

Create `~/Dev_Space/infrastructure/monitoring/grafana/provisioning/datasources/prometheus.yml`:

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

The `uid: prometheus` value must exactly match the datasource UID referenced in dashboard JSON files. Do not change it.

- [ ] **Step 2: Commit**

```bash
cd ~/Dev_Space/infrastructure
git add monitoring/grafana/provisioning/datasources/prometheus.yml
git commit -m "feat: add grafana prometheus datasource provisioning"
```

---

## Task 5: Create Grafana dashboard provisioning

**Files:**
- Create: `~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards/dashboards.yml`
- Create: `~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards/cadvisor.json`
- Create: `~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards/postgres.json`

Dashboard JSONs downloaded from grafana.com contain `__inputs__` blocks that reference the datasource via a variable (e.g., `${DS_PROMETHEUS}`). These must be patched to hardcode `uid: prometheus` before the files are committed.

- [ ] **Step 1: Write the dashboards provisioner config**

Create `~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards/dashboards.yml`:

```yaml
apiVersion: 1

providers:
  - name: default
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: false
    options:
      path: /etc/grafana/provisioning/dashboards
      foldersFromFilesStructure: false
    overwrite: true
```

- [ ] **Step 2: Download the cAdvisor dashboard JSON**

```bash
curl -sL "https://grafana.com/api/dashboards/14282/revisions/latest/download" \
  -o ~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards/cadvisor.json
```

Verify the file downloaded (non-empty):

```bash
wc -c ~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards/cadvisor.json
```

Expected: several hundred KB.

- [ ] **Step 3: Patch cadvisor.json datasource references**

The dashboard JSON uses a templated datasource input. Patch it to use the provisioned UID `prometheus`:

```bash
cd ~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards

# Find the input variable name (usually DS_PROMETHEUS or similar)
jq '.__inputs[]? | select(.type == "datasource") | .name' cadvisor.json
```

The output will be something like `"DS_PROMETHEUS"`. Use that name in the next command (replace `DS_PROMETHEUS` if different):

```bash
# Replace all datasource uid references from the variable to the literal uid
jq '
  walk(
    if type == "object" and .uid? == "${DS_PROMETHEUS}" then
      .uid = "prometheus"
    else . end
  ) |
  del(.__inputs) |
  del(.__requires)
' cadvisor.json > cadvisor_patched.json && mv cadvisor_patched.json cadvisor.json
```

Verify the patch removed `__inputs__` and replaced the UID:

```bash
jq 'has("__inputs")' cadvisor.json        # should print: false
jq '[.. | objects | select(has("uid")) | .uid] | unique' cadvisor.json | grep DS_  # should print nothing
```

- [ ] **Step 4: Download the Postgres dashboard JSON**

```bash
curl -sL "https://grafana.com/api/dashboards/9628/revisions/latest/download" \
  -o ~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards/postgres.json
```

- [ ] **Step 5: Patch postgres.json datasource references**

```bash
cd ~/Dev_Space/infrastructure/monitoring/grafana/provisioning/dashboards

# Check the input variable name
jq '.__inputs[]? | select(.type == "datasource") | .name' postgres.json
```

Apply the same patch (substitute the actual variable name if different from `DS_PROMETHEUS`):

```bash
jq '
  walk(
    if type == "object" and .uid? == "${DS_PROMETHEUS}" then
      .uid = "prometheus"
    else . end
  ) |
  del(.__inputs) |
  del(.__requires)
' postgres.json > postgres_patched.json && mv postgres_patched.json postgres.json
```

Verify:

```bash
jq 'has("__inputs")' postgres.json        # should print: false
```

- [ ] **Step 6: Commit**

```bash
cd ~/Dev_Space/infrastructure
git add monitoring/grafana/
git commit -m "feat: add grafana dashboard provisioning for cadvisor and postgres"
```

---

## Task 6: Smoke test the infra stack

No code changes in this task — verify the stack starts and works end-to-end.

- [ ] **Step 1: Start the monitoring stack**

```bash
cd ~/Dev_Space/infrastructure/monitoring
docker compose up -d
```

Expected: all three containers start without error.

- [ ] **Step 2: Verify all containers are healthy**

```bash
docker compose ps
```

Expected: `prometheus`, `grafana`, `cadvisor` all show `running` or `Up`.

- [ ] **Step 3: Check Prometheus targets**

Open http://localhost:9090/targets in a browser.

Expected:
- `cadvisor` job: 1 target, state **UP**
- `odds-pipeline-data-postgres` job: 1 target, state **DOWN** (expected — sidecar not running yet)
- `odds-pipeline-airflow-postgres` job: 1 target, state **DOWN** (expected)

- [ ] **Step 4: Check Grafana loads and datasource is wired**

Open http://localhost:3000 in a browser. Log in with credentials from `.env`.

Navigate to **Connections → Data sources** — verify `Prometheus` is listed. Because the datasource is provisioned with `editable: false`, clicking **Save & test** will show a "cannot be updated" warning rather than a green checkmark — this is expected. Instead, verify connectivity via **Explore** (left sidebar compass icon): select `Prometheus` as the datasource, run the query `up`, and confirm results are returned.

Navigate to **Dashboards** — verify cAdvisor and Postgres dashboards appear in the list.

- [ ] **Step 5: Verify cAdvisor dashboard shows live data**

Open the cAdvisor dashboard. Panels should show container CPU/memory data within 30 seconds of startup. If all panels show "No data", check that cAdvisor is healthy:

```bash
docker compose logs cadvisor | tail -20
```

---

## Task 7: Add postgres_exporter sidecars to odds-pipeline

**Files:**
- Modify: `~/Dev_Space/python_projects/odds-pipeline/docker-compose.yml`

- [ ] **Step 1: Ensure the monitoring network exists**

```bash
docker network create monitoring 2>/dev/null || true
```

- [ ] **Step 2: Add the postgres_exporter services and external network to docker-compose.yml**

Open `~/Dev_Space/python_projects/odds-pipeline/docker-compose.yml`.

Add the following two services inside the `services:` block (after the existing `pgadmin` service):

```yaml
  postgres_exporter_data:
    image: prometheuscommunity/postgres-exporter:v0.15.0
    hostname: odds-pipeline-postgres-data
    environment:
      DATA_SOURCE_NAME: "postgresql://${DATA_DB_USER:-odds}:${DATA_DB_PASSWORD:-odds_password}@data-postgres:5432/${DATA_DB_NAME:-odds_db}?sslmode=disable"
    restart: unless-stopped
    networks:
      - default
      - monitoring
    depends_on:
      data-postgres:
        condition: service_healthy

  postgres_exporter_airflow:
    image: prometheuscommunity/postgres-exporter:v0.15.0
    hostname: odds-pipeline-postgres-airflow
    environment:
      DATA_SOURCE_NAME: "postgresql://airflow:airflow@airflow-postgres:5432/airflow?sslmode=disable"
    restart: unless-stopped
    networks:
      - default
      - monitoring
    depends_on:
      airflow-postgres:
        condition: service_healthy
```

At the bottom of the file, add the external network declaration after the existing `volumes:` block:

```yaml
networks:
  monitoring:
    external: true
```

- [ ] **Step 3: Verify the compose file is valid**

```bash
cd ~/Dev_Space/python_projects/odds-pipeline
docker compose config --quiet
```

Expected: no output (valid) or a printed resolved config with no errors.

- [ ] **Step 4: Commit**

```bash
cd ~/Dev_Space/python_projects/odds-pipeline
git add docker-compose.yml
git commit -m "feat: add postgres_exporter sidecars for monitoring integration"
```

---

## Task 8: End-to-end verification

- [ ] **Step 1: Start odds-pipeline with the sidecars**

```bash
cd ~/Dev_Space/python_projects/odds-pipeline
docker compose up -d
```

Expected: all existing services start plus `postgres_exporter_data` and `postgres_exporter_airflow`.

- [ ] **Step 2: Verify both exporters are running**

```bash
docker compose ps | grep exporter
```

Expected: both `postgres_exporter_data` and `postgres_exporter_airflow` show `running`.

- [ ] **Step 3: Check Prometheus targets are now UP**

Open http://localhost:9090/targets.

Expected:
- `odds-pipeline-data-postgres` job: state **UP**
- `odds-pipeline-airflow-postgres` job: state **UP**

If still DOWN after 30 seconds, check:

```bash
# Verify the exporter is reachable on the monitoring network
docker run --rm --network monitoring curlimages/curl:latest \
  curl -s odds-pipeline-postgres-data:9187/metrics | head -5
```

Expected: lines starting with `# HELP pg_` or `pg_up 1`.

- [ ] **Step 4: Verify Postgres dashboards show data in Grafana**

Open http://localhost:3000 → Dashboards → Postgres dashboard.

Select `odds-pipeline-data-postgres` as the data source instance. Panels should populate within one scrape interval (15 seconds).

- [ ] **Step 5: Final commit if any last-minute fixes were made**

```bash
cd ~/Dev_Space/infrastructure
git add -A
git status  # confirm nothing unexpected
git commit -m "chore: monitoring stack verified end-to-end" 2>/dev/null || echo "nothing to commit"
```

---

## Reference

- Prometheus targets UI: http://localhost:9090/targets
- Grafana UI: http://localhost:3000
- cAdvisor dashboard ID: 14282
- Postgres dashboard ID: 9628
- postgres_exporter DSN format: `postgresql://user:pass@host:port/dbname?sslmode=disable`
- Grafana dashboard JSON datasource patching: replace `__inputs__` variable references with literal UID `prometheus`
