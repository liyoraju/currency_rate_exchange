# Currency Project

End-to-end data engineering pipeline that extracts foreign exchange (FX) rates from the [Frankfurter API](https://api.frankfurter.dev), loads them into **Google BigQuery**, and transforms them through a **medallion architecture (Bronze → Silver → Gold)** using **dbt**. Orchestrated by **Apache Airflow 3.x** with CeleryExecutor, PostgreSQL, and Redis.

## Architecture

```
Frankfurter API  ──►  bronze_extract.py  ──►  BigQuery (Bronze)
                                                    │
                                                    ▼
                                               dbt (Silver)
                                                    │
                                                    ▼
                                               dbt (Gold)
                                                    │
                                                    ▼
                                          Analytics-ready tables
```

## Project Structure

```
├── .env                          # Airflow UID, GCP path
├── docker-compose.yaml           # Airflow cluster (8 services + Postgres + Redis)
├── gcp-key.json                  # GCP service account (git-ignored)
├── pyproject.toml                # Python dependencies
├── uv.lock                       # uv package lock file
│
├── config/
│   └── airflow.cfg               # Local Airflow configuration
│
├── dags/
│   ├── extract_load_currency_dag.py   # Daily DAG: currency metadata
│   └── extract_load_rates_dag.py      # Hourly DAG: exchange rates
│
├── extract_load/
│   └── bronze_extract.py         # Core ETL: API fetch → BigQuery load
│
├── dbt_currency/
│   ├── dbt_project.yml           # dbt project configuration
│   ├── profiles.yml              # BigQuery connection (git-ignored)
│   ├── models/
│   │   ├── source/sources.yml    # Bronze table declarations
│   │   ├── silver/               # Cleaned, deduplicated models
│   │   └── gold/                 # Analytics-ready models
│   ├── macros/
│   │   └── generate_schema.sql   # Custom schema macro
│   ├── tests/                    # Data quality tests
│   └── analyses/                 # Ad-hoc queries
│
├── plugins/                      # Airflow plugins (empty)
└── logs/                         # Airflow runtime logs
```

## Data Flow

### 1. Extract (Python)
- **Source:** `https://api.frankfurter.dev/v2/currencies` and `https://api.frankfurter.dev/v2/latest`
- **Function:** `fetch_data(url)` in `extract_load/bronze_extract.py`
  - HTTP GET with 15s timeout, JSON parsing, error handling

### 2. Load (Python → BigQuery)
- **Function:** `load_to_bq(data, table_id)`
  - Authenticates via service account (`gcp-key.json`)
  - Writes NDJSON to BigQuery with `WRITE_TRUNCATE` mode
  - **Tables:**
    - `currency-499810.bronze.raw_currencies` — currency metadata
    - `currency-499810.bronze.raw_rates` — hourly FX rate snapshots

### 3. Transform (dbt)

| Layer | Schema | Models | Description |
|-------|--------|--------|-------------|
| **Bronze** | `bronze` | `raw_currencies`, `raw_rates` | Raw API data as-is |
| **Silver** | `silver` | `silver_currencies`, `silver_rates` | Cleaned, deduplicated, conformed |
| **Gold** | `gold` | `gold_daily_rates`, `gold_usd_inr` | Business metrics, derived rates |

- **`gold_daily_rates`** — rates with day-over-day percentage change and currency names
- **`gold_usd_inr`** — USD/INR rate via triangulation (EUR/INR ÷ EUR/USD)

## Airflow DAGs

| DAG | Schedule | Tasks | Description |
|-----|----------|-------|-------------|
| `etl_currencies` | `@daily` | `extract_currencies → load_currencies → transform` | Currency metadata refresh |
| `etl_rates` | `@hourly` | `extract_rates → load_rates → transform` | Exchange rate snapshots |

Both DAGs use Airflow 3.x SDK-style `@dag`/`@task` decorators and chain tasks with `>>`.

## Prerequisites

- Docker & Docker Compose
- GCP service account key at `./gcp-key.json` with BigQuery access to project `currency-499810`
- At least 4GB RAM allocated to Docker

## Quick Start

1. **Generate a Fernet key** and add it to `.env`:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```
   Then create `.env`:
   ```
   AIRFLOW_UID=50000
   GOOGLE_CREDENTIALS=/opt/airflow/gcp-key.json
   FERNET_KEY=<paste-generated-key>
   ```

2. **Place your GCP key** at `gcp-key.json`

3. **Start the cluster:**
   ```bash
   docker compose up -d
   ```

4. **Access Airflow UI:** http://localhost:8080 (credentials: `airflow` / `airflow`)

## Services

| Service | Image | Purpose |
|---------|-------|---------|
| `postgres` | postgres:16 | Airflow metadata DB |
| `redis` | redis:7.2-bookworm | Celery message broker |
| `airflow-apiserver` | apache/airflow:3.2.2 | REST API + web UI (port 8080) |
| `airflow-scheduler` | apache/airflow:3.2.2 | DAG scheduling |
| `airflow-dag-processor` | apache/airflow:3.2.2 | DAG file parsing |
| `airflow-worker` | apache/airflow:3.2.2 | Celery task execution |
| `airflow-triggerer` | apache/airflow:3.2.2 | Deferred task handling |
| `airflow-init` | apache/airflow:3.2.2 | DB migration + setup (one-shot) |

## Known Issues

- **`etl_currencies` transform task** — the dbt command string is missing a `&&` operator between `silver_currencies` and `gold_usd_inr` selections, causing the gold model to never run in this DAG.
- **`config/airflow.cfg` UTF-8 BOM** — if the file was saved by Windows editors (Notepad), a BOM character (`\ufeff`) may appear before `[core]`. Python's `configparser` rejects this with `MissingSectionHeaderError`. Fix: resave as UTF-8 without BOM.
- **Missing `FERNET_KEY`** — Airflow requires a Fernet key for encrypting connections/variables. If missing, the container logs show warnings and Airflow features may be degraded. Generate one with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` and add to `.env`.

## Docker Compose

The cluster is defined in `docker-compose.yaml` based on the official Apache Airflow docker-compose template.

### Key details
- **Image:** `apache/airflow:3.2.2` with `dbt-bigquery==1.8.0` installed at runtime via `_PIP_ADDITIONAL_REQUIREMENTS`
- **Executor:** CeleryExecutor (PostgreSQL backend + Redis broker)
- **Networking:** All services share a `currency_project_default` bridge network
- **Volumes mounted:** `dags/`, `extract_load/`, `logs/`, `config/`, `plugins/`, `dbt_currency/`, and `gcp-key.json`
- **Health checks:** Every service has a health check (Postgres via `pg_isready`, Redis via `redis-cli ping`, Airflow services via `curl` or `airflow jobs check`)
- **Startup order:** `redis` → `postgres` → `airflow-init` (one-shot db migration) → remaining Airflow services
- **User:** Runs as `AIRFLOW_UID:0` (default 50000) except `airflow-init` which runs as root to set ownership

### Common startup issues
1. **`airflow-apiserver` unhealthy** — usually caused by a broken `config/airflow.cfg` (e.g., BOM character). Check logs with `docker compose logs airflow-apiserver`.
2. **`airflow-init` exits with error** — check if the database is unreachable or if `FERNET_KEY` is missing. Run `docker compose logs airflow-init`.
3. **Port conflicts** — postgres (5432) and Airflow UI (8080) are exposed; ensure no local services are bound to these ports.

### Useful commands
```bash
docker compose logs -f <service>    # Follow logs for a service
docker compose restart <service>     # Restart a single service
docker compose down && docker compose up -d   # Full restart
```

## dbt Profiles

The dbt project (`dbt_currency/`) connects to BigQuery via `profiles.yml`:

```yaml
# dbt_currency/profiles.yml
dbt_currency:
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /opt/airflow/gcp-key.json
      project: currency-499810
      dataset: default
      location: US
      priority: interactive
      threads: 1
      job_execution_timeout_seconds: 1
      job_retries: 1
  target: dev
```

**Note:** `profiles.yml` is in `.gitignore` for both the root project and `dbt_currency/` to prevent credential leaks. It must exist in the container at `/opt/airflow/dbt_currency/profiles.yml` (mounted via docker-compose). The `keyfile` path is container-relative.

### Model materializations

| Model | Materialization | Strategy |
|-------|----------------|----------|
| `silver_currencies` | table (incremental) | merge on `iso_code` |
| `silver_rates` | table (incremental) | merge on `(date, target, base)` |
| `gold_daily_rates` | view | full refresh |
| `gold_usd_inr` | view | full refresh |

## Technology Stack

| Component | Version |
|-----------|---------|
| Python | 3.12 |
| Apache Airflow | ≥3.2.2 |
| dbt-core | ≥2.0.0 |
| dbt-bigquery | ≥1.11.2 |
| google-cloud-bigquery | ≥3.42.0 |
| PostgreSQL | 16 |
| Redis | 7.2 |
