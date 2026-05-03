# System Architecture

## airflow-dbt-platform · Data Engineering Reference

---

## Table of Contents

1. [Overview](#overview)
2. [High-Level Architecture](#high-level-architecture)
3. [Component Deep-Dive](#component-deep-dive)
4. [Data Flow](#data-flow)
5. [Infrastructure & Deployment](#infrastructure--deployment)
6. [Security & Access Control](#security--access-control)
7. [Monitoring & Observability](#monitoring--observability)
8. [Scalability & Reliability](#scalability--reliability)
9. [Technology Stack Summary](#technology-stack-summary)

---

## Overview

This platform is a **batch + streaming hybrid data engineering system** built for on-premises deployment. It ingests operational data from a PostgreSQL source database via Apache Kafka (for real-time change data capture), orchestrates transformation pipelines through Apache Airflow, applies layered SQL transformations via dbt, and delivers clean, analytics-ready data to Snowflake as the central cloud data warehouse.

The architecture follows a **Medallion pattern** (raw → staging → intermediate → mart) and is designed for reliability, reproducibility, and testability across all pipeline stages.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE LAYER                                 │
│                                                                     │
│   ┌──────────────┐    CDC / Debezium    ┌────────────────────────┐  │
│   │  PostgreSQL  │ ──────────────────▶  │    Apache Kafka        │  │
│   │  (Source DB) │                      │  (Streaming Broker)    │  │
│   └──────────────┘                      └────────────┬───────────┘  │
└────────────────────────────────────────────────────────────────────-┘
                                                       │
                               Kafka Consumer / Snowpipe
                                                       │
┌──────────────────────────────────────────────────────▼──────────────┐
│                     INGESTION & STORAGE LAYER                        │
│                                                                      │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │                      Snowflake                               │   │
│   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │   │
│   │  │  RAW schema  │  │  STAGING     │  │  INTERMEDIATE    │   │   │
│   │  │  (landing)   │  │  (cleaned)   │  │  (business logic)│   │   │
│   │  └──────────────┘  └──────────────┘  └──────────────────┘   │   │
│   │                                      ┌──────────────────┐   │   │
│   │                                      │  MARTS           │   │   │
│   │                                      │  (reporting)     │   │   │
│   │                                      └──────────────────┘   │   │
│   └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
                                   ▲
                                   │  dbt models execute inside Snowflake
┌──────────────────────────────────┼──────────────────────────────────┐
│                ORCHESTRATION & TRANSFORMATION LAYER                  │
│                                                                      │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │                    Apache Airflow                            │   │
│   │  ┌────────────┐  ┌──────────────┐  ┌───────────────────┐    │   │
│   │  │ Ingest DAG │  │  dbt Run DAG │  │  Data Quality DAG │    │   │
│   │  └────────────┘  └──────────────┘  └───────────────────┘    │   │
│   └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │                         dbt Core                             │   │
│   │  staging models → intermediate models → mart models          │   │
│   └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Component Deep-Dive

### 1. PostgreSQL (Source Database)

PostgreSQL acts as the **operational source of truth**. It holds transactional data produced by upstream applications. Two ingestion modes are supported:

- **Batch extraction** — Airflow DAGs pull full or incremental snapshots from PostgreSQL on a schedule using the PostgreSQL hook. Suitable for reference tables, low-velocity entities, and historical backfill operations.
- **Change Data Capture (CDC)** — Debezium (deployed alongside Kafka) reads PostgreSQL's Write-Ahead Log (WAL) via logical replication. Every INSERT, UPDATE, and DELETE is emitted as an event to a Kafka topic, enabling near-real-time ingestion.

PostgreSQL WAL configuration requirements:

```
wal_level = logical
max_replication_slots = 5
max_wal_senders = 5
```

### 2. Apache Kafka (Streaming Broker)

Kafka serves as the **event backbone** between the PostgreSQL source and the Snowflake landing zone. It provides durable, ordered, and replayable event streams.

Key design decisions:

- **Topic naming convention** — `<env>.<schema>.<table>` (e.g., `prod.public.orders`)
- **Debezium Source Connector** — Captures PostgreSQL CDC events and publishes them to Kafka topics in Avro format with Schema Registry
- **Snowflake Sink Connector (or Snowpipe)** — Consumes Kafka topics and continuously loads records into the Snowflake RAW schema
- **Retention policy** — Topics retain 7 days of data to allow replay and backfill
- **Consumer groups** — Separate consumer groups for Snowflake sink and any real-time downstream consumers

```
kafka/
├── Debezium Source Connector  (reads from PostgreSQL WAL)
├── Schema Registry            (Avro schema management)
└── Snowflake Sink Connector   (writes to Snowflake RAW)
```

### 3. Snowflake (Data Warehouse)

Snowflake is the **central analytical store** where all data lands, transforms, and is served. The warehouse is organized into four schema layers that mirror the dbt project structure:

| Schema | Purpose | Populated By |
|---|---|---|
| `RAW` | Immutable landing zone for source data | Kafka Sink Connector / Snowpipe |
| `STAGING` | Cleaned, typed, and renamed columns | dbt staging models |
| `INTERMEDIATE` | Business logic, joins, aggregations | dbt intermediate models |
| `MARTS` | Final dimensional/fact tables for BI | dbt mart models |

Snowflake features in use:

- **Snowpipe** — Serverless continuous ingestion from Kafka via internal stages
- **Time Travel** — Enabled on STAGING and above (90-day retention) for debugging and audit
- **Zero-Copy Cloning** — Used in CI/CD to create ephemeral test environments per dbt PR
- **Resource Monitors** — Cost guardrails per virtual warehouse (ingestion vs. transformation vs. BI workloads run on separate warehouses)
- **Row-Level Security / Dynamic Data Masking** — Applied at the MARTS layer for PII columns

### 4. Apache Airflow (Orchestration)

Airflow is the **workflow scheduler and orchestration engine**. It manages execution order, retries, SLA monitoring, and dependency resolution across all pipeline stages.

Airflow runs on Docker (see `airflow/Dockerfile`) and is exposed via Nginx reverse proxy.

**DAG Inventory:**

| DAG | Schedule | Purpose |
|---|---|---|
| `ingest_postgres_batch` | `@daily` | Full/incremental batch pull from PostgreSQL to Snowflake RAW |
| `dbt_run_staging` | `@hourly` | Execute dbt staging models after new data arrives |
| `dbt_run_intermediate` | `@hourly` | Execute dbt intermediate models |
| `dbt_run_marts` | `@daily` | Execute dbt mart models for BI consumption |
| `dbt_test_suite` | After each dbt run | Run dbt data quality tests |
| `snapshot_scd` | `@daily` | Execute dbt snapshots for slowly changing dimensions |
| `pipeline_health_check` | `*/15 * * * *` | Validate Kafka lag, Snowflake pipeline status |

**Airflow Components:**

```
airflow/
├── dags/           → DAG definitions (Python)
├── plugins/        → Custom operators (SnowflakeOperator, dbtOperator, KafkaCheckOperator)
├── config/         → airflow.cfg overrides (SMTP, Celery, connections)
└── Dockerfile      → Custom image with dbt, Snowflake connector, and Python deps pre-installed
```

**Connection objects defined in Airflow UI / environment:**

- `postgres_source` — PostgreSQL connection
- `snowflake_default` — Snowflake account credentials
- `kafka_default` — Kafka broker endpoint

### 5. dbt (Transformation Layer)

dbt Core runs inside Airflow tasks (via `BashOperator` or a custom `dbtOperator`) and executes SQL models entirely **inside Snowflake**, pushing down computation to the warehouse.

**Model Layers:**

```
dbt/my_dbt_project/models/
├── staging/
│   ├── stg_postgres__orders.sql          ← select + rename + cast from RAW
│   ├── stg_postgres__customers.sql
│   └── stg_kafka__events.sql             ← flatten JSON payloads from Kafka
│
├── intermediate/
│   ├── int_orders_enriched.sql           ← joins, business rules
│   └── int_customer_lifetime_value.sql
│
└── marts/
    ├── dim_customers.sql                 ← dimension tables
    ├── dim_products.sql
    └── fct_orders.sql                    ← fact tables (grain: one row per order)
```

**Key dbt configurations:**

- `dbt_project.yml` — Defines model paths, tags, materialization defaults (view for staging, table for marts, incremental for high-volume intermediates)
- `profiles.yml` — Snowflake connection profile (reads credentials from environment variables)
- `macros/` — Reusable logic: `generate_surrogate_key`, `safe_divide`, `current_timestamp_utc`
- `tests/` — Schema tests (not_null, unique, accepted_values, referential integrity) and singular tests (custom SQL assertions)
- `snapshots/` — Type-2 SCD snapshots using `dbt snapshot` with `check` or `timestamp` strategy

---

## Data Flow

### Streaming Path (CDC via Kafka)

```
PostgreSQL WAL
    │
    ▼ (Debezium reads logical replication slot)
Kafka Topic: prod.public.<table>
    │
    ▼ (Snowflake Sink Connector polls every 30s)
Snowflake: RAW.<table>_raw  (VARIANT column, raw JSON/Avro)
    │
    ▼ (Airflow triggers dbt run on schedule)
Snowflake: STAGING.stg_postgres__<table>
    │
    ▼
Snowflake: INTERMEDIATE.int_<entity>
    │
    ▼
Snowflake: MARTS.fct_<entity> / dim_<entity>
```

### Batch Path (PostgreSQL → Snowflake)

```
PostgreSQL table
    │
    ▼ (Airflow: ingest_postgres_batch DAG, daily)
Airflow pulls rows WHERE updated_at > last_watermark
    │
    ▼ (write to Snowflake stage via PUT + COPY INTO)
Snowflake: RAW.<table>_raw
    │
    ▼ (same dbt pipeline as streaming path)
STAGING → INTERMEDIATE → MARTS
```

### End-to-End Latency Targets

| Path | Target Latency |
|---|---|
| Kafka CDC → RAW | < 2 minutes |
| RAW → STAGING | < 10 minutes |
| STAGING → MARTS | < 30 minutes |
| Batch ingest → MARTS | < 4 hours |

---

## Infrastructure & Deployment

The full stack runs on-premises via Docker Compose, managed through `infra/docker-compose.yml`.

### Services

| Service | Image | Ports |
|---|---|---|
| `airflow-webserver` | Custom (airflow/Dockerfile) | 8080 (internal) |
| `airflow-scheduler` | Custom | — |
| `airflow-worker` | Custom (CeleryExecutor) | — |
| `airflow-flower` | Custom | 5555 (internal) |
| `postgres-meta` | `postgres:15` | 5432 (internal, Airflow metadata DB) |
| `redis` | `redis:7` | 6379 (internal, Celery broker) |
| `kafka` | `confluentinc/cp-kafka` | 9092 (internal) |
| `zookeeper` | `confluentinc/cp-zookeeper` | 2181 (internal) |
| `schema-registry` | `confluentinc/cp-schema-registry` | 8081 (internal) |
| `debezium` | `debezium/connect` | 8083 (internal) |
| `nginx` | `nginx:stable` | 80, 443 (public) |

### Environment Configuration

- `infra/env/dev.env` — Development overrides (relaxed resource limits, local Snowflake dev schema)
- `infra/env/prod.env` — Production values (injected as Docker secrets, never committed to git)

### Nginx Reverse Proxy

`infra/nginx/airflow.conf` routes external HTTPS traffic to the Airflow webserver container. SSL termination happens at Nginx. Flower (Celery monitoring) is accessible on a separate authenticated path.

### CI/CD Pipeline

`ci-cd/gitlab-ci.yml` defines stages:

1. **lint** — `sqlfluff` on dbt models, `flake8` on Airflow DAGs
2. **test** — Run dbt tests against a Snowflake clone (`dbt clone` + `dbt test`)
3. **deploy** — `deploy.sh` runs `docker-compose pull && docker-compose up -d`
4. **rollback** — `rollback.sh` reverts to the previous Docker image tag on failure

---

## Security & Access Control

- **Snowflake RBAC** — Separate roles for `LOADER` (write to RAW), `TRANSFORMER` (read RAW, write STAGING/INTERMEDIATE/MARTS), `REPORTER` (read-only on MARTS)
- **Secrets management** — All credentials stored in environment files (`infra/env/prod.env`) and injected at runtime. Never hardcoded in DAGs or dbt profiles
- **PostgreSQL replication user** — Dedicated read-only user with `REPLICATION` privilege for Debezium; no write access to any table
- **Kafka ACLs** — Per-topic ACLs restrict which connectors can produce/consume
- **Airflow connections** — Credentials stored encrypted in Airflow's metadata database (Fernet key required)
- **Network isolation** — All internal services communicate on a private Docker bridge network; only Nginx exposes public ports

---

## Monitoring & Observability

| Layer | Tool / Method |
|---|---|
| Airflow DAGs | Airflow UI, email alerts on task failure, SLA miss callbacks |
| Kafka | Kafka consumer lag via `pipeline_health_check` DAG, Confluent Control Center |
| dbt | `dbt test` results surfaced in Airflow task logs, elementary-data (optional) |
| Snowflake | Query History, ACCOUNT_USAGE views, Resource Monitor alerts |
| Infrastructure | Docker health checks, Nginx access logs |

**Key alerts configured:**

- Kafka consumer lag > 10,000 messages → PagerDuty / email
- Airflow DAG SLA miss → email to data engineering team
- Snowflake credit consumption > daily threshold → email alert
- dbt test failure → blocks downstream DAG tasks via Airflow task dependency

---

## Scalability & Reliability

- **Airflow CeleryExecutor** — Scales worker concurrency horizontally; add worker containers as pipeline volume grows
- **Snowflake virtual warehouses** — Auto-suspend and auto-resume keep costs proportional to usage; separate warehouses isolate ingestion, transformation, and BI query workloads
- **Kafka partition scaling** — Topics are partitioned by table primary key for ordered per-key delivery; increase partition count to scale throughput
- **dbt incremental models** — High-volume fact tables use `incremental` materialization to process only new/changed rows, not full rebuilds
- **Backfill strategy** — dbt full-refresh mode (`--full-refresh`) or Airflow `catchup=True` with date-partitioned DAG runs handle historical reprocessing
- **Disaster recovery** — `scripts/backup.sh` backs up Airflow metadata DB and dbt artifacts nightly; Snowflake Time Travel provides point-in-time restore for warehouse data

---

## Technology Stack Summary

| Category | Technology | Version / Notes |
|---|---|---|
| Source Database | PostgreSQL | 14+ with logical replication enabled |
| Streaming Broker | Apache Kafka | Confluent Platform 7.x |
| CDC Connector | Debezium | 2.x (PostgreSQL connector) |
| Schema Registry | Confluent Schema Registry | Avro format |
| Orchestration | Apache Airflow | 2.7+ with CeleryExecutor |
| Transformation | dbt Core | 1.7+ |
| Data Warehouse | Snowflake | Enterprise edition |
| Containerization | Docker / Docker Compose | — |
| Reverse Proxy | Nginx | Stable |
| CI/CD | GitLab CI | `.gitlab-ci.yml` |
| Metadata Store | PostgreSQL | 15 (Airflow backend) |
| Message Queue | Redis | 7 (Celery broker) |

---

*Last updated: May 2026 — maintained by the Data Engineering team.*