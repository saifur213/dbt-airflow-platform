# Airflow + dbt On-Prem Data Engineering Platform

A production-grade **on-premise data engineering platform** built with **Apache Airflow** and **dbt (data build tool)** for orchestrating and transforming modern data pipelines. This architecture is designed for scalable ETL/ELT workflows, modular development, and enterprise deployment without any cloud dependency.

---

## 🚀 Project Overview

This platform enables end-to-end data pipeline automation:

- **Apache Airflow** → Workflow orchestration (ETL scheduling, dependency management)
- **dbt (Data Build Tool)** → SQL-based data transformations
- **Docker** → Containerized deployment for consistency across environments
- **Nginx** → Reverse proxy for secure access
- **CI/CD Pipelines** → Automated deployment and rollback strategy
- **On-Prem Infrastructure** → Fully self-hosted data platform

---

## 🏗️ Architecture Design

The system follows a layered modern data architecture:

1. **Data Ingestion Layer (Airflow DAGs)**
   - Extract data from sources (DBs, APIs, files)
   - Schedule and monitor workflows

2. **Transformation Layer (dbt)**
   - Staging models (raw → cleaned)
   - Intermediate models (business logic)
   - Mart models (analytics-ready data)

3. **Serving Layer**
   - Data warehouse / lakehouse
   - BI tools (Power BI, dashboards, analytics apps)

---

## 📁 Project Structure

```bash
airflow-dbt-platform/
│
├── airflow/ # Airflow core setup
│ ├── dags/ # ETL DAGs (workflow orchestration)
│ ├── plugins/ # Custom operators & hooks
│ ├── logs/ # Execution logs (mounted volume)
│ ├── config/ # airflow.cfg overrides
│ └── Dockerfile # Custom Airflow image
│
├── dbt/ # dbt transformation layer
│ └── my_dbt_project/
│ ├── models/
│ │ ├── staging/ # Raw data cleaning models
│ │ ├── intermediate/ # Business logic models
│ │ └── marts/ # Final reporting models
│ ├── macros/ # Reusable SQL logic
│ ├── tests/ # Data quality tests
│ ├── snapshots/ # Slowly changing dimensions
│ ├── dbt_project.yml # dbt configuration
│ └── profiles.yml # database connections
│
├── infra/ # Infrastructure setup (on-prem)
│ ├── docker-compose.yml # Full stack orchestration
│ ├── nginx/ # Reverse proxy configuration
│ │ └── airflow.conf
│ └── env/ # Environment configurations
│ ├── dev.env
│ └── prod.env
│
├── scripts/ # Automation utilities
│ ├── init_airflow.sh # Initialize Airflow setup
│ ├── run_dbt.sh # Execute dbt models
│ └── backup.sh # Backup pipelines & metadata
│
├── ci-cd/ # CI/CD automation pipelines
│ ├── gitlab-ci.yml # CI/CD pipeline definition
│ ├── deploy.sh # Deployment automation script
│ └── rollback.sh # Rollback strategy script
│
├── tests/ # Testing framework
│ ├── airflow_tests/ # DAG & workflow tests
│ └── dbt_tests/ # dbt model & data tests
│
├── docs/ # Documentation
│ ├── architecture.md # System design
│ ├── deployment.md # Deployment guide
│ └── troubleshooting.md # Issue resolution guide
│
└── README.md
```

---


---

## ⚙️ Tech Stack

- **Apache Airflow** → Workflow orchestration
- **dbt Core** → Data transformation framework
- **Docker & Docker Compose** → Containerization
- **PostgreSQL / Data Warehouse** → Data storage layer
- **Nginx** → Reverse proxy & routing
- **Bash Scripts** → Automation layer
- **CI/CD (GitLab CI / GitHub Actions)** → Deployment automation

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone <repository-url>
cd airflow-dbt-platform
2. Start Infrastructure
cd infra
docker-compose up -d

This will start:

Airflow Webserver
Airflow Scheduler
Metadata Database
Optional dbt container
3. Initialize Airflow
bash scripts/init_airflow.sh
4. Run dbt Transformations
bash scripts/run_dbt.sh

Or manually:

cd dbt/my_dbt_project
dbt run
dbt test
🔄 Workflow Execution
Airflow DAG triggers pipeline execution
Data is extracted from source systems
dbt transforms raw data into structured models
Data quality tests are executed
Final curated datasets are stored in marts layer
BI tools consume the final data
📊 CI/CD Pipeline

The CI/CD pipeline ensures automated deployment:

Code Commit
Automated Testing (Airflow + dbt)
Docker Image Build
Deployment to On-Prem Server
Health Checks & Validation
Rollback (if failure occurs)
🧪 Testing Strategy
Airflow Tests → DAG validation and task execution checks
dbt Tests → Schema, uniqueness, and data integrity tests
Integration Tests → End-to-end pipeline validation
🔐 Security & Best Practices
Environment variables stored securely in /infra/env
No secrets committed to Git repository
Airflow RBAC enabled for role-based access
Isolated Docker network for service communication
Reverse proxy via Nginx for secure exposure
📦 Deployment Options
Single-node Docker deployment
Multi-container orchestration setup
On-premise server deployment
Persistent storage volumes for data & logs
📚 Documentation

Detailed documentation is available in the /docs directory:

System architecture
Deployment procedures
Troubleshooting guide
Operational best practices
🧠 Future Enhancements
Kafka-based streaming ingestion
Great Expectations for advanced data validation
Kubernetes-based scaling architecture
Real-time analytics pipeline
Observability with Prometheus & Grafana
👨‍💻 Author

Built as a production-ready on-prem data engineering platform integrating Airflow + dbt for scalable ETL/ELT workflows and enterprise analytics solutions.