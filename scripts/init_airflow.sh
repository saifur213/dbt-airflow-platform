#!/usr/bin/env bash
set -euo pipefail

echo "→ Starting Airflow + dbt platform..."
cd "$(dirname "$0")/../infra"

# Build custom image
docker compose build --no-cache

# Start infrastructure
docker compose up -d airflow-postgres redis

echo "→ Waiting for Postgres..."
until docker compose exec airflow-postgres pg_isready -U airflow; do sleep 2; done

# Initialize Airflow DB and create admin user
docker compose up airflow-init

# Start remaining services
docker compose up -d

echo "→ Setting Airflow connections..."
docker compose exec airflow-webserver airflow connections add 'postgres_source' \
  --conn-type postgres \
  --conn-host "$POSTGRES_HOST" \
  --conn-login "$POSTGRES_USER" \
  --conn-password "$POSTGRES_PASSWORD" \
  --conn-port "$POSTGRES_PORT" \
  --conn-schema "$POSTGRES_DB"

docker compose exec airflow-webserver airflow connections add 'snowflake_default' \
  --conn-type snowflake \
  --conn-login "$SNOWFLAKE_USER" \
  --conn-password "$SNOWFLAKE_PASSWORD" \
  --conn-extra "{\"account\": \"$SNOWFLAKE_ACCOUNT\", \"warehouse\": \"COMPUTE_WH\", \"role\": \"LOADER\"}"

echo "✓ Platform ready at http://localhost:8080 (admin/admin)"