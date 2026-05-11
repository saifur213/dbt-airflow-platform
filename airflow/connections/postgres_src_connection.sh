#!/bin/bash

# Move to project root (optional but recommended)
cd "$(dirname "$0")/../.."

# Load environment variables from .env
set -a
source infra/.env
set +a

docker compose -f infra/docker-compose.yml exec airflow-webserver airflow connections add postgres_source \
    --conn-type postgres \
    --conn-host host.docker.internal \
    --conn-schema $SOURCE_POSTGRES_DB \
    --conn-login $SOURCE_POSTGRES_USER \
    --conn-password $SOURCE_POSTGRES_PASSWORD \
    --conn-port $SOURCE_POSTGRES_PORT