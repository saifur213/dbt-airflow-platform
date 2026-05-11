#!/bin/bash

# Move to project root (optional but recommended)
cd "$(dirname "$0")/../.."

# Load environment variables from .env
set -a
source infra/.env
set +a

docker compose -f infra/docker-compose.yml exec airflow-webserver airflow connections add postgres_target \
    --conn-type postgres \
    --conn-host host.docker.internal \
    --conn-schema $TARGET_POSTGRES_DB \
    --conn-login $TARGET_POSTGRES_USER \
    --conn-password $TARGET_POSTGRES_PASSWORD \
    --conn-port $TARGET_POSTGRES_PORT