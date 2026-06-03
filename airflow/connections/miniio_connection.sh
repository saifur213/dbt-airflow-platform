#!/bin/bash

# Move to project root
cd "$(dirname "$0")/../.."

# Load environment variables from .env
set -a
source infra/.env
set +a

docker compose -f infra/docker-compose.yml exec airflow-webserver airflow connections add minio_default \
    --conn-type aws \
    --conn-login $MINIO_ROOT_USER \
    --conn-password $MINIO_ROOT_PASSWORD \
    --conn-extra "{\"endpoint_url\": \"http://minio:9000\", \"region_name\": \"us-east-1\"}"