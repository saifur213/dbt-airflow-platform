#!/bin/bash

# Move to project root
cd "$(dirname "$0")/../.."

# Load environment variables from .env
set -a
source infra/.env
set +a

docker compose -f infra/docker-compose.yml exec airflow-webserver airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077 \
    --conn-extra '{"queue": "root.default", "deploy-mode": "client", "spark-binary": "spark-submit"}'