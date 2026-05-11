#!/bin/bash

# Move to project root (optional but recommended)
cd "$(dirname "$0")/../.."

# Load environment variables from .env
set -a
source infra/.env
set +a

docker compose -f infra/docker-compose.yml exec airflow-webserver airflow connections add snowflake_default \
    --conn-type snowflake \
    --conn-login $SNOWFLAKE_USER \
    --conn-password $SNOWFLAKE_PASSWORD \
    --conn-schema $SNOWFLAKE_SCHEMA \
    --conn-extra "{\"account\":\"$SNOWFLAKE_ACCOUNT\",\"warehouse\":\"$SNOWFLAKE_WAREHOUSE\",\"role\":\"$SNOWFLAKE_ROLE\",\"database\":\"$SNOWFLAKE_DATABASE\"}"