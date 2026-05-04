#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG=${1:-latest}
COMPOSE_FILE="/opt/airflow-dbt/infra/docker-compose.yml"

echo "→ Deploying image tag: $IMAGE_TAG"

# Pull latest image
docker pull "$CI_REGISTRY_IMAGE:$IMAGE_TAG"

# Blue-green swap: update compose to new tag
export AIRFLOW_IMAGE="$CI_REGISTRY_IMAGE:$IMAGE_TAG"

# Zero-downtime: restart workers first, then scheduler, then webserver
docker compose -f "$COMPOSE_FILE" up -d --no-deps airflow-worker
docker compose -f "$COMPOSE_FILE" up -d --no-deps airflow-scheduler
docker compose -f "$COMPOSE_FILE" up -d --no-deps airflow-webserver

echo "→ Waiting for webserver health..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8080/health | grep -q '"status": "healthy"'; then
    echo "→ Deployment successful"
    exit 0
  fi
  sleep 5
done

echo "✗ Health check failed — rolling back"
bash "$(dirname "$0")/rollback.sh"
exit 1