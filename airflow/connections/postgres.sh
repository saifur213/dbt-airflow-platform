docker compose exec airflow-webserver airflow connections add postgres_source \
    --conn-type postgres \
    --conn-host host.docker.internal \
    --conn-schema source_db \
    --conn-login etl_user \
    --conn-password etluser26 \
    --conn-port 5432