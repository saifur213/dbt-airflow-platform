docker compose exec airflow-webserver airflow connections add snowflake_default \
    --conn-type snowflake \
    --conn-login saifur \
    --conn-password Snowflake@2026 \
    --conn-schema PUBLIC \
    --conn-extra '{"account":"SYTOCXB-RP97423","warehouse":"DEV_WH","role":"ACCOUNTADMIN","database":"RAW"}'