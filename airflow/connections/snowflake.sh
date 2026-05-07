airflow connections add snowflake_default \
    --conn-type snowflake \
    --conn-login saifur \
    --conn-password Snowflake@2026 \
    --conn-account SYTOCXB-RP97423 \
    --conn-schema PUBLIC \
    --conn-database RAW \
    --conn-extra '{"warehouse":"DEV_WH","role":"ACCOUNTADMIN"}'