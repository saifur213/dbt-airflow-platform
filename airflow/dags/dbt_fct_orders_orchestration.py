import os
from dotenv import load_dotenv
from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from config.default_args import DEFAULT_ARGS
from pathlib import Path


# -------------------------
# Load infra/.env explicitly
# -------------------------
load_dotenv(dotenv_path="infra/.env")


# -------------------------
# Postgres Profile Config
# -------------------------
profile_config = ProfileConfig(
    profile_name="analytics",
    target_name="postgres_dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_target",
        profile_args={
            "host": "host.docker.internal",
            "port": int(os.getenv("TARGET_POSTGRES_PORT")),
            "dbname": os.getenv("TARGET_POSTGRES_DB"),
            "schema": "TARGET_POSTGRES_SCHEMA",
            "user": os.getenv("TARGET_POSTGRES_USER"),
            "password": os.getenv("TARGET_POSTGRES_PASSWORD"),
        },
    ),
)


# -------------------------
# Cosmos DBT DAG
# -------------------------
dbt_dag = DbtDag(
    dag_id="dbt_fct_orders_pipeline",

    project_config=ProjectConfig(
        Path("/opt/dbt/analytics")
    ),

    profile_config=profile_config,

    execution_config=ExecutionConfig(
        dbt_executable_path="/home/airflow/.local/bin/dbt"
    ),

    default_args=DEFAULT_ARGS,

    schedule_interval="0 4 * * *",

    catchup=False,

    max_active_runs=1,

    tags=["dbt", "orders", "postgres"],

    operator_args={
        "dbt_cmd_flags": ["--select", "stg_customers stg_orders fct_orders"]
    },
)