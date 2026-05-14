import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from config.default_args import DEFAULT_ARGS
from pathlib import Path


profile_config = ProfileConfig(
    profile_name="analytics",
    target_name="postgres_dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_target",
        profile_args={
            "host": "host.docker.internal",
            "port": int(os.getenv("TARGET_POSTGRES_PORT")),
            "dbname": os.getenv("TARGET_POSTGRES_DB"),
            "schema": os.getenv("TARGET_POSTGRES_SCHEMA"),
            "user": os.getenv("TARGET_POSTGRES_USER"),
            "password": os.getenv("TARGET_POSTGRES_PASSWORD"),
        },
    ),
)


dbt_dag = DbtDag(
    dag_id="dbt_fct_orders_pipeline",

    project_config=ProjectConfig(
        dbt_project_path="/opt/dbt/analytics",
        manifest_path="/opt/dbt/analytics/target/manifest.json",
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
)