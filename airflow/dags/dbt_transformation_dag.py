from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from config.default_args import DEFAULT_ARGS
from pathlib import Path

profile_config = ProfileConfig(
    profile_name="analytics",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "PROD",
            "schema": "PUBLIC",
            "warehouse": "COMPUTE_WH",
            "role": "TRANSFORMER",
        },
    ),
)

dbt_dag = DbtDag(
    dag_id="dbt_transformation",
    project_config=ProjectConfig(Path("/opt/dbt/my_dbt_project")),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path="/home/airflow/.local/bin/dbt"),
    default_args=DEFAULT_ARGS,
    schedule_interval="0 4 * * *",  # After ingestion completes
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "transformation", "snowflake"],
)