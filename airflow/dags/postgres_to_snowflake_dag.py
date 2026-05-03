import logging
import pandas as pd

from config.default_args import DEFAULT_ARGS

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook



logger = logging.getLogger(__name__)

TABLES_TO_EXTRACT = [
    {"table": "orders", "incremental_col": "updated_at"},
    {"table": "customers", "incremental_col": "updated_at"},
    {"table": "products", "incremental_col": "updated_at"},
]

def extract_and_load(table: str, incremental_col: str, **context) -> None:
    """Extract from Postgres and load to Snowflake RAW layer."""
    pg_hook = PostgresHook(postgres_conn_id="postgres_source")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    # Get last watermark from XCom or default
    last_run = context["data_interval_start"].isoformat()

    query = f"""
        SELECT *, CURRENT_TIMESTAMP as _extracted_at
        FROM {table}
        WHERE {incremental_col} >= '{last_run}'
    """

    logger.info(f"Extracting {table} since {last_run}")
    df = pg_hook.get_pandas_df(query)

    if df.empty:
        logger.info(f"No new rows for {table}")
        return

    # Write to Snowflake RAW schema (append + dedup happens in dbt)
    sf_hook.run(f"CREATE SCHEMA IF NOT EXISTS RAW.PUBLIC")
    
    with sf_hook.get_conn() as conn:
        df.to_sql(
            name=table,
            con=conn,
            schema="PUBLIC",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10000,
        )
    
    logger.info(f"Loaded {len(df)} rows for {table}")
    context["ti"].xcom_push(key=f"{table}_row_count", value=len(df))


with DAG(
    dag_id="postgres_to_snowflake_incremental",
    default_args=DEFAULT_ARGS,
    description="Incremental extract from Postgres → Snowflake RAW",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "postgres", "snowflake"],
    doc_md="""
    ## Postgres → Snowflake Incremental Load
    Extracts changed records from the source Postgres DB and lands them
    in the Snowflake RAW layer for dbt to process.
    """,
) as dag:

    extract_tasks = []
    for config in TABLES_TO_EXTRACT:
        task = PythonOperator(
            task_id=f"extract_{config['table']}",
            python_callable=extract_and_load,
            op_kwargs=config,
        )
        extract_tasks.append(task)