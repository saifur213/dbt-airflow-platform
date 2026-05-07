import logging
import pandas as pd

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

from config.default_args import DEFAULT_ARGS

logger = logging.getLogger(__name__)

TABLES_TO_EXTRACT = [
    {"table": "orders", "incremental_col": "updated_at"},
    {"table": "customers", "incremental_col": "updated_at"},
    {"table": "products", "incremental_col": "updated_at"},
]


def get_snowflake_engine():
    conn = BaseHook.get_connection("snowflake_default")

    extra = conn.extra_dejson

    engine = create_engine(URL(
        account=extra["account"],
        user=conn.login,
        password=conn.password,
        database=extra.get("database"),
        schema=extra.get("schema", "PUBLIC"),
        warehouse=extra["warehouse"],
        role=extra.get("role"),
    ))

    return engine


def extract_and_load(table: str, incremental_col: str, **context) -> None:
    pg_hook = PostgresHook(postgres_conn_id="postgres_source")

    last_run = context["data_interval_start"].isoformat()

    query = f"""
        SELECT *,
               CURRENT_TIMESTAMP as _extracted_at
        FROM {table}
        WHERE {incremental_col} >= '{last_run}'
    """

    logger.info(f"Extracting {table} since {last_run}")

    df = pg_hook.get_pandas_df(query)

    if df.empty:
        logger.info(f"No new rows for {table}")
        return

    engine = get_snowflake_engine()

    # ensure schema exists
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS RAW.PUBLIC")

    # load data
    df.to_sql(
        name=table,
        con=engine,
        schema="PUBLIC",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000,
    )

    logger.info(f"Loaded {len(df)} rows into Snowflake for {table}")

    context["ti"].xcom_push(key=f"{table}_row_count", value=len(df))


with DAG(
    dag_id="postgres_to_snowflake_incremental",
    default_args=DEFAULT_ARGS,
    description="Incremental extract from Postgres → Snowflake RAW",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "postgres", "snowflake"],
) as dag:

    for config in TABLES_TO_EXTRACT:
        PythonOperator(
            task_id=f"extract_{config['table']}",
            python_callable=extract_and_load,
            op_kwargs=config,
        )