import logging

from sqlalchemy import create_engine

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


def get_target_postgres_engine():
    conn = BaseHook.get_connection("postgres_target")

    engine = create_engine(
        f"postgresql+psycopg2://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )

    return engine


def extract_and_load(table: str, incremental_col: str, **context) -> None:
    source_hook = PostgresHook(postgres_conn_id="postgres_source")
    ti = context["ti"]

    # ✅ Get last processed watermark
    last_watermark = ti.xcom_pull(key=f"{table}_watermark", task_ids=f"extract_{table}")

    is_full_load = last_watermark is None

    if is_full_load:
        logger.info(f"FIRST RUN → Full load for {table}")
        query = f"""
            SELECT *,
                   CURRENT_TIMESTAMP as _extracted_at
            FROM {table}
        """
    else:
        logger.info(f"INCREMENTAL LOAD → {table} since {last_watermark}")
        query = f"""
            SELECT *,
                   CURRENT_TIMESTAMP as _extracted_at
            FROM {table}
            WHERE {incremental_col} > '{last_watermark}'
        """

    df = source_hook.get_pandas_df(query)

    logger.info(f"Total rows found in {table}: {len(df)}")

    if df.empty:
        logger.info(f"No new rows for {table}")
        return

    engine = get_target_postgres_engine()

    # Ensure schema exists
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS raw")

    # Load data
    df.to_sql(
        name=table,
        con=engine,
        schema="raw",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000,
    )

    logger.info(f"Loaded {len(df)} rows into target Postgres for {table}")

    # ✅ Update watermark (max updated_at)
    new_watermark = df[incremental_col].max()

    ti.xcom_push(key=f"{table}_watermark", value=str(new_watermark))
    ti.xcom_push(key=f"{table}_row_count", value=len(df))


with DAG(
    dag_id="postgres_to_postgres_incremental",
    default_args=DEFAULT_ARGS,
    description="Full load first run + incremental Postgres → Postgres",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "postgres"],
) as dag:

    for config in TABLES_TO_EXTRACT:
        PythonOperator(
            task_id=f"extract_{config['table']}",
            python_callable=extract_and_load,
            op_kwargs=config,
        )