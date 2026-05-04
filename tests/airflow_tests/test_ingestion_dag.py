import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder="airflow/dags", include_examples=False)

def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"

def test_ingestion_dag_exists(dag_bag):
    assert "postgres_to_snowflake_incremental" in dag_bag.dags

def test_dbt_dag_exists(dag_bag):
    assert "dbt_transformation" in dag_bag.dags

def test_ingestion_dag_schedule(dag_bag):
    dag = dag_bag.get_dag("postgres_to_snowflake_incremental")
    assert dag.schedule_interval == "0 2 * * *"

def test_ingestion_dag_tags(dag_bag):
    dag = dag_bag.get_dag("postgres_to_snowflake_incremental")
    assert "ingestion" in dag.tags