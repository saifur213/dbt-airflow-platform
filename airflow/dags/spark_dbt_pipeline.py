# # airflow/dags/spark_dbt_pipeline.py
# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.standard.operators.bash import BashOperator
# from datetime import datetime

# with DAG("spark_dbt_pipeline", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:

#     # Step 1: Spark ingests and processes raw data
#     spark_transform = SparkSubmitOperator(
#         task_id="spark_heavy_transform",
#         application="/opt/spark/jobs/heavy_transforms.py",
#         conn_id="spark_default",           # configure in Airflow Connections
#         executor_memory="4g",
#         total_executor_cores=4,
#         application_args=["--date", "{{ ds }}"],
#     )

#     # Step 2: dbt reads Spark output and builds marts
#     dbt_run = BashOperator(
#         task_id="dbt_run",
#         bash_command="cd /opt/dbt/analytics && dbt run --select staging+ --vars '{run_date: {{ ds }}}'",
#     )

#     dbt_test = BashOperator(
#         task_id="dbt_test",
#         bash_command="cd /opt/dbt/analytics && dbt test",
#     )

#     spark_transform >> dbt_run >> dbt_test