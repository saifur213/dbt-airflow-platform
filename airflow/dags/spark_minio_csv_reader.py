# airflow/dags/spark_minio_csv_reader.py

import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from config.default_args import DEFAULT_ARGS

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="spark_minio_csv_reader",
    default_args=DEFAULT_ARGS,
    description="List and read CSV files from MinIO raw-data bucket using Spark",
    schedule_interval=None,          # trigger manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "minio", "csv"],
) as dag:

    # ── Task 1: List CSV files in the bucket ──────────────────────────────────
    def list_csv_files(**context):
        hook = S3Hook(aws_conn_id="minio_default")
        keys = hook.list_keys(bucket_name="raw-data")

        if not keys:
            raise ValueError("No files found in raw-data bucket!")

        csv_files = [k for k in keys if k.endswith(".csv")]
        if not csv_files:
            raise ValueError("No CSV files found in raw-data bucket!")

        print(f"Found {len(csv_files)} CSV file(s):")
        for f in csv_files:
            print(f"  - {f}")

        # push file list to XCom so next task can use it
        context["ti"].xcom_push(key="csv_files", value=csv_files)
        return csv_files

    list_files_task = PythonOperator(
        task_id="list_csv_files",
        python_callable=list_csv_files,
    )

    # # ── Task 2: Submit Spark job to read the CSV files ────────────────────────
    # spark_read_task = SparkSubmitOperator(
    #     task_id="spark_read_csv",
    #     conn_id="spark_default",
    #     application="/opt/airflow/dags/spark_jobs/minio_csv_reader.py",
    #     name="minio_csv_reader",
    #     verbose=True,
    #     conf={
    #         # MinIO S3A config
    #         "spark.hadoop.fs.s3a.endpoint":          "http://minio:9000",
    #         "spark.hadoop.fs.s3a.access.key":        "{{ var.value.get('minio_access_key', 'minioadmin') }}",
    #         "spark.hadoop.fs.s3a.secret.key":        "{{ var.value.get('minio_secret_key', 'minioadmin123') }}",
    #         "spark.hadoop.fs.s3a.path.style.access": "true",
    #         "spark.hadoop.fs.s3a.impl":              "org.apache.hadoop.fs.s3a.S3AFileSystem",
    #         "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    #         # JARs required for S3A
    #         "spark.jars.packages": (
    #             "org.apache.hadoop:hadoop-aws:3.3.4,"
    #             "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    #         ),
    #     },
    # )

    # list_files_task >> spark_read_task
    list_files_task