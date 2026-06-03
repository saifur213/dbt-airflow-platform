# # airflow/dags/spark_jobs/minio_csv_reader.py

# from pyspark.sql import SparkSession

# def main():
#     spark = (
#         SparkSession.builder
#         .appName("MinIO CSV Reader")
#         # S3A / MinIO config (also set via SparkSubmitOperator conf, but
#         # kept here too so the script can be run standalone)
#         .config("spark.hadoop.fs.s3a.endpoint",               "http://minio:9000")
#         .config("spark.hadoop.fs.s3a.path.style.access",      "true")
#         .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#         .getOrCreate()
#     )

#     spark.sparkContext.setLogLevel("WARN")

#     # ── Read all CSVs from raw-data bucket ────────────────────────────────────
#     s3_path = "s3a://raw-data/*.csv"

#     print(f"Reading CSV files from: {s3_path}")

#     df = (
#         spark.read
#         .option("header",         "true")
#         .option("inferSchema",    "true")
#         .option("multiLine",      "true")
#         .option("escape",         '"')
#         .csv(s3_path)
#     )

#     # ── Basic profiling ───────────────────────────────────────────────────────
#     print("\n── Schema ───────────────────────────────────────")
#     df.printSchema()

#     print(f"\n── Row count: {df.count()} ──────────────────────")

#     print("\n── Sample rows (top 20) ─────────────────────────")
#     df.show(20, truncate=False)

#     print("\n── Summary statistics ───────────────────────────")
#     df.describe().show(truncate=False)

#     # ── Write processed output back to MinIO ──────────────────────────────────
#     output_path = "s3a://processed-data/csv_output"
#     print(f"\nWriting output to: {output_path}")

#     (
#         df.write
#         .mode("overwrite")
#         .option("header", "true")
#         .parquet(output_path)
#     )

#     print("Done!")
#     spark.stop()


# if __name__ == "__main__":
#     main()