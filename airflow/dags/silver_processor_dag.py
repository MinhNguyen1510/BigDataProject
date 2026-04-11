"""
DAG: silver_spark_processor

Task: Lấy dữ liệu từ lớp Bronze (Parquet) -> Lọc trùng, Upsert, Xóa hard delete -> Ghi vào Silver (Delta Lake).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

TABLES_CONFIG = [
    {"table": "customers", "merge_key": "customer_id", "is_full_load": "true"},
    {"table": "sellers", "merge_key": "seller_id", "is_full_load": "true"},
    {"table": "products", "merge_key": "product_id", "is_full_load": "true"},
    {"table": "product_category_name_translation", "merge_key": "product_category_name", "is_full_load": "true"},
    {"table": "geolocation", "merge_key": "geolocation_zip_code_prefix", "is_full_load": "true"},
    {"table": "order_payments", "merge_key": "id", "is_full_load": "false"},
    {"table": "order_reviews", "merge_key": "review_id", "is_full_load": "false"},
    {
        "table": "orders",
        "merge_key": "order_id",
        "is_cdc": "true",
        "is_full_load": "false",
        "json_schema": "order_id STRING, customer_id STRING, order_status STRING, order_purchase_timestamp STRING, order_approved_at STRING, order_delivered_carrier_date STRING, order_delivered_customer_date STRING, order_estimated_delivery_date STRING"
    },
    {
        "table": "order_items",
        "merge_key": "order_id, order_item_id",
        "is_cdc": "true",
        "is_full_load": "false",
        "json_schema": "order_id STRING, order_item_id STRING, product_id STRING, seller_id STRING, shipping_limit_date STRING, price STRING, freight_value STRING"
    }
]

with DAG(
    dag_id="silver_spark_processor",
    description="[Silver] Xử lý Data Nóng và Hard Delete bằng PySpark & Delta Lake",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 5),
    catchup=False,
    tags=["silver", "spark", "delta", "olist"],
) as dag:

    start_task = EmptyOperator(task_id="start_silver_processing")
    end_task = EmptyOperator(task_id="end_silver_processing")

    prev_task = start_task

    for config in TABLES_CONFIG:
        table_name = config["table"]
        merge_key = config["merge_key"]

        spark_task = SparkSubmitOperator(
            task_id=f"process_silver_{table_name}",
            conn_id="spark_default",
            application="/opt/airflow/etl/silver/main_silver.py",
            name=f"airflow_silver_{table_name}",
            executor_cores=2,
            num_executors=2,
            executor_memory="2g",
            driver_memory="512m",
            application_args=[
                "--table_name", table_name,
                "--merge_key", merge_key,
                "--is_cdc", config.get("is_cdc", "false"),
                "--json_schema", config.get("json_schema", ""),
                "--is_full_load", config.get("is_full_load", "false")
            ],

            jars="/opt/airflow/etl/jars/hadoop-aws-3.3.2.jar,/opt/airflow/etl/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/etl/jars/delta-core_2.12-2.3.0.jar,/opt/airflow/etl/jars/delta-storage-2.3.0.jar,/opt/airflow/etl/jars/mysql-connector-java-8.0.28.jar",
            verbose=True,
            conf = {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
                "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
                "spark.hadoop.fs.s3a.access.key": "minio",
                "spark.hadoop.fs.s3a.secret.key": "minio123",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.sql.shuffle.partitions": "4",
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.databricks.delta.autoCompact.enabled": "true",
                "spark.sql.parquet.enableVectorizedReader": "false",
                "spark.sql.legacy.parquet.nanosAsLong": "true",
                # Ép Spark hiểu cách chuyển đổi thời gian kiểu mới (Proleptic Gregorian)
                "spark.sql.parquet.int64RebaseModeInRead": "LEGACY",
                "spark.sql.parquet.datetimeRebaseModeInRead": "LEGACY",
                "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",
                "spark.sql.parquet.datetimeRebaseModeInWrite": "LEGACY",
                "spark.executor.memoryOverhead": "512m",
                "spark.driver.memoryOverhead": "512m",
                "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
                "spark.hadoop.fs.s3a.fast.upload": "true"
            }
        )

        prev_task >> spark_task

        prev_task = spark_task

    prev_task >> end_task
