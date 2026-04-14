"""
DAG: bronze_mysql_to_minio
"""

import os
import logging
from datetime import datetime, timedelta
from airflow.models import Variable

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.utils import MySQLClient, MinIOClient
from etl.bronze import (
    extract_customers,
    extract_sellers,
    extract_products,
    extract_order_payments,
    extract_order_reviews,
    extract_product_category,
    extract_geolocation,
)

logger = logging.getLogger(__name__)

MYSQL_CONFIG = {
    "host":     "mysql",
    "port":     int(3306),
    "database": os.getenv("DB_NAME", "olist_db"),
    "user":     "root",
    "password": os.getenv("MLFLOW_DB_ROOT_PASS", "admin"),
}

MINIO_CONFIG = {
    "endpoint_url":     os.getenv("MINIO_ENDPOINT", "minio:9000"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY", "minio"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY", "minio123"),
    "bucket":           os.getenv("DATALAKE_BUCKET", "lakehouse"),
}


def make_dim_task(extract_fn):

    def _run(**context):
        logical_date = context["data_interval_end"]

        mysql = MySQLClient(MYSQL_CONFIG)
        minio = MinIOClient(MINIO_CONFIG)

        metadata = extract_fn(mysql, minio, logical_date=logical_date)
        logger.info(f" {extract_fn.__name__} | {metadata}")
        return metadata

    _run.__name__ = extract_fn.__name__
    return _run

def make_fact_task(extract_fn, table_name):
    def _run(**context):
        logical_date = context["data_interval_end"]
        mysql = MySQLClient(MYSQL_CONFIG)
        minio = MinIOClient(MINIO_CONFIG)

        var_key = f"watermark_{table_name}"
        last_watermark = Variable.get(var_key, default_var=None)

        metadata = extract_fn(
            mysql,
            minio,
            last_watermark=last_watermark,
            logical_date=logical_date
        )
        logger.info(f" {extract_fn.__name__} | {metadata}")

        # Cập nhật Watermark MỚI cho lần chạy ngày mai
        # Chúng ta dùng luôn mốc thời gian chạy xong (logical_date) làm mốc quét cho ngày mai
        if metadata.get("status") != "skipped" and "new_watermark" in metadata:
            new_watermark = metadata["new_watermark"]
            Variable.set(var_key, new_watermark)
            logger.info(f" Đã cập nhật watermark cho '{table_name}' thành: {new_watermark}")

        return metadata

    _run.__name__ = extract_fn.__name__
    return _run


default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="bronze_mysql_to_minio",
    description="[Bronze] Extract Olist MySQL → MinIO (Partitioning & Incremental)",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 4, 12),
    catchup=False,
    tags=["bronze", "mysql", "minio", "olist"],
) as dag:

    t_product_category = PythonOperator(task_id="extract_product_category", python_callable=make_dim_task(extract_product_category))
    t_geolocation = PythonOperator(task_id="extract_geolocation", python_callable=make_dim_task(extract_geolocation))
    t_customers = PythonOperator(task_id="extract_customers", python_callable=make_dim_task(extract_customers))
    t_products = PythonOperator(task_id="extract_products", python_callable=make_dim_task(extract_products))
    t_sellers = PythonOperator(task_id="extract_sellers", python_callable=make_dim_task(extract_sellers))
    t_order_payments = PythonOperator(
        task_id="extract_payments",
        python_callable=make_fact_task(extract_order_payments, "order_payments")
    )
    t_order_reviews = PythonOperator(
        task_id="extract_order_reviews",
        python_callable=make_fact_task(extract_order_reviews, "order_reviews")
    )

    t_product_category >> t_products

    [t_products, t_customers, t_sellers, t_geolocation, t_order_payments] >> t_order_reviews