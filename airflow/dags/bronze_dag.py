"""
DAG: bronze_mysql_to_minio

Nhiệm vụ DUY NHẤT của file này:
  - Đọc config từ biến môi trường
  - Tạo các Task và định nghĩa thứ tự chạy
  - GỌI logic từ etl/ — KHÔNG tự xử lý data

Mọi logic extract/transform/load đều nằm trong etl/
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Toàn bộ logic data đến từ etl/ ──
from etl.utils import MySQLClient, MinIOClient
from etl.bronze import (
    extract_customers,
    extract_sellers,
    extract_products,
    extract_orders,
    extract_order_items,
    extract_payments,
    extract_order_reviews,
    extract_product_category,
    extract_geolocation,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG — đọc từ .env (docker-compose truyền vào container)
# ─────────────────────────────────────────────

MYSQL_CONFIG = {
    "host":     os.getenv("MYSQL_HOST", "mysql"),
    "port":     int(os.getenv("MYSQL_PORT", 3306)),
    "database": os.getenv("MYSQL_DATABASE", "olist"),
    "user":     os.getenv("MYSQL_ROOT_USER", "root"),
    "password": os.getenv("MYSQL_ROOT_PASSWORD", "admin"),
}

MINIO_CONFIG = {
    "endpoint_url":     os.getenv("MINIO_ENDPOINT", "minio:9000"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY", "minio"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY", "minio123"),
    "bucket":           os.getenv("DATALAKE_BUCKET", "lakehouse"),
}


# ─────────────────────────────────────────────
# FACTORY: bọc hàm etl/ thành Airflow callable
# ─────────────────────────────────────────────

def make_task(extract_fn):
    """
    Nhận vào 1 hàm từ etl/bronze, trả về callable cho PythonOperator.
    Task chỉ khởi tạo client rồi gọi hàm — không làm gì thêm.
    """
    def _run(**context):
        mysql = MySQLClient(MYSQL_CONFIG)
        minio = MinIOClient(MINIO_CONFIG)
        metadata = extract_fn(mysql, minio)
        logger.info(f"✅ {extract_fn.__name__} | {metadata}")
        return metadata  # tự động đẩy vào XCom

    _run.__name__ = extract_fn.__name__
    return _run


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="bronze_mysql_to_minio",
    description="[Bronze] Extract toàn bộ bảng Olist từ MySQL → MinIO (Parquet)",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "mysql", "minio", "olist"],
) as dag:

    # ── Khai báo task — mỗi dòng = 1 bảng ──
    # Thêm bảng mới: thêm hàm vào etl/bronze, thêm 1 dòng ở đây
    t_product_category = PythonOperator(task_id="extract_product_category", python_callable=make_task(extract_product_category))
    t_geolocation      = PythonOperator(task_id="extract_geolocation",      python_callable=make_task(extract_geolocation))
    t_sellers          = PythonOperator(task_id="extract_sellers",          python_callable=make_task(extract_sellers))
    t_customers        = PythonOperator(task_id="extract_customers",        python_callable=make_task(extract_customers))
    t_products         = PythonOperator(task_id="extract_products",         python_callable=make_task(extract_products))
    t_orders           = PythonOperator(task_id="extract_orders",           python_callable=make_task(extract_orders))
    t_order_items      = PythonOperator(task_id="extract_order_items",      python_callable=make_task(extract_order_items))
    t_payments         = PythonOperator(task_id="extract_payments",         python_callable=make_task(extract_payments))
    t_order_reviews    = PythonOperator(task_id="extract_order_reviews",    python_callable=make_task(extract_order_reviews))

    # ── Thứ tự chạy (theo FOREIGN KEY) ──
    t_product_category >> t_products
    t_customers        >> t_orders
    t_sellers          >> t_orders
    t_products         >> t_order_items
    t_sellers          >> t_order_items
    t_orders           >> [t_order_items, t_payments, t_order_reviews]