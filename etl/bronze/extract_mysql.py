"""
Bronze Layer — Extract MySQL → MinIO

Mỗi hàm extract_<table>() là một đơn vị logic độc lập,
Airflow chỉ gọi các hàm này chứ không chứa bất kỳ logic data nào.
"""

import logging
from etl.utils.mysql_client import MySQLClient
from etl.utils.minio_client import MinIOClient
from datetime import datetime

logger = logging.getLogger(__name__)

LAYER = "bronze"

# HELPER CHUNG: Full Load

def _extract_full_load(
    mysql: MySQLClient,
    minio: MinIOClient,
    table_name: str,
    schema: str,
    sql: str = None,
    logical_date: datetime = None,
) -> dict:
    """
    Đọc toàn bộ bảng từ MySQL, lưu Parquet lên MinIO.
    Trả về metadata dict — Airflow task sẽ đẩy vào XCom nếu cần.
    """
    query = sql or f"SELECT * FROM {table_name};"
    logger.info(f"[{table_name}] Full Load | Query: {query}")

    df = mysql.extract_data(query)
    logger.info(f"[{table_name}] Đã đọc: {df.shape[0]} dòng x {df.shape[1]} cột")

    object_key = minio.save(df, layer=LAYER, schema=schema, table=table_name, logical_date=None, file_name="data.parquet")
    logger.info(f"[{table_name}] Đã lưu MinIO: {object_key}")

    return {
        "table": table_name,
        "layer": LAYER,
        "schema": schema,
        "row_count": df.shape[0],
        "column_count": df.shape[1],
        "columns": df.columns,
        "minio_path": object_key,
        "mode": "full",
    }

# HELPER 2: Incremental Load (Dành cho bảng Transaction lớn)
def _extract_incremental_load(
    mysql: MySQLClient,
    minio: MinIOClient,
    table_name: str,
    schema: str,
    watermark_col: str,
    last_watermark: str,
    logical_date: datetime = None,
) -> dict:
    # Chỉ lấy dữ liệu mới hơn watermark
    query = f"SELECT * FROM {table_name} WHERE {watermark_col} > '{last_watermark}';"
    logger.info(f"[{table_name}] Incremental Load | Sau {last_watermark}")

    df = mysql.extract_data(query)
    logger.info(f"[{table_name}] Đã đọc dữ liệu mới: {df.shape[0]} dòng")

    if df.shape[0] == 0:
        logger.info(f"[{table_name}] Không có dữ liệu mới.")
        return {"table": table_name, "mode": "incremental", "row_count": 0, "status": "skipped"}

    max_watermark_in_data = str(df[watermark_col].max())

    object_key = minio.save(df, layer=LAYER, schema=schema, table=table_name, logical_date=logical_date)
    logger.info(f"[{table_name}] Đã lưu MinIO: {object_key}")

    return {
        "table": table_name,
        "layer": LAYER,
        "schema": schema,
        "row_count": df.shape[0],
        "minio_path": object_key,
        "mode": "incremental",
        "watermark_used": last_watermark,
        "new_watermark": max_watermark_in_data,
        "logical_date": str(logical_date) if logical_date else None
    }

def extract_customers(mysql: MySQLClient, minio: MinIOClient, logical_date: datetime = None) -> dict:
    return _extract_full_load(mysql, minio, "customers", "olist", logical_date=logical_date)

def extract_products(mysql: MySQLClient, minio: MinIOClient, logical_date: datetime = None) -> dict:
    return _extract_full_load(mysql, minio, "products", "olist", logical_date=logical_date)

def extract_product_category(mysql: MySQLClient, minio: MinIOClient, logical_date: datetime = None) -> dict:
    return _extract_full_load(mysql, minio, "product_category_name_translation", "olist", logical_date=logical_date)

def extract_geolocation(mysql: MySQLClient, minio: MinIOClient, logical_date: datetime = None) -> dict:
    return _extract_full_load(mysql, minio, "geolocation", "olist", logical_date=logical_date)

def extract_sellers(mysql: MySQLClient, minio: MinIOClient, logical_date: datetime = None) -> dict:
    return _extract_full_load(mysql, minio, "sellers", "olist", logical_date=logical_date)

def extract_order_payments(mysql: MySQLClient, minio: MinIOClient, last_watermark: str = None, logical_date: datetime = None) -> dict:
    if not last_watermark:
        logger.info("[order_payments] Lần chạy đầu tiên -> Kéo toàn bộ lịch sử")
        last_watermark = "1900-01-01 00:00:00"

    return _extract_incremental_load(
        mysql, minio, "order_payments", "olist",
        watermark_col="last_modified_date",
        last_watermark=last_watermark,
        logical_date=logical_date
    )

def extract_order_reviews(mysql: MySQLClient, minio: MinIOClient, last_watermark: str = None, logical_date: datetime = None) -> dict:
    if not last_watermark:
        logger.info("[order_reviews] Lần chạy đầu tiên -> Kéo toàn bộ lịch sử")
        last_watermark = "1900-01-01 00:00:00"

    return _extract_incremental_load(
        mysql, minio, "order_reviews", "olist",
        watermark_col="last_modified_date",
        last_watermark=last_watermark,
        logical_date=logical_date
    )