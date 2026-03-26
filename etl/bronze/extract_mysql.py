"""
Bronze Layer — Extract MySQL → MinIO

Mỗi hàm extract_<table>() là một đơn vị logic độc lập,
Airflow chỉ gọi các hàm này chứ không chứa bất kỳ logic data nào.
"""

import logging
from etl.utils.mysql_client import MySQLClient
from etl.utils.minio_client import MinIOClient

logger = logging.getLogger(__name__)

LAYER = "bronze"

# HELPER CHUNG: Full Load

def _extract_full_load(
    mysql: MySQLClient,
    minio: MinIOClient,
    table_name: str,
    schema: str,
    sql: str = None,
) -> dict:
    """
    Đọc toàn bộ bảng từ MySQL, lưu Parquet lên MinIO.
    Trả về metadata dict — Airflow task sẽ đẩy vào XCom nếu cần.
    """
    query = sql or f"SELECT * FROM {table_name};"
    logger.info(f"[{table_name}] Full Load | Query: {query}")

    df = mysql.extract_data(query)
    logger.info(f"[{table_name}] Đã đọc: {df.shape[0]} dòng x {df.shape[1]} cột")

    object_key = minio.save(df, layer=LAYER, schema=schema, table=table_name)
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


# ─────────────────────────────────────────────────────────
# MỖI HÀM = MỘT BẢNG
# ─────────────────────────────────────────────────────────

def extract_customers(mysql: MySQLClient, minio: MinIOClient) -> dict:
    return _extract_full_load(mysql, minio, table_name="customers", schema="customer")


def extract_sellers(mysql: MySQLClient, minio: MinIOClient) -> dict:
    return _extract_full_load(mysql, minio, table_name="sellers", schema="seller")


def extract_products(mysql: MySQLClient, minio: MinIOClient) -> dict:
    return _extract_full_load(mysql, minio, table_name="products", schema="product")


def extract_orders(mysql: MySQLClient, minio: MinIOClient) -> dict:
    return _extract_full_load(mysql, minio, table_name="orders", schema="order")


def extract_order_items(mysql: MySQLClient, minio: MinIOClient) -> dict:
    return _extract_full_load(mysql, minio, table_name="order_items", schema="orderitem")


def extract_payments(mysql: MySQLClient, minio: MinIOClient) -> dict:
    return _extract_full_load(mysql, minio, table_name="payments", schema="payment")


def extract_order_reviews(mysql: MySQLClient, minio: MinIOClient) -> dict:
    return _extract_full_load(mysql, minio, table_name="order_reviews", schema="orderreview")


def extract_product_category(mysql: MySQLClient, minio: MinIOClient) -> dict:
    return _extract_full_load(
        mysql, minio,
        table_name="product_category_name_translation",
        schema="productcategory",
    )


def extract_geolocation(mysql: MySQLClient, minio: MinIOClient) -> dict:
    return _extract_full_load(mysql, minio, table_name="geolocation", schema="geolocation")