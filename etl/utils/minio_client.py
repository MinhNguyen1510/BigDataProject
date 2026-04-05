import os
import uuid
import polars as pl
from minio import Minio
from datetime import datetime


class MinIOClient:
    """
    Client kết nối MinIO, tái sử dụng  cho mọi layer (bronze/silver/gold).
    """

    def __init__(self, config: dict):
        self._config = config
        self._client = Minio(
            endpoint=config["endpoint_url"],
            access_key=config["minio_access_key"],
            secret_key=config["minio_secret_key"],
            secure=False,
        )
        self._bucket = config["bucket"]
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Tạo bucket nếu chưa tồn tại."""
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    def _make_object_key(self, layer: str, schema: str, table: str, logical_date: datetime = None) -> str:
        """
        Tạo đường dẫn object trên MinIO có phân mảnh thời gian.
        Ví dụ: bronze/olist/customers/year=2026/month=03/day=29/customers_20260329_103000.parquet
        """
        run_time = logical_date or datetime.now()

        year = run_time.strftime("%Y")
        month = run_time.strftime("%m")
        day = run_time.strftime("%d")
        timestamp = run_time.strftime("%Y%m%d_%H%M%S")

        # Áp dụng partition cho lớp Bronze
        if layer.lower() == "bronze":
            return f"{layer}/{schema}/{table}/year={year}/month={month}/day={day}/{table}_{timestamp}.parquet"

        # Lớp Silver/Gold có thể dùng đường dẫn tĩnh nếu dùng Delta Lake
        return f"{layer}/{schema}/{table}.parquet"

    def _make_tmp_path(self, layer: str, schema: str, table: str) -> str:
        unique_id = uuid.uuid4().hex
        return f"/tmp/file_{layer}_{schema}_{table}_{unique_id}.parquet"

    def save(self, df: pl.DataFrame, layer: str, schema: str, table: str, logical_date: datetime = None) -> str:
        """
        Lưu Polars DataFrame lên MinIO dưới dạng Parquet.
        Trả về object key (đường dẫn trên MinIO).
        """
        object_key = self._make_object_key(layer, schema, table, logical_date=logical_date)
        tmp_path = self._make_tmp_path(layer, schema, table)
        try:
            df.write_parquet(tmp_path)
            self._client.fput_object(self._bucket, object_key, tmp_path)
            return object_key
        except Exception as e:
            raise Exception(f"Error saving to MinIO [{object_key}]: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def load(self, layer: str, schema: str, table: str) -> pl.DataFrame:
        """
        Tải file Parquet từ MinIO, trả về Polars DataFrame.
        """
        object_key = self._make_object_key(layer, schema, table)
        tmp_path = self._make_tmp_path(layer, schema, table)
        try:
            self._client.fget_object(self._bucket, object_key, tmp_path)
            return pl.read_parquet(tmp_path)
        except Exception as e:
            raise Exception(f"Error loading from MinIO [{object_key}]: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)