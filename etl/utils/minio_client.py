import os
import uuid
import polars as pl
from minio import Minio
from datetime import datetime
import io
import logging


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
        else:
            pass

    def _make_object_key(self, layer: str, schema: str, table: str, logical_date: datetime = None) -> str:
        run_time = logical_date or datetime.now()

        year = run_time.strftime("%Y")
        month = run_time.strftime("%m")
        day = run_time.strftime("%d")
        timestamp = run_time.strftime("%Y%m%d_%H%M%S")

        random_suffix = uuid.uuid4().hex[:6]

        if layer.lower() == "bronze":
            return f"{layer}/{schema}/{table}/year={year}/month={month}/day={day}/{table}_{timestamp}_{random_suffix}.parquet"

        return f"{layer}/{schema}/{table}.parquet"

    def _make_tmp_path(self, layer: str, schema: str, table: str) -> str:
        unique_id = uuid.uuid4().hex
        return f"/tmp/file_{layer}_{schema}_{table}_{unique_id}.parquet"

    def save(self, df: pl.DataFrame, layer: str, schema: str, table: str, logical_date: datetime = None, file_name = None) -> str:
        """
        Lưu Polars DataFrame lên MinIO dưới dạng Parquet.
        Trả về object key (đường dẫn trên MinIO).
        """
        if file_name:
            # Dành cho Full Load: Ép cứng tên file (VD: data.parquet) để tự động Ghi đè (Overwrite)
            object_key = f"{layer}/{schema}/{table}/{file_name}"
        else:
            object_key = self._make_object_key(layer, schema, table, logical_date=logical_date)
        tmp_path = self._make_tmp_path(layer, schema, table)
        try:
            parquet_buffer = io.BytesIO()

            df.write_parquet(parquet_buffer, use_pyarrow=True)
            parquet_buffer.seek(0)

            self._client.put_object(
                bucket_name=self._bucket,
                object_name=object_key,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            return object_key
        except Exception as e:
            raise Exception(f"Error saving to MinIO [{object_key}]: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def load(self, layer: str, schema: str, table: str, file_name = None) -> pl.DataFrame:
        """
        Tải file Parquet từ MinIO, trả về Polars DataFrame.
        """
        if file_name:
            object_key = f"{layer}/{schema}/{table}/{file_name}"
        else:
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