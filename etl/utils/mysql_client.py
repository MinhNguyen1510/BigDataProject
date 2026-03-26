import pandas as pd
import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert as mysql_insert


class MySQLClient:
    """
    Client kết nối MySQL, tái sử dụng cho mọi layer (bronze/silver/gold).
    """

    def __init__(self, config: dict):
        self._config = config
        conn_str = (
            f"mysql+pymysql://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}"
            f"/{config['database']}"
        )
        try:
            self._engine = create_engine(conn_str)
        except Exception as e:
            raise Exception(f"Failed to create MySQL engine: {e}")

    def extract_data(self, sql: str) -> pl.DataFrame:
        """
        Chạy câu SQL, trả về Polars DataFrame.
        Dùng pandas làm cầu nối vì tương thích tốt với SQLAlchemy.
        """
        try:
            df_pandas = pd.read_sql(sql, self._engine)
            return pl.from_pandas(df_pandas)
        except Exception as e:
            raise Exception(f"Error extracting data from MySQL: {e}")

    def _upsert_method(self, table, conn, keys, data_iter):
        """INSERT ... ON DUPLICATE KEY UPDATE"""
        data = [dict(zip(keys, row)) for row in data_iter]
        if not data:
            return
        stmt = mysql_insert(table.table).values(data)
        update_stmt = stmt.on_duplicate_key_update(
            {col.name: col for col in stmt.inserted}
        )
        conn.execute(update_stmt)

    def insert_data(self, table_name: str, df: pl.DataFrame) -> int:
        """
        Ghi Polars DataFrame vào MySQL bằng UPSERT.
        Trả về số dòng đã ghi.
        """
        df_pandas = df.to_pandas() if hasattr(df, "to_pandas") else df
        try:
            df_pandas.to_sql(
                table_name,
                con=self._engine,
                if_exists="append",
                index=False,
                chunksize=1000,
                method=self._upsert_method,
            )
            return len(df_pandas)
        except Exception as e:
            raise Exception(f"Error writing to MySQL table '{table_name}': {e}")