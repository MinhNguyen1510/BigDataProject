import argparse
import logging
import os
from pyspark.sql import SparkSession

from silver_processor import process_silver_layer

def build_spark_session(app_name="Silver_Layer_Processing"):
   # init spark session
    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
             .config("spark.sql.streaming.schemaInference", "true")
             .getOrCreate())

    # giảm log cho đỡ spam
    spark.sparkContext.setLogLevel("WARN")

    return spark

# setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Layer Processor")
    parser.add_argument("--table_name", required=True, help="Tên bảng cần xử lý")
    parser.add_argument("--merge_key", required=True, help="Khóa chính để Upsert/Anti-join")
    parser.add_argument("--is_cdc", type=str, default="false", help="Bật chế độ CDC (true/false)")
    parser.add_argument("--json_schema", type=str, default="", help="Chuỗi DDL Schema để bóc JSON")
    parser.add_argument("--is_full_load", type=str, default="false", help="Bảng Full Load (true/false)")
    args = parser.parse_args()

    # convert string sang bool
    is_cdc_flag = args.is_cdc.lower() == "true"
    is_full_load_flag = args.is_full_load.lower() == "true"

    # config MySQL (dùng để check hard delete)
    mysql_config = {
        "host": "mysql",
        "port": int(3306),
        "database": os.getenv("DB_NAME", "olist_db"),
        "user": "root",
        "password": os.getenv("MLFLOW_DB_ROOT_PASS", "admin")
    }

    logger.info(f"Khởi tạo Spark Job cho bảng: {args.table_name}")

    # tạo spark job riêng cho từng table
    spark = build_spark_session(f"Silver_Processor_{args.table_name}")

    spark.conf.set("spark.app.name", f"Silver_Processor_{args.table_name}")

    # đảm bảo database tồn tại
    spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://lakehouse/silver/'")

    try:
        # gọi hàm xử lý chính
        process_silver_layer(
            spark=spark,
            table_name=args.table_name,
            merge_key=args.merge_key,
            mysql_config=mysql_config,
            watermark_col="last_modified_date",
            is_cdc=is_cdc_flag,
            json_schema=args.json_schema,
            is_full_load=is_full_load_flag
        )
    except Exception as e:
        # log lỗi dễ debug
        logger.error(f"Lỗi khi xử lý lớp Silver bảng {args.table_name}: {e}")
        raise e
    finally:
        # nhớ stop spark (không là leak source)
        spark.stop()