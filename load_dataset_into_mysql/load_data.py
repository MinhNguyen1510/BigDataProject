
import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os
import logging
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration từ biến môi trường
# ============================================================================
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": "root",
    "password": os.getenv("MLFLOW_DB_ROOT_PASS"),
    "database": os.getenv("DB_NAME"),
}

DATASET_PATH = os.getenv("DATASET_PATH", "/tmp/dataset")

# Mapping: CSV file -> Table name
CSV_MAPPING = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_sellers_dataset.csv": "sellers",
    "olist_products_dataset.csv": "products",
    "product_category_name_translation.csv": "product_category_name_translation",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
}


class MySQLDataLoader:
    """Class để load dữ liệu CSV vào MySQL"""
    
    def __init__(self, config):
        """Initialize connection"""
        self.config = config
        self.engine = None
        
    def connect(self):
        """Kết nối tới MySQL - retry nếu thất bại"""
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(1, max_retries + 1):
            try:
                connection_string = (
                    f"mysql+pymysql://{self.config['user']}:{self.config['password']}"
                    f"@{self.config['host']}:{self.config['port']}"
                    f"/{self.config['database']}"
                )
                self.engine = create_engine(connection_string)
                
                # Test connection
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                logger.info(f"✓ Kết nối MySQL thành công: {self.config['host']}:{self.config['port']}/{self.config['database']}")
                return True
            except Exception as e:
                logger.warning(f"⚠️  Lần {attempt}/{max_retries}: Kết nối thất bại - {str(e)}")
                if attempt < max_retries:
                    logger.info(f"   Chờ {retry_delay}s rồi retry...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"✗ Kết nối MySQL thất bại sau {max_retries} lần: {str(e)}")
                    return False
        
        return False
    
    def is_table_empty(self, table_name):
        """Kiểm tra xem bảng đã có dữ liệu chưa"""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(f"SELECT COUNT(*) as cnt FROM {table_name}"))
                count = result.fetchone()[0]
                return count == 0
        except Exception as e:
            # Nếu bảng không tồn tại hoặc lỗi, coi như trống
            return True
    
    def load_csv_to_mysql(self, csv_file, table_name):
        """
        Load dữ liệu từ CSV vào MySQL
        Nếu bảng đã có dữ liệu thì bỏ qua (chỉ load lần đầu)
        """
        try:
            csv_path = os.path.join(DATASET_PATH, csv_file)
            
            # Kiểm tra file tồn tại
            if not os.path.exists(csv_path):
                logger.error(f"✗ File không tồn tại: {csv_path}")
                return False
            
            logger.info(f"\n{'='*70}")
            logger.info(f"Kiểm tra: {csv_file} -> {table_name}")
            logger.info(f"{'='*70}")
            
            # Kiểm tra xem bảng đã có dữ liệu chưa
            if not self.is_table_empty(table_name):
                logger.info(f"  ⏭️  Bảng {table_name} đã có dữ liệu - BỎ QUA")
                return True
            
            # Đọc CSV
            logger.info(f"  Đang đọc CSV: {csv_file}")
            df = pd.read_csv(csv_path)
            logger.info(f"  ✓ Đọc thành công: {len(df)} dòng")
            
            # Remove rows with all null values
            df = df.dropna(how='all')
            
            # Remove duplicates
            df = df.drop_duplicates()
            logger.info(f"  ✓ Sau xóa duplicate: {len(df)} dòng")
            
            # Insert vào MySQL
            logger.info(f"  Đang insert vào bảng {table_name}...")
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            logger.info(f"  ✓ Insert thành công: {len(df)} dòng")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Lỗi load file {csv_file}: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def verify_data(self):
        """Kiểm verify dữ liệu đã load"""
        logger.info(f"\n{'='*70}")
        logger.info("📊 Kiểm verify tổng số dữ liệu")
        logger.info(f"{'='*70}")
        
        tables = list(set(CSV_MAPPING.values()))
        total_records = 0
        
        for table in sorted(tables):
            try:
                with self.engine.connect() as connection:
                    result = connection.execute(text(f"SELECT COUNT(*) as cnt FROM {table}"))
                    count = result.fetchone()[0]
                    logger.info(f"  {table:40} : {count:10,} bản ghi")
                    total_records += count
            except Exception as e:
                logger.warning(f"  {table:40} : Lỗi - {str(e)}")
        
        logger.info(f"{'-'*70}")
        logger.info(f"  {'TỔNG CỘNG':40} : {total_records:10,} bản ghi")
        logger.info(f"{'='*70}")


def main():
    """Main function"""
    logger.info("🚀 Bắt đầu load dữ liệu CSV vào MySQL")
    logger.info(f"📁 Database: {MYSQL_CONFIG['database']}")
    logger.info(f"📂 Dataset path: {DATASET_PATH}")
    
    # Initialize loader
    loader = MySQLDataLoader(MYSQL_CONFIG)
    
    # Connect
    if not loader.connect():
        sys.exit(1)
    
    # Load mỗi file CSV
    logger.info(f"\n⏳ Bắt đầu load {len(CSV_MAPPING)} files...")
    success_count = 0
    
    for csv_file, table_name in CSV_MAPPING.items():
        if loader.load_csv_to_mysql(csv_file, table_name):
            success_count += 1
    
    # Verify data
    loader.verify_data()
    
    logger.info(f"\n✅ Hoàn thành: {success_count}/{len(CSV_MAPPING)} files đã xử lý thành công")
    
    if success_count == len(CSV_MAPPING):
        logger.info("🎉 Tất cả dữ liệu đã được load thành công!")
        sys.exit(0)
    else:
        logger.error(f"⚠️  Có {len(CSV_MAPPING) - success_count} file load thất bại")
        sys.exit(1)


if __name__ == "__main__":
    main()
